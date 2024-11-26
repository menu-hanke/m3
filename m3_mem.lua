local cdef = require "m3_cdef"
local constify = require "m3_constify"
local debugevent = require("m3_debug").event
local environment = require "m3_environment"
local ffi = require "ffi"
local C, cast, ffi_copy, typeof, sizeof, alignof, intptr_t, voidptr = ffi.C, ffi.cast, ffi.copy, ffi.typeof, ffi.sizeof, ffi.alignof, ffi.typeof("intptr_t"), ffi.typeof("void *")
local band, bnot, tonumber = bit.band, bit.bnot, tonumber

---- Mmap stacks ---------------------------------------------------------------

local function oom()
	error("stack mapping out of memory")
end

local function stack_bump(stack, size, align)
	local cursor = band(stack.cursor-size, cast(intptr_t, -align))
	stack.cursor = cursor
	if cursor < stack.base then
		stack:expand()
	end
	return cursor
end

local function stack_xbump(stack, size, align)
	return (cast(voidptr, stack_bump(stack, size, align)))
end

local function stack_xrealloc(stack, oldptr, oldsize, newsize, align)
	local newptr = stack_xbump(stack, newsize, align)
	if oldsize > 0 then
		ffi_copy(newptr, oldptr, oldsize)
	end
	return newptr
end

-- ffi.typeof("$*", ...) isn't compiled, so this caches the results instead.
local ctptr = {}

local function typeofptr(ctype)
	local ctid = tonumber(ctype)
	local p = ctptr[ctid]
	if not p then
		p = typeof("$*", ctype)
		ctptr[ctid] = p
	end
	return p
end

local function stack_new(stack, ctype)
	ctype = typeof(ctype)
	return (cast(typeofptr(ctype), stack_bump(stack, sizeof(ctype), alignof(ctype))))
end

local function stack_newarray(stack, ctype, num)
	ctype = typeof(ctype)
	return (cast(typeofptr(ctype), stack_bump(stack, num*sizeof(ctype), alignof(ctype))))
end

local stack_expand, stack_map, stack_unmap
if cdef.M3_MEM_VIRTUALALLOC then

	stack_map = function(stack, size)
		stack.bottom = ffi.cast("intptr_t", C.m3__mem_map_stack(size))
		stack.base = stack.bottom+size
		stack.top = stack.base
		stack.cursor = stack.base
	end

	stack_expand = function(stack)
		if C.m3__mem_grow(stack) ~= 0 then
			oom()
		end
	end

	stack_unmap = function(stack)
		C.m3__mem_unmap(cast(voidptr, stack.bottom))
	end
else

	stack_map = function(stack, size)
		stack.base = ffi.cast("intptr_t", C.m3__mem_map_stack(size))
		stack.top = stack.base+size
		stack.cursor = stack.top
	end

	stack_expand = oom

	stack_unmap = function(stack)
		C.m3__mem_unmap(ffi.cast("void *", stack.base), stack.top - stack.base)
	end

end

ffi.metatype(
	"m3_Stack",
	{
		__index = {
			oom      = oom,
			bump     = stack_bump,
			xbump    = stack_xbump,
			xrealloc = stack_xrealloc,
			new      = stack_new,
			newarray = stack_newarray,
			expand   = stack_expand,
			map      = stack_map,
			unmap    = stack_unmap
		}
	}
)

---- Heap & savepoints ---------------------------------------------------------

local heapsize = constify.new()

local ss = ffi.gc(
	ffi.new("m3_SaveState"),
	function(self) self.stack:unmap() end
)
ss.stack:map(environment.stack)
local sstop = ss.stack.top
ss.base = ss.stack.top
ss.stack:new("uint64_t")[0] = 0
ss.mask = -1ull

local function dirty(mask)
	return band(ss.mask, mask) ~= 0
end

local function setmask(mask)
	if dirty(mask) then
		debugevent("mask", mask)
		C.m3__mem_setmask(ss, mask)
	end
end

local anchor = setmetatable({}, {
	__call = function(self, v)
		self[v] = true
		return v
	end
})

local function setheap(ptr, size)
	ss.heap = anchor(ptr)
	constify.set(heapsize, size)
end

-- note: iswritable() only works for the main stack.
-- if it's needed for the scratch stack too, then map both together
local function iswritable(ptr)
	return cast(intptr_t, ptr) < ss.base
end

local function mem_save()
	local cursor = band(ss.stack.cursor, bnot(64))
	cursor = cursor - heapsize()
	ss.stack.cursor = cursor-16
	if ss.stack.cursor < ss.stack.base then
		ss.stack:expand()
	end
	cast("uint64_t *", cursor)[-1] = -1ull
	cast("intptr_t *", cursor)[-2] = ss.base
	ss.base = cursor
	ss.mask = -1ull
	local fp = tonumber(cursor-sstop)
	debugevent("save", fp)
	return fp
end

local function mem_load(fp)
	debugevent("load", fp)
	C.m3__mem_load(ss, sstop+fp)
end

---- Scratch management --------------------------------------------------------

local scratch = ffi.gc(ffi.new("m3_Stack"), function(self) self:unmap() end)
scratch:map(environment.stack)

local scratch_top = scratch.top
local function resetx()
	scratch.cursor = scratch_top
end

-- temporary scratch space to prevent some allocations
local tmp = ffi.new [[
	struct {
		int64_t i64;
	}
]]

--------------------------------------------------------------------------------

return {
	stack      = ss.stack,
	scratch    = scratch,
	resetx     = resetx,
	tmp        = tmp,
	setheap    = setheap,
	iswritable = iswritable,
	dirty      = dirty,
	setmask    = setmask,
	save       = mem_save,
	load       = mem_load,
	anchor     = anchor
}
