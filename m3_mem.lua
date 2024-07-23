local access = require "m3_access"
local cdata = require "m3_cdata"
local cdef = require "m3_cdef"
local constify = require "m3_constify"
local environment = require "m3_environment"
local event = require "m3_event"
local ffi = require "ffi"
local C, cast, ffi_copy, typeof, sizeof, alignof, intptr_t, voidptr = ffi.C, ffi.cast, ffi.copy, ffi.typeof, ffi.sizeof, ffi.alignof, ffi.typeof("intptr_t"), ffi.typeof("void *")
local band, bnot, tonumber = bit.band, bit.bnot, tonumber
local dispatch = event.dispatch

-- TODO: if heap consists of 1 block, ignore all setmask(...)s and always make a copy of the heap
-- on savepoint

local HEAPBMAX = cdef.M3_MEM_HEAPBMAX
local BSIZEMIN = cdef.M3_MEM_BSIZEMIN

local slots = {}

local function ctype_type(ct)
	return bit.rshift(ffi.typeinfo(ffi.typeof(ct)).info, 28)
end

local function ctype_isstruct(ct)
	return ctype_type(ct) == 1
end

local function slot_read(slot)
	return access.defer(function()
		return load(string.format(
			"local ptr = ... return function() return ptr%s end",
			ctype_isstruct(slot.ctype) and "" or "[0]"
		))(slot.ptr)
	end)
end

local function slot_write(slot)
	return access.defer(function()
		return access.capture(load([[
			local ptr = ...
			return function(v)
				if v == nil then
					return ptr
				else
					ptr[0] = v
				end
			end
		]])(slot.ptr))
	end)
end

local function slot_map_(slot, tab, col)
	return string.format(
		"model(%s) %s = lds.%s(0x%x)",
		tab,
		col,
		require("m3_fhk").typesuffix(slot.ctype),
		ffi.cast("intptr_t", slot.ptr)
	)
end

local function slot_map(slot, tab, col)
	access.read(slot)
	return function() return slot_map_(slot, tab, col) end
end

local slot_mt = {
	data = {
		type  = "mem.slot",
		read  = slot_read,
		write = slot_write,
		map   = slot_map
	}
}

-- slot fields:
-- before startup:
--   * ctype
-- after startup:
--   * block   heap block (heap only)
--   * ptr     mem pointer (all, heap=>unique)
local function slot(slot, init)
	if type(slot) == "string" or type(slot) == "cdata" then
		slot = {ctype=slot}
	end
	if init then slot.init = init end
	slot = setmetatable(slot or {}, slot_mt)
	table.insert(slots, slot)
	return slot
end

local function slotmask(...)
	local mask = 0ull
	for _,slot in ipairs({...}) do
		if slot.block then
			mask = bit.bor(mask, bit.lshift(1ull, slot.block))
		end
	end
	return mask
end

local function maskstr(mask)
	return mask and string.format("0x%d%s", mask, mask >= 2^53 and "ull" or "") or "0"
end

local function oom()
	error("stack mapping out of memory")
end

local function stack_bump(stack, size, align)
	local cursor = band(stack.cursor-size, cast(intptr_t, -align))
	if cursor < stack.base then
		stack:expand()
	end
	stack.cursor = cursor
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
if cdef.M3_VIRTUALALLOC then

	stack_map = function(stack, size)
		error("TODO")
	end

	stack_expand = function(cursor)
		error("TODO")
	end

	stack_unmap = function(stack)
		error("TODO")
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

local ss = ffi.gc(
	ffi.new("m3_SaveState"),
	function(self) self.stack:unmap() end
)
ss.stack:map(environment.stack)
local sstop = ss.stack.top
ss.base = ss.stack.top
ss.stack:new("uint64_t")[0] = 0
ss.mask = -1ull

local function iswritable(ptr)
	return cast(intptr_t, ptr) < ss.base
end

-- note: iswritable() only works for the main stack.
-- if it's needed for the scratch stack too, then map both together
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

local heapsize = constify.new()

local function dirty(mask)
	return band(ss.mask, mask) ~= 0
end

local function setmask(mask)
	if dirty(mask) then
		dispatch("mask", mask)
		C.m3__mem_setmask(ss, mask)
	end
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
	dispatch("save", fp)
	return fp
end

local function mem_load(fp)
	dispatch("load", fp)
	C.m3__mem_load(ss, sstop+fp)
end

local function slot_cmp(a, b)
	if a.region ~= b.region then
		return a.region < b.region
	else
		return ffi.alignof(a.ctype) > ffi.alignof(b.ctype)
	end
end

local function blockct(size, align)
	return ffi.typeof(string.format([[
		__attribute__((aligned(%d)))
		struct { uint8_t data[%d]; }
	]], align, size))
end

local anchor = setmetatable({}, {
	__call = function(self, v)
		self[v] = true
		return v
	end
})

local function allocheap(rw)
	local size, maxsize = 0, 0
	for _, slot in ipairs(rw) do
		if type(slot.region) == "function" then
			slot.region = slot:region()
		end
		slot.region = string.format("%p", slot.region)
		size = size + ffi.sizeof(slot.ctype)
		maxsize = math.max(maxsize, ffi.sizeof(slot.ctype))
	end
	table.sort(rw, slot_cmp)
	blocksize = bit.band(
		math.max(maxsize, math.ceil(size/HEAPBMAX)) + BSIZEMIN-1,
		bit.bnot(BSIZEMIN-1)
	)
	::again::
	local block, ptr = 0, 0
	for _, slot in ipairs(rw) do
		local align = ffi.alignof(slot.ctype)
		local size = ffi.sizeof(slot.ctype)
		ptr = bit.band(ptr + align-1, bit.bnot(align-1))
		if ptr+size > blocksize then
			block, ptr = block+1, 0
			if block >= HEAPBMAX then
				blocksize = blocksize*2
				goto again
			end
		end
		slot.block = block
		slot.ofs = ptr
		ptr = ptr+size
	end
	local block_ct = blockct(blocksize, BSIZEMIN)
	-- use luajit allocator for the heap so that const heap references become
	-- relative addresses in machine code.
	local heap = anchor(ffi.new(ffi.typeof("$[?]", block_ct), block+1))
	ss.heap = heap
	constify.set(heapsize, blocksize*(block+1))
	for _, slot in ipairs(rw) do
		slot.ptr = ffi.cast(
			ffi.typeof("$*", slot.ctype),
			ffi.cast("intptr_t", ffi.cast("void *", heap[slot.block])) + slot.ofs
		)
	end
end

local function malloc(size)
	return ffi.gc(C.malloc(size), C.free)
end

local function allocdummy(slots)
	local size, align = 0, 1
	for _, slot in ipairs(slots) do
		size = math.max(ffi.sizeof(slot.ctype), size)
		align = math.max(ffi.alignof(slot.ctype), align)
	end
	if size == 0 then
		return
	end
	local region
	if align <= 16 then
		region = malloc(size)
	else
		region = ffi.new(blockct(size, align))
	end
	anchor(region)
	for _, slot in ipairs(slots) do
		slot.ptr = ffi.cast(ffi.typeof("$*", slot.ctype), region)
	end
end

local function slotdummy(slot)
	return cdata.dummy(slot.ctype)
end

local function startup()
	local accs = { r={}, w={}, rw={} }
	for _,slot in ipairs(slots) do
		local acc = access.get(slot)
		if acc ~= "" then
			if type(slot.ctype) == "function" then
				slot.ctype = slot:ctype()
			end
			slot.ctype = ffi.typeof(slot.ctype)
			if acc == "w" and ctype_isstruct(slot.ctype) then
				acc = "rw"
			end
			table.insert(accs[acc], slot)
		end
	end
	allocheap(accs.rw)
	allocdummy(accs.r)
	allocdummy(accs.w)
	for _, slot in ipairs(slots) do
		if slot.ptr then
			local init = slot.init
			if init == false then
				init = slotdummy(slot)
			end
			if type(init) == "function" then
				init = init(slot)
			end
			if init == nil then
				-- explicit zeroing not needed here because heap was allocated with ffi.new
				-- ffi.fill(slot.ptr, ffi.sizeof(slot.ctype))
			else
				slot.ptr[0] = init
			end
		end
	end
	dispatch("heap", slots)
	slots = nil
end

return {
	stack      = ss.stack,
	scratch    = scratch,
	resetx     = resetx,
	tmp        = tmp,
	slot       = slot,
	slotmask   = slotmask,
	maskstr    = maskstr,
	iswritable = iswritable,
	dirty      = dirty,
	setmask    = setmask,
	save       = mem_save,
	load       = mem_load,
	anchor     = anchor,
	startup    = startup
}
