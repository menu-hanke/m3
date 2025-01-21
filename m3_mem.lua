local cdef = require "m3_cdef"
local constify = require "m3_constify"
local environment = require "m3_environment"
local shutdown = require "m3_shutdown"
local ffi = require "ffi"
local C, cast, ffi_copy, typeof, sizeof, alignof, intptr_t, voidptr = ffi.C, ffi.cast, ffi.copy, ffi.typeof, ffi.sizeof, ffi.alignof, ffi.typeof("intptr_t"), ffi.typeof("void *")
local band, bnot, tonumber = bit.band, bit.bnot, tonumber
local event = require("m3_debug").event

local VMSIZE = environment.stack or cdef.M3_VMSIZE_DEFAULT

---- Mmap arenas ---------------------------------------------------------------

local function oom()
	error("arena mapping out of memory")
end

local function arena_bump(arena, size, align)
	local cursor = band(arena.cursor-size, cast(intptr_t, -align))
	arena.cursor = cursor
	if cursor < arena.base then
		arena:expand()
	end
	return cursor
end

local function arena_xbump(arena, size, align)
	return (cast(voidptr, arena_bump(arena, size, align)))
end

local function arena_xrealloc(arena, oldptr, oldsize, newsize, align)
	local newptr = arena_xbump(arena, newsize, align)
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

local function arena_new(arena, ctype)
	ctype = typeof(ctype)
	return (cast(typeofptr(ctype), arena_bump(arena, sizeof(ctype), alignof(ctype))))
end

local function arena_newarray(arena, ctype, num)
	ctype = typeof(ctype)
	return (cast(typeofptr(ctype), arena_bump(arena, num*sizeof(ctype), alignof(ctype))))
end

local arena_expand, arena_map, arena_unmap
if cdef.M3_MEM_VIRTUALALLOC then

	arena_map = function(arena, size)
		arena.bottom = ffi.cast("intptr_t", C.m3__mem_map_arena(size))
		arena.base = arena.bottom+size
		arena.top = arena.base
		arena.cursor = arena.base
	end

	arena_expand = function(arena)
		if C.m3__mem_grow(arena) ~= 0 then
			oom()
		end
	end

	arena_unmap = function(arena)
		C.m3__mem_unmap(cast(voidptr, arena.bottom))
	end
else

	arena_map = function(arena, size)
		arena.base = ffi.cast("intptr_t", C.m3__mem_map_arena(size))
		arena.top = arena.base+size
		arena.cursor = arena.top
	end

	arena_expand = oom

	arena_unmap = function(arena)
		C.m3__mem_unmap(ffi.cast("void *", arena.base), arena.top - arena.base)
	end

end

ffi.metatype(
	"m3_Arena",
	{
		__index = {
			oom      = oom,
			bump     = arena_bump,
			xbump    = arena_xbump,
			xrealloc = arena_xrealloc,
			new      = arena_new,
			newarray = arena_newarray,
			expand   = arena_expand,
			map      = arena_map,
			unmap    = arena_unmap
		}
	}
)

local global_savestate = ffi.new("m3_SaveState")
local global_scratch   = ffi.new("m3_Arena")

global_savestate.arena:map(VMSIZE)
global_scratch:map(VMSIZE)

shutdown(function()
	global_savestate.arena:unmap()
	global_scratch:unmap()
end)

---- Heap & savepoints ---------------------------------------------------------

local heapsize = constify.new()
local sstop = global_savestate.arena.top
global_savestate.base = global_savestate.arena.top
global_savestate.arena:new("uint64_t")[0] = 0
global_savestate.mask = -1ull

local function dirty(mask)
	return band(global_savestate.mask, mask) ~= 0
end

local function setmask(mask)
	if dirty(mask) then
		event("mask", mask)
		C.m3__mem_setmask(global_savestate, mask)
	end
end

local function setheap(ptr, blocksize, num)
	global_savestate.heap = shutdown(ptr, "anchor")
	global_savestate.blocksize = blocksize
	constify.set(heapsize, blocksize*num)
end

-- note: iswritable() only works for the main arena.
-- if it's needed for the scratch arena too, then map both together
local function iswritable(ptr)
	return cast(intptr_t, ptr) < global_savestate.base
end

-- TODO: only save if it's dirty, otherwise return current savepoint
local function mem_save()
	local cursor = band(global_savestate.arena.cursor, bnot(63))
	cursor = cursor - heapsize()
	global_savestate.arena.cursor = cursor-16
	if global_savestate.arena.cursor < global_savestate.arena.base then
		global_savestate.arena:expand()
	end
	cast("uint64_t *", cursor)[-1] = -1ull
	cast("intptr_t *", cursor)[-2] = global_savestate.base
	global_savestate.base = cursor
	global_savestate.mask = -1ull
	local fp = tonumber(cursor-sstop)
	event("save", fp)
	return fp
end

-- TODO: load-and-pop
local function mem_load(fp)
	event("load", fp)
	C.m3__mem_load(global_savestate, sstop+fp)
end

--------------------------------------------------------------------------------

return {
	arena        = global_savestate.arena,
	scratch      = global_scratch,
	setheap      = setheap,
	iswritable   = iswritable,
	dirty        = dirty,
	setmask      = setmask,
	save         = mem_save,
	load         = mem_load,
}
