local C = require "m3_C"
local ffi = require "ffi"
local band, bor, bnot = bit.band, bit.bor, bit.bnot
local cast, copy = ffi.cast, ffi.copy
local uintptr_t, voidptr, objidptr = ffi.typeof("uintptr_t"), ffi.typeof("void *"), ffi.typeof("int32_t *")
local check = C.check
local event = require("m3_debug").event

local CONFIG_MEM_BLOCKSIZE_MIN = C.CONFIG_MEM_BLOCKSIZE_MIN
local CONFIG_MEM_BLOCKNUM_MAX  = 64
local TARGET_CACHELINE_SIZE = C.TARGET_CACHELINE_SIZE
local SIZEOF_OBJID = 4
local FRAME_OBJS = 1
local FRAME_ALIVE = 2
local FRAME_NOTALIVE = bnot(FRAME_ALIVE)
local FRAME_ALIVEMASK = FRAME_ALIVE + FRAME_OBJS
local FRAME_CHILD = 4

local mem = ffi.gc(ffi.new("m3_Mem"), C.m3_mem_destroy)

-- explicit nil to ensure accesses are always compiled as array tables, even when nothing has
-- been inserted yet
local lref = { [0]=nil }
local lfin = { [0]=nil }

local function mem_save()
	local fp = C.m3_mem_save(mem)
	event("save", fp)
	return fp
end

local function mem_write(mask)
	mem.diff = bor(mem.diff, mask)
	if band(mem.unsaved, mask) ~= 0 then
		C.m3_mem_write(mem, mask)
	end
end

local function mem_load(fp)
	event("load", fp)
	C.m3_mem_load(mem, fp)
end

local function detach(fp)
	local prev = mem.ftab[fp].prev
	local state = mem.ftab[prev].state - FRAME_CHILD
	mem.ftab[prev].state = state
	-- if fp == mem.frame then
	-- 	-- C code maintains the invariant child.save ⊂  parent.save,
	-- 	-- so the next time we do a load, we can just load the diff from the parent.
	-- 	-- TODO: this doesn't work if the next save/load is a save because it can reuse this savepoint
	-- 	mem.diff = bor(mem.diff, mem.ftab[fp].diff)
	-- 	mem.frame = prev
	-- 	mem.unsaved = bnot(mem.ftab[prev].save)
	-- end
	if state < FRAME_ALIVE then
		-- use a tail call here rather than a loop because this has a very low and static iteration
		-- count so we don't want the jit compiler to compile a looping trace
		return detach(prev)
	end
end

-- TODO (?): delete+load: can continue from chunk, but need to store cursor in frame
local function mem_delete(fp)
	local state = mem.ftab[fp].state
	if state <= FRAME_ALIVEMASK then
		mem.ftab[fp].state = band(state, FRAME_OBJS)
		return detach(fp)
	else
		mem.ftab[fp].state = band(state, FRAME_NOTALIVE)
	end
end

local function work_init(size)
	local bsize = CONFIG_MEM_BLOCKSIZE_MIN
	local wnum = math.min(CONFIG_MEM_BLOCKNUM_MAX, math.ceil(size/bsize))
	while wnum*bsize < size do bsize = 2*bsize end
	-- use luajit allocator for the heap so that const heap references become
	-- relative addresses in machine code.
	local block_ct = ffi.typeof(string.format([[
		__attribute__((aligned(%d)))
		struct { uint8_t _[%d]; }
	]], TARGET_CACHELINE_SIZE, bsize))
	local work = ffi.new(ffi.typeof("$[?]", block_ct), wnum)
	_G._M3_ANCHOR_WORK_HEAP = work
	mem.work = work
	mem.bsize = bsize
	mem.wnum = wnum
	C.m3_mem_init(mem)
	return mem.work, mem.bsize
end

local function iswritable(ptr)
	return cast(uintptr_t, ptr) - cast(uintptr_t, mem.chunk) < mem.chunktop
end

local function mem_alloc(size, align)
	if size > mem.cursor then
		check(C.m3_mem_chunk_new(mem, size))
	end
	local cursor = band(mem.cursor-size, -align)
	mem.cursor = cursor
	local ptr = cast(voidptr, cast(uintptr_t, mem.chunk) + mem.cursor)
	event("alloc", size, align, ptr)
	return ptr
end

local function mem_realloc(oldptr, oldsize, newsize, align)
	local newptr = mem_alloc(newsize, align)
	if oldsize > 0 then
		copy(newptr, oldptr, oldsize)
	end
	return newptr
end

local function mem_lref()
	local lfreen = mem.lfreen - SIZEOF_OBJID
	if lfreen >= 0 then
		mem.lfreen = lfreen
		return cast(objidptr, mem.lfree.data+lfreen)[0]
	else
		return (C.m3_mem_newobjref(mem))
	end
end

local function mem_objref(obj, fin)
	local idx = mem_lref()
	local oldfin = lfin[idx]
	if oldfin then
		oldfin(lref[idx])
	end
	lref[idx] = obj
	lfin[idx] = fin
	return idx
end

local function mem_getobj(idx)
	return lref[idx]
end

return {
	state      = mem,
	save       = mem_save,
	write      = mem_write,
	load       = mem_load,
	delete     = mem_delete,
	iswritable = iswritable,
	alloc      = mem_alloc,
	realloc    = mem_realloc,
	objref     = mem_objref,
	getobj     = mem_getobj,
	work_init  = work_init,
}
