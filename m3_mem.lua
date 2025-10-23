local C = require "m3_C"
local ffi = require "ffi"
local band, bor = bit.band, bit.bor
local cast, copy = ffi.cast, ffi.copy
local uintptr_t, voidptr = ffi.typeof("uintptr_t"), ffi.typeof("void *")
local assert = assert
local event = require("m3_debug").event

local CONFIG_BLOCKSIZE = C.CONFIG_BLOCKSIZE
local CONFIG_MAXBLOCKS = 64
local TARGET_CACHELINE_SIZE = C.TARGET_CACHELINE_SIZE

local block_ct = ffi.typeof(string.format([[
	__attribute__((aligned(%d)))
	struct { uint8_t _[%d]; }
]], TARGET_CACHELINE_SIZE, CONFIG_BLOCKSIZE))

local FRAME_ACTIVE = 1
local FRAME_ALIVE  = 2
local FRAME_CHILD  = 4
local FRAME_ALIVE_ACTIVE = FRAME_ALIVE + FRAME_ACTIVE

local mem = ffi.gc(ffi.new("m3_Mem"), C.m3_mem_destroy)

-- explicit nil to ensure accesses are always compiled as array tables, even when nothing has
-- been inserted yet
local lref = { [0]=nil }
local lfin = { [0]=nil }

local function mem_state()
	return mem
end

local function mem_createworkspace(size)
	local numblocks = math.ceil(size / CONFIG_BLOCKSIZE)
	-- use luajit allocator for work memory so that luajit uses relative addresses for constant
	-- heap references. if we use malloc or m3's allocator, they will (usually) end up too far
	-- away and luajit emits an extra mov absolute address.
	local work = ffi.new(ffi.typeof("$[?]", block_ct), numblocks)
	_G._M3_ANCHOR_WORK_HEAP = work -- must be anchored somewhere so it's not gced
	mem.work = work
	mem.sizework = numblocks*CONFIG_BLOCKSIZE
	C.m3_mem_init(mem)
	return work
end

local function mem_ofs2mask(ofs, size)
	local first = math.min(math.floor(ofs/CONFIG_BLOCKSIZE), CONFIG_MAXBLOCKS-1)
	local last = math.min(math.floor((ofs+size-1)/CONFIG_BLOCKSIZE), CONFIG_MAXBLOCKS-1)
	return bit.lshift(1ull, last+1) - bit.lshift(1ull, first)
end

local function mem_save()
	local fp = C.m3_mem_save(mem)
	event("save", fp)
	return fp
end

local function mem_load(fp)
	event("load", fp)
	C.m3_mem_load(mem, fp)
end

local function detach(fp)
	local parent = mem.ftab[fp].parent
	local state = mem.ftab[parent].state - FRAME_CHILD
	mem.ftab[parent].state = state
	if state <= FRAME_ACTIVE then
		-- use a tail call here rather than a loop because this has a very low and static iteration
		-- count so we don't want the jit compiler to compile a looping trace
		return detach(parent)
	end
end

-- idiom:
--   local fp = mem.save()
--   ...
--   mem.load(fp)
--   ...
--   mem.delete(fp)    <--    state = FRAME_ACTIVE|FRAME_ALIVE
local function mem_delete(fp)
	event("delete", fp)
	local state = mem.ftab[fp].state
	if state == FRAME_ALIVE_ACTIVE then
		-- common case (idiom): delete savepoint without children (i.e. previous savepoint)
		mem.ftab[fp].state = FRAME_ACTIVE
		detach(fp)
	elseif state == FRAME_ALIVE then
		-- delete savepoint that is not entered and has no children
		mem.ftab[fp].state = 0
		detach(fp)
	else
		assert(band(state, FRAME_ALIVE) ~= 0, "attempt to delete savepoint twice")
		-- delete savepoint that is not entered and has children.
		-- don't detach it yet, it will be detached when all its children are detached.
		mem.ftab[fp].state = state - FRAME_ALIVE
	end
end

local function mem_write(mask)
	mem.diff = bor(mem.diff, mask)
	-- print("mem_write", mask, mem.unsaved, band(mem.unsaved, mask))
	if band(mem.unsaved, mask) ~= 0 then
		event("mask", mask)
		C.m3_mem_write(mem, mask)
	end
end

local function mem_iswritable(ptr)
	return cast(uintptr_t, ptr) - cast(uintptr_t, mem.framealloc.chunk) < mem.framealloc.chunktop
end

local function alloc(ap, size, align)
	local ptr
	if size < ap.cursor then
		local cursor = band(ap.cursor-size, -align)
		ap.cursor = cursor
		ptr = cast(voidptr, cast(uintptr_t, ap.chunk) + ap.cursor)
	else
		ptr = C.m3_mem_alloc(mem.err, ap, size, align)
	end
	event("alloc", ap, size, align, ptr)
	return ptr
end

local function realloc(ap, oldptr, oldsize, newsize, align)
	local newptr = alloc(ap, newsize, align)
	if oldsize > 0 then
		copy(newptr, oldptr, oldsize)
	end
	return newptr
end

local function mem_alloc(size, align)
	return alloc(mem.alloc, size, align)
end

local function mem_realloc(oldptr, oldsize, newsize, align)
	return realloc(mem.alloc, oldptr, oldsize, newsize, align)
end

local function mem_objhandle()
	if mem.nfreeobj > 0 then
		local idx = mem.nfreeobj - 1
		mem.nfreeobj = idx
		return mem.freeobj[idx]
	else
		return C.m3_mem_newobjref(mem)
	end
end

local function mem_objref(obj, fin)
	local idx = mem_objhandle()
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
	state           = mem_state,
	createworkspace = mem_createworkspace,
	ofs2mask        = mem_ofs2mask,
	save            = mem_save,
	load            = mem_load,
	delete          = mem_delete,
	write           = mem_write,
	iswritable      = mem_iswritable,
	alloc           = mem_alloc,
	realloc         = mem_realloc,
	objref          = mem_objref,
	getobj          = mem_getobj
}
