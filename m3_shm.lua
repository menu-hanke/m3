local cdef = require "m3_cdef"
local environment = require "m3_environment"
local ffi = require "ffi"
local C = ffi.C

assert(environment.mode == "mp")

local VMSIZE_PROC = cdef.M3_VMSIZE_PROC

-- map from lowest to highest addr
--   * shared heap
--   * main process heap
--   * `environment.parallel` × worker heaps
-- and one extra region for alignment
local mapsize = VMSIZE_PROC*(environment.parallel+3)
local mapping = C.m3__mem_map_shared(mapsize)
local base = bit.band(ffi.cast("intptr_t", mapping)+(VMSIZE_PROC-1), bit.bnot(VMSIZE_PROC-1))
local shared = ffi.cast("m3_Shared *", base)
shared.heap.cursor = base + ffi.sizeof("m3_Shared")
ffi.gc(shared, function() C.m3__mem_unmap(mapping, mapsize) end)

local tmpsize_t = ffi.new("size_t[1]")

local function heap_get_free(heap, size)
	tmpsize_t[0] = size
	local ptr = C.m3__mp_heap_get_free(heap, tmpsize_t)
	return ptr, tmpsize_t[0]
end

local function heap_new(heap, ctype)
	ctype = ffi.typeof(ctype)
	return ffi.cast(ffi.typeof("$*", ctype), C.m3__mp_heap_alloc(heap, ffi.sizeof(ctype)))
end

local heap_ct = ffi.metatype(
	"m3_Heap",
	{
		__index = {
			bump         = C.m3__mp_heap_bump,
			bump_cls     = C.m3__mp_heap_bump_cls,
			get_free     = heap_get_free,
			get_free_cls = C.m3__mp_heap_get_free_cls,
			alloc        = C.m3__mp_heap_alloc,
			free         = C.m3__mp_heap_free,
			new          = heap_new
		}
	}
)

local function mutex_call(mutex, f, ...)
	C.m3__mp_mutex_lock(mutex)
	local ok, x = pcall(f, ...)
	C.m3__mp_mutex_unlock(mutex)
	if not ok then error(string.format("error in mutex_with callback: %s", x)) end
	return x
end

ffi.metatype(
	"m3_Mutex",
	{
		__index = {
			lock         = C.m3__mp_mutex_lock,
			unlock       = C.m3__mp_mutex_unlock,
			call         = mutex_call
		}
	}
)

local function with_shared_heap(f)
	return mutex_call(shared.lock, f, shared.heap)
end

local heap = heap_ct()
local proc_ptr

local function proc_startup()
	heap.cursor = base + C.m3__mp_proc_id*VMSIZE_PROC
	-- proc must be the first thing in the heap
	proc_ptr = ffi.cast("m3_Proc *", heap:alloc(ffi.sizeof("m3_Proc")))
end

local function proc()
	return proc_ptr
end

return {
	proc_startup     = proc_startup,
	heap             = heap,
	with_shared_heap = with_shared_heap,
	proc             = proc
}
