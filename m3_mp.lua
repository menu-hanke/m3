assert(require("m3_environment").parallel)

local cdata = require "m3_cdata"
local cdef = require "m3_cdef"
local environment = require "m3_environment"
local shutdown = require "m3_shutdown"
local ffi = require "ffi"
local buffer = require "string.buffer"
local C, cast, copy = ffi.C, ffi.cast, ffi.copy

local CONFIG_MP_PROC_MEMORY = cdef.CONFIG_MP_PROC_MEMORY

local nproc = environment.parallel
if nproc == "auto" then
	nproc = C.m3__mp_num_cpus()
end

---- Shared memory management --------------------------------------------------

-- map from lowest to highest addr
--   * shared heap
--   * main process heap
--   * `environment.parallel` × worker heaps
-- and one extra region for alignment
local mapsize = CONFIG_MP_PROC_MEMORY*(nproc+3)
local global_shared_mapping = (function()
	local ptr = ffi.new("void *[1]")
	cdata.check(C.m3_mem_map_shared(mapsize, ptr))
	local map = ptr[0]
	shutdown(function() C.m3_mem_unmap(map, mapsize) end)
	return map
end)()
local global_shared_base = bit.band(
	ffi.cast("intptr_t", global_shared_mapping)+(CONFIG_MP_PROC_MEMORY-1),
	bit.bnot(CONFIG_MP_PROC_MEMORY-1)
)
local global_shared_mem = ffi.cast("m3_Shared *", global_shared_base)
global_shared_mem.heap.cursor = global_shared_base + ffi.sizeof("m3_Shared")

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
	if not ok then error(string.format("error in mutex_call callback: %s", x)) end
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
	return mutex_call(global_shared_mem.lock, f, global_shared_mem.heap)
end

local proc_heap = heap_ct()
local proc_hook = {}
local proc_ptr

local function proc_doinit(id)
	for _,hook in ipairs(proc_hook) do
		hook(id)
	end
	proc_hook = id
end

local function proc_init(hook)
	if type(proc_hook) == "table" then
		table.insert(proc_hook, hook)
	else
		hook(proc_hook)
	end
end

proc_init(function(id)
	proc_heap.cursor = global_shared_base + (id+1)*CONFIG_MP_PROC_MEMORY
	-- proc must be the first thing in the heap
	proc_ptr = ffi.cast("m3_Proc *", proc_heap:alloc(ffi.sizeof("m3_Proc")))
end)

---- Message handling ----------------------------------------------------------

local msg_size = ffi.sizeof("m3_Message")
local msgtab_ct = ffi.typeof("m3_Message *[?]")
local msgptr_ct = ffi.typeof("m3_Message *")

-- all messages ever allocated by this process
-- (if we want to get fancier this could hold a separate table per size class)
local allmsg_cap = 64
local allmsg_num = 0
local allmsg = ffi.new(msgtab_ct, allmsg_cap)

-- reused encoder & decoder buffers
local encoder = buffer.new(0)
local decoder = buffer.new()

local function message_alloc_new(cls)
	if allmsg_num == allmsg_cap then
		allmsg_cap = 2*allmsg_cap
		local ptr = ffi.new(msgtab_ct, allmsg_cap)
		ffi.copy(ptr, allmsg, allmsg_num*ffi.sizeof("m3_Message *"))
		allmsg = ptr
	end
	local msg = ffi.cast(msgptr_ct, proc_heap:bump_cls(cls))
	msg.cls = cls
	allmsg[allmsg_num] = msg
	allmsg_num = allmsg_num+1
	return msg
end

local function message_alloc_sweep(cls)
	C.m3__mp_msg_sweep(proc_heap, allmsg, allmsg_num)
	local msg = proc_heap:get_free_cls(cls)
	if msg == nil then
		return message_alloc_new(cls)
	else
		msg = cast(msgptr_ct, msg)
	end
	return msg
end

local function message_alloc(len)
	local msg, cls = proc_heap:get_free(msg_size+len)
	if msg == nil then
		msg = message_alloc_sweep(cls)
	else
		msg = cast(msgptr_ct, msg)
	end
	msg.state = 1
	msg.len = len
	msg.cls = cls
	return msg
end

local function message_free(msg)
	msg.state = 2
end

local function encode(chan, msg)
	local data, len = encoder:reset():encode(msg):ref()
	local node = message_alloc(len)
	copy(node.data, data, len)
	node.chan = chan
	return node
end

local function decode(ptr)
	local node = cast(msgptr_ct, ptr)
	local chan = node.chan
	local msg = decoder:set(node.data, node.len):decode()
	message_free(node)
	return chan, msg
end

---- Processes -----------------------------------------------------------------

local function park(timeout)
	if timeout then
		-- return true if timed out
		return C.m3__mp_proc_park_timeout(proc_ptr, timeout) ~= 0
	else
		C.m3__mp_proc_park(proc_ptr)
	end
end

---- Futures -------------------------------------------------------------------

local function fut_completed(fut)
	return C.m3__mp_future_completed(fut) ~= 0
end

local function fut_wait_sync(fut)
	if not fut_completed(fut) then
		repeat
			park()
		until fut_completed(fut)
	end
end

ffi.metatype(
	"m3_Future",
	{
		__index = {
			completed = fut_completed,
			wait_sync = fut_wait_sync
		}
	}
)

---- Events --------------------------------------------------------------------

ffi.metatype(
	"m3_Event",
	{
		__index = {
			wait = C.m3__mp_event_wait,
			set  = C.m3__mp_event_set
		}
	}
)

local function event()
	local event = with_shared_heap(function(heap) return heap:new("m3_Event") end)
	event.waiters = nil
	event.lock.state = 0
	return event
end

---- Queues & channels ---------------------------------------------------------

local function prefork()
	error("channel pipe cannot be used before fork", 2)
end

local function channel_template(chan)
	return load(string.format([[
		local write = ...
		return function(x)
			return write(x, %d)
		end
	]], chan))(prefork)
end

local function dispatch_channel(dispatch, recv)
	local chanid = #dispatch+1
	local chan = {
		send = channel_template(chanid),
		recv = recv
	}
	dispatch[chanid] = chan
	return chan
end

local function dispatch_proc_init_recv(dispatch)
	local disp = table.new(#dispatch, 0)
	for id, chan in ipairs(dispatch) do
		disp[id] = chan.recv
	end
	local queue = dispatch.queue
	table.clear(dispatch)
	local fut = proc_heap:new("m3_Future")
	C.m3__mp_queue_read(queue, fut)
	return function()
		assert(fut.state == -1ULL, "recv() called with uncompleted future")
		local chan, msg = decode(fut.data)
		C.m3__mp_queue_read(queue, fut)
		return disp[chan](msg)
	end, fut
end

local function dispatch_proc_init_send(dispatch, await)
	local queue = dispatch.queue
	local fut = proc_heap:new("m3_Future")
	local send = function(msg, chan)
		C.m3__mp_queue_write(queue, cast("uintptr_t", encode(chan, msg)), fut)
		await(fut)
	end
	for _, chan in ipairs(dispatch) do
		debug.setupvalue(chan.send, 1, send)
	end
	table.clear(dispatch)
end

local dispatch_mt = {
	__index = {
		channel = dispatch_channel,
		proc_init_recv = dispatch_proc_init_recv,
		proc_init_send = dispatch_proc_init_send
	}
}

local function dispatch(size)
	return setmetatable({
		queue = with_shared_heap(function(heap)
			return C.m3__mp_queue_new(heap, size)
		end)
	}, dispatch_mt)
end

--------------------------------------------------------------------------------

local function fork(f)
	local pid = C.m3__mp_fork()
	if pid == 0 then
		-- we are in the worker process.
		-- this function may not exit or horrible things will happen.
		-- don't place *anything* between the pcall and the exit.
		-- in particular don't do anything that might allocate, since an oom here would not be fun.
		os.exit(pcall(f), false)
	elseif pid > 0 then
		-- this is the main process
		return pid
	else
		-- fork failed
		error("fork failed")
	end
end

local function init()
	require("m3_sqlite").disconnect(false) -- don't reuse sqlite objects across fork
	local pids = {}
	for i=1, nproc do
		local pid = fork(function()
			proc_doinit(i)
			return require("m3_mp_worker")(i)
		end)
		if pid then
			pids[i] = pid
		else
			error("TODO: handle fork failed")
		end
	end
	proc_doinit(0)
	local mp = require("m3_mp")
	mp.pids = pids
	mp.wait = require("m3_mp_main").wait
end

-- TODO make input/output queue sizes configurable
local main = dispatch(8 * nproc) -- worker->main mpsc
local work = dispatch(8 * nproc) -- main->worker spmc

return {
	nproc     = nproc,
	proc_init = proc_init,
	heap      = proc_heap,
	main      = main,
	work      = work,
	park      = park,
	progress  = main:channel(),
	crash     = main:channel(),
	exit      = event(),
	init      = init
}
