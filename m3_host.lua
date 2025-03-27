local C, bcload = ...
local ffi = require "ffi"
local buffer = require "string.buffer"
require "table.clear"
local select, setmetatable, type = select, setmetatable, type
local ffi_cast, ffi_copy = ffi.cast, ffi.copy

package.preload["m3.sqlite"] = function() return bcload("sqlite")(C) end
package.preload["m3.cli"] = function() return bcload("m3_cli")() end

---- Environments --------------------------------------------------------------

-- these are temporary buffers, do not store anything here that lives across calls
local tmp_buf = ffi.new("m3_Buf")
local encoder = buffer.new()
local decoder = buffer.new()

local function check(r)
	if r ~= 0 then
		error(ffi.string(tmp_buf.ptr, tmp_buf.len), 0)
	end
end

local function encodepack1(buf, n, v, ...)
	if n > 0 then
		buf:encode(v)
		return encodepack1(buf, n-1, ...)
	end
end

local function encodepack(buf, ...)
	encodepack1(buf, select("#", ...), ...)
end

local function encodeargs(...)
	encoder:reset()
	encodepack(encoder, ...)
	return encoder:ref()
end

local function decoderet()
	if #decoder > 0 then
		return decoder:decode(), decoderet()
	end
end

local function decoderets()
	decoder:set(tmp_buf.ptr, tmp_buf.len)
	return decoderet()
end

local function decodeunpack(t,i,n)
	if i<=n then
		t[i] = decoder:decode()
		return decodeunpack(t,i+1,n)
	end
end

local function env_eval(L, src, ...)
	local ptr, len = encodeargs(...)
	if type(src) == "number" then
		check(C.m3_env_exec(L, src, ptr, len, tmp_buf))
	else
		check(C.m3_env_eval(L, src, ptr, len, tmp_buf))
	end
	return decoderets()
end

local function env_func(L, src, ...)
	return env_eval(L, 0, src, ...)
end

local function env_init(L)
	return env_eval(L, "m3.init()")
end

local function env_close(L)
	env_eval(L, "if _m3_shutdown then _m3_shutdown() end")
	C.m3_env_close(ffi.gc(L, nil))
end

ffi.metatype("m3_State", {
	__index = {
		eval  = env_eval,
		func  = env_func,
		init  = env_init,
		close = env_close
	}
})

local function newenv()
	return ffi.gc(C.m3_env_newstate(), C.m3_env_close)
end

---- Futures -------------------------------------------------------------------

local function unpack1(xs, i, j)
	if i <= j then
		return xs[i], unpack1(xs, i+1, j)
	end
end

local function future_poll(fut)
	if fut[0] then
		return true, unpack1(fut, 1, fut[0])
	else
		return false
	end
end

local future_mt = {
	poll = future_poll
}
future_mt.__index = future_mt

local function newfuture()
	return setmetatable({}, future_mt)
end

---- Serial pools --------------------------------------------------------------

local SERIAL_MAXBUF = 1024*1024
local SERIAL_FLUSH  = 1

local function serial_flush(serial)
	local ptr, len = serial.encoder:ref()
	local optr, olen = env_eval(serial.L, SERIAL_FLUSH, ffi.cast("uintptr_t", ptr), len)
	decoder:set(ffi.cast("const void *", optr), olen)
	serial.encoder:reset()
	local i = 0
	while #decoder > 0 do
		local n = decoder:decode()
		local fut = serial.pending[i]
		fut[0] = n
		decodeunpack(fut, 1, n)
		i = i+1
	end
	table.clear(serial.pending)
	serial.n_pending = 0
end

local function serial_eval(serial, src, ...)
	local id = serial.n_pending
	serial.n_pending = id+1
	local fut = newfuture()
	serial.pending[id] = fut
	serial.encoder:encode(src):encode(select("#", ...))
	encodepack(serial.encoder, ...)
	if #serial.encoder > SERIAL_MAXBUF then
		serial_flush(serial)
	end
	return fut
end

local function serial_func(serial, ...)
	return env_func(serial.L, ...)
end

local function serial_close(serial)
	env_close(serial.L)
end

local serial_mt = {
	eval  = serial_eval,
	func  = serial_func,
	close = serial_close,
	type  = "serial"
}
serial_mt.__index = serial_mt

local function serial_new(L)
	return setmetatable({
		L         = L,
		encoder   = buffer.new(),
		n_pending = 0,
		pending   = {}
	}, serial_mt)
end

local function serial_preinit(L)
	local flush = env_func(L, [[
local ffi = require "ffi"
local buffer = require "string.buffer"
local eval = m3.eval
local encoder = buffer.new()
local decoder = buffer.new()

local function unpackdecode(n)
	if n > 0 then
		return decoder:decode(), unpackdecode(n-1)
	end
end

local function packencode1(n, v, ...)
	if n > 0 then
		encoder:encode(v)
		return packencode1(n-1, ...)
	end
end

local function capture(...)
	local n = select("#", ...)
	encoder:encode(n)
	packencode1(n, ...)
end

return function(ptr, len)
	decoder:set(ffi.cast("const void *", ptr), len)
	encoder:reset()
	while #decoder > 0 do
		local src = decoder:decode()
		local n = decoder:decode()
		capture(eval(src, unpackdecode(n)))
	end
	local optr, olen = encoder:ref()
	return ffi.cast("uintptr_t", optr), olen
end
	]])
	assert(flush == SERIAL_FLUSH)
end

---- Fork pools ----------------------------------------------------------------

-- TODO: consider batching results (and possibly requests?) like serial does.

local function fork_decode(fut, i)
	if #decoder > 0 then
		fut[i+1] = decoder:decode()
		return fork_decode(fut, i+1)
	else
		return i
	end
end

local function fork_recv(pool, msg)
	local id = msg.chan
	pool.freelist[pool.nfree] = id
	pool.nfree = pool.nfree+1
	local fut = pool.pending[id]
	decoder:set(msg.data, msg.len)
	local ok = decoder:decode()
	local err
	if ok then
		fut[0] = fork_decode(fut, 0)
	else
		err = decoder:decode()
		fut[0] = 0
		pool.error = true
	end
	msg.state = 2 -- MSG_DEAD
	return ok, err
end

local function fork_tick(pool)
	if C.m3_mp_future_completed(pool.read_fut) == 0 then return end
	local ok, err = fork_recv(pool, ffi_cast("m3_Message *", pool.read_fut.data))
	C.m3_mp_queue_read(pool.work2main, pool.read_fut)
	if not ok then error(err, 0) end
	-- don't turn this into a loop, we don't always want a looping trace here.
	return fork_tick(pool)
end

local function fork_waitwrite(pool)
	fork_tick(pool)
	if C.m3_mp_future_completed(pool.write_fut) ~= 0 then return end
	C.m3_mp_proc_park(pool.proc)
	-- don't turn this into a loop, it almost never loops (and if it does it's super slow because
	-- it parks the process) and we don't want a looping trace.
	return fork_waitwrite(pool)
end

local function fork_newfuture(pool)
	local id
	if pool.nfree == 0 then
		id = pool.nfut
		pool.nfut = id+1
		assert(id <= 0xffff, "future id overflow")
	else
		id = pool.freelist[pool.nfree-1]
		pool.nfree = pool.nfree-1
	end
	local fut = newfuture()
	pool.pending[id] = fut
	return id, fut
end

local function fork_eval(pool, ...)
	local id, fut = fork_newfuture(pool)
	local ptr, len = encodeargs(...)
	local msg = C.m3_mp_proc_alloc_message(pool.pp, id, len)
	ffi_copy(msg.data, ptr, len)
	C.m3_mp_queue_write(pool.main2work, ffi_cast("uintptr_t", msg), pool.write_fut)
	fork_waitwrite(pool)
	return fut
end

local function fork_func()
	-- TODO: this can be supported, but requires direct communication with each worker process,
	-- which we don't currently have.
	error("TODO")
end

local function fork_close(pool)
	if pool.exit_event.flag ~= 0 then
		-- already shut down due to previous error
		return
	end
	C.m3_mp_event_set(pool.exit_event, 1)
	-- wait until all children have exited.
	-- we can't just waitpid() on them because the output queue may fill up causing us to end up
	-- in a deadlock, so we have to keep checking the output queue while waiting.
	local pids = pool.pids
	local npids = pool.parallel
	local timeout = 10 * 1e6
	while npids > 0 do
		while true do
			fork_tick(pool)
			if C.m3_mp_proc_park_timeout(pool.proc, timeout) ~= 0 then
				-- timed out, check chidren
				break
			end
		end
		local i = 1
		while i <= npids do
			local r = C.m3_sys_waitpid(pids[i])
			if r == 0 then
				-- not exited yet
				i = i+1
			else
				pids[i] = pids[npids]
				npids = npids-1
			end
		end
	end
	-- finish any remaining work once more after all children exited.
	fork_tick(pool)
	C.m3_mem_unmap(pool.map, pool.mapsize)
end

local fork_mt = {
	eval  = fork_eval,
	func  = fork_func,
	close = fork_close,
	type = "fork"
}
fork_mt.__index = fork_mt

local function fork(f)
	local pid = C.m3_sys_fork()
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

local function fork_new(L,p)
	-- map from lowest to highest addr
	--   * shared heap
	--   * main process heap
	--   * `environment.parallel` × worker heaps
	-- and one extra region for alignment
	local mapsize = C.CONFIG_MP_PROC_MEMORY*(p+3)
	local ptr = ffi.new("void *[1]")
	C.check(C.m3_mem_map_shared(mapsize, ptr))
	local map = ptr[0]
	local base = bit.band(
		ffi.cast("intptr_t", map)+(C.CONFIG_MP_PROC_MEMORY-1),
		bit.bnot(C.CONFIG_MP_PROC_MEMORY-1)
	)
	local mem = ffi.cast("m3_Shared *", base)
	mem.heap.cursor = base + ffi.sizeof("m3_Shared")
	-- TODO: make queue size configurable
	local main2work = C.m3_mp_queue_new(mem.heap, 8*p)
	local work2main = C.m3_mp_queue_new(mem.heap, 8*p)
	local exit_event = ffi.cast("m3_Event *", C.m3_mp_heap_alloc(mem.heap, ffi.sizeof("m3_Event")))
	env_eval(L, "require('m3_db').disconnect(false)")
	ffi.gc(L, nil)
	local pids = {}
	for i=1, p do
		pids[i] = fork(function()
			-- this is divided in two parts so that the upvalues become closed and addresses are
			-- constified.
			local fid = env_func(L, [[
local C = require "m3_C"
local db = require "m3_db"
local buffer = require "string.buffer"
local ffi = require "ffi"
local eval, disconnect, ffi_cast, ffi_copy, xpcall, traceback = m3.eval, db.disconnect, ffi.cast, ffi.copy, xpcall, debug.traceback
local pid, heap, main2work, work2main, exit_event = ...
_G.M3_PROC_ID = pid

local main2work = ffi.cast("m3_Queue *", main2work)
local work2main = ffi.cast("m3_Queue *", work2main)
local exit_event = ffi.cast("m3_Event *", exit_event)

local pp = ffi.new("m3_ProcPrivate")
pp.heap.cursor = heap
local proc = ffi.cast("m3_Proc *", C.m3_mp_heap_alloc(pp.heap, ffi.sizeof("m3_Proc")))
local write_fut = ffi.cast("m3_Future *", C.m3_mp_heap_alloc(pp.heap, ffi.sizeof("m3_Future")))
local read_fut = ffi.cast("m3_Future *", C.m3_mp_heap_alloc(pp.heap, ffi.sizeof("m3_Future")))
local exit_fut = ffi.cast("m3_Future *", C.m3_mp_heap_alloc(pp.heap, ffi.sizeof("m3_Future")))
C.m3_mp_queue_read(main2work, read_fut)
C.m3_mp_event_wait(exit_event, 0, exit_fut)
write_fut.state = -1ull

local encoder = buffer.new()
local decoder = buffer.new()

local function decode1()
	if #decoder > 0 then
		return decoder:decode(), decode1()
	end
end

local function encode1(n, v, ...)
	if n>0 then
		encoder:encode(v)
		return encode1(n-1, ...)
	end
end

local function encode(...)
	return encode1(select("#", ...), ...)
end

local function doeval(msg, ...)
	msg.state = 2 -- MSG_DEAD
	C.m3_mp_queue_read(main2work, read_fut)
	return encode(xpcall(eval, traceback, ...))
end

return function()
	while true do
		if C.m3_mp_future_completed(write_fut) ~= 0 and C.m3_mp_future_completed(read_fut) ~= 0 then
			local msg = ffi_cast("m3_Message *", read_fut.data)
			local chan = msg.chan
			decoder:set(msg.data, msg.len)
			encoder:reset()
			doeval(msg, decode1())
			local response = C.m3_mp_proc_alloc_message(pp, chan, #encoder)
			ffi_copy(response.data, encoder:ref())
			C.m3_mp_queue_write(work2main, ffi_cast("uintptr_t", response), write_fut)
		elseif C.m3_mp_future_completed(exit_fut) ~= 0 then
			break
		else
			C.m3_mp_proc_park(proc)
		end
	end
	disconnect(false)
end
			]],
			i,
			base + (i+1)*C.CONFIG_MP_PROC_MEMORY,
			ffi.cast("uintptr_t", main2work),
			ffi.cast("uintptr_t", work2main),
			ffi.cast("uintptr_t", exit_event)
			)
			env_eval(L, fid)
		end)
	end
	-- we don't need the worker state in the host process any more.
	env_close(L)
	-- initialize ourselves
	local pp = ffi.new("m3_ProcPrivate")
	pp.heap.cursor = base + C.CONFIG_MP_PROC_MEMORY
	-- proc must be the first allocation
	local proc = ffi.cast("m3_Proc *", C.m3_mp_heap_alloc(pp.heap, ffi.sizeof("m3_Proc")))
	local write_fut = ffi.cast("m3_Future *", C.m3_mp_heap_alloc(pp.heap, ffi.sizeof("m3_Future")))
	local read_fut = ffi.cast("m3_Future *", C.m3_mp_heap_alloc(pp.heap, ffi.sizeof("m3_Future")))
	C.m3_mp_queue_read(work2main, read_fut)
	return setmetatable({
		map        = map,
		mapsize    = mapsize,
		write_fut  = write_fut,
		read_fut   = read_fut,
		proc       = proc,
		pp         = pp,
		exit_event = exit_event,
		main2work  = main2work,
		work2main  = work2main,
		pids       = pids,
		parallel   = p,
		pending    = {},
		freelist   = {},
		nfree      = 0,
		nfut       = 0
	}, fork_mt)
end

local function fork_wait(pool)
	while true do
		fork_tick(pool)
		if pool.nfree >= pool.nfut or pool.error then
			return
		end
		C.m3_mp_proc_park(pool.proc)
	end
end

---- Pool management -----------------------------------------------------------

local function capture(...)
	return {n=select("#", ...), ...}
end

local function parseconfig(config)
	if config == nil then
		if jit.os == "Windows" then
			return "serial"
		else
			return "fork", C.m3_sys_num_cpus()
		end
	elseif type(config) == "table" then
		local mode = config.mode
		return mode, mode == "serial" and 0 or (config.parallel or C.m3_sys_num_cpus())
	elseif config == false or tonumber(config) == 0 then
		return "serial"
	elseif type(config) == "number" or (type(config) == "string" and tonumber(config)) then
		return C.MODE_DEFAULT, tonumber(config)
	elseif type(config) == "string" then
		local m, p = string.match(config, "^([^,])*,?(.*)$")
		return m, tonumber(p) or C.m3_sys_num_cpus()
	else
		error(string.format("expected mode definition: `%s'", config))
	end
end

local function newpool(init, config)
	local mode, parallel = parseconfig(config)
	local L = newenv()
	if mode == "serial" then
		serial_preinit(L)
	end
	local ret = capture(init(L))
	env_init(L)
	local pool
	if mode == "serial" then
		pool = serial_new(L)
	elseif mode == "fork" then
		pool = fork_new(L, parallel)
	end
	return pool, unpack(ret, 1, ret.n)
end

-- TODO: a general wait function that takes varargs:
--   * true -> wait for all pending futures
--   * pool -> wait for all pending futures in this pool
--   * future -> wait for this future
local function wait(x)
	if x.type == "serial" then
		serial_flush(x)
	elseif x.type == "fork" then
		fork_wait(x)
	end
end

--------------------------------------------------------------------------------

return {
	new     = newenv,
	pool    = newpool,
	wait    = wait,
	version = C.version
}
