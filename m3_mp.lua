assert(require("m3_environment").mode == "mp")

local environment = require "m3_environment"
local ipc = require "m3_ipc"
local shm = require "m3_shm"
local ffi = require "ffi"

local C = ffi.C

local main = ipc.dispatch() -- worker->main mpsc
local work = ipc.dispatch() -- main->worker spmc

local function newqueue(size)
	return shm.with_shared_heap(function(heap)
		return C.m3__mp_queue_new(heap, size)
	end)
end

local function newevent()
	local event = shm.with_shared_heap(function(heap) return heap:new("m3_Event") end)
	event.waiters = nil
	event.lock.state = 0
	return event
end

local function startup()
	local host = require "m3_host"
	-- TODO make inbuf/outbuf configurable
	local inbuf = 4 * environment.parallel
	local outbuf = 8 * environment.parallel
	local mp = package.loaded["m3_mp"]
	mp.out_queue = newqueue(outbuf)
	mp.work_cycle = newevent()
	mp.work_cycle.flag = 1
	if host.sync == nil then host.sync = #work > 0 end
	if host.sync then
		mp.in_queue = newqueue(inbuf)
		mp.write_cycle = newevent()
		mp.write_cycle.flag = 1
	end
	local pids = {}
	for i=1, environment.parallel do
		local pid = environment.fork(function()
			C.m3__mp_proc_id = i+1
			mp.role = "worker"
			return require("m3_mp_worker")()
		end)
		if pid then
			pids[i] = pid
		else
			error("TODO: handle fork failed")
		end
	end
	mp.role = "main"
	mp.pids = pids
	local main = require("m3_mp_main")
	mp.shutdown = main.shutdown
	return main.run
end

--------------------------------------------------------------------------------

return {
	main         = main,
	work         = work,
	worker_idle  = main:channel(),
	worker_crash = main:channel(),
	startup      = startup
}
