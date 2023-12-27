assert(require("m3_state").mode == "mp")

local channel = require "m3_channel"
local pipe = require "m3_pipe"
local shm = require "m3_shm"
local state = require "m3_state"
local ffi = require "ffi"

local C = ffi.C

local worker_idle = pipe.shared_output()
local worker_crash = pipe.shared_output()

local function fut_completed(fut)
	return fut.state == -1ULL
end

local function fut_complete(fut)
	fut.state = -1ULL
end

local function fut_wait_sync(fut)
	if not fut_completed(fut) then
		repeat
			C.m3__mp_proc_park(shm.proc())
		until fut_completed(fut)
	end
end

ffi.metatype(
	"m3_Future",
	{
		__index = {
			completed = fut_completed,
			complete  = fut_complete,
			wait_sync = fut_wait_sync
		}
	}
)

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
	local inbuf = 4 * state.parallel
	local outbuf = 8 * state.parallel
	local mp = package.loaded["m3_mp"]
	mp.out_queue = newqueue(outbuf)
	mp.work_cycle = newevent()
	mp.work_cycle.flag = 1
	if host.sync == nil then host.sync = not not next(channel.inputs) end
	if host.sync then
		mp.in_queue = newqueue(inbuf)
		mp.write_cycle = newevent()
		mp.work_cycle.flag = 1
	end
	local pids = {}
	for i=1, state.parallel do
		local pid = state.fork(function()
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
	worker_idle  = worker_idle,
	worker_crash = worker_crash,
	startup      = startup
}
