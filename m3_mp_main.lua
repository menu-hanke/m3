assert(require("m3_mp").role == "main")

local shm = require "m3_shm"
shm.proc_startup()

local channel = require "m3_channel"
local host = require "m3_host"
local ipc = require "m3_ipc"
local loop = require "m3_loop"
local mp = require "m3_mp"
local state = require "m3_state"
local ffi = require "ffi"

local C, cast = ffi.C, ffi.cast
local cycle = mp.work_cycle
local run_noblock, run_until = loop.run_noblock, loop.run_until
local parallel = state.parallel

local numidle
local cyclenum = cycle.flag

---- Input ---------------------------------------------------------------------

if mp.in_queue then
	local uintptr_ct = ffi.typeof("uintptr_t")
	local encode = ipc.encode
	local queue = mp.in_queue
	local await_sync = loop.await_sync
	local fut = loop.future()
	channel.setinput(function(x, chan)
		--print("main: write", x, "on channel", chan)
		C.m3__mp_queue_write(queue, cast(uintptr_ct, encode(chan, x)), fut)
		await_sync(fut)
		-- TODO: tick every `n` calls (for some n=10-ish?)
	end)
end

---- Output --------------------------------------------------------------------

channel.settarget(mp.worker_idle, function(msg)
	if msg.cycle == cyclenum then
		numidle = numidle+1
		--print("idle:", numidle, "on cycle", cyclenum)
	end
end)

channel.settarget(mp.worker_crash, function(msg)
	error(msg)
end)

do
	local outputs = channel.dispoutput()
	local decode = ipc.decode
	local out_queue = mp.out_queue
	local await = loop.await
	local fut = loop.future()
	loop.submit(function()
		while true do
			C.m3__mp_queue_read(out_queue, fut)
			local ptr = await(fut)
			local chan, msg = decode(ptr)
			outputs[chan](msg)
		end
	end)
end

---- Main loop -----------------------------------------------------------------

local function nextcycle()
	numidle = 0
	cyclenum = cyclenum+1
	C.m3__mp_event_set(cycle, cyclenum)
	--print("main: cycle", cyclenum)
end

local function allidle()
	return numidle == parallel
end

local function waitworkers()
	run_until(allidle)
end

local main_cycle
if host.sync then
	local work = host.work
	local write_cycle = mp.write_cycle
	main_cycle = function()
		nextcycle()
		while work() ~= false do end
		-- no more input for this cycle.
		C.m3__mp_event_set(write_cycle, cyclenum)
		waitworkers()
	end
else
	main_cycle = function()
		nextcycle()
		waitworkers()
	end
end

local function main_shutdown()
	if cycle.flag == -1 then
		-- already shut down due to previous error.
		return
	end
	C.m3__mp_event_set(cycle, -1)
	if mp.write_cycle then
		C.m3__mp_event_set(mp.write_cycle, -1)
	end
	-- wait until all children have exited.
	-- we can't just waitpid() on them because the output queue may fill up causing us to end up
	-- in a deadlock, so we have to keep checking the output queue while waiting.
	local pids = mp.pids
	local npids = #pids
	local timeout = 10 * 1e6
	while npids > 0 do
		while true do
			run_noblock()
			if C.m3__mp_proc_park_timeout(shm.proc(), timeout) ~= 0 then break end
		end
		local i = 1
		while i <= npids do
			local r = C.m3__mp_reap(pids[i])
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
	run_noblock()
end

local function main_run()
	if cycle.flag == -1 then
		error("process pool has been shut down")
	end
	local ok, err = pcall(main_cycle)
	if not ok then
		main_shutdown()
		if type(err) == "table" and err.id and err.err then
			error(string.format("worker process %d crashed: %s", err.id-1, err.err))
		else
			error(string.format("main process crashed: %s", err))
		end
	end
end

return {
	run      = main_run,
	shutdown = main_shutdown
}
