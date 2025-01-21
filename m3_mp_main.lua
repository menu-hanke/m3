local loop = require "m3_loop"
local mp = require "m3_mp"
local shutdown = require "m3_shutdown"
local ffi = require "ffi"
local C = ffi.C

local global_done = 0
local global_sent = 0

function mp.progress.recv(n)
	global_done = global_done + n
end

function mp.crash.recv(err)
	global_done = math.huge
	error(string.format("worker process %d crashed: %s", err.id-1, err.err), 0)
end

mp.work:proc_init_send(function(fut)
	global_sent = global_sent + 1
	loop.await_sync(fut)
end)

do
	local await = loop.await
	local recv, fut = mp.main:proc_init_recv()
	loop.submit(function()
		while true do
			await(fut)
			recv()
		end
	end)
end

local function kill()
	if mp.exit.flag ~= 0 then
		-- already shut down due to previous error
		return
	end
	mp.exit:set(1)
	-- wait until all children have exited.
	-- we can't just waitpid() on them because the output queue may fill up causing us to end up
	-- in a deadlock, so we have to keep checking the output queue while waiting.
	local pids = mp.pids
	local npids = #pids
	local timeout = 10 * 1e6
	while npids > 0 do
		while true do
			loop.run_noblock()
			if mp.park(timeout) then break end
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
	loop.run_noblock()
end

shutdown(kill)

local function alldone()
	return global_done >= global_sent
end

local function wait()
	loop.run_until(alldone)
end

return {
	wait = wait
}
