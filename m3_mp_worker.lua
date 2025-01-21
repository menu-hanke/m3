local mp = require "m3_mp"
local sqlite = require "m3_sqlite"

mp.main:proc_init_send(function(fut) fut:wait_sync() end)
local recv, work_fut = mp.work:proc_init_recv()
local exit_fut = mp.heap:new("m3_Future")

local function mainloop()
	mp.exit:wait(0, exit_fut)
	local done = 0
	while true do
		if work_fut:completed() then
			-- there is new work to do
			recv()
			done = done+1
		elseif done > 0 then
			-- inform main process that we have made some progress
			mp.progress.send(done)
			done = 0
		elseif exit_fut:completed() then
			-- we are being asked to exit immediately
			break
		else
			-- wait for more work
			mp.park()
		end
	end
	-- flush any buffered statements
	sqlite.disconnect(false)
end

return function(pid)
	local ok, err = xpcall(mainloop, debug.traceback)
	io.stdout:flush()
	if not ok then
		mp.crash.send({id=pid, err=err})
		return 1
	else
		return 0
	end
end
