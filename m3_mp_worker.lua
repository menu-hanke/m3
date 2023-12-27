assert(require("m3_mp").role == "worker")

local shm = require "m3_shm"
shm.proc_startup()

local channel = require "m3_channel"
local host = require "m3_host"
local ipc = require "m3_ipc"
local mem = require "m3_mem"
local mp = require "m3_mp"
local ffi = require "ffi"

local proc = shm.proc()
local C, cast = ffi.C, ffi.cast
local worker_idle, worker_crash = mp.worker_idle, mp.worker_crash
local cycle = mp.work_cycle
local cycle_fut = shm.heap:new("m3_Future")
cycle_fut.data = 1
cycle_fut:complete()

---- Input ---------------------------------------------------------------------

local work = host.sync and (function()
	local inputs = channel.dispinput()
	local decode = ipc.decode
	local in_queue = mp.in_queue
	local wcycle = mp.write_cycle
	local wcycle_fut = shm.heap:new("m3_Future")
	local fut = shm.heap:new("m3_Future")
	C.m3__mp_queue_read(in_queue, fut)
	C.m3__mp_event_wait(wcycle, -2, wcycle_fut)
	return function()
		local endcycle = false
		-- return false when we observe ALL of the following true simultaneously:
		--   * wcycle_fut:completed()
		--   * wcycle_fut.data >= cycle_fut.data
		--   * not fut:completed()
		-- NOTE: some assumptions are made here:
		--   (1) wcycle_fut:completed() has acquire semantics
		--   (2) fut:completed() is not moved/eliminated by the compiler
		-- (1) is true on x86, (2) is currently true.
		-- this should probably be written in C.
		::again::
		if fut:completed() then
			local chan, msg = decode(fut.data)
			inputs[chan](msg)
			C.m3__mp_queue_read(in_queue, fut)
			return true
		elseif endcycle then
			return false
		else
			if wcycle_fut:completed() then
				-- is this cycle finished?
				-- this also handles the case when we are exiting and wcycle_fut.data = -1
				endcycle = wcycle_fut.data >= cycle_fut.data
				if not endcycle then
					-- no, read write counter again
					C.m3__mp_event_wait(wcycle, wcycle_fut.data, wcycle_fut)
				end
				-- else: check fut:completed() again, since it may have been completed
				-- after we checked it.
			else
				-- neither fut or wcycle_fut insta-completed so the main process will unpark
				-- us when either of those completes.
				C.m3__mp_proc_park(proc)
			end
			-- this is a goto and not a loop because in the fast path there's no loop here,
			-- the inner loop is the one in mainloop().
			-- (if we have a while ... loop here then luajit won't inline this into the main loop)
			goto again
		end
	end
end)() or host.work

---- Output --------------------------------------------------------------------

do
	local uintptr_ct = ffi.typeof("uintptr_t")
	local encode = ipc.encode
	local queue = mp.out_queue
	local fut = shm.heap:new("m3_Future")
	channel.setoutput(function(x, chan)
		C.m3__mp_queue_write(queue, cast(uintptr_ct, encode(chan, x)), fut)
		fut:wait_sync()
	end)
end

---- Main loop -----------------------------------------------------------------

local memload = mem.load
local id = C.m3__mp_proc_id
local fp = mem.save()

local function mainloop()
	while true do
		-- wait for the next cycle.
		C.m3__mp_event_wait(cycle, cycle_fut.data, cycle_fut)
		cycle_fut:wait_sync()
		-- can't test for -1ULL here because event.flag is uint32_t
		if cycle_fut.data == 0xffffffff then break end
		while true do if work() == false then break end memload(fp) end
		-- we are done with this cycle.
		--print("worker", id, "::", "idle on cycle", tonumber(cycle_fut.data))
		worker_idle({id=id, cycle=tonumber(cycle_fut.data)})
	end
end

return function()
	local ok, err = xpcall(mainloop, debug.traceback)
	io.stdout:flush()
	if not ok then
		worker_crash({id=id, err=err})
		return 1
	else
		return 0
	end
end
