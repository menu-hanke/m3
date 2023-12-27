-- note: nothing in the implementation assumes that this is the main process,
-- the purpose of this assert is to make sure this module is not required
-- if it's not used.
-- also, this ensures future() is not called before forking.
assert(require("m3_mp").role == "main")

local shm = require "m3_shm"
local ffi = require "ffi"

local C, cast = ffi.C, ffi.cast
local proc, heap = shm.proc(), shm.heap

local futureptr_ct = ffi.typeof("m3_Future *")
local sizeof_future = ffi.sizeof "m3_Future"

-- pending futures
local npending = 0
local pending_fut = {}
local pending_coro = {}

-- deferred coroutines
local ndef1, ndef2 = 0, 0
local deferred, deferred2 = {}, {}

local function future()
	return cast(futureptr_ct, heap:alloc(sizeof_future))
end

--local function freefuture(fut)
--	heap:free(fut, sizeof_future)
--end

local function await(fut)
	if not fut:completed() then
		pending_fut[npending] = fut
		pending_coro[npending] = coroutine.running()
		npending = npending+1
		coroutine.yield()
	end
	return fut.data
end

--local function await_any(futs)
--	local n = #futs
--	for i=1, n do
--		if futs[i]:completed() then
--			return
--		end
--	end
--	local coro = coroutine.running()
--	for i=1, n do
--		pending_fut[npending] = futs[i]
--		pending_coro[npending] = coro
--		npending = npending+1
--	end
--	coroutine.yield()
--	local i = 0
--	while i < npending do
--		if pending_coro[i] == coro then
--			freefuture(pending_fut[i])
--			pending_fut[i], pending_coro[i] = pending_fut[npending], pending_coro[npending]
--			npending = npending-1
--		else
--			i = i+1
--		end
--	end
--end

local function schedule(coro)
	deferred2[ndef2] = coro
	ndef2 = ndef2+1
end

local function yield()
	schedule(coroutine.running())
	coroutine.yield()
end

local function submit(func)
	schedule(coroutine.create(func))
end

local function step(coro)
	local ok, x = coroutine.resume(coro)
	if not ok then
		if type(x) == "string" then
			error(string.format("uncaught error in coroutine: %s", x))
		else
			error(x)
		end
	end
end

local function tick_rundeferred()
	if ndef1 == 0 then return end
	for i=0, ndef1-1 do
		step(deferred[i])
	end
end

local function tick_runfinished()
	if npending == 0 then return end
	local i = 0
	while i < npending do
		local fut = pending_fut[i]
		if fut:completed() then
			local coro = pending_coro[i]
			pending_fut[i], pending_coro[i] = pending_fut[npending], pending_coro[npending]
			npending = npending-1
			--freefuture(fut)
			step(coro)
		else
			i = i+1
		end
	end
end

local function tick()
	tick_rundeferred()
	tick_runfinished()
	deferred, deferred2 = deferred2, deferred
	ndef1, ndef2 = ndef2, 0
end

local function run_noblock()
	repeat
		tick()
	until ndef1 == 0 and ndef2 == 0
	-- TODO: check if the process has been notified. if so, park() doesn't block.
end

local function park()
	assert(npending > 0, "park called without pending coroutines")
	C.m3__mp_proc_park(proc)
end

local function await_sync(fut)
	if fut:completed() then return end
	while true do
		run_noblock()
		if fut:completed() then return end
		park()
	end
end

local function run_until(check)
	while true do
		run_noblock()
		if check() then return end
		park()
	end
end

local function run()
	while true do
		run_noblock()
		if npending > 0 then
			park()
		else
			break
		end
	end
end

return {
	future      = future,
	await       = await,
	--await_any = await_any,
	yield       = yield,
	submit      = submit,
	run_noblock = run_noblock,
	await_sync  = await_sync,
	run_until   = run_until,
	run         = run
}
