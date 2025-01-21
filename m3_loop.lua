local mp = require "m3_mp"

-- pending futures
local npending = 0
local pending_fut = {}
local pending_coro = {}

-- deferred coroutines
local ndef1, ndef2 = 0, 0
local deferred, deferred2 = {}, {}

local function await(fut)
	if not fut:completed() then
		pending_fut[npending] = fut
		pending_coro[npending] = coroutine.running()
		npending = npending+1
		coroutine.yield()
	end
	return fut.data
end

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
	local ok, err = coroutine.resume(coro)
	if not ok then error(err, 0) end
end

local function tick_rundeferred()
	while ndef1 > 0 do
		ndef1 = ndef1-1
		step(deferred[ndef1])
	end
end

local function tick_runfinished()
	local i = 0
	while i < npending do
		local fut = pending_fut[i]
		if fut:completed() then
			local coro = pending_coro[i]
			pending_fut[i], pending_coro[i] = pending_fut[npending], pending_coro[npending]
			npending = npending-1
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
	mp.park()
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
	await       = await,
	--await_any = await_any,
	yield       = yield,
	submit      = submit,
	run_noblock = run_noblock,
	await_sync  = await_sync,
	run_until   = run_until,
	run         = run
}
