-- vim: ft=lua

local called = false
local function f() called = true end
control.simulate = setmetatable({}, {
	__m3_control = function() return f end
})

test.post(function() assert(called) end)
