-- vim: ft=lua

data.define [[
model global x = call Lua ["return function(x) return x end"] (query.x)
]]

local q1 = data.transaction():bind("x", 1):read("x")
local q2 = data.transaction():bind("x", 2):read("x")

control.simulate = function()
	assert(q1() == 1)
	assert(q2() == 2)
end
