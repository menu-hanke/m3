-- vim: ft=lua

data.define [[
table A
model A default'x = 123
]]

local getx = data.transaction():read("A.x")

control.simulate = function()
	assert(getx() == 123)
end
