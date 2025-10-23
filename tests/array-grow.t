-- vim: ft=lua

data.define [[
table T[N]
]]

local getx = data.transaction():read("T.x")
local insert = data.transaction():insert("T", {x=data.arg()})

control.simulate = function()
	local x = getx()
	assert(#x == 0)
	insert({1,2,3})
	x = getx()
	assert(#x == 3 and x[0] == 1 and x[1] == 2 and x[2] == 3)
	insert({4,5,6})
	x = getx()
	assert(#x == 6 and x[0] == 1 and x[1] == 2 and x[2] == 3 and x[3] == 4 and x[4] == 5
		and x[5] == 6)
end
