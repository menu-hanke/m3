-- vim: ft=lua

local n = 0

control.simulate = control.loop(function()
	if n == 10 then return true end
	n = n+1
end)

test.post(function() assert(n == 10) end)
