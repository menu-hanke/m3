-- vim: ft=lua

data.define [[
	table X[N]
	table Y[M]
]]

local newxs = data.transaction():insert("X", {x=data.arg()})
local getxs = data.transaction():read("X.x")
local newys = data.transaction():insert("Y", {y=data.arg()})
local getys = data.transaction():read("Y.y")

control.simulate = control.callcc(function(continue)
	-- allocate in pending frame
	newxs { 1, 2, 3, 4 }
	-- this *should* be a no-op
	control.delete(control.save())
	control.delete(control.save())
	newys { 10, 11, 12, 13 }
	-- print(getxs(), getys())
	assert(getxs()[0] == 1)
	assert(getys()[0] == 10)
	return continue()
end)
