-- vim: ft=lua

data.define [[
table T[N]
model T x0 = call Lua ["return function(x) return x end"] (x)
# hack: the purpose of the query parameter is to prevent the inlining of x0
model T xx = x0 + query.dummy
]]

-- TODO: transaction():delete("T") should just (be made to) work
local clear = data.transaction():delete("T", "true")
local insert = data.transaction():insert("T", {x=data.arg()})
local get0 = data.transaction():read("0")
local getxx = data.transaction():read("T.xx"):bind("dummy", 0)

control.simulate = function()
	insert({1,2,3})
	-- trigger instance allocation in outer frame
	get0()
	local sp = control.save()
	-- trigger allocation of T.x0 in inner frame
	getxx()
	-- reset alloc pointer
	control.load(sp)
	-- overwrite data in new frame
	clear()
	insert({4,5,6})
	getxx()
	-- undo allocation
	control.load(sp)
	-- alloc of T0 should have been propagated into the outer frame
	local xx = getxx()
	-- insert in previous frame should not have overwritten xx
	assert(xx[0] == 1)
end
