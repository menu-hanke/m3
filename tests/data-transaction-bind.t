-- vim: ft=lua

local slot = data.cdata("int32_t")

local setslot = data.transaction():write(slot)

local getslot = data.transaction()
	:bind("value", slot)
	:read("query.value")

control.simulate = function()
	setslot(123)
	assert(getslot() == 123)
end
