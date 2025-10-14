-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	ct.node(1),
	control.callcc(function(continue)
		local sp = control.save()
		ct.put(2)
		local r = continue()
		if r ~= nil then goto out end
		control.load(sp)
		ct.put(3)
		r = continue()
		::out::
		control.delete(sp)
		return r
	end),
	ct.node(4)
}

test.post(ct.check, {1, {2,4}, {3,4}})
