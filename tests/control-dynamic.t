-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	ct.node(1),
	control.dynamic(function()
		return control.all { ct.node(2) }
	end)
}

test.post(ct.check, {1,2})
