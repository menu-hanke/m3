-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.any {
	control.all {
		function() return false end,
		ct.node(1)
	},
	ct.node(2)
}

test.post(ct.check, 2)
