-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.any {
	ct.node(1),
	control.all {
		ct.node(2),
		control.any {}
	},
	ct.node(3)
}

test.post(ct.check, 1, 3)
