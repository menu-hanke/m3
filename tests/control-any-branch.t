-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	control.any {
		ct.node(1),
		ct.node(2),
		ct.node(3)
	},
	ct.node(4)
}

test.post(ct.check, {1,4}, {2,4}, {3,4})
