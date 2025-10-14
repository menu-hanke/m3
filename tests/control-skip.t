-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.any {
	control.all {
		control.skip,
		ct.node(1)
	},
	ct.node(2)
}

test.post(ct.check, 2)
