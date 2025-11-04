-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	control.single(control.optional(control.all {
		control.skip,
		ct.node(1)
	})),
	control.single(control.optional(ct.node(2)))
}

test.post(ct.check, 2)
