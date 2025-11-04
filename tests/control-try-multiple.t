-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	control.try(control.any {
		ct.node(1),
		ct.node(2)
	}),
	ct.node(3)
}

test.post(ct.check, {1, 3}, {2, 3})
