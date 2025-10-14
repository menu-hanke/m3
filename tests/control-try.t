-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	control.try(control.all {
		control.skip,
		ct.node(1)
	}),
	control.try(ct.node(2))
}

test.post(ct.check, 2)
