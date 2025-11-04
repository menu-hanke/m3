-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.single(control.any {
	control.skip,
	control.single(control.any {
		ct.node(1),
		ct.node(2)
	}),
	ct.node(3)
})

test.post(ct.check, 1)
