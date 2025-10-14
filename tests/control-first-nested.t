-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.first(control.any {
	control.skip,
	control.first(control.any {
		ct.node(1),
		ct.node(2)
	}),
	ct.node(3)
})

test.post(ct.check, 1)
