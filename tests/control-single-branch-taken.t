-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.single(control.any {
	ct.node(1),
	ct.node(2)
})

test.post(ct.check, 1)
