-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	ct.node(1),
	control.any {}
}

test.post(ct.check)
