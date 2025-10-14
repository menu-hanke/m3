-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	control.all {},
	ct.node(1)
}

test.post(ct.check, 1)
