-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.all {
	"false",
	ct.node(1)
}

test.post(ct.check)
