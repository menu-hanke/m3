-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.try(control.all {
	"true",
	ct.node(1)
})

test.post(ct.check, 1)
