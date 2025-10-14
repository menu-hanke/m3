-- vim: ft=lua

local ct = require "controltest"

control.simulate = control.any { ct.node(1) }

test.post(ct.check, 1)
