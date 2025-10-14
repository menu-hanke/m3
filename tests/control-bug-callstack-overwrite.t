-- vim: ft=lua

local ct = require "controltest"

local function nop() end
local nopbarrier = control.all { nop, nop }

local branch = control.all {
	control.any {
		nopbarrier,
		nopbarrier,
	},
	nopbarrier,
}

control.simulate = control.all {
	branch,
	branch,
	ct.node(1)
}

test.post(ct.check, 1, 1, 1, 1)
