-- vim: ft=lua

local n = 0

local insn = control.all {
	function()
		if n == 10 then return false end
		n = n+1
	end
}

table.insert(insn, insn)

control.simulate = insn

test.post(function() assert(n == 10) end)
