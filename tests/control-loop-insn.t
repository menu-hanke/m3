-- vim: ft=lua

local ct = require "controltest"

local state = data.cdata { ctype="struct { uint32_t bit; uint32_t value; }" }
local toggle = data.transaction():mutate(state, function(s) s.value = s.value+2^s.bit end)
local nextbit = data.transaction():mutate(state, function(s) s.bit = s.bit+1 end)
local getstate = data.transaction():read(state)

control.simulate = control.all {
	control.loop(control.all {
		function() if getstate().bit >= 3 then return true end end,
		control.optional(toggle),
		nextbit,
	}),
	function() ct.put(getstate().value) end
}

test.post(ct.check,  0b111, 0b011, 0b101, 0b001, 0b110, 0b010, 0b100, 0b000 )
