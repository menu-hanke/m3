-- WARNING: (somewhat) CURSED.
-- this module allows creating functions that, to the compiler, look like they return
-- constant values, when they really don't.

local function new(value)
	return load([[
		local value = ...
		return function() return value end
	]])(value)
end

local function set(f, v)
	debug.setupvalue(f, 1, v)
	jit.flush()
end

return {
	new = new,
	set = set
}
