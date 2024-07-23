local nop = function() end

local dispatch = load([[
	local target = ...
	return function(...) return target(...) end
]])(nop)

local function listener(f)
	if f == nil then
		return debug.getupvalue(dispatch, 1)
	else
		debug.setupvalue(dispatch, 1, f)
		jit.flush()
	end
end

return {
	dispatch = dispatch,
	listener = listener
}
