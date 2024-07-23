local dynamic_mt = {
	data = true
}

local function new(x)
	return setmetatable(x, dynamic_mt)
end

return {
	new = new
}
