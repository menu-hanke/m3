local function findupvalue(f, u)
	if type(u) ~= "string" then
		return u
	end
	for i=1, math.huge do
		local uv = debug.getupvalue(f, i)
		if uv == nil then return nil end
		if uv == u then return i end
	end
end

local function getupvalue(f, u)
	local _, v = debug.getupvalue(f, findupvalue(f, u))
	return v
end

local function setupvalue(f, u, v)
	debug.setupvalue(f, findupvalue(f, u), v)
end

local function setupvalues(f, uvs)
	for i=1, math.huge do
		local uv = debug.getupvalue(f, i)
		if uv == nil then return end
		local v = uvs[uv]
		if v ~= nil then debug.setupvalue(f, i, v) end
	end
end

return {
	findupvalue = findupvalue,
	getupvalue  = getupvalue,
	setupvalue  = setupvalue,
	setupvalues = setupvalues
}
