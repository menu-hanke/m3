local effect = require "m3_effect"
local ffi = require "ffi"

-- for `patchptr` protos
local ptrproto = setmetatable({}, {__mode="k"})

local function proto_setfields(proto, fields)
	for k,v in pairs(fields) do
		local ctype = ffi.typeof(v)
		if proto[k] then
			if proto[k].ctype ~= ctype then
				error(string.format(
					"inconsistent ctype %s ~= %s (proto of %s)",
					proto[k].ctype, ctype, proto
				))
			end
		else
			effect.change()
			proto[k] = {ctype=ctype}
		end
	end
end

local proto_mt = {
	__call = proto_setfields
}

local function new(fields)
	local proto = setmetatable({}, proto_mt)
	if fields then proto_setfields(proto, fields) end
	return proto
end

local function get(x)
	if getmetatable(x) == proto_mt then
		-- is it already a proto?
		return x
	elseif (ptrproto and ptrproto[x]) then
		-- is it a patchptr?
		return ptrproto[x]
	else
		-- is it something that has a proto?
		local ok, proto = pcall(function() return x["m3$proto"] end)
		if ok then return proto end
	end
end

local function toproto(x)
	local proto = get(x)
	if proto then return proto else return new(x) end
end

local function setpatchptr(ptr, proto)
	ptrproto[ptr] = proto
end

local function startup()
	ptrproto = nil
	-- TODO: lock all protos
end

return {
	new         = new,
	get         = get,
	toproto     = toproto,
	setpatchptr = setpatchptr,
	startup     = startup
}
