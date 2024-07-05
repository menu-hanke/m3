local objs = {}

local function parse(x)
	local container, field = x:match("^([^#]+)#(.+)$")
	if container then
		return container, field
	else
		return x
	end
end

local index

local function data(name, obj)
	if not name then return objs end
	local container, field = parse(name)
	if obj then
		if field then
			-- TODO: allow attaching auxiliary data (eg. side arrays to data frames)
			error("TODO")
		end
		assert(not objs[container], "name is already registered")
		objs[container] = obj
	else
		obj = objs[container]
		if field then
			obj = index(obj, field)
		end
	end
	return obj
end

local function meta(x)
	local mt = getmetatable(x)
	return mt and mt["m3$meta"]
end

local function todata(x)
	if type(x) == "string" then
		x = data(x)
	end
	return x
end

local function desc(x)
	x = todata(x)
	local m = meta(x)
	if not m then return end
	while m.descriptor do
		local desc = m.descriptor
		if desc then
			if type(desc) == "function" then
				desc = desc(x)
			end
			x = desc
			m = meta(x)
		end
	end
	return x, m
end

index = function (obj, key)
	obj = todata(obj)
	local desc, meta = desc(obj)
	return meta.index(desc, key)
end

local function typeof(x)
	local _, meta = desc(todata(x))
	return meta and meta.type
end

local function dpairs(x)
	local desc, meta = desc(x)
	return meta.pairs(desc)
end

return {
	data   = data,
	meta   = meta,
	desc   = desc,
	typeof = typeof,
	index  = index,
	pairs  = dpairs
}
