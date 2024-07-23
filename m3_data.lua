local cdata = require "m3_cdata"

local objs = {}

local function parse(x)
	local container, field = x:match("^([^#]+)#(.+)$")
	if container then
		return container, field
	else
		return x
	end
end

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
			obj = obj[field]
		end
	end
	return obj
end

local function triples()
	return coroutine.wrap(function()
		for name, container in pairs(objs) do
			if type(name) == "string" then
				for field, desc in pairs(container) do
					if type(field) == "string" then
						coroutine.yield(name, field, desc)
					end
				end
			end
		end
	end)
end

local function meta(x)
	local mt = getmetatable(x)
	if mt then
		local data = mt.data
		if data == true then
			return x
		elseif data then
			return meta(data) or data
		end
	end
end

local function todata(x)
	if type(x) == "string" then
		x = data(x)
	end
	return x
end

local function typeof(x)
	local meta = meta(todata(x))
	return meta and meta.type
end

local function ctype(x)
	x = todata(x)
	if x.ctype then return x.ctype end
	local meta = meta(x)
	return meta and meta.ctype(x)
end

local function dummy(x)
	x = todata(x)
	if x.dummy then return x.dummy end
	local meta = meta(x)
	if meta and meta.dummy then return meta.dummy end
	local ctype = ctype(x)
	return ctype and cdata.dummy(ctype)
end

return {
	data    = data,
	triples = triples,
	meta    = meta,
	typeof  = typeof,
	ctype   = ctype,
	dummy   = dummy
}
