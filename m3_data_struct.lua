local access = require "m3_access"
local cdata = require "m3_cdata"
local effect = require "m3_effect"
local mem = require "m3_mem"
local buffer = require "string.buffer"

local function field_default_ctype()
	return "double"
end

local function field_new(struct)
	return mem.slot { region=struct, ctype=field_default_ctype, init=false }
end

local function struct_index(struct, name)
	name = cdata.ident(name)
	local field = field_new(struct)
	struct[name] = field
	return field
end

local function struct_write(struct)
	local sink = {}
	effect.effect(function()
		for name,field in pairs(struct) do
			if (not sink[name]) and access.get(field):match("r") then
				sink[name] = field
				effect.change()
			end
		end
	end)
	return access.use(
		access.defer(function()
			local buf = buffer.new()
			buf:put("local sink = ...\n")
			for name in pairs(sink) do
				buf:putf("local field_%s = sink.%s.ptr\n", name, name)
			end
			buf:put("return function(v) if v ~= nil then\n")
			for name in pairs(sink) do
				buf:putf([[
					do
						local x = v.%s
						if x ~= nil then field_%s[0] = x end
					end
				]], name, name)
			end
			buf:put("end end")
			return load(buf)(sink)
		end),
		access.write(access.sink(sink))
	)
end

local function struct_map(_, _, name)
	return string.format("model(global) %s = {0}", name)
end

local struct_mt = {
	data = {
		type  = "struct",
		map   = struct_map,
		write = struct_write
	},
	__index = struct_index
}

local function new()
	return setmetatable({}, struct_mt)
end

return {
	new = new
}
