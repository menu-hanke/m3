local cdata = require "m3_cdata"
local effect = require "m3_effect"
local fhk = require "m3_fhk"
local layout = require "m3_layout"
local mem = require "m3_mem"
local ffi = require "ffi"
local buffer = require "string.buffer"

local function field_emitread(field)
	return load("local ptr = ... return function() return ptr[0] end")(field.ptr)
end

local function field_emitwrite(field)
	return load([[
		local ptr = ...
		return function(value) if value == nil then return ptr else ptr[0] = value end end
	]])(field.ptr), field.name
end

local function field_emitgraph(field, tab, col)
	return string.format(
		"model(`%s`) `%s` = lds.%s(0x%x)",
		tab,
		col,
		fhk.typesuffix(field.ctype),
		ffi.cast("intptr_t", field.ptr)
	)
end

local field_mt = {
	["m3$meta"] = {
		type  = "struct.field",
		read  = field_emitread,
		write = field_emitwrite,
		graph = field_emitgraph
	}
}

local function struct_newfield(struct, name)
	local field = setmetatable({name=name}, field_mt)
	field.proxy = effect.proxy(field)
	getmetatable(field.proxy)["m3$meta"] = { descriptor = field }
	struct.fields[name] = field
	return field
end

local function emit_fhktablen(_, _, col)
	return string.format("model(global) `%s` = {0}", col)
end

local len = setmetatable({}, {
	["m3$meta"] = {
		datatype = "struct.len",
		graph    = emit_fhktablen
	}
})

local function struct_len()
	return len
end

local function struct_index(struct, name)
	name = cdata.ident(name)
	return struct.fields[name] or struct_newfield(struct, name)
end

local function struct_pairs(struct)
	return pairs(struct.fields)
end

local function field_cmp(a, b)
	if a.align ~= b.align then
		return a.align < b.align
	else
		return a.name < b.name
	end
end

local function allocstruct(proto, region)
	if not next(proto) then return end
	local fields = {}
	for name, field in pairs(proto) do
		table.insert(fields, {proto=field, name=name, align=ffi.alignof(field.ctype)})
	end
	table.sort(fields, field_cmp)
	for _, field in ipairs(fields) do
		field.proto.ptr = mem.new(field.proto.ctype, region)
	end
end

local function struct_layout(fields)
	local proto_r, proto_rw = {}, {}
	for name, field in pairs(fields) do
		if type(name) == "string" then
			field.ctype = ffi.typeof(field.ctype or "double")
			if field.read and not field.write then
				proto_r[name] = field
			elseif field.write then
				proto_rw[name] = field
			end
		end
	end
	allocstruct(proto_r, "frame")
	allocstruct(proto_rw, "vstack")
end

local function emitwriter(fields)
	local buf = buffer.new()
	buf:put("local struct = ...\n")
	for name in pairs(fields) do
		buf:putf("local _%s = struct.%s.ptr\n", name, name)
	end
	buf:put("return function(x) if x ~= nil then \n")
	for name, field in pairs(fields) do
		-- TODO proper defaults, nested types, etc
		buf:putf("_%s[0] = x.%s or %s\n", name, name, field.default or 0)
	end
	buf:put("end end")
	return assert(load(buf))(fields)
end

local function struct_emitwrite(struct)
	if not struct.writer then
		struct.writer = emitwriter(struct.fields)
	end
	return struct.writer
end

local struct_mt = {
	["m3$meta"] = {
		type  = "struct",
		write = struct_emitwrite,
		len   = struct_len,
		index = struct_index,
		pairs = struct_pairs
	}
}

local function struct_new()
	return setmetatable({
		fields = {}
	}, struct_mt)
end

local function new()
	local struct = struct_new()
	local proxy = newproxy(true)
	local mt = getmetatable(proxy)
	mt.__index = function(_, name) return struct_index(struct, name).proxy end
	mt["m3$meta"] = { descriptor = struct }
	layout.call(struct_layout, struct.fields)
	return proxy
end

return {
	new = new
}
