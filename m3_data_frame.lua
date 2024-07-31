local access = require "m3_access"
local array = require "m3_array"
local cdata = require "m3_cdata"
local fhk = require "m3_fhk"
local mem = require "m3_mem"
local ffi = require "ffi"

local DF_OBJ = -1

local function col_read(col)
	access.read(col.df[DF_OBJ])
	return access.defer(function()
		return load(string.format([[
			local df, slice = ...
			return function() return slice(df.%s, df.num) end
		]], col.name))(col.df[DF_OBJ].ptr, array.slice_of(col.ctype))
	end)
end

local function col_map_(col, tab, name)
	local slot = col.df[DF_OBJ]
	return string.format(
		"model(global) %s#%s = ldv.%s(lds.i64(0x%x), lds.u32(0x%x))",
		tab,
		name,
		fhk.typesuffix(col.ctype),
		ffi.cast("intptr_t", slot.ptr) + ffi.offsetof(slot.ctype, col.name),
		ffi.cast("intptr_t", slot.ptr) + ffi.offsetof(slot.ctype, "num")
	)
end

local function col_map(col, tab, name)
	access.read(col.df)
	return function() return col_map_(col, tab, name) end
end

local function col_write(col)
	return access.use(
		access.defer(
			function()
				return access.capture(load(string.format([[
					local df = ...
					return function(v)
						if v == nil then
							return df.%s
						else
							df:overwrite(%q, v)
						end
					end
				]], col.name, col.name))(col.df[DF_OBJ].ptr))
			end
		),
		access.write(col.df)
	)
end

local col_mt = {
	data = {
		type  = "dataframe.col",
		read  = col_read,
		write = col_write,
		map   = col_map
	}
}

local function col_new(df, name)
	return setmetatable({df=df, name=name}, col_mt)
end

local function df_index(df, name)
	name = cdata.ident(name)
	local col = col_new(df, name)
	df[name] = col
	return col
end

local function df_read(df)
	return df[DF_OBJ]
end

local function df_map_(df, name)
	local slot = df[DF_OBJ]
	return string.format(
		"model(global) %s = {..lds.u32(0x%x)}",
		name,
		ffi.cast("intptr_t", slot.ptr) + ffi.offsetof(slot.ctype, "num")
	)
end

local function df_map(df, _, name)
	access.read(df)
	return function() return df_map_(df, name) end
end

local function df_write(df)
	local slot = df[DF_OBJ]
	return access.use(
		access.defer(function()
			return access.capture(load([[
				local ptr = ...
				return function(v)
					if v == nil then
						return ptr
					else
						return ptr:settab(v)
					end
				end
			]])(slot.ptr))
		end),
		access.write(slot)
	)
end

local function df_ctype(slot)
	local proto = {}
	for name, col in pairs(slot.df) do
		if type(name) == "string" and access.get(col) ~= "" then
			col.ctype = ffi.typeof(col.ctype or "double")
			if col.dummy == nil then
				col.dummy = cdata.dummy(col.ctype)
			end
			table.insert(proto, col)
		end
	end
	return array.df_of(proto)
end

local df_mt = {
	data = {
		type  = "dataframe",
		read  = df_read,
		write = df_write,
		map   = df_map
	},
	__index = df_index
}

local function new()
	local df = setmetatable({ [DF_OBJ] = mem.slot { ctype=df_ctype }, }, df_mt)
	df[DF_OBJ].df = df
	return df
end

return {
	new = new
}
