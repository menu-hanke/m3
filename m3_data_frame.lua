local array = require "m3_array"
local cdata = require "m3_cdata"
local effect = require "m3_effect"
local fhk = require "m3_fhk"
local layout = require "m3_layout"
local ffi = require "ffi"

local function emitread(df, field)
	return load(string.format("local df = ... return function() return df.%s end", field))(df.obj)
end

local function emitslice(col)
	return assert(load(string.format([[
		local slice, df = ...
		return function() return slice(df.%s, df.num) end
	]], col.name)))(array.slice_of(col.ctype), col.df.obj)
end

local function col_emitread(col, ctx)
	if ctx:checkmark({direct=true}) then
		return emitread(col.df, col.name)
	else
		return emitslice(col)
	end
end

local function col_emitwrite(col)
	return load(string.format([[
		local df = ...
		return function(value)
			if value == nil then
				return df.%s
			else
				df:overwrite('%s', value)
			end
		end
	]], col.name, col.name))(col.df.obj), col.name
end

local function col_emitgraph(col, tab, name)
	local dfct = ffi.typeof(col.df.obj[0])
	local base = ffi.cast("intptr_t", col.df.obj)
	return string.format(
		"model(global) `%s`#`%s` = ldv.%s(lds.i64(0x%x), lds.u32(0x%x))",
		tab,
		name,
		fhk.typesuffix(col.ctype),
		base + ffi.offsetof(dfct, col.name),
		base + ffi.offsetof(dfct, "num")
	)
end

local col_mt = {
	["m3$meta"] = {
		type  = "dataframe.col",
		read  = col_emitread,
		write = col_emitwrite,
		graph = col_emitgraph
	}
}

local function df_newcol(df, name)
	local col = setmetatable({df=df, name=name}, col_mt)
	col.proxy = effect.proxy(col)
	getmetatable(col.proxy)["m3$meta"] = { descriptor = col }
	df.cols[name] = col
	return col
end

local function len_emitread(len)
	return emitread(len.df, "num")
end

local function len_emitgraph(len, _, name)
	return string.format(
		"model(global) `%s` = {..lds.u32(0x%x)}",
		name,
		ffi.cast("intptr_t", len.df.obj) + ffi.offsetof(len.df.obj[0], "num")
	)
end

local len_mt = {
	["m3$meta"] = {
		type  = "dataframe.len",
		read  = len_emitread,
		graph = len_emitgraph
	}
}

local function df_len(df)
	if not df.len then
		df.len = setmetatable({df=df}, len_mt)
	end
	return df.len
end

local function df_index(df, name)
	name = cdata.ident(name)
	return df.cols[name] or df_newcol(df, name)
end

local function df_pairs(df)
	return coroutine.wrap(function()
		if df.len then coroutine.yield("", df.len) end
		for name, col in pairs(df.cols) do
			coroutine.yield(name, col)
		end
	end)
end

local function df_emitread(df)
	if not df.reader then
		df.reader = load("local df = ... return function() return df end")(df.obj)
	end
	return df.reader
end

local function df_emitwrite(df)
	if not df.writer then
		df.writer = load([[
		local df = ...
		return function(v)
			if v == nil then
				return df
			else
				return df:settab(v)
			end
		end
	]])(df.obj)
	end
	return df.writer
end

local function df_layout(df)
	local proto = {}
	for name, col in pairs(df.cols) do
		if col.read or col.write then
			col.ctype = ffi.typeof(col.ctype or "double")
			proto[name] = col.ctype
		end
	end
	df.obj = array.dataframe(proto)
end

local df_mt = {
	["m3$meta"] = {
		type  = "dataframe",
		read  = df_emitread,
		write = df_emitwrite,
		len   = df_len,
		index = df_index,
		pairs = df_pairs
	}
}

local function df_new()
	return setmetatable({
		cols = {}
	}, df_mt)
end

local function new()
	local df = df_new()
	local proxy = newproxy(true)
	local mt = getmetatable(proxy)
	mt.__index = function(_, name) return df_index(df, name).proxy end
	mt.__len = function() return df_len(df) end
	mt["m3$meta"] = { descriptor = df }
	layout.call(df_layout, df)
	return proxy
end

return {
	new = new
}
