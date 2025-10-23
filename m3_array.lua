local C = require "m3_C"
local cdata = require "m3_cdata"
local code = require "m3_code"
local mem = require "m3_mem"
local fhk = require "fhk"
local ffi = require "ffi"
local buffer = require "string.buffer"
require "table.new"
local load = code.load
local mem_alloc, mem_realloc, mem_state, iswritable = mem.alloc, mem.realloc, mem.state(), mem.iswritable
local tonumber, type = tonumber, type
local alignof, cast, ffi_copy, ffi_fill, sizeof, typeof = ffi.alignof, ffi.cast, ffi.copy, ffi.fill, ffi.sizeof, ffi.typeof
local band, bor, lshift, rshift = bit.band, bit.bor, bit.lshift, bit.rshift
local max, min = math.max, math.min

local direct_cache = {} -- ctypeid -> function

local function tensor_directbuf(x)
	return x.e
end

local function directfunc(ct)
	if fhk.istensor(ct) then return tensor_directbuf end
	return false
end

local function direct(x)
	local f = direct_cache[tonumber(typeof(x))]
	if f then return f(x) end
	if f == nil then
		f = directfunc(typeof(x))
		direct_cache[tonumber(typeof(x))] = f
		if f then return f(x) end
	end
	return x
end

-- size of pointed element. carefully tuned so that it compiles to a constant.
local null = ffi.cast("char *", 0)
local function sizeofp(p)
	return cast("char *", cast(typeof(p+0), null)+1) - null
end

-- NOTE: this is slow because all types share this same loop.
-- if needed this can be sped up by generating a new loop for each combination.
local function copy_fallback(dst, src, i, j, num)
	for k=0, num-1 do
		dst[k+i] = src[k+j]
	end
end

local function copy(dst, src, num)
	if num == nil then num = #src end
	if type(src) == "cdata" then
		src = direct(src)
		if type(dst) == "cdata" then
			dst = direct(dst)
			if typeof(dst) == typeof(src) then
				return ffi_copy(dst, src, num*sizeofp(dst))
			else
				return copy_fallback(dst, src, 0, 0, num)
			end
		else
			return copy_fallback(dst, src, 1, 0, num)
		end
	else
		if type(dst) == "cdata" then
			dst = direct(dst)
			return copy_fallback(dst, src, 0, 1, num)
		else
			return copy_fallback(dst, src, 1, 1, num)
		end
	end
end

local function fill(dst, i, v, n)
	for k=0, n-1 do
		dst[k+i] = v
	end
end

local function copyext(dst, dstnum, src, srcnum, dummy)
	if type(src) == "number" then
		fill(dst, 0, src, dstnum)
		return
	end
	if srcnum > 0 then
		copy(dst, src, min(dstnum, srcnum))
	end
	if srcnum < dstnum then
		if not dummy then
			error("too few elements and no filler value")
		end
		fill(dst, srcnum, dummy, dstnum-srcnum)
	end
end

local function df_mutate(df, col, realloc)
	if col then
		if realloc or not iswritable(df[col]) then
			local size = df["m3$size"][col]
			local align = df["m3$align"][col]
			df[col] = mem_realloc(df[col], size*df.num, size*df.cap, align)
		end
	else
		if df.num > 0 then
			C.check(C.m3_array_mutate(mem_state, df["m3$cproto"], df))
		else
			df.cap = 0
		end
	end
	return df
end

local function df_write(df, col, realloc)
	if realloc or not iswritable(df[col]) then
		local size = df["m3$size"][col]
		local align = df["m3$align"][col]
		df[col] = mem_alloc(size*df.cap, align)
	end
	return df
end

local function df_alloc(df, n)
	local num = df.num
	if num+n > df.cap then
		C.check(C.m3_array_grow(mem_state, df["m3$cproto"], df, n))
	else
		df.num = num+n
		df_mutate(df)
	end
	return num
end

local function df_copyrowfunc(cols)
	local buf = buffer.new()
	buf:put("return function(df, idx, row)")
	for _,col in ipairs(cols) do
		-- this `if` is free (after compilation) because there's a typecheck
		-- when loading the table field anyway.
		-- alternatively you can use alloc() and manual writes.
		-- TODO: this doesn't handle non-scalar ctypes yet, should use ffi.fill for those.
		buf:putf([[
			do
				local v = row.%s
				if v == nil then
					df.%s[idx] = %s
				else
					df.%s[idx] = v
				end
			end
		]], col.name, col.name, code.embedconst(col.dummy or 0), col.name)
	end
	buf:put("end")
	return load(buf)()
end

local function df_addrow(df, row)
	local idx = df:alloc(1)
	df:copyrow(idx, row)
end

local function df_addrowsfunc()
	return load([[
		return function(df, rows)
			local num = #rows
			local idx = df:alloc(num)
			for i=0, #rows-1 do
				df:copyrow(idx+i, rows[i+1])
			end
		end
	]])()
end

local uint64_p = typeof("uint64_t *")

local function df_clear(df, idx)
	if idx == nil then
		df.num = 0
		df.cap = 0
	elseif #idx > 0 then
		local size = 8*(1+rshift(df.num,6))
		-- scratch pointer is aligned here because len=0
		local deletemap = cast(uint64_p, C.m3_mem_tmp(mem_state, size))
		ffi_fill(deletemap, size)
		for i=1, #idx do
			local j = idx[i]
			local k = rshift(j, 6)
			deletemap[k] = bor(deletemap[k], lshift(1ull, band(j, 0x3f)))
		end
		-- this also resets the scratch buffer
		C.check(C.m3_array_delete_bitmap(mem_state, df["m3$cproto"], df))
	elseif type(idx) == "number" then
		-- TODO: do retain_sparse with two spans
		error("TODO")
	end
end

local sizeof_span = sizeof("m3_Span")
local span_p = typeof("m3_Span *")

local function df_clearmask(df, mask)
	local n = df.num
	if n == 0 then return end
	local i = 0
	local remain = 0
	if mask[0] then goto ones end
	::zeros::
	do
		local i0 = i
		while i < n do
			if mask[i] then
				break
			end
			i = i+1
			remain = remain+1
		end
		local span = cast(span_p, C.m3_mem_tmp(mem_state, sizeof_span))
		span.ofs = i0
		span.num = i-i0
	end
	if i == n then goto clear end
	::ones::
	i = i+1
	while i < n do
		if not mask[i] then
			goto zeros
		end
		i = i+1
	end
	::clear::
	C.check(C.m3_array_retain_spans(mem_state, df["m3$cproto"], df, remain))
end

local function df_overwrite(df, col, src)
	df_write(df, col)
	copy(df[col], src)
	return df
end

local function dim(x)
	if type(x) == "number" then
		return 1
	elseif x then
		return #x
	end
end

local function df_addcolsfunc(cols)
	if #cols == 0 then return function() end end
	local buf = buffer.new()
	buf:put([[
		local max = math.max
		local copyext, dim = ...
		return function(df, cols, num)
	]])
	for i,c in ipairs(cols) do
		buf:putf("local num%d = dim(cols.%s)", i, c.name)
		if c.dummy ~= nil then
			buf:put(" or 0")
		end
		buf:put("\n")
	end
	buf:putf("if not num then num = max(")
	for i,c in ipairs(cols) do
		if i>1 then buf:put(",") end
		buf:putf("num%d", i)
		if c.dummy == nil then
			buf:put(" or 0")
		end
	end
	buf:put(") end\nif num == 0 then return end local base = df:alloc(num)\n")
	for i,c in ipairs(cols) do
		if c.dummy == nil then
			buf:putf(
				[[if not num%d then error("column without dummy must be given a value: `%s'") end]],
				i, c.name
			)
			buf:put("\n")
		end
	end
	for i,c in ipairs(cols) do
		buf:putf(
			"copyext(df.%s+base, num, cols.%s, num%d, %s)\n",
			c.name, c.name, i, code.embedconst(c.dummy)
		)
	end
	buf:put("end")
	return load(buf)(copyext, dim)
end

local function df_extend(df, tab, num)
	-- TODO: this should handle missing fields:
	--   * set to dummy if not given,
	--   * error if no dummy specified
	if num or (tab[1] == nil) then
		return df:addcols(tab, num)
	else
		return df:addrows(tab)
	end
end

local function df_settab(df, tab, num)
	df_clear(df)
	return df_extend(df, tab, num)
end

local function df_len(df)
	return df.num
end

-- table("col") -> column as table
-- table()      -> table of columns
-- table(true)  -> table of rows
local function df_table(df, col)
	if col then
		return cdata2tab(df[col], df.num)
	elseif col == true then
		local tab = table.new(#df, 0)
		for i=1, #df do tab[i] = {} end
		for c in pairs(df["m3$size"]) do
			for i=1, #df do
				tab[i][c] = df[c][i-1]
			end
		end
		return tab
	else
		local tab = {}
		for c in pairs(df["m3$size"]) do
			tab[c] = df_table(df, c)
		end
		return tab
	end
end

local function df_tostring(df)
	local cols = {}
	local data = {}
	for col in pairs(df["m3$size"]) do
		table.insert(cols, col)
		data[col] = {}
	end
	table.sort(cols)
	local colf = {}
	for c,col in ipairs(cols) do
		-- this will choke badly on mixed types but it doesn't matter,
		-- this function doesn't need to be fast.
		local cw = #col
		for i=1, #df do
			local s = tostring(df[col][i-1])
			cw = max(cw, #s)
			data[col][i] = s
		end
		colf[col] = "%"..(cw+(c>1 and 2 or 0)).."s"
	end
	local buf = buffer.new()
	for _,col in ipairs(cols) do
		buf:putf(colf[col], col)
	end
	for i=1, #df do
		buf:put("\n")
		for _,col in ipairs(cols) do
			buf:putf(colf[col], data[col][i])
		end
	end
	return tostring(buf)
end

local function df_cproto(proto)
	local size = {}
	for _,col in ipairs(proto) do
		table.insert(size, sizeof(col.ctype))
	end
	return ffi.new("m3_DfProto", #size, {
		num   = #proto,
		align = proto[1] and ffi.alignof(proto[1].ctype) or 0,
		size  = size
	})
end

local function df_of(proto)
	local size, align = {}, {}
	local ctdef = buffer.new()
	local ctarg = {}
	-- layout must match mem.c
	ctdef:put("struct { uint32_t num; uint32_t cap; ")
	for _, col in ipairs(proto) do
		if col.name ~= cdata.ident(col.name) then
			error(string.format("dataframe column name is not a valid identifier: `%s'", col.name))
		end
		size[col.name] = sizeof(col.ctype)
		align[col.name] = alignof(col.ctype)
		ctdef:putf("$ *%s; ", col.name)
		table.insert(ctarg, col.ctype)
	end
	ctdef:put("}")
	local df_addcols = df_addcolsfunc(proto)
	local df_copyrow = df_copyrowfunc(proto)
	local df_addrows = df_addrowsfunc()
	return ffi.metatype(typeof(tostring(ctdef), unpack(ctarg)), {
		__index = {
			["m3$size"]   = size,
			["m3$align"]  = align,
			["m3$cproto"] = df_cproto(proto),
			addcols       = df_addcols,
			alloc         = df_alloc,
			copyrow       = df_copyrow,
			addrows       = df_addrows,
			addrow        = df_addrow,
			extend        = df_extend,
			settab        = df_settab,
			clear         = df_clear,
			clearmask     = df_clearmask,
			mutate        = df_mutate,
			write         = df_write,
			overwrite     = df_overwrite,
			table         = df_table
		},
		__len             = df_len,
		__tostring        = df_tostring
	})
end

return {
	df_of = df_of
}
