local mem = require "m3_mem"
local ffi = require "ffi"
local buffer = require "string.buffer"
require "table.new"

local iswritable, scratch, stack, tmp = mem.iswritable, mem.scratch, mem.stack, mem.tmp
local tonumber, type = tonumber, type
local C, alignof, cast, ffi_copy, ffi_fill, sizeof, typeof = ffi.C, ffi.alignof, ffi.cast, ffi.copy, ffi.fill, ffi.sizeof, ffi.typeof
local band, bor, lshift, rshift = bit.band, bit.bor, bit.lshift, bit.rshift
local max, min = math.max, math.min

---- slices --------------------------------------------------------------------

local function slice_index(slice, index)
	return slice.data[index]
end

local function slice_newindex(slice, index, value)
	slice.data[index] = value
end

local function slice_len(slice)
	return slice.num
end

local function slice_inext(slice, index)
	index = index+1
	if index < slice.num then
		return index, slice.data[index]
	end
end

local function slice_ipairs(slice)
	return slice_inext, slice, -1
end

local function slice_tostring(slice)
	local buf = buffer.new()
	buf:put("[")
	-- TODO: use cdata pretty print here
	for i=0, slice.num-1 do
		buf:put(" ", tostring(slice.data[i]))
	end
	buf:put(" ]")
	return tostring(buf)
end

local slice_mt = {
	__index    = slice_index,
	__newindex = slice_newindex,
	__len      = slice_len,
	__ipairs   = slice_ipairs,
	__tostring = slice_tostring
}

local function slice_newct(ctype)
	return ffi.metatype(typeof([[
		struct {
			$ *data;
			uint32_t num;
		}
	]], ctype), slice_mt)
end

local slicetab = {}

local function slice_of(ctype)
	ctype = typeof(ctype)
	local ctid = tonumber(ctype)
	local ct = slicetab[ctid]
	if not ct then
		ct = slice_newct(ctype)
		slicetab[ctid] = ct
	end
	return ct
end

---- vector copies -------------------------------------------------------------

local isslice_cache = {}

local function isslice(x)
	local ctid = tonumber(typeof(x))
	local isc = isslice_cache[ctid]
	if isc == nil then
		isc = pcall(function() return x.data, x.num end)
		isslice_cache[ctid] = isc
	end
	return isc
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
	if type(dst) == "cdata" then
		if type(src) == "cdata" then
			local dstp, srcp = dst, src
			if isslice(dst) then dstp = dst.data end
			if isslice(src) then srcp = src.data end
			if typeof(dstp) == typeof(srcp) then
				ffi_copy(dstp, srcp, num*sizeofp(dstp))
			else
				return copy_fallback(dst, src, 0, 0, num)
			end
		else
			return copy_fallback(dst, src, 0, 1, num)
		end
	else
		if type(src) == "cdata" then
			return copy_fallback(dst, src, 1, 0, num)
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
	if srcnum > 0 then
		copy(dst, src, min(dstnum, srcnum))
	end
	if srcnum < dstnum and dummy then
		fill(dst, srcnum, dummy, dstnum-srcnum)
	end
end

---- vectors -------------------------------------------------------------------

local function vec_append(vec, x)
	local idx = vec.num
	vec.num = idx+1
	if idx >= vec.cap then
		local cap = max(2*vec.cap, 8)
		vec.cap = cap
		local size = vec["m3$size"]
		vec.data = stack:xrealloc(vec.data, size*idx, size*cap, vec["m3$align"])
	end
	vec.data[idx] = x
end

local function vec_alloc(vec, n)
	local idx, cap = vec.num, vec.cap
	local num = idx + (n or 1)
	vec.num = num
	if num > cap then
		cap = max(cap, 4)
		repeat cap = cap*2 until cap >= num
		vec.cap = cap
		local size = vec["m3$size"]
		vec.data = stack:xrealloc(vec.data, size*idx, size*cap, vec["m3$align"])
	end
	return idx
end

local function vec_mutate(vec, realloc)
	if realloc or not iswritable(vec.data) then
		local size = vec["m3$size"]
		vec.data = stack:xrealloc(vec.data, size*vec.num, size*vec.cap, vec["m3$align"])
	end
end

local function vec_write(vec, realloc)
	if realloc or not iswritable(vec.data) then
		local size = vec["m3$size"]
		vec.data = stack:xbump(size*vec.cap, vec["m3align"])
	end
end

local function vec_extend(vec, xs)
	local n = #xs
	local idx = vec_alloc(vec, n)
	copy(vec.data+idx, xs, n)
end

local function copylistset(idx)
	local ptr = cast("uint64_t *", scratch.cursor)
	ptr[0] = bor(ptr[0], lshift(1ull, band(idx, 0x3f)))
end

local function buildcopylist(idx, num)
	local size = 8*(rshift(num+63, 6))
	scratch:bump(size, 8)
	ffi_fill(cast("uint64_t *", scratch.cursor), size)
	if type(idx) == "table" then
		if #idx == 0 then return 0 end
		for i=1, #idx do
			copylistset(idx[i])
		end
	elseif type(idx) == "number" then
		copylistset(idx)
	else
		error("idx should be a table or number")
	end
	local top = scratch.cursor
	local nnum = C.m3__mem_buildcopylist(scratch, num)
	if nnum < 0 then scratch:oom() end
	-- NOTE: shift here adjusts it to CopySpan-sized units, see mem.c
	return nnum, rshift(top - scratch.cursor, 3)
end

local function vec_clear(vec, idx)
	if idx == nil then
		vec.num = 0
		if not iswritable(vec.data) then
			vec.cap = 0
		end
	else
		local size, align = vec["m3$size"], vec["m3$align"]
		tmp.i64 = scratch.cursor
		local num, nc = buildcopylist(idx, vec.num)
		vec.num = num
		if nc > 0 then
			if C.m3__mem_copy_list1(stack, scratch.cursor, nc, size, align, vec) < 0 then
				stack:oom()
			end
		end
		scratch.cursor = tmp.i64
	end
end

local function cdata2tab(data, num)
	local tab = table.new(num, 0)
	for i=0, num-1 do
		tab[i+1] = data[i]
	end
	return tab
end

local function vec_table(vec)
	return cdata2tab(vec.data, vec.num)
end

local function vec_newct(ctype)
	-- layout must match a dataframe with a single column
	return ffi.metatype(typeof([[
		struct {
			uint32_t num;
			uint32_t cap;
			$ *data;
		}
	]], ctype), {
		__index = {
			["m3$size"]  = sizeof(ctype),
			["m3$align"] = alignof(ctype),
			append       = vec_append,
			alloc        = vec_alloc,
			mutate       = vec_mutate,
			write        = vec_write,
			extend       = vec_extend,
			clear        = vec_clear,
			table        = vec_table
		},
		__len            = slice_len,
		__ipairs         = slice_ipairs,
		__tostring       = slice_tostring
	})
end

local vectab = {}

local function vec_of(ctype)
	ctype = ffi.typeof(ctype)
	local ctid = tonumber(ctype)
	if not vectab[ctid] then
		vectab[ctid] = vec_newct(ctype)
	end
	return vectab[ctid]
end

---- data frames ---------------------------------------------------------------

local function df_allocfunc(cols)
	if #cols == 0 then return function() end end
	local buf = buffer.new()
	buf:put([[
		local max = math.max
		local stack = ...
		return function(df, n)
			local idx, cap = df.num, df.cap
			local num = idx + (n or 1)
			df.num = num
			if num > cap then
				cap = max(cap, 4)
				repeat cap = cap*2 until cap >= num
				df.cap = cap
	]])
	for _,col in ipairs(cols) do
		buf:putf(
			"df.%s = stack:xrealloc(df.%s, %d*idx, %d*cap, %d)\n",
			col.name, col.name, sizeof(col.ctype), sizeof(col.ctype), alignof(col.ctype)
		)
	end
	buf:put([[
			end
			return idx
		end
	]])
	return assert(load(buf))(stack)
end

local function embedconst(k)
	if k == k then
		return tostring(k)
	else
		return "0/0"
	end
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
		]], col.name, col.name, embedconst(col.dummy or 0), col.name)
	end
	buf:put("end")
	return assert(load(buf))()
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

local function df_clear(df, idx)
	if idx == nil then
		df.num = 0
		df.cap = 0
	else
		tmp.i64 = scratch.cursor
		local proto = df["m3$cproto"]
		local num, nc = buildcopylist(idx, df.num)
		df.num = num
		if nc > 0 then
			if C.m3__mem_copy_list(stack, scratch.cursor, nc, proto, df) < 0 then
				stack:oom()
			end
		end
		scratch.cursor = tmp.i64
	end
end

local function df_mutate(df, col, realloc)
	if realloc or not iswritable(df[col]) then
		local size = df["m3$size"][col]
		local align = df["m3$align"][col]
		df[col] = stack:xrealloc(df[col], size*df.num, size*df.cap, align)
	end
end

local function df_write(df, col, realloc)
	if realloc or not iswritable(df[col]) then
		local size = df["m3$size"][col]
		local align = df["m3$align"][col]
		df[col] = stack:xbump(size*df.cap, align)
	end
end

local function df_overwrite(df, col, src)
	df_write(df, col)
	copy(df[col], src)
end

local function df_addcolsfunc(cols)
	if #cols == 0 then return function() end end
	local buf = buffer.new()
	buf:put([[
		local max = math.max
		local copyext = ...
		return function(df, cols, num)
			num = num or max(
	]])
	for i,c in ipairs(cols) do
		if i>1 then buf:put(",") end
		buf:putf("cols.%s and #cols.%s or 0", c.name, c.name)
	end
	buf:put(")\n")
	buf:put([[
		if num == 0 then return end
		local base = df:alloc(num)
	]])
	for _,col in ipairs(cols) do
		buf:putf(
			"copyext(df.%s+base, num, cols.%s, cols.%s and #cols.%s or 0, %s)",
			col.name, col.name, col.name, col.name, embedconst(col.dummy)
		)
	end
	buf:put("end")
	return assert(load(buf))(copyext)
end

local function df_extend(df, tab, num)
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

local function df_pretty(df, ...)
	de.putpp(df_table(df), ...)
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
	table.sort(proto, function(a, b) return ffi.alignof(a.ctype) > ffi.alignof(b.ctype) end)
	local size, align = {}, {}
	local ctdef = buffer.new()
	local ctarg = {}
	-- layout must match mem.c
	ctdef:put("struct { uint32_t num; uint32_t cap; ")
	for _, col in ipairs(proto) do
		size[col.name] = sizeof(col.ctype)
		align[col.name] = alignof(col.ctype)
		ctdef:putf("$ *%s; ", col.name)
		table.insert(ctarg, col.ctype)
	end
	ctdef:put("}")
	local df_addcols = df_addcolsfunc(proto)
	local df_alloc = df_allocfunc(proto)
	local df_copyrow = df_copyrowfunc(proto)
	local df_addrows = df_addrowsfunc()
	return ffi.metatype(typeof(tostring(ctdef), unpack(ctarg)), {
		__index = {
			["m3$size"]   = size,
			["m3$align"]  = align,
			["m3$cproto"] = df_cproto(proto),
			["m3$pretty"] = df_pretty,
			addcols       = df_addcols,
			alloc         = df_alloc,
			copyrow       = df_copyrow,
			addrows       = df_addrows,
			addrow        = df_addrow,
			extend        = df_extend,
			settab        = df_settab,
			clear         = df_clear,
			mutate        = df_mutate,
			write         = df_write,
			overwrite     = df_overwrite,
			table         = df_table
		},
		__len             = df_len,
		__tostring        = df_tostring
	})
end

--------------------------------------------------------------------------------

return {
	slice_of = slice_of,
	vec_of   = vec_of,
	df_of    = df_of,
	copy     = copy
}
