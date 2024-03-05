local de = require "m3_debug"
local mem = require "m3_mem"
local patchptr = require "m3_patchptr"
local prototype = require "m3_prototype"
local state = require "m3_state"
local buffer = require "string.buffer"
local ffi = require "ffi"
require "table.new"

local allocp, reallocp, iswritable = mem.allocp, mem.reallocp, mem.iswritable
local memstate, zeros = mem.state, mem.zeros
local type = type
local C, alignof, cast, copy, sizeof, typeof = ffi.C, ffi.alignof, ffi.cast, ffi.copy, ffi.sizeof, ffi.typeof
local u32p = typeof("uint32_t *")
local band = bit.band
local max = math.max

local function buildskiplist(idx, p, num)
	local idxp = cast(u32p, band(p, -4))
	local skn
	if type(idx) == "number" then
		skn = 1
		idxp[-1] = idx
	else
		skn = 0
		while true do
			local j = skn+1
			local k = idx[j]
			if not k then break end
			idxp[-j] = k
			skn = j
		end
	end
	return (C.m3__mem_skiplist_build(idxp-skn, skn, num))
end

-- note: maybe should generate this for mixed types?
local function cdata2tab(data, num)
	local tab = table.new(num, 0)
	for i=0, num-1 do
		tab[i+1] = data[i]
	end
	return tab
end

local function assertrealloc(self, p)
	if p == nil then
		self.cap = nil
		error("failed to allocate memory")
	end
	return p
end

function assertptr(p, msg)
	-- `assert` will not work here since cdata NULL is truthy
	if p == nil then
		error(msg or "failed to allocate memory")
	end
	return p
end


---- vectors -------------------------------------------------------------------

local function vec_append(vec, x)
	local idx = vec.num
	vec.num = idx+1
	if idx >= vec.cap then
		local cap = max(2*vec.cap, 8)
		vec.cap = cap
		local size = vec["m3$size"]
		vec.data = assertrealloc(vec,
			reallocp(vec.data, size*idx, size*cap, vec["m3$align"], "frame"))
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
		vec.data = assertrealloc(vec,
			reallocp(vec.data, size*idx, size*cap, vec["m3$align"], "frame"))
	end
	return idx
end

local function vec_mutate(vec, realloc)
	if realloc or not iswritable(vec.data) then
		local size = vec["m3$size"]
		vec.data = assertptr(reallocp(vec.data, size*vec.num, size*vec.cap, vec["m3$align"], "frame"))
	end
end

local function vec_write(vec, realloc)
	if realloc or not iswritable(vec.data) then
		local size = vec["m3$size"]
		vec.data = allocp(size*vec.cap, vec["m3align"], "frame")
	end
end

local function vec_extend(vec, xs)
	local n = #xs
	local idx = vec_alloc(vec, n)
	if type(xs) == "table" then
		for i=1, n do
			vec.data[idx] = xs[i]
			idx = idx+1
		end
	else -- xs is vec
		if typeof(vec) == typeof(xs) then
			copy(vec.data+idx, xs.data, n*vec["m3$size"])
		else
			for i=0, n-1 do
				vec.data[idx+i] = xs.data[i]
			end
		end
	end
end

local function vecct_new(ct, xs)
	local vec = mem.new(ct, "vstack")
	vec.data = zeros
	vec.num = 0
	vec.cap = 0
	if xs then
		vec_extend(vec, xs)
	end
	return vec
end

local function vecct_zeros(ct, n)
	local vec = mem.new(ct, "vstack")
	vec.data = zeros
	vec.num = n
	vec.cap = n
	return vec
end

local function vec_clear(vec)
	vec.num = 0
	if not iswritable(vec.data) then
		vec.cap = 0
	end
end

local function vec_delete(vec, idx)
	local txp = memstate.x.cursor
	local skp = buildskiplist(idx, txp, vec.num)
	vec.num = skp[0]
	vec.data = assertptr(C.m3__mem_skiplist_realloc(memstate, vec.data, vec["m3$size"], vec.cap,
		vec["m3$align"], skp))
	memstate.x.cursor = txp
end

local function vec_table(vec)
	return cdata2tab(vec.data, vec.num)
end

local function vec_len(vec)
	return vec.num
end

local function vec_tostring(vec)
	local buf = buffer.new()
	buf:put("[")
	-- TODO: use cdata pretty print here
	for i=0, vec.num-1 do
		buf:put(" ", tostring(vec.data[i]))
	end
	buf:put(" ]")
	return tostring(buf)
end

local function vec_newct(ctype)
	return ffi.metatype(typeof([[
		struct {
			$ *data;
			uint32_t num;
			uint32_t cap;
		}
	]], ctype), {
		__index = {
			["m3$type"]  = "vec",
			["m3$size"]  = sizeof(ctype),
			["m3$align"] = alignof(ctype),
			new          = vecct_new,
			zeros        = vecct_zeros,
			append       = vec_append,
			alloc        = vec_alloc,
			mutate       = vec_mutate,
			write        = vec_write,
			extend       = vec_extend,
			clear        = vec_clear,
			delete       = vec_delete,
			table        = vec_table
		},
		__len            = vec_len,
		__tostring       = vec_tostring
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

local function vec_new(ctype, init)
	return vec_of(ctype):new(init)
end

---- data frames ---------------------------------------------------------------

local df_needpatch = setmetatable({}, {__mode="k"})

local function df_allocfunc(cols)
	local buf = buffer.new()
	buf:put([[
		local C, assertrealloc, reallocp, max = ...
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
			"df.%s = assertrealloc(df, reallocp(df.%s, %d*idx, %d*cap, %d, 'frame'))\n",
			col.name, col.name, sizeof(col.ctype), sizeof(col.ctype), alignof(col.ctype)
		)
	end
	buf:put([[
			end
			return idx
		end
	]])
	return assert(load(buf))(C, assertrealloc, reallocp, max)
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
					df.%s[idx] = 0
				else
					df.%s[idx] = v
				end
			end
		]], col.name, col.name, col.name)
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

local function df_clear(df)
	df.num = 0
	df.cap = 0
end

local function df_setrows(df, rows)
	df_clear(df)
	df:addrows(rows)
end

local function df_deletefunc(cols)
	local buf = buffer.new()
	buf:put([[
		local C, assertptr, memstate, buildskiplist = ...
		return function(df, rows)
			local txp = memstate.x.cursor
			local skp = buildskiplist(rows, txp, df.num)
			df.num = skp[0]
	]])
	for _,col in ipairs(cols) do
		buf:putf(
			"df.%s = assertptr(C.m3__mem_skiplist_realloc(memstate, df.%s, %d, df.cap, %d, skp))\n",
			col.name, col.name, sizeof(col.ctype), alignof(col.ctype)
		)
	end
	buf:put("memstate.x.cursor = txp\n")
	buf:put("end")
	return assert(load(buf))(C, assertptr, memstate, buildskiplist)
end

local function df_mutate(df, col, realloc)
	if realloc or not iswritable(df[col]) then
		local size = df["m3$size"][col]
		local align = df["m3$align"][col]
		df[col] = assertptr(reallocp(df[col], size*df.num, size*df.cap, align, "frame"))
	end
end

local function df_write(df, col, realloc)
	if realloc or not iswritable(df[col]) then
		local size = df["m3$size"][col]
		local align = df["m3$align"][col]
		df[col] = allocp(size*df.cap, align, "frame")
	end
end

local function df_emptyfunc(cols)
	local buf = buffer.new()
	buf:put([[
		local new, zeros = ...
		return function(ct, n)
			n = n or 0
			local df = new(ct, "vstack")
			df.num = n
			df.cap = n
	]])
	for _,col in ipairs(cols) do
		buf:putf("df.%s = zeros\n", col.name)
	end
	buf:put([[
		return df
		end
	]])
	return assert(load(buf))(mem.new, zeros)
end

local function copycolfunc_new(ctype)
	-- note: could use ffi.copy if the ctype matches, like in vec_extend.
	-- for now this function assumes x is either a table or a vec.
	-- arbitrary cdata wouldn't work anyway because fromcols() requires something with a length.
	return assert(load(string.format([[
		local allocp, cast, ctypep = ...
		local type = type
		return function(x, num)
			local col = cast(ctypep, allocp(num*%d, %d, "frame"))
			if type(x) == "table" then
				for i=0, num-1 do col[i] = x[i+1] end
			else
				for i=0, num-1 do col[i] = x.data[i] end
			end
			return col
		end
	]], sizeof(ctype), alignof(ctype))))(allocp, cast, ffi.typeof("$*", ctype))
end

local copycolfuncs = {}
local function copycolfunc(ctype)
	local ctid = tonumber(ctype)
	if not copycolfuncs[ctid] then
		copycolfuncs[ctid] = copycolfunc_new(ctype)
	end
	return copycolfuncs[ctid]
end

local function df_setcolsfunc(cols)
	if #cols == 0 then return function() end end
	local buf = buffer.new()
	buf:put("local zeros ")
	local copyct, copyf = {}, {}
	for _,c in ipairs(cols) do
		local ctid = tonumber(c.ctype)
		if not copyct[ctid] then
			buf:putf(", copy%d", ctid)
			copyct[ctid] = true
			table.insert(copyf, copycolfunc(c.ctype))
		end
	end
	buf:put([[
		= ...
		return function(df, cols)
			local ref =
	]])
	for i,col in ipairs(cols) do
		if i > 1 then buf:put(" or ") end
		buf:putf("cols.%s", col.name)
	end
	buf:putf([[
			if ref == nil then df:clear() return end
			local num = #ref
			df.num = num
			df.cap = num
	]], cols[1].name)
	for _,col in ipairs(cols) do
		buf:putf([[
			do
				local v = cols.%s
				if v == nil then
					df.%s = zeros
				else
					df.%s = copy%d(v, num)
				end
			end
		]], col.name, col.name, col.name, tonumber(col.ctype))
	end
	buf:put([[
		end
	]])
	return assert(load(buf))(zeros, unpack(copyf))
end

local function df_settab(df, tab)
	if #tab > 0 then
		df_setrows(df, tab)
	else
		df:setcols(tab)
	end
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

local function df_newct(proto)
	local c = {}
	local size, align = {}, {}
	local ctdef = buffer.new()
	local ctarg = {}
	ctdef:put("struct { uint32_t num; uint32_t cap; ")
	for name, col in pairs(proto) do
		local ctype = col.ctype
		table.insert(c, {name=name, ctype=ctype})
		size[name] = sizeof(ctype)
		align[name] = alignof(ctype)
		ctdef:putf("$ *%s; ", name)
		table.insert(ctarg, ctype)
	end
	ctdef:put("}")
	local dfct_empty = df_emptyfunc(c)
	local df_setcols = df_setcolsfunc(c)
	local df_alloc = df_allocfunc(c)
	local df_delete = df_deletefunc(c)
	local df_copyrow = df_copyrowfunc(c)
	local df_addrows = df_addrowsfunc()
	return ffi.metatype(ffi.typeof(tostring(ctdef), unpack(ctarg)), {
		__index = {
			["m3$type"]   = "dataframe",
			["m3$size"]   = size,
			["m3$align"]  = align,
			["m3$proto"]  = proto,
			["m3$settab"] = df_settab,
			["m3$pretty"] = df_pretty,
			empty         = dfct_empty,
			setcols       = df_setcols,
			alloc         = df_alloc,
			delete        = df_delete,
			copyrow       = df_copyrow,
			addrows       = df_addrows,
			addrow        = df_addrow,
			setrows       = df_setrows,
			clear         = df_clear,
			mutate        = df_mutate,
			write         = df_write,
			table         = df_table
		},
		__len             = df_len,
		__tostring        = df_tostring
	})
end

local df_ctcache = setmetatable({}, {
	__index = function(self, proto)
		--prototype.lock(proto) --TODO?
		self[proto] = df_newct(proto)
		return self[proto]
	end
})

local function df_protoct(proto)
	return df_ctcache[proto]
end

local function df_new(proto)
	proto = prototype.toproto(proto)
	if state.ready then
		return df_protoct(proto):empty()
	else
		local ptr = patchptr.new()
		prototype.setpatchptr(ptr, proto)
		df_needpatch[ptr] = proto
		return ptr
	end
end

local function startup()
	for ptr, proto in pairs(df_needpatch) do
		local df = df_protoct(proto):empty()
		patchptr.patch(ptr, typeof(df), df)
	end
	df_needpatch = nil
end

--------------------------------------------------------------------------------

return {
	vec       = vec_new,
	vec_ctype = vec_of,
	dataframe = df_new,
	startup   = startup
}
