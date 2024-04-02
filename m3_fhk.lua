local data = require "m3_data"
local mem = require "m3_mem"
local prototype = require "m3_prototype"
local buffer = require "string.buffer"
local ffi = require "ffi"
local fhk = require "fhk"

-- before startup: {G=fhk_Graph, queries={...}}
-- after startup:  {P=fhk_Program}
local ctx

local function getctx()
	if not ctx then
		ctx = {
			G = fhk.newgraph(),
			queries = {}
		}
	end
	return ctx
end

local function define(src)
	return getctx().G:define(src)
end

local function readfile(src)
	local fp = assert(io.open(src))
	local def = fp:read("*a")
	fp:close()
	define(def)
end

local ctype2fhk = {}
for c,f in pairs {
	uint8_t = "u8",   int8_t = "i8",
	uint16_t = "u16", int16_t = "i16",
	uint32_t = "u32", int32_t = "i32",
	uint64_t = "u64", int64_t = "i64",
	float    = "f32", double  = "f64"
} do ctype2fhk[tonumber(ffi.typeof(c))] = f end

local function ptrarith(p)
	return p+0
end

local function toref(p)
	if pcall(ptrarith, p) then
		-- it's a pointer
		return p[0]
	else
		-- it's a reference
		return p
	end
end

-- TODO: allow also proto + instance is udata
-- TODO: when parametrized commands are implemented use them instead here
-- TODO: use const instead of model when it's implemented properly
local function map_struct(buf, group, struct)
	local proto = prototype.get(struct)
	if not proto then
		-- TODO: reflect
		error("TODO")
	end
	local base = ffi.cast("uintptr_t", ffi.cast("void *", struct))
	struct = toref(struct)
	for f, p in pairs(proto) do
		local ftype = ctype2fhk[tonumber(p.ctype)]
		-- TODO: map nested structs etc. needs reflect.
		if ftype then
			buf:putf("const(`%s`) `%s` -> lds.%s(0x%x)\n", group, f, ftype,
				base + ffi.offsetof(struct, f))
		end
	end
end

-- TODO: same as above
local function map_dataframe(buf, group, df)
	local proto = assert(prototype.get(df), "object is not a dataframe")
	df = toref(df)
	local base = ffi.cast("intptr_t", ffi.cast("void *", df))
	for f,p in pairs(proto) do
		local ftype = ctype2fhk[tonumber(p.ctype)]
		if ftype then
			buf:putf(
				"const(global) `%s`#`%s` -> ldv.%s(0x%x, lds.u32(0x%x))\n",
				group, f,
				ftype,
				base + ffi.offsetof(df, f),
				base + ffi.offsetof(df, "num")
			)
		end
	end
end

local function map_vars(buf, group, what, obj)
	if what == "struct" then
		map_struct(buf, group, obj)
	elseif what == "dataframe" then
		map_dataframe(buf, group, obj)
	else
		error("TODO")
	end
end

local function map_space(buf, group, what, obj)
	buf:putf("const(global) `%s` -> ", group)
	if what == "struct" then
		buf:put("1")
	elseif what == "dataframe" then
		obj = toref(obj)
		buf:putf(
			"{..lds.u32(0x%x)}",
			ffi.cast("intptr_t", ffi.cast("void *", obj)) + ffi.offsetof(obj, "num")
		)
	else
		assert(false)
	end
	buf:put("\n")
end

local function map_group(buf, group, data)
	local space = group == "global"
	for o in pairs(data) do
		local ok, what = pcall(function() return o["m3$type"] end)
		if ok and what then
			if not space then
				-- TODO: if there's multiple objects make sure the space maps agree.
				-- probably the cleanest way to do this is to define one var per object,
				-- then one var which asserts that they are all equal (add an fhk intrinsic for this)
				map_space(buf, group, what, o)
				space = what
			end
			map_vars(buf, group, what, o)
		else
			-- TODO: this is possible to implement but requires stack swapping
			-- (it doesn't necessarily require changes to fhk but it's better to implement
			--  on the fhk side since the lua driver already has a stack swapping
			--  implementation anyway)
			error("TODO non-cdata")
		end
	end
	return space
end

local function map(data)
	local buf = buffer.new()
	local spaces = {}
	for group, vars in pairs(data) do
		local space = map_group(buf, group, vars)
		if group ~= "global" then
			table.insert(spaces, {group=group, space=space})
		end
	end
	-- TODO: relations between data structures should go in m3_data,
	-- this can be used for reading/writing as well.
	--
	-- this just implements a simple special case: structs act similarly to globals,
	-- ie. maps to struct groups are {0} and maps from struct groups are space
	for i=1, #spaces do
		for j=i+1, #spaces do
			local a = spaces[i]
			local b = spaces[j]
			if a.space ~= "struct" then a,b = b,a end
			if a.space == "struct" then
				buf:putf("const(`%s`) ~{`%s`} -> global#`%s`\n", a.group, b.group, b.group)
				buf:putf("const(`%s`) ~{`%s`} -> {0}\n", b.group, a.group)
				buf:putf("map `%s`#~{`%s`} `%s`#~{`%s`}\n", a.group, b.group, b.group, a.group)
			end
		end
	end
	define(buf)
end

local function uncompiled()
	error("attmpt to execute query on uncompiled graph")
end

local function query(src)
	-- TODO: group?
	local id = assert(define(string.format("query(global) %s", src)))
	local trampoline = load("local f = ... return function() return f() end")(uncompiled)
	ctx.queries[id] = trampoline
	return trampoline
end

local function makequery(P, state, id)
	local src = buffer.new()
	-- TODO: track changes and don't recreate the entire state here.
	--       this is why state is shared among all query functions.
	-- TODO: check that it was succesful, reraise errors
	-- TODO: udata binding
	src:putf([[
		local ffi = require "ffi"
		local C = ffi.C
		local P, state, fmem = ...
		return function()
			state[0] = P:newstate(C.m3__mem_extalloc, fmem)
			return state[0]:query(%d)
		end
	]], id)
	return assert(load(src))(P, state, mem.state.f)
end

local function startup()
	if not ctx then return end
	map(data.data)
	local P = ctx.G:compile()
	local state = mem.new(P.ctype_ptr, "vstack")
	state[0] = nil
	for id=0, math.huge do
		local trampoline = ctx.queries[id]
		if not trampoline then break end
		debug.setupvalue(trampoline, 1, makequery(P, state, id))
	end
	ctx = {P=P}
end

return {
	define   = define,
	readfile = readfile,
	query    = query,
	startup  = startup
}
