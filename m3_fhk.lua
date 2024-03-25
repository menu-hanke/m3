local array = require "m3_array"
local data = require "m3_data"
local mem = require "m3_mem"
local prototype = require "m3_prototype"
local buffer = require "string.buffer"
local ffi = require "ffi"

local C = ffi.C

ffi.cdef [[
typedef struct fhk_Graph fhk_Graph;
typedef struct fhk_Program fhk_Program;
typedef struct fhk_State fhk_State;
typedef struct fhk_ResultInfo {
  int16_t ofs;
  int16_t ofs_len;
  uint8_t size;
  uint8_t fp;
  uint8_t sign;
} fhk_ResultInfo;
typedef void *(*fhk_Alloc)(void*, uintptr_t, uintptr_t);
struct fhk_Graph *fhk_newgraph(void);
int32_t fhk_define(struct fhk_Graph *G, const int8_t *input);
struct fhk_Program *fhk_compile(struct fhk_Graph *G);
int32_t fhk_result_info(const struct fhk_Program *P,
                        int32_t query,
                        int32_t result,
                        struct fhk_ResultInfo *info);
struct fhk_State *fhk_newstate(const struct fhk_Program *P, fhk_Alloc alloc, void *udata);
void fhk_query(const struct fhk_State *state, int32_t query);
void fhk_result(const struct fhk_State *state, int32_t idx, const void **data, uintptr_t *size);
int32_t fhk_typeof(const struct fhk_Graph *graph, int32_t query, int32_t idx);
void fhk_destroy(struct fhk_Program *P);
]]

-- before startup: {G=fhk_Graph, queries={...}}
-- after startup:  {P=fhk_Program}
local ctx

local function getctx()
	if not ctx then
		ctx = {
			G = C.fhk_newgraph(),
			queries = {}
		}
	end
	return ctx
end

local function define(src)
	print("fhk_define", src)
	if type(src) == "userdata" then
		-- null terminate string buffer
		-- TODO: fix this on the fhk side: just make the api take a pointer and a length.
		--       it's converted to a rust string anyway.
		src:put("\0")
	end
	if #src > 0 then
		return C.fhk_define(getctx().G, src)
	else
		return 0
	end
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
	local id = define(string.format("query(global) %s", src))
	local trampoline = load("local f = ... return function() return f() end")(uncompiled)
	ctx.queries[id] = trampoline
	return trampoline
end

-- [fp][sign][size]
local info2ctype = {
	[0] = {
		[0] = { [1] = "uint8_t", [2] = "uint16_t", [4] = "uint32_t", [8] = "uint64_t" },
		[1] = { [1] = "int8_t",  [2] = "int16_t",  [4] = "int32_t",  [8] = "int64_t" }
	},
	[1] = { [1] = { [4] = "float", [8] = "double" } },
}

local function makequery(P, state, newstate, query)
	local src = buffer.new()
	src:put([[
		local ffi = require "ffi"
		local C, cast = ffi.C, ffi.cast
		local u8p, u32p, voidpp = ffi.typeof("uint8_t *"), ffi.typeof("uint32_t *"), ffi.typeof("void **")
		local state, newstate
	]])
	local args = {state, newstate}
	local fields = {}
	local info = ffi.new("fhk_ResultInfo")
	for i=0, math.huge do
		local ok = C.fhk_result_info(P, query, i, info)
		if ok == 0 then break end
		local v = ffi.typeof(info2ctype[info.fp][info.sign][info.size])
		if info.ofs_len ~= 0 then
			-- TODO: implement a readonly vec in m3_array?
			v = array.vec(v)
		else
			v = ffi.typeof("$*", v)
		end
		table.insert(args, v)
		src:putf(", v%d", i+1)
		table.insert(fields, { ofs = info.ofs, ofs_len = info.ofs_len })
	end
	-- TODO: track changes and don't recreate the entire state here.
	--       this is why state is shared among all query functions.
	-- TODO: check that it was succesful, reraise errors
	-- TODO: udata binding
	src:putf([[
		= ...
		return function()
			state[0] = newstate()
			C.fhk_query(state[0], %d)
			local p = cast(u8p, state[0])
	]], query)
	for i,f in ipairs(fields) do
		if f.ofs_len ~= 0 then
			src:putf([[
				v%d.data = cast(voidpp, p+%d)[0]
				v%d.num = cast(u32p, p+%d)[0]
			]], i, f.ofs, i, f.ofs_len)
		end
	end
	src:put("return ")
	for i,f in ipairs(fields) do
		if i>1 then src:put(", ") end
		if f.ofs_len ~= 0 then
			src:putf("v%d", i)
		else
			src:putf("cast(v%d, p+%d)[0]", i, f.ofs)
		end
	end
	src:put("\nend")
	return assert(load(src))(unpack(args))
end

local function startup()
	if not ctx then return end
	map(data.data)
	local state = mem.new("fhk_State *", "vstack")
	state[0] = nil
	local P = ffi.gc(C.fhk_compile(ctx.G), C.fhk_destroy)
	local function newstate() return C.fhk_newstate(P, C.m3__mem_extalloc, mem.state.f)  end
	for id,trampoline in ipairs(ctx.queries) do
		debug.setupvalue(trampoline, 1, makequery(P, state, newstate, id))
	end
	ctx = {P=P}
end

return {
	define   = define,
	readfile = readfile,
	query    = query,
	startup  = startup
}
