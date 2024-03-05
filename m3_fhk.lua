local array = require "m3_array"
local mem = require "m3_mem"
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
		local u8p, u32p, voidp = ffi.typeof("uint8_t *"), ffi.typeof("uint32_t *"), ffi.typeof("void *")
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
				v%d.data = cast(voidp, p+%d)
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
