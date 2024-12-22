local array = require "m3_array"
local cdef = require "m3_cdef"
local de = require "m3_debug"
local environment = require "m3_environment"
local mem = require "m3_mem"
local fhk = require "fhk"
local ffi = require "ffi"
local buffer = require "string.buffer"
require "table.clear"
local debugevent = de.event

local G = fhk.newgraph()

local D = {
	data     = nil, -- assigned below (struct)
	G_state  = nil, -- assigned below (memslot)
	trampoline = {
		read  = {}, -- [dataobj] -> trampoline
		write = {}, -- [dataobj] -> trampoline
	},
	slots    = {}, -- [slot]
	apply    = {}, -- [name] -> {trampoline,r,w}
}

---- Pretty printing -----------------------------------------------------------

local function gettag(x)
	local mt = getmetatable(x)
	return mt and mt["m3$tag"]
end

local function pretty(o, buf, _, fmt)
	local tag = gettag(o)
	local putcolor = fmt.putcolor
	putcolor(
		buf,
		string.format(
			"<%s[%s%s]> ",
			tag,
			o.read and "r" or "-",
			o.write and "w" or "-"
		),
		"special"
	)
	if tag == "memslot" then
		buf:put(tostring(o.ctype))
		if o.ptr then
			buf:putf(" [0x%x]", ffi.cast("intptr_t", o.ptr))
		end
		if o.block then
			buf:putf(" (block %d)", o.block)
		end
	elseif tag == "column" then
		buf:put(tostring(o.ctype))
		if o.df.slot.ptr then
			buf:putf(" [0x%x]", ffi.cast("intptr_t", o.df.slot.ptr)
				+ ffi.offsetof(o.df.slot.ctype, o.name))
		end
	elseif tag == "struct" then
		return o.fields
	elseif tag == "dataframe" then
		if o.slot.block then
			buf:putf("(block %d) ", o.slot.block)
		end
		return o.columns
	end
end

---- Data objects --------------------------------------------------------------

local function newmeta(tag)
	return { ["m3$tag"] = tag, ["m3$pretty"] = pretty }
end

-- static memory slot
local memslot_mt = newmeta "memslot"
local function memslot(ctype, init)
	local o = setmetatable({
		ctype = ctype,
		init  = init
	}, memslot_mt)
	table.insert(D.slots, o)
	return o
end

-- collection of named objects
local struct_mt = newmeta "struct"
local function struct()
	return setmetatable({ fields = {}  }, struct_mt)
end
D.data = struct()

-- dataframe column
local column_mt = newmeta "column"
local function column(df, name, ctype)
	return setmetatable({ df=df, name=name, ctype=ctype }, column_mt)
end

-- dataframe
local dataframe_mt = newmeta "dataframe"
local function dataframe()
	local df = setmetatable({
		slot    = memslot(),
		columns = {}
	}, dataframe_mt)
	df.slot.dataframe = df
	return df
end

-- size of an object
local size_mt = newmeta "size"
local function size(o)
	return setmetatable({ obj=o }, size_mt)
end

-- varargs support for read and write
local splat_mt = newmeta "splat"
local function splat(values)
	return setmetatable({ values = values }, splat_mt)
end

-- constant value
local literal_mt = newmeta "literal"
local function literal(value)
	return setmetatable({ value = value }, literal_mt)
end

-- fhk expression
local expr_mt = newmeta "expr"
local function expr(e)
	return setmetatable({ e = G:expr("global", e) }, expr_mt)
end

-- pipe
local pipe_mt = newmeta "pipe"
local function pipe()
	return setmetatable({ sink={} }, pipe_mt)
end

-- user
local dynamic_mt = newmeta "dynamic"
local function dynamic(obj)
	return setmetatable(obj or {}, dynamic_mt)
end

local function func(f)
	return dynamic {
		writer = function() return f end,
		reader = function() return f end
	}
end

---- Dataobj functions ---------------------------------------------------------

local function visit(o, f)
	local tag = gettag(o)
	if tag == "struct" then
		for k,v in pairs(o.fields) do
			f(k, v)
		end
	elseif tag == "dataframe" then
		f(nil, o.slot)
		for k,v in pairs(o.columns) do
			f(k, v)
		end
	elseif tag == "size" then
		f(nil, o.obj)
	elseif tag == "splat" then
		for _,v in ipairs(o.values) do
			f(nil, v)
		end
	elseif tag == "pipe" then
		f(nil, o.sink)
	elseif tag == "dynamic" and o.visit then
		return o:visit(f)
	end
end

local function visitrec(o, f)
	local function g(s,x) f(s,x) return visit(x,g) end
	return g(nil,o)
end

local function getsubfield(o, name)
	local tag = gettag(o)
	if tag == "struct" then
		return o.fields[name]
	elseif tag == "dataframe" then
		return o.columns[name]
	end
end

local function newsubfield(o, name)
	local tag = gettag(o)
	if tag == "struct" then
		local slot = memslot()
		o.fields[name] = slot
		return slot
	elseif tag == "dataframe" then
		local col = column(o, name)
		o.columns[name] = col
		return col
	else
		error(string.format("`%s' object doesn't support sub-fields", tag))
	end
end

local function isvarlen(o)
	return gettag(o) == "dataframe"
end

---- Dataflow ------------------------------------------------------------------

local function setmark(df, o, m)
	if not o[m] then
		df.fixpoint = false
		o[m] = true
	end
	local tag = gettag(o)
	if tag == "dataframe" then
		setmark(df, o.slot, m)
	elseif tag == "column" then
		setmark(df, o.df, m)
	end
end

local gvisit

local function visitalive(df, tab, o)
	local flag = G:var(tab, "m3'alive", false)
	if flag then
		if o.g_alive then
			error("TODO: this should maybe be supported? (multiple alive flags caused by aliases)")
		end
		o.g_alive = flag
		gvisit(df, flag)
	end
end

local function apexpr(ap, e)
	if not ap.query then
		ap.query = G:newquery("global")
	end
	return ap.query:add(e)
end

local function visitapsub(df, tab, col, o)
	for name,ap in pairs(D.apply) do
		local var = G:var(tab, "m3'[$:$]", false, col, name)
		-- might make sense to check that `var` is dependent here?
		if var then
			setmark(df, o, "write")
			ap.sub[o] = apexpr(ap, G:expr("global", "$.$", tab, var.name))
			gvisit(df, var)
		end
	end
end

local function visitapcat(df, tab, col, o)
	for name,ap in pairs(D.apply) do
		-- TODO: non global tables too
		local var = G:var(nil, "global.m3'[$:$:$]", false, col, name, tab)
		if var then
			setmark(df, o, "write")
			ap.cat[o] = apexpr(ap, G:expr("global", var.name))
			gvisit(df, var)
		end
	end
end

-- * for each table definition of the form:
--     table A[n]
--   add `A` to `n.m3_tablen`
-- * for each model equation:
--     x = ...
--   add the model to `x.m3_models`
local function updategraph(df)
	while true do
		local o = df.last.next
		if not o then break end
		if o.op == "TAB" then
			if #o.shape.fields == 1 then
				local expr = o.shape.fields[1]
				if expr.op == "VGET" and #expr.idx == 0 then
					if not expr.var.m3_tablen then
						expr.var.m3_tablen = {}
					end
					table.insert(expr.var.m3_tablen, o)
				end
			end
		elseif o.op == "MOD" then
			for _,vset in ipairs(o.value) do
				if not vset.var.m3_models then
					vset.var.m3_models = {}
				end
				table.insert(vset.var.m3_models, o)
			end
		end
		df.last = o
	end
end

gvisit = function(df, g)
	if g.m3_visited then return end
	g.m3_visited = true
	for _,gg in ipairs(fhk.refs(g)) do gvisit(df, gg) end
	if g.op == "VAR" then
		updategraph(df)
		if g.m3_models then
			-- dependent variable: visit definitions
			for _,model in ipairs(g.m3_models) do
				gvisit(df, model)
			end
		elseif g.m3_tablen then
			-- independent variable that is the length of a table.
			-- note that because the table references the variable (but not vice versa),
			-- we necessarily visit here before we visit the table.
			-- TODO: consider whether it makes sense to allow the variable to be the length
			-- of multiple tables.
			assert(#g.m3_tablen == 1, "NYI (shared table length)")
			local tab = g.m3_tablen[1]
			local tabname = tostring(tab.name)
			local tabo = D.data.fields[tabname]
			if not tabo then
				tabo = dataframe()
				D.data.fields[tabname] = tabo
			end
			tab.m3_mapping = tabo
			g.m3_mapping = size(tabo)
			setmark(df, g.m3_mapping, "read")
		else
			-- independent variable: add mapping
			local name = tostring(g.name)
			local map = getsubfield(g.tab.m3_mapping, name) or newsubfield(g.tab.m3_mapping, name)
			g.m3_mapping = map
			setmark(df, map, "read")
		end
	elseif g.op == "TAB" then
		-- tabs with nonzero dimension are already visited when the length var is visited
		if #g.shape.fields == 0 then
			local name = tostring(g.name)
			local o = D.data.fields[name]
			if not o then
				o = struct()
				D.data.fields[name] = o
			end
			g.m3_mapping = o
			setmark(df, o, "read")
		end
	end
end

local function dataflow_iter(df)
	for r in pairs(D.trampoline.read) do
		visitrec(r, function(_,o)
			setmark(df, o, "read")
			if gettag(o) == "expr" then
				if not D.G_state then
					-- set this lazily so that the graph isn't compiled when it's not used.
					D.G_state = memslot "struct { void *instance; uint64_t mask; }"
					D.G_state.read = true
					D.G_state.write = true
				end
				if not r.query then
					r.query = G:newquery("global")
					r.query_field = {}
				end
				if not r.query_field[o.e] then
					gvisit(df, o.e)
					r.query_field[o.e] = r.query:add(o.e)
				end
			end
		end)
	end
	local setw = function(_,o) setmark(df, o, "write") end
	for w in pairs(D.trampoline.write) do
		visitrec(w, setw)
	end
	for name,obj in pairs(D.data.fields) do
		-- TODO: if obj looks like a table, visit obj.m3'alive
		if obj.read then
			if not df.ap_visited[name] then df.ap_visited[name] = {} end
			local visited = df.ap_visited[name]
			local varlen = isvarlen(obj)
			if varlen then
				if not visited["m3$alive"] then
					visited["m3$alive"] = true
					visitalive(df, name, obj)
				end
			end
			visit(obj, function(sub, o)
				if type(sub) == "string" and o.read and not visited[sub] then
					visited[sub] = true
					visitapsub(df, name, sub, o)
					if varlen then
						visitapcat(df, name, sub, o)
					end
				end
			end)
		end
	end
end

local function dataflow()
	local df = { ap_visited = {}, last=G.objs[0] }
	for _=1, 1000 do
		df.fixpoint = true
		dataflow_iter(df)
		if df.fixpoint then return end
	end
	error("dataflow did not converge")
end

---- Mask computation ----------------------------------------------------------

-- TODO: update the layouter to allow splitting a dataframe into so that each column has its own
-- slot but they are still contiguous in memory (ie. `region` field to memslot and require that
-- slots with the same region are contiguous, also add an `order` field so that len/cap goes first)
local function visitflatten(o, s)
	if gettag(o) == "column" then
		-- this case can be removed when the above TODO is implemented
		visitflatten(o.df.slot, s)
	end
	visitrec(o, function(_,x) s[x] = true end)
end

local function computemasks()
	-- flatten writes for each write and apply
	local wset = {}
	for w in pairs(D.trampoline.write) do
		wset[w] = {}
		visitflatten(w, wset[w])
	end
	for _,ap in pairs(D.apply) do
		wset[ap] = {}
		for w in pairs(ap.sub) do
			visitflatten(w, wset[ap])
		end
		for w in pairs(ap.cat) do
			-- TODO: group cats somehow more logically. this also modifies len and cap.
			error("TODO")
		end
	end
	-- compute fhk masks
	for g in G:objects() do
		if g.op == "VAR" and g.m3_mapping then
			for w,ws in pairs(wset) do
				if ws[g.m3_mapping] then
					if not w.reset then
						w.reset = G:newreset()
						wset[w][D.G_state] = true
					end
					w.reset:add(g)
				end
			end
		end
	end
	-- compute write mask
	for w,ws in pairs(wset) do
		for o in pairs(ws) do
			if gettag(o) == "memslot" then
				w.mmask = bit.bor(w.mmask or 0ull, bit.lshift(1ull, o.block))
			end
		end
	end
end

---- Graph compilation ---------------------------------------------------------

local ctype2fhk = {}
for c,f in pairs {
	uint8_t = "u8",   int8_t = "i8",
	uint16_t = "u16", int16_t = "i16",
	uint32_t = "u32", int32_t = "i32",
	uint64_t = "u64", int64_t = "i64",
	float    = "f32", double  = "f64"
} do ctype2fhk[tonumber(ffi.typeof(c))] = f end

local function fhktypename(ct)
	return ctype2fhk[tonumber(ct)]
end

local map_obj = {}

function map_obj.memslot(g)
	return string.format(
		"model %s %s = load'%s(0x%x)",
		g.tab.name,
		g.name,
		fhktypename(g.m3_mapping.ctype),
		ffi.cast("intptr_t", g.m3_mapping.ptr)
	)
end

function map_obj.column(g)
	local df = g.m3_mapping.df
	local dfbase = ffi.cast("intptr_t", df.slot.ptr)
	-- TODO use load'u32 for the length here when fhk supports it.
	return string.format(
		"model global %s.%s = load'%s(load'ptr(0x%x), load'i32(0x%x))",
		g.tab.name,
		g.name,
		fhktypename(g.m3_mapping.ctype),
		dfbase + ffi.offsetof(df.slot.ctype, g.m3_mapping.name),
		dfbase -- `num` is at offset zero
	)
end

function map_obj.size(g)
	local obj = g.m3_mapping.obj
	local tag = gettag(obj)
	if tag == "dataframe" then
		-- TODO: make fhk accept any integer type for table size
		return string.format(
			"model %s %s = load'i32(0x%x)",
			g.tab.name,
			g.name,
			ffi.cast("intptr_t", obj.slot.ptr)
		)
	else
		error(string.format("NYI (map size: %s)", tag))
	end
end

function map_obj.dynamic(g)
	local map = assert(g.m3_mapping.map, "dynamic object doesn't implement map")
	return map(g)
end

local function compilegraph()
	if not D.G_state then G = nil return end
	local buf = buffer.new()
	for g in G:objects() do
		if g.op == "VAR" and g.m3_mapping then
			local map = map_obj[gettag(g.m3_mapping)]
			if not map then
				error(string.format("`%s' obj cannot be used in graph", gettag(g.m3_mapping)))
			end
			buf:put(map(g), "\n")
		end
	end
	G:define(buf)
	-- compile!
	G = assert(G:compile("g"))
	-- pre-create instance so that instance creation can assume we always have a non-null instance
	-- available
	D.G_state.ptr.instance = G:newinstance(ffi.C.m3__mem_extalloc, mem.stack)
	D.G_state.ptr.mask = 0
end

---- Access --------------------------------------------------------------------

local function islit(x)
	return type(x) == "number" or type(x) == "string"
end

local todataobj

local function convdataobj(x)
	if type(x) == "string" then
		return expr(x)
	elseif type(x) == "function" then
		return func(x)
	elseif type(x) == "table" then
		local s = struct()
		for k,v in pairs(x) do
			if not islit(k) then
				k = todataobj(k)
			end
			s.fields[k] = todataobj(v)
		end
		return s
	else
		return literal(x)
	end
end

local dataobj_cache = {}
local function todataobj(x)
	if gettag(x) then
		return x
	end
	local o = dataobj_cache[x]
	if not o then
		o = convdataobj(x)
		dataobj_cache[x] = o
	end
	return o
end

local function todataobjs(...)
	if select("#", ...) == 1 then
		return todataobj(...)
	else
		local s = {...}
		for i,x in ipairs(s) do
			s[i] = todataobj(x)
		end
		return splat(s)
	end
end

local trampoline_template = {
	read  = "local target return function() return target() end",
	write = "local target return function(...) return target(...) end"
}

local function uncompiled()
	error("attempt to execute uncompiled statement")
end

local function access(what, ...)
	local o = todataobjs(...)
	local t = D.trampoline[what][o]
	if not t then
		t = load(trampoline_template[what])(uncompiled)
		D.trampoline[what][o] = t
	end
	return t
end

local function define(src)
	G:define(src)
end

-- TODO fhk lexer should support streaming
local function include(name)
	local fp = assert(io.open(name, "r"))
	define(fp:read("*a"))
	fp:close()
end

local function apply(name)
	local ap = D.apply[name]
	if not ap then
		local trampoline = load("local target return function() return target() end")(uncompiled)
		ap = {
			sub        = {},
			cat        = {},
			trampoline = trampoline
		}
		D.apply[name] = ap
	end
	return ap.trampoline
end

local shared = {}
if environment.mode == "mp" then
	local mp = require "m3_mp"
	local function shpipe(dispatch)
		local source = pipe()
		local sink = pipe()
		sink.sink = source.sink
		source.channel = dispatch:channel(access("write", sink))
		return source
	end
	shared.input = function() return shpipe(mp.work) end
	shared.output = function() return shpipe(mp.main) end
else
	shared.input = pipe
	shared.output = pipe
end

---- Access emit ---------------------------------------------------------------

local function upvalname(ctx, v)
	if type(v) == "nil" or type(v) == "number" or type(v) == "boolean" then
		return v
	elseif type(v) == "string" then
		return string.format("%q", v)
	else
		local name = string.format("u%p", v)
		ctx.uv[name] = v
		return name
	end
end

local function newname(ctx)
	ctx.nameid = ctx.nameid+1
	return string.format("v%d", ctx.nameid)
end

local function indexexpr(ctx, k)
	if type(k) == "string" and k:match("^[%a_][%w_]*$") then
		return string.format(".%s", k)
	else
		return string.format("[%s]", upvalname(ctx, k))
	end
end

-- Read --------------------------------

local emit_read = {}
local emitread

local function ctype_type(ct)
	return bit.rshift(ffi.typeinfo(ffi.typeof(ct)).info, 28)
end

local function ctype_isstruct(ct)
	return ctype_type(ct) == 1
end

function emit_read.memslot(ctx, slot)
	local ptr = upvalname(ctx, slot.ptr)
	if ctype_isstruct(slot.ctype) then
		return ptr
	else
		local name = newname(ctx)
		ctx.buf:putf("local %s = %s[0]\n", name, ptr)
		return name
	end
end

function emit_read.struct(ctx, struct)
	local result = newname(ctx)
	ctx.buf:putf("local %s = {}\n", result)
	for k,v in pairs(struct.fields) do
		ctx.buf:putf("%s%s = %s\n", result, indexexpr(ctx, k), emitread(ctx, v))
	end
	return result
end

function emit_read.column(ctx, col)
	return string.format("%s.%s", upvalname(ctx, col.df.slot.ptr), col.name)
end

function emit_read.dataframe(ctx, df)
	return upvalname(ctx, df.slot.ptr)
end

function emit_read.size(ctx, size)
	local tag = gettag(size.obj)
	if tag == "dataframe" then
		return string.format("%s.num", upvalname(ctx, df.slot.ptr))
	else
		error(string.format("NYI (size %s)", tag))
	end
end

function emit_read.splat(ctx, splat)
	local results = {}
	for i,v in ipairs(splat.values) do
		results[i] = emitread(ctx, v)
	end
	return table.concat(results, ", ")
end

function emit_read.literal(_, literal)
	return tostring(literal.value)
end

function emit_read.dynamic(ctx, dyn)
	local resname = newname(ctx)
	ctx.buf:putf("local %s = %s()\n", resname, upvalname(ctx, dyn:reader()))
	return resname
end

function emit_read.expr(ctx, expr)
	return string.format("Q.%s", ctx.query_field[expr.e])
end

emitread = function(ctx, x)
	return emit_read[gettag(x)](ctx, x)
end

-- Write -------------------------------

local emit_write = {}
local emitwrite

function emit_write.memslot(ctx, slot, value)
	local ptr = upvalname(ctx, slot.ptr)
	ctx.buf:putf("if %s ~= nil then %s[0] = %s end\n", value, ptr, value)
	if ctype_isstruct(slot.ctype) then
		return ptr
	end
end

function emit_write.struct(ctx, struct, value)
	local vname = newname(ctx)
	local result
	for k,v in pairs(struct.fields) do
		local pos = #ctx.buf
		ctx.buf:putf("do local %s = (%s) and (%s)%s\n", vname, value, value, indexexpr(ctx, k))
		local r = emitwrite(ctx, v, vname)
		if r then
			if not result then
				result = newname(ctx)
				local buf = buffer.new()
				buf:put(ctx.buf:get(pos))
				buf:putf("local %s = {}\n", result)
				buf:put(ctx.buf)
				ctx.buf = buf
			end
			ctx.buf:putf("%s%s = %s\n", result, indexexpr(ctx, k), r)
		end
		ctx.buf:put("end\n")
	end
	return result
end

function emit_write.column(ctx, col, value)
	ctx.buf:putf("%s:overwrite('%s', %s)\n", upvalname(ctx, col.df.slot.ptr), col.name, value)
end

function emit_write.dataframe(ctx, df, value)
	local result = newname(ctx)
	ctx.buf:putf("local %s = %s:settab(%s)\n", result, upvalname(ctx, df.slot.ptr), value)
	return result
end

function emit_write.splat(ctx, splat, value)
	local names = {}
	for i=1, #writes do
		names[i] = newname(ctx)
	end
	ctx.buf:putf("local %s = %s\n", table.concat(names, ", "), value)
	local results = {}
	local haveresults = false
	for i,v in ipairs(splat.values) do
		local r = emitwrite(ctx, v, names[i])
		results[i] = r or "nil"
		if r then haveresults = true end
	end
	if haveresults then
		return table.concat(results, ", ")
	end
end

function emit_write.dynamic(ctx, dyn, value)
	ctx.buf:putf("%s(%s)\n", upvalname(ctx, dyn:writer()), value)
end

function emit_write.pipe(ctx, pipe, value)
	if pipe.map_f then
		local v = newname(ctx)
		ctx.buf:putf("local %s = %s(%s)\n", v, upvalname(ctx, pipe.map_f), value)
		value = v
	end
	if pipe.filter_f then
		ctx.buf:putf("if %s(%s) then\n", upvalname(ctx, pipe.filter_f), value)
	end
	if pipe.channel then
		ctx.buf:putf("%s(%s)\n", upvalname(ctx, pipe.channel.send), value)
	else
		for _,sink in ipairs(pipe.sink) do
			emitwrite(ctx, sink, value)
		end
	end
	if pipe.filter_f then
		ctx.buf:put("end\n")
	end
end

emitwrite = function(ctx, x, v)
	return emit_write[gettag(x)](ctx, x, v)
end

----------------------------------------

local function newemit()
	return {
		uv     = {},
		nameid = 0,
		buf    = buffer.new(),
	}
end

local function emitupvalues(uv, buf)
	local vs = {}
	if next(uv) then
		local comma = "local "
		for k,v in pairs(uv) do
			table.insert(vs, v)
			buf:put(comma, k)
			comma = ","
		end
		buf:put(" = ...\n")
	end
	return vs
end

local function compileread(o, graph_instance)
	local ctx = newemit()
	if o.query then
		ctx.uv.query = o.query.query
		ctx.uv.graph_instance = graph_instance
		ctx.query_field = o.query_field
	end
	local value = emitread(ctx, o)
	local buf = buffer.new()
	local uv = emitupvalues(ctx.uv, buf)
	buf:put("return function()\n")
	if o.query then
		buf:put("local Q = query(graph_instance())\n")
	end
	buf:put(ctx.buf)
	buf:putf("return %s end\n", value)
	return load(buf)(unpack(uv))
end

local function emitmask(ctx, o)
	if o.mmask then
		ctx.uv.mem_setmask = mem.setmask
		ctx.buf:putf("mem_setmask(0x%xull)\n", o.mmask)
	end
	if o.reset then
		ctx.uv.G_state = D.G_state.ptr
		ctx.uv.bor = bit.bor
		ctx.buf:putf("G_state.mask = bor(G_state.mask, 0x%xull)\n", o.reset.mask)
	end
end

-- TODO: re-check alive variables depending on mask
-- TODO: this doesn't need varargs, just take as many args as the splat has, or one if it's
-- not a splat
local function compilewrite(o)
	local ctx = newemit()
	emitmask(ctx, o)
	local value = emitwrite(ctx, o, "...")
	local buf = buffer.new()
	local uv = emitupvalues(ctx.uv, buf)
	buf:put("return function(...)\n")
	buf:put(ctx.buf)
	if value then
		buf:putf("return %s\n", value)
	end
	buf:put("end\n")
	return load(buf)(unpack(uv))
end

local function nop() end

-- TODO: this is logically a write so everything above applies
local function compileapply(ap, debugapply, graph_instance)
	if not ap.query then return nop end
	local ctx = newemit()
	ctx.uv.graph_instance = graph_instance
	ctx.uv.query = ap.query.query
	-- query must happen before emitmask here
	ctx.buf:put("local Q = query(graph_instance())\n")
	emitmask(ctx, ap)
	if debugapply then
		ctx.uv.debugapply = debugapply
		ctx.buf:put("debugapply{sub={")
		for o,v in pairs(ap.sub) do
			ctx.buf:putf("[%s] = Q.%s, ", upvalname(ctx, o), v)
		end
		ctx.buf:put("}}\n")
	end
	for o,v in pairs(ap.sub) do
		emitwrite(ctx, o, string.format("Q.%s", v))
	end
	for o,v in pairs(ap.cat) do
		error("TODO: emitcat")
	end
	local buf = buffer.new()
	local uv = emitupvalues(ctx.uv, buf)
	buf:put("return function()\n")
	buf:put(ctx.buf)
	buf:put("end\n")
	return load(buf)(unpack(uv))
end

local function debug_read(event, ...)
	event(...)
	return ...
end

-- TODO: this should take a query mask parameter and only create a new instance if the intersection
-- is nonzero
local function graph_instancefunc()
	return load(string.format([[
		local state, G, C, stack, setmask, iswritable = ...
		return function()
			if not iswritable(state.instance) then
				setmask(0x%xull)
				goto new
			end
			if state.mask == 0 then
				return state.instance
			end
			::new::
			local instance = G:newinstance(C.m3__mem_extalloc, stack, state.instance, state.mask)
			state.instance = instance
			state.mask = 0
			return instance
		end
	]], bit.lshift(1ULL, D.G_state.block)))(D.G_state.ptr, G, ffi.C, mem.stack, mem.setmask,
		mem.iswritable)
end

local function compileaccess()
	local debugread, debugwrite = de.gettrace("read"), de.gettrace("write")
	local debugapply = de.gettrace("apply")
	local graph_instance
	if D.G_state then
		graph_instance = graph_instancefunc()
	end
	for r,t in pairs(D.trampoline.read) do
		r = compileread(r, graph_instance)
		if debugread then
			local rf = r
			r = function() return debug_read(debugread, rf()) end
		end
		debug.setupvalue(t, 1, r)
	end
	for w,t in pairs(D.trampoline.write) do
		w = compilewrite(w)
		if debugwrite then
			local wf = w
			w = function(...) debugwrite(...) return wf(...) end
		end
		debug.setupvalue(t, 1, w)
	end
	for _,ap in pairs(D.apply) do
		debug.setupvalue(ap.trampoline, 1, compileapply(ap, debugapply, graph_instance))
	end
end

---- Connect -------------------------------------------------------------------

local function connect(source, sink)
	source = todataobj(source)
	sink = todataobj(sink)
	local tag = gettag(source)
	if tag == "pipe" then
		table.insert(source.sink, sink)
	elseif tag == "dynamic" and source.connect then
		return source:connect(sink)
	else
		-- TODO: allow arbitrary data objects here, eg. if source is an fhk expression,
		-- then make a pipe that outputs a value whenever the expression changes
		error(string.format("TODO connect %s -> %s", gettag(source), gettag(sink)))
	end
	return sink
end

---- Pipe functions ------------------------------------------------------------

local function pipe_map(p, f)
	local new = pipe()
	new.map_f = f
	return connect(p, new)
end

local function pipe_filter(p, f)
	local new = pipe()
	new.filter_f = f
	return connect(p, new)
end

pipe_mt.__index = {
	map    = pipe_map,
	filter = pipe_filter
}

---- Memory layouting ----------------------------------------------------------

-- TODO: if heap consists of 1 block, ignore all setmask(...)s and always make a copy of the heap
-- on savepoint

local function slot_cmp(a, b)
	if a.region ~= b.region then
		return a.region < b.region
	else
		return ffi.alignof(a.ctype) > ffi.alignof(b.ctype)
	end
end

local function blockct(size, align)
	return ffi.typeof(string.format([[
		__attribute__((aligned(%d)))
		struct { uint8_t data[%d]; }
	]], align, size))
end

local function createheap(slots)
	local size, maxsize = 0, 0
	for _, slot in ipairs(slots) do
		if type(slot.region) == "function" then
			slot.region = slot:region()
		end
		slot.region = string.format("%p", slot.region)
		size = size + ffi.sizeof(slot.ctype)
		maxsize = math.max(maxsize, ffi.sizeof(slot.ctype))
	end
	table.sort(slots, slot_cmp)
	blocksize = bit.band(
		math.max(maxsize, math.ceil(size/cdef.M3_MEM_HEAPBMAX)) + cdef.M3_MEM_BSIZEMIN-1,
		bit.bnot(cdef.M3_MEM_BSIZEMIN-1)
	)
	::again::
	local block, ptr = 0, 0
	for _, slot in ipairs(slots) do
		local align = ffi.alignof(slot.ctype)
		local size = ffi.sizeof(slot.ctype)
		ptr = bit.band(ptr + align-1, bit.bnot(align-1))
		if ptr+size > blocksize then
			block, ptr = block+1, 0
			if block >= cdef.M3_MEM_HEAPBMAX then
				blocksize = blocksize*2
				goto again
			end
		end
		slot.block = block
		slot.ofs = ptr
		ptr = ptr+size
	end
	local block_ct = blockct(blocksize, cdef.M3_MEM_BSIZEMIN)
	-- use luajit allocator for the heap so that const heap references become
	-- relative addresses in machine code.
	local heap = ffi.new(ffi.typeof("$[?]", block_ct), block+1)
	mem.setheap(heap, blocksize, block+1)
	for _, slot in ipairs(slots) do
		slot.ptr = ffi.cast(
			ffi.typeof("$*", slot.ctype),
			ffi.cast("intptr_t", ffi.cast("void *", heap[slot.block])) + slot.ofs
		)
	end
end

local function malloc(size)
	return ffi.gc(ffi.C.malloc(size), ffi.C.free)
end

local function createdummy(slots)
	local size, align = 0, 1
	for _, slot in ipairs(slots) do
		size = math.max(ffi.sizeof(slot.ctype), size)
		align = math.max(ffi.alignof(slot.ctype), align)
	end
	if size == 0 then
		return
	end
	local region
	if align <= 16 then
		region = malloc(size)
	else
		region = ffi.new(blockct(size, align))
	end
	mem.anchor(region)
	for _, slot in ipairs(slots) do
		slot.ptr = ffi.cast(ffi.typeof("$*", slot.ctype), region)
	end
end

local function dataframe_ctype(df)
	local proto = {}
	for name,col in pairs(df.columns) do
		if col.read or col.write then
			col.ctype = ffi.typeof(col.ctype or "double")
			table.insert(proto, {name=name, ctype=col.ctype})
		end
	end
	return array.df_of(proto)
end

local function memlayout()
	local ro, wo, rw = {}, {}, {}
	for _,o in ipairs(D.slots) do
		if o.read or o.write then
			if not o.ctype then
				if o.dataframe then
					-- TODO: this should filter out columns that are not read or written
					o.ctype = dataframe_ctype(o.dataframe)
				elseif o.read and o.write then
					-- TODO: design a good mechanism for default types
					o.ctype = "double"
				else
					o.ctype = "uint8_t"
				end
			end
			o.ctype = ffi.typeof(o.ctype)
			local t
			if o.read and (o.write or ctype_isstruct(o.ctype)) then
				t = rw
			elseif o.read then
				t = ro
			elseif o.read then
				t = wo
			end
			if t then
				table.insert(t, o)
			end
		end
	end
	createheap(rw)
	createdummy(ro)
	createdummy(wo)
	for _,o in ipairs(D.slots) do
		if o.ptr and o.init then
			o.ptr[0] = o.init
		end
	end
end

--------------------------------------------------------------------------------

local function startup()
	-- solve read/write statements and mappings
	dataflow()
	-- compute ctype and address for each memslot + compute heap layout
	memlayout()
	-- compute mem and fhk masks for writes and applies
	computemasks()
	-- compile fhk graph
	compilegraph()
	-- compile access functions (read, write, apply)
	compileaccess()
	-- free memory
	debugevent("data", D)
	table.clear(D)
end

return {
	memslot = memslot,
	pipe    = pipe,
	shared  = shared,
	dynamic = dynamic,
	read    = function(...) return access("read", ...) end,
	write   = function(...) return access("write", ...) end,
	globals = function() return D.data end,
	define  = define,
	include = include,
	apply   = apply,
	connect = connect,
	startup = startup,
}
