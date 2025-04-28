local array = require "m3_array"
local C = require "m3_C"
local db = require "m3_db"
local cdata = require "m3_cdata"
local code = require "m3_code"
local dbg = require "m3_debug"
local mem = require "m3_mem"
local fhk = require "fhk"
local sqlite = require "sqlite"
local buffer = require "string.buffer"
local ffi = require "ffi"
require "table.clear"

-- TODO: reorganize this file: data defs -> API -> init

local load = code.load
local event, enabled = dbg.event, dbg.enabled
local mem_getobj = mem.getobj

local G = fhk.newgraph()

local D = {
	transactions   = {},
	mapping        = {}, -- node -> data
	mapping_tables = {}, -- list of table nodes (rebuilt each iteration)
	expr           = {},
}

-- note: consider implementing this in fhk itself? either in the language or query api.
-- note 2: consider using a scoped name here?
G:define [[
	macro var $table __m3_eval'$expr = $expr
]]

---- Data objects --------------------------------------------------------------

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
		if not fmt.short then
			return o.fields
		end
	elseif tag == "dataframe" then
		if o.slot.block then
			buf:putf("(block %d) ", o.slot.block)
		end
		if not fmt.short then
			return o.columns
		end
	end
end

local function newmeta(tag)
	return { ["m3$tag"] = tag, ["m3$pretty"] = pretty }
end

-- static memory slot
local memslot_mt = newmeta "memslot"
local function memslot(slot)
	return setmetatable(slot or {}, memslot_mt)
end

-- collection of named objects
local struct_mt = newmeta "struct"
local function struct(fields)
	return setmetatable({ fields = fields or {}  }, struct_mt)
end

-- dataframe column
local column_mt = newmeta "column"
local function column(df, name, ctype)
	return setmetatable({ df=df, name=name, ctype=ctype }, column_mt)
end

local function dataframe_ctype(df)
	local proto = {}
	for name,col in pairs(df.columns) do
		if col.mark then
			col.ctype = ffi.typeof(col.ctype or "double")
			if col.dummy == true then
				col.dummy = cdata.dummy(col.ctype)
			end
			table.insert(proto, {name=name, ctype=col.ctype, dummy=col.dummy})
		end
	end
	return array.df_of(proto)
end

-- dataframe
local dataframe_mt = newmeta "dataframe"
local function dataframe()
	local df = setmetatable({
		slot    = memslot(),
		columns = {}
	}, dataframe_mt)
	df.slot.ctype = function() return dataframe_ctype(df) end
	return df
end

-- size of an object
local size_mt = newmeta "size"
local function size(o)
	return setmetatable({ data=o }, size_mt)
end

-- varargs
local splat_mt = newmeta "splat"
local function splat(values)
	return setmetatable({ values = values or {} }, splat_mt)
end

-- constant value
local literal_mt = newmeta "literal"
local function literal(value)
	return setmetatable({ value = value }, literal_mt)
end

local mutate_mt = newmeta "mutate"
local function mutate(data, f)
	return setmetatable({ data=data, f=f}, mutate_mt)
end

-- function argument
local arg_mt = newmeta "arg"
local function arg(idx)
	return setmetatable({idx=idx}, arg_mt)
end

-- function return value
local ret_mt = newmeta "ret"
local function ret(idx)
	return setmetatable({idx=idx}, ret_mt)
end

-- fhk expression
local expr_mt = newmeta "expr"
local function expr(e)
	local expr = D.expr[e]
	if not expr then
		expr = setmetatable({ e = G:expr("global", e) }, expr_mt)
		if type(e) == "string" then
			D.expr[e] = expr
		end
	end
	return expr
end

-- sql DML
local dml_mt = newmeta "dml"
local function dml(sql, n)
	return setmetatable({sql=sql, n=n}, dml_mt)
end

-- orm-style automagic sql SELECT
local autoselect_mt = newmeta "autoselect"
local function autoselect(obj, tab, sql, rename)
	return setmetatable({obj=obj, tab=tab, sql=sql, rename=rename}, autoselect_mt)
end

-- user
local func_mt = newmeta "func"
local function func(func, args)
	return setmetatable({func=func, args=args}, func_mt)
end
local function call(func, n, buf)
	return setmetatable({func=func, n=n, buf=buf}, func_mt)
end

-- buffer
local buf_mt = newmeta "buf"
local function databuf()
	return setmetatable({
		tail = memslot { ctype="int32_t", init=0 },
	}, buf_mt)
end

-- TODO: use table dispatch here
local function visit(o, f, ...)
	local tag = gettag(o)
	f(o, ...)
	if tag == "struct" then
		return visit(o.fields, f, ...)
	elseif tag == "dataframe" then
		visit(o.slot, f, ...)
		return visit(o.columns, f, ...)
	elseif tag == "size" then
		return f(o.data, ...)
	elseif tag == "splat" then
		return visit(o.values, f, ...)
	elseif tag == "mutate" then
		return visit(o.data, f, ...)
	elseif tag == "dml" then
		return visit(D.actions, f, ...)
	elseif tag == "autoselect" then
		return visit(o.obj, f, ...)
	elseif tag == "func" then
		if o.args then
			return visit(o.args, f, ...)
		elseif o.buf then
			return visit(o.buf, f, ...)
		end
	elseif tag == "buf" then
		return visit(o.tail, f, ...)
	elseif not tag then
		for _,v in pairs(o) do
			f(v, ...)
		end
	end
end

local function dowalk(o, seen, f, ...)
	if seen[o] then return end
	seen[o] = true
	f(o, ...)
	return visit(o,dowalk,seen,f,...)
end

local function walk(o, f, ...)
	return dowalk(o,{},f,...)
end

local function mappednode(d)
	for node,data in pairs(D.mapping) do
		if data == d then
			return node
		end
	end
end

local function describe(d)
	local buf = buffer.new()
	buf:putf("<%s>", gettag(d))
	local node = mappednode(d)
	if node then
		buf:put(" (mapped to `")
		if node.op == "VAR" then
			buf:putf("%s.", node.tab.name)
		end
		buf:putf("%s')", node.name)
	end
	return tostring(buf)
end

D.G_state = memslot { ctype = "struct { void *instance; uint64_t mask; }", init = false }
D.actions = databuf()

---- Dataflow ------------------------------------------------------------------
-- fixpoint rules:
-- * every node that is an input to a transaction is visited
-- * every parent node of a visited node is visited
-- * `map_table` is called for each visited table
-- * `map_var` is called for each visited variable that has no models

local function map_tab(node)
	local mapping
	if #node.shape.fields == 0 then
		mapping = struct()
	else
		local s = node.shape.fields[1]
		if #node.shape.fields == 1 and s.op == "VGET" and #s.idx == 0 then
			assert(not D.mapping[s.var], "NYI (shared table length)")
			mapping = dataframe()
			D.mapping[s.var] = size(mapping)
		else
			-- don't map it
			return
		end
	end
	D.mapping[node] = mapping
end

local function map_var(node)
	local tab = D.mapping[node.tab]
	local tag = gettag(tab)
	local name = tostring(node.name)
	local mapping
	if tag == "struct" then
		mapping = tab.fields[name]
		if not mapping then
			mapping = memslot()
			tab.fields[name] = mapping
		end
	elseif tag == "dataframe" then
		mapping = tab.columns[name]
		if not mapping then
			mapping = column(tab, name)
			tab.columns[name] = mapping
		end
	else
		-- variable with no models and the table is not mapped (ie. uncomputable variable).
		return
	end
	D.mapping[node] = mapping
	local default = G:var(node.tab, "default'{$}", false, node.name)
	if default then
		node.m3_default = default
		if not mapping.dummy then
			mapping.dummy = true
		end
	end
end

local function islit(x)
	return type(x) == "number" or type(x) == "string"
end

local resolvetab

local function resolve(o)
	if type(o) == "string" then
		return expr(o)
	elseif type(o) == "function" then
		return func(o)
	elseif type(o) == "table" then
		local tag = gettag(o)
		if not tag then
			return struct(resolvetab(o))
		elseif tag == "struct" then
			return struct(resolvetab(o.fields))
		elseif tag == "splat" then
			return splat(resolvetab(o.values))
		elseif tag == "func" then
			if o.args then
				return func(o.func, resolvetab(o.args))
			end
		end
		-- add other as needed
		return o
	else
		return literal(o)
	end
end

resolvetab = function(t)
	local r = {}
	for k,v in pairs(t) do
		if not islit(k) then
			k = resolve(k)
		end
		r[k] = resolve(v)
	end
	return r
end

local function dataflow_transaction(tx)
	tx.actions = {}
	local action = function(i,o)
		table.insert(tx.actions, {
			input  = resolve(i),
			output = resolve(o),
		})
	end
	for _,a in ipairs(tx) do
		if type(a) == "table" then
			action(a.input, a.output)
		else
			a(action)
		end
	end
end

local function dataflow_updategraph(dcx)
	while dcx.last.next do
		local o = dcx.last.next
		if o.op == "TAB" then
			for _,expr in ipairs(o.shape.fields) do
				if expr.op == "VGET" and #expr.idx == 0 then
					expr.var.m3_tablen = true
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
		dcx.last = o
	end
end

local function dataflow_visit(dcx, node)
	if node.m3_visited then return end
	node.m3_visited = true
	dcx.fixpoint = false
	for _,parent in ipairs(fhk.refs(node)) do
		dataflow_visit(dcx, parent)
	end
	if node.op == "VAR" then
		if node.m3_models then
			for _,model in ipairs(node.m3_models) do
				dataflow_visit(dcx, model)
			end
		elseif not node.m3_tablen then
			map_var(node)
			if node.m3_default then
				table.insert(dcx.backlog, node.m3_default)
			end
		end -- else: tablen is mapped by `map_tab`
	elseif node.op == "TAB" then
		map_tab(node)
	end
end

local function dataflow_visitexpr(o, dcx)
	if gettag(o) == "expr" then
		dataflow_visit(dcx, o.e)
	end
end

local function dataflow_lookup()
	D.mapping_tables = {}
	for node in pairs(D.mapping) do
		if node.op == "TAB" then
			node.m3_vars = {}
			table.insert(D.mapping_tables, node)
		end
	end
	for node in pairs(D.mapping) do
		if node.op == "VAR" then
			table.insert(node.tab.m3_vars, node)
		end
	end
end

local function dataflow_iter(dcx)
	-- this must be done first, because transactions use this
	dataflow_lookup()
	-- this may add more nodes to the graph, so resolve everything first before visiting.
	for _,tx in ipairs(D.transactions) do
		dataflow_transaction(tx)
	end
	-- compute models and tablen
	if dcx.last.next then
		dcx.fixpoint = false
		dataflow_updategraph(dcx)
	end
	-- visit backlogged nodes from previous iteration.
	-- this must be done after dataflow_updategraph (which is why they were backlogged in the
	-- first place)
	local backlog = dcx.backlog
	if #backlog > 0 then
		dcx.backlog = {}
		for _,node in ipairs(backlog) do
			dataflow_visit(dcx, node)
		end
	end
	-- visit read variables again
	for _,tx in ipairs(D.transactions) do
		for _,a in ipairs(tx.actions) do
			walk(a.input, dataflow_visitexpr, dcx)
		end
	end
end

local function dataflow()
	local dcx = {
		last = G.objs[0],
		backlog = {}
	}
	for _=1, 1000 do
		dcx.fixpoint = true
		dataflow_iter(dcx)
		if dcx.fixpoint then return end
	end
	error("dataflow did not converge")
end

---- Memory layouting ----------------------------------------------------------

local function slot_cmp(a, b)
	return ffi.alignof(a.ctype) > ffi.alignof(b.ctype)
end

local function worklayout(slots)
	table.sort(slots, slot_cmp)
	local ptr = 0
	for _,slot in ipairs(slots) do
		local align = ffi.alignof(slot.ctype)
		ptr = bit.band(ptr + align-1, bit.bnot(align-1))
		slot.ofs = ptr
		ptr = ptr + ffi.sizeof(slot.ctype)
	end
	local work, bsize = mem.work_init(ptr)
	D.bsize = bsize
	for _, slot in ipairs(slots) do
		slot.ptr = ffi.cast(
			ffi.typeof("$*", slot.ctype),
			ffi.cast("intptr_t", work) + slot.ofs
		)
	end
end

ffi.cdef [[
	void *malloc(size_t);
	void free(void *);
]]

local function malloc(size)
	return ffi.gc(ffi.C.malloc(size), ffi.C.free)
end

local function dummylayout(slots)
	local size, align = 0, 1
	for _, slot in ipairs(slots) do
		size = math.max(ffi.sizeof(slot.ctype), size)
		align = math.max(ffi.alignof(slot.ctype), align)
	end
	if size == 0 then
		return
	end
	assert(align <= 16, "TODO")
	local region = malloc(size)
	_G._M3_ANCHOR_DUMMY_REGION = region
	for _, slot in ipairs(slots) do
		slot.ptr = ffi.cast(ffi.typeof("$*", slot.ctype), region)
	end
end

local function ctype_type(ct)
	return bit.rshift(ffi.typeinfo(ffi.typeof(ct)).info, 28)
end

local function ctype_isstruct(ct)
	return ctype_type(ct) == 1
end

local function markslot(o,m,all)
	local tag = gettag(o)
	if tag == "memslot" then
		all[o] = true
		o[m] = true
	elseif tag == "column" then
		o.mark = true -- for dataframe_ctype / autoselect
		markslot(o.df.slot, m, all)
	elseif tag == "buf" then
		markslot(o.tail, m, all)
		if m == "write" then
			o.tail.read = true
		end
	end
end

local function memlayout()
	local all = {}
	markslot(D.G_state, "read", all)
	markslot(D.G_state, "write", all)
	markslot(D.actions.tail, "read", all)
	for _,tx in ipairs(D.transactions) do
		for _,a in ipairs(tx.actions) do
			walk(a.input, markslot, "read", all)
			walk(a.output, markslot, "write", all)
		end
	end
	for _,data in pairs(D.mapping) do
		walk(data, markslot, "read", all)
	end
	local ro, wo, rw = {}, {}, {}
	for o in pairs(all) do
		if type(o.ctype) == "function" then
			o.ctype = o.ctype()
		end
		if not o.ctype then
			-- TODO: design a good mechanism for default types
			o.ctype = "double"
		end
		o.ctype = ffi.typeof(o.ctype)
		if o.dummy == true then
			o.dummy = cdata.dummy(o.ctype)
		end
		if o.init == nil then
			o.init = o.dummy
		end
		if o.read and (not o.write) and o.init == nil then
			error(string.format("%s is read but never written and has no initializer", describe(o)))
		end
		local t
		if o.read and (o.write or ctype_isstruct(o.ctype)) then
			t = rw
		elseif o.read then
			t = ro
		elseif o.write then
			t = wo
		end
		if t then
			table.insert(t, o)
		end
	end
	worklayout(rw)
	dummylayout(ro)
	dummylayout(wo)
	for o in pairs(all) do
		if o.init then
			if type(o.init) == "function" then
				o.ptr[0] = o.init()
			else
				o.ptr[0] = o.init
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

function map_obj.memslot(obj, tab, var)
	return string.format(
		"model %s %s = load'%s(0x%x)",
		tab,
		var,
		fhktypename(obj.ctype),
		ffi.cast("intptr_t", obj.ptr)
	)
end

function map_obj.column(obj, tab, var)
	local dfbase = ffi.cast("intptr_t", obj.df.slot.ptr)
	-- TODO use load'u32 for the length here when fhk supports it.
	return string.format(
		"model global %s.%s = load'%s(load'ptr(0x%x), load'i32(0x%x))",
		tab,
		var,
		fhktypename(obj.ctype),
		dfbase + ffi.offsetof(obj.df.slot.ctype, obj.name),
		dfbase -- `num` is at offset zero
	)
end

function map_obj.size(obj, tab, var)
	local tag = gettag(obj.data)
	if tag == "dataframe" then
		-- TODO: make fhk accept any integer type for table size
		return string.format(
			"model %s %s = load'i32(0x%x)",
			tab,
			var,
			ffi.cast("intptr_t", obj.data.slot.ptr)
		)
	else
		error(string.format("NYI (map size: %s)", tag))
	end
end

local function map_default(obj, var)
	local default = var.m3_default
	if not (default and default.m3_models) then return end
	assert(obj.dummy ~= nil, "default without dummy")
	-- TODO: allow custom dummy values
	local testdummy = obj.dummy
	if testdummy ~= testdummy then
		testdummy = "let x = %s in x != x"
	else
		testdummy = "%s != " .. testdummy
	end
	local dataname = string.format("data'{%s}", var.name)
	testdummy = string.format(testdummy, dataname)
	local src = string.format(
		"model %s { %s = %s where %s %s = %s }",
		--[[model]] var.tab.name,
		var.name, --[[=]] default.name, --[[where]] testdummy,
		var.name, --[[=]] dataname
	)
	return src, dataname
end

local function makemappings()
	local buf = buffer.new()
	for node,obj in pairs(D.mapping) do
		if node.op == "VAR" then
			local src, name = map_default(obj, node)
			if src then
				buf:put(src, "\n")
			else
				name = tostring(node.name)
			end
			local map = map_obj[gettag(obj)]
			if not map then
				error(string.format("`%s' obj cannot be used in graph", gettag(obj)))
			end
			buf:put(map(obj, tostring(node.tab.name), name), "\n")
		end
	end
	event("gmap", buf)
	G:define(buf)
end

local function visit_expr(o, tx)
	if gettag(o) == "expr" then
		if not tx.query then
			tx.query = G:newquery("global")
			tx.query_field = {}
		end
		if not tx.query_field[o.e] then
			tx.query_field[o.e] = tx.query:add(o.e)
		end
	end
end

local function makequeries()
	for _,tx in ipairs(D.transactions) do
		for _,a in ipairs(tx.actions) do
			walk(a.input, visit_expr, tx)
		end
	end
end

local function visit_reset(o, tx, mapping_inverse)
	if mapping_inverse[o] then
		if not tx.reset then
			tx.reset = G:newreset()
		end
		for _,node in ipairs(mapping_inverse[o]) do
			if node.m3_default then
				node = string.format("%s.data'{%s}", node.tab.name, node.name)
			end
			tx.reset:add(node)
		end
	end
end

local function makeresets()
	local mapping_inverse = {}
	-- note that mapping is not necessarily one-to-one.
	-- one data object may be mapped to multiple variables.
	for node,data in pairs(D.mapping) do
		if node.op == "VAR" then
			if gettag(data) == "size" then
				-- size of a dataframe is changed by mutating the dataframe
				data = data.data.slot
			end
			if not mapping_inverse[data] then
				mapping_inverse[data] = {}
			end
			table.insert(mapping_inverse[data], node)
		end
	end
	-- compute a reset mask for every access that writes to objects that are mapped to variables
	for _,tx in ipairs(D.transactions) do
		for _,a in ipairs(tx.actions) do
			walk(a.output, visit_reset, tx, mapping_inverse)
		end
	end
end

local function compilegraph(alloc)
	makequeries()
	makeresets()
	makemappings()
	G = assert(G:compile())
	-- pre-create instance so that instance creation can assume we always have a non-null instance
	-- available
	D.G_state.ptr.instance = G:newinstance(alloc, mem.state)
	D.G_state.ptr.mask = 0
end

---- Action buffers ------------------------------------------------------------

local function abuf_trampoline(f, n)
	local buf = buffer.new()
	buf:put("local f = ...\nreturn function(o,b,i)\nf(")
	for i=1, n do
		if i>1 then buf:put(",") end
		buf:putf("o._%d", i)
	end
	buf:put(") if i>=0 then return b[i]:call(b,i-1) end end\n")
	return load(buf, code.chunkname(string.format("action %s", dbg.describe(f))))(f)
end

local function abuf_commit_(tail)
	local buf = {}
	local n = 0
	while true do
		local o = mem_getobj(tail)
		-- prev=nil -> already flushed
		if (not o) or (not o.prev) then break end
		buf[n] = o
		n = n+1
		tail = o.prev
		o.prev = nil
	end
	if n > 0 then
		return buf[n-1]:call(buf, n-2)
	end
end

local abuf_commit = load([[
	local abuf_commit_ = ...
	local tail
	return function()
		return abuf_commit_(tail[0])
	end
]])(abuf_commit_)

local function stmt_buffer(stmt, ...)
	return stmt:buffer(...)
end

---- Transaction compilation ---------------------------------------------------

-- Read --------------------------------

local function newvar(ctx, value)
	local name = ctx:name()
	ctx.buf:putf("local %s = %s\n", name, value)
	return name
end

-- reading an empty splat returns "", this converts it to at least one (but possibly more) value(s)
local function tovalue(v)
	if v == "" then return "nil" end
	return v
end

local emit_read = {}
local emitread

function emit_read.memslot(ctx, slot)
	local ptr = ctx.uv[slot.ptr]
	if ctype_isstruct(slot.ctype) then
		return ptr
	else
		return newvar(ctx, string.format("%s[0]", ptr))
	end
end

function emit_read.struct(ctx, struct)
	local result = newvar(ctx, "{}")
	for k,v in pairs(struct.fields) do
		ctx.buf:putf("%s%s = %s\n", result, code.index(ctx.uv, k), tovalue(emitread(ctx, v)))
	end
	return result
end

function emit_read.column(ctx, col)
	return newvar(ctx, string.format("%s.%s", ctx.uv[col.df.slot.ptr], col.name))
end

function emit_read.dataframe(ctx, df)
	return ctx.uv[df.slot.ptr]
end

function emit_read.size(ctx, size)
	local tag = gettag(size.obj)
	if tag == "dataframe" then
		return newvar(ctx, string.format("%s.num", ctx.uv[df.slot.ptr]))
	else
		error(string.format("NYI (size %s)", tag))
	end
end

function emit_read.splat(ctx, splat)
	local results = {}
	for i,v in ipairs(splat.values) do
		local r = emitread(ctx, v)
		if r ~= "" then
			results[i] = r
		end
	end
	return table.concat(results, ", ")
end

function emit_read.literal(ctx, literal)
	return ctx.uv[literal.value]
end

function emit_read.func(ctx, call)
	return newvar(ctx, string.format("local %s = %s()\n", resname, ctx.uv[call.func]))
end

function emit_read.expr(ctx, expr)
	return string.format("Q.%s", ctx.query_field[expr.e])
end

function emit_read.arg(ctx, arg)
	local idx = arg.idx or ctx.narg+1
	ctx.narg = math.max(ctx.narg, idx)
	return string.format("arg%d", idx)
end

emitread = function(ctx, x)
	return emit_read[gettag(x)](ctx, x)
end

-- Write -------------------------------

local emit_write = {}
local emitwrite

function emit_write.memslot(ctx, slot, value)
	local ptr = ctx.uv[slot.ptr]
	if value and value ~= "" then
		ctx.buf:putf("if %s ~= nil then %s[0] = %s end\n", value, ptr, value)
	end
	if (not value) or ctype_isstruct(slot.ctype) then
		return ptr
	end
end

function emit_write.struct(ctx, struct, value)
	assert(value, "cannot mutate struct")
	if value == "" then return end
	local vname = ctx:name()
	local result
	for k,v in pairs(struct.fields) do
		local pos = #ctx.buf
		ctx.buf:putf("do local %s = (%s) and (%s)%s\n", vname, value, value, code.index(ctx.uv, k))
		local r = emitwrite(ctx, v, vname)
		if r then
			if not result then
				result = ctx:name()
				local buf = buffer.new()
				buf:put(ctx.buf:get(pos))
				buf:putf("local %s = {}\n", result)
				buf:put(ctx.buf)
				ctx.buf = buf
			end
			ctx.buf:putf("%s%s = %s\n", result, code.index(ctx.uv, k), r)
		end
		ctx.buf:put("end\n")
	end
	return result
end

function emit_write.column(ctx, col, value)
	if value then
		ctx.buf:putf("%s:overwrite('%s', %s)\n", ctx.uv[col.df.slot.ptr], col.name, value)
	else
		error("TODO")
	end
end

function emit_write.dataframe(ctx, df, value)
	local ptr = ctx.uv[df.slot.ptr]
	if value and value ~= "" then
		ctx.buf:putf("if %s ~= nil then %s:settab(%s) end\n", value, ptr, value)
	end
	return ptr
end

function emit_write.splat(ctx, splat, value)
	assert(value, "cannot mutate splat")
	local names = {}
	for i=1, #splat.values do
		names[i] = ctx:name()
	end
	ctx.buf:putf("local %s = %s\n", table.concat(names, ", "), tovalue(value))
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

-- TODO: reuse trampoline for f/n pairs
local function emitbufcall(ctx, buf, f, n, value)
	ctx.uv.objref = mem.objref
	local tail = ctx.uv[buf.tail.ptr]
	local name = ctx:name()
	ctx.buf:putf("do local %s = {prev=%s[0], call=%s}\n", name, tail, ctx.uv[abuf_trampoline(f, n)])
	if n>0 then
		for i=1, n do
			if i>1 then ctx.buf:put(",") end
			ctx.buf:putf("%s._%d", name, i)
		end
		ctx.buf:putf(" = %s\n", tovalue(value))
	end
	ctx.buf:putf("%s[0] = objref(%s) end\n", tail, name)
end

function emit_write.func(ctx, call, value)
	if call.buf then
		emitbufcall(ctx, call.buf, call.func, call.n, value)
	else
		ctx.buf:putf("%s(%s)\n", ctx.uv[call.func], value)
	end
end

-- TODO: vectors should be stored in the actionbuf as vectors, and then expanded inside the action
-- (ie. stmt_buffer)
function emit_write.dml(ctx, dml, value)
	local args = ctx.uv[db.statement(dml.sql)]
	if dml.n > 0 then args = string.format("%s, %s", args, tovalue(value)) end
	emitbufcall(ctx, D.actions, stmt_buffer, 1+dml.n, args)
end

function emit_write.autoselect(ctx, asel, value)
	assert(value, "cannot mutate autoselect")
	local tag = gettag(asel.obj)
	local schema = db.schema(asel.tab)
	if tag == "struct" then
		local values, cols = {}, {}
		for k,v in pairs(asel.obj.fields) do
			if gettag(v) ~= "size" then
				local col = asel.rename(k)
				table.insert(cols, sqlite.escape(col))
				table.insert(values, {value=v, null=schema.columns[col].nullable})
			end
		end
		if #cols == 0 then return end
		local name = ctx:name()
		local sql = {sqlite.sql("SELECT", unpack(cols)),
			sqlite.sql("FROM", sqlite.escape(asel.tab)), asel.sql}
		ctx.uv.assert = assert
		ctx.buf:putf(
			"local %s = %s.sqlite3_stmt\n%s:bindargs(%s)\nassert(%s:step(), 'query returned no rows')\n",
			name, ctx.uv[db.statement(sql)],
			name, value,
			name
		)
		for i=1, #cols do
			if values[i].null then
				ctx.buf:putf("do local v = %s:col(%d) if v then\n", name, i-1)
				emitwrite(ctx, values[i].value, "v")
				ctx.buf:put("end end\n")
			else
				emitwrite(ctx, values[i].value, string.format("%s:double(%d)", name, i-1))
			end
		end
		ctx.buf:putf("%s:reset()\n", name)
	elseif tag == "dataframe" then
		local tname = sqlite.escape(asel.tab)
		local cols, names, dummies, nulls = {}, {}, {}, {}
		for name,c in pairs(asel.obj.columns) do
			if c.mark then
				local colname = asel.rename(name)
				local col = schema.columns[colname]
				if col then
					if col.null and c.dummy ~= nil then nulls[c] = true end
					table.insert(cols, c)
					table.insert(names, string.format("%s.%s", tname, sqlite.escape(colname)))
				elseif c.dummy ~= nil then
					table.insert(dummies, c)
				else
					error(string.format("column `%s.%s' is not in the schema and has no dummy value",
						asel.tab, name))
				end
			end
		end
		if #cols == 0 and #dummies == 0 then return end
		local ptr = ctx.uv[asel.obj.slot.ptr]
		local sqlcount = {sqlite.sql("SELECT", "COUNT(*)"), sqlite.sql("FROM", tname), asel.sql}
		ctx.buf:putf(
			"do\nlocal s=%s.sqlite3_stmt s:bindargs(%s) s:step() local num=s:int(0) local base=%s:alloc(num) s:reset()\n",
			ctx.uv[db.statement(sqlcount)], value, ptr
		)
		if #names == 0 then
			-- hack to ensure the SELECT statement compiles.
			-- a proper implementation would just omit in the following loop.
			names[1] = "0"
		end
		local sql = {sqlite.sql("SELECT", unpack(names)), sqlite.sql("FROM", tname), asel.sql}
		ctx.buf:putf(
			"local r=%s.sqlite3_stmt r:bindargs(%s) for i=0, num-1 do r:step()\n",
			ctx.uv[db.statement(sql)], value
		)
		for i,c in ipairs(cols) do
			ctx.buf:putf("%s.%s[base+i] = ", ptr, c.name)
			if nulls[c] then
				ctx.buf:putf("r:col(%d) or %s\n", i-1, ctx.uv[c.dummy])
			else
				ctx.buf:putf("r:%s(%d)\n", cdata.isfp(c.ctype) and "double" or "int", i-1)
			end
		end
		for _,c in ipairs(dummies) do
			ctx.buf:putf("%s.%s[base+i] = %s\n", ptr, c.name, ctx.uv[c.dummy])
		end
		ctx.buf:put("end\nr:reset() end\n")
	else
		error(string.format("cannot autoselect into %s", tag))
	end
end

function emit_write.ret(ctx, ret, value)
	assert(value, "cannot mutate return value")
	local idx = ret.idx or ctx.nret+1
	ctx.nret = math.max(ctx.nret, idx)
	ctx.buf:putf("local ret%d = %s\n", idx, tovalue(value))
end

function emit_write.mutate(ctx, mut, value)
	ctx.buf:putf("%s(%s", ctx.uv[mut.f], emitwrite(ctx, mut.data))
	if value and value ~= "" then
		ctx.buf:putf(", %s", value)
	end
	ctx.buf:put(")\n")
end

emitwrite = function(ctx, x, v)
	return emit_write[gettag(x)](ctx, x, v)
end

----------------------------------------

local function cdatamask(ofs, size)
	if type(ofs) == "table" then
		ofs, size = ofs.ofs, ffi.sizeof(ofs.ctype)
	end
	local first = math.floor(ofs / D.bsize)
	local last = math.floor((ofs+size-1) / D.bsize)
	return bit.lshift(1ull, last+1) - bit.lshift(1ull, first)
end

local function visit_mmask(o, ctx)
	local tag = gettag(o)
	local ofs
	if tag == "memslot" then
		ofs = o.ofs
	elseif tag == "column" then
		ofs = o.df.slot.ofs + ffi.offsetof(o.df.slot.ctype, o.name)
	else
		return
	end
	ctx.mmask = bit.bor(ctx.mmask or 0ull, cdatamask(ofs, ffi.sizeof(o.ctype)))
end

local function compiletransaction(tx, graph_instance)
	local ctx = code.new()
	ctx.narg = 0
	ctx.nret = 0
	-- query, if any, must happen before any masks are set, because it may create a new instance.
	if tx.query then
		ctx.uv.query = tx.query.query
		ctx.uv.graph_instance = graph_instance
		ctx.query_field = tx.query_field
		ctx.buf:put("local Q = query(graph_instance())\n")
	end
	for _,a in ipairs(tx.actions) do
		walk(a.output, visit_mmask, ctx)
	end
	if ctx.mmask and tx.reset then
		-- if we modify the mask, then make sure the old mask is saved.
		-- this is not required for correctness, since it's always ok to set more bits
		-- in the mask, but this reduces unnecessary resets.
		visit_mmask(D.G_state, ctx)
	end
	if ctx.mmask then
		ctx.uv.mem_write = mem.write
		ctx.buf:putf("mem_write(0x%xull)\n", ctx.mmask)
	end
	if tx.reset then
		ctx.uv.G_state = D.G_state.ptr
		if tx.query then
			ctx.buf:putf("G_state.mask = 0x%xull\n", tx.reset.mask)
		else
			ctx.uv.bor = bit.bor
			ctx.buf:putf("G_state.mask = bor(G_state.mask, 0x%xull)\n", tx.reset.mask)
		end
	end
	local inputs = {}
	for i,a in ipairs(tx.actions) do
		inputs[i] = emitread(ctx, a.input)
	end
	for i,a in ipairs(tx.actions) do
		emitwrite(ctx, a.output, inputs[i])
	end
	local buf = buffer.new()
	local uv = code.emitupvalues(ctx.uv, buf)
	buf:put("return function(")
	if ctx.narg > 0 then
		buf:put("arg1")
		for i=2, ctx.narg do
			buf:putf(", arg%d", i)
		end
	end
	buf:put(")\n")
	buf:put(ctx.buf)
	if ctx.nret > 0 then
		buf:put("return ret1")
		for i=2, ctx.nret do
			buf:putf(", ret%d", i)
		end
		buf:put("\n")
	end
	buf:put("end\n")
	return load(buf, code.chunkname(string.format("transaction %p", tx)))(unpack(uv))
end

-- TODO: this should take a query mask parameter and only create a new instance if the intersection
-- is nonzero
local function graph_instancefunc(alloc)
	return load(string.format([[
		local state, G, alloc, memstate, write, iswritable = ...
		return function()
			if not iswritable(state.instance) then
				write(0x%xull)
				goto new
			end
			if state.mask == 0 then
				return state.instance
			end
			::new::
			local instance = G:newinstance(alloc, memstate, state.instance, state.mask)
			state.instance = instance
			state.mask = 0
			return instance
		end
	]], cdatamask(D.G_state)))(D.G_state.ptr, G, alloc, mem.state, mem.write,
		mem.iswritable)
end

local tx_compiled_mt = {
	["m3$transaction"] = true,
	__call = function(self, ...) return self.func(...) end
}

local function compiletransactions(alloc)
	local graph_instance = graph_instancefunc(alloc)
	for _,tx in ipairs(D.transactions) do
		local func = compiletransaction(tx, graph_instance)
		table.clear(tx)
		setmetatable(tx, tx_compiled_mt).func = func
	end
end

---- Initialization ------------------------------------------------------------

local function collect(o, all)
	local tag = gettag(o)
	if tag == "memslot" or tag == "dataframe" then
		all[o] = true
	end
end

local function traceobjs()
	local all = {}
	for _,tx in ipairs(D.transactions) do
		for _,a in ipairs(tx.actions) do
			walk(a.input, collect, all)
			walk(a.output, collect, all)
		end
	end
	for node,data in pairs(D.mapping) do
		all[data] = node.tab and string.format("%s.%s", node.tab.name, node.name)
			or tostring(node.name)
	end
	local objs = {}
	for o,m in pairs(all) do
		table.insert(objs, {obj=o, map=m ~= true and m or nil})
	end
	table.sort(objs, function(a, b)
		if a.map or b.map then
			if a.map and b.map then
				return a.map < b.map
			else
				return a.map ~= nil
			end
		end
		a, b = a.obj, b.obj
		local atag = gettag(a)
		local btag = gettag(b)
		if atag ~= btag then
			return atag < btag
		end
		if atag == "dataframe" then
			a, b = a.slot, b.slot
		end
		return ffi.cast("intptr_t", a.ptr or 0) < ffi.cast("intptr_t", b.ptr or 0)
	end)
	return objs
end

local function trace_alloc(_, size, align)
	local ptr = C.m3_mem_alloc(nil, mem.state, size, align)
	event("alloc", tonumber(size), tonumber(align), ptr)
	return ptr
end

local function init()
	dataflow()
	memlayout()
	local alloc = enabled("alloc") and ffi.cast("void *(*)(void *, size_t, size_t)", trace_alloc)
		or C.m3_mem_alloc
	compilegraph(alloc)
	if enabled("data") then
		event("data", traceobjs())
	end
	compiletransactions(alloc)
	code.setupvalue(abuf_commit, "tail", D.actions.tail.ptr)
	table.clear(D)
end

---- API -----------------------------------------------------------------------

local function define(src)
	G:define(src)
end

local function defined(tab, name, ...)
	return G:var(tab, name, false, ...) ~= nil
end

-- TODO fhk lexer should support streaming
local function include(name)
	local fp = assert(io.open(name, "r"))
	define(fp:read("*a"))
	fp:close()
end

local function transaction_action(transaction, action)
	table.insert(transaction, action)
	return transaction
end

local function totablefilter(x)
	if type(x) == "function" then
		return x
	else
		return function(node)
			return tostring(node.name) == x
		end
	end
end

local function indexfunc(tab)
	return function(key) return tab[key] end
end

-- TODO: put this somewhere else, like the expr constructor?
local function evalexpr(tab, expr)
	if tostring(tab) ~= "global" then
		expr = string.format("%s.__m3_eval'{%s}", tab, expr)
	end
	return expr
end

-- transaction_update(func: tab,field -> expr)
-- transaction_update("tab", {field=expr})
-- transaction_update("tab", func: field -> expr)
local function transaction_update(transaction, a, b)
	if b == nil then
		return transaction_action(transaction, function(action)
			for node,data in pairs(D.mapping) do
				if node.op == "VAR" then
					local tab = tostring(node.tab.name)
					local value = a(tab, tostring(node.name))
					if value then
						if type(value) == "string" then value = evalexpr(tab, value) end
						action(value, data)
					end
				end
			end
		end)
	else
		local tab = totablefilter(a)
		if type(b) == "table" then
			b = indexfunc(b)
		end
		return transaction_action(transaction, function(action)
			for _,t in ipairs(D.mapping_tables) do
				if tab(t) then
					for _,node in ipairs(t.m3_vars) do
						local value = b(tostring(node.name))
						if value then
							if type(value) == "string" then
								value = evalexpr(tostring(t.name), value)
							end
							action(value, D.mapping[node])
						end
					end
				end
			end
		end)
	end
end

local function df_extend(ptr, values)
	return ptr:extend(values)
end

-- transaction_insert(func: tab,field -> expr)
-- transaction_insert("tab", {field=expr})
-- transaction_insert("tab", func: field -> expr)
-- (TODO: this needs some optimization for not creating intermediate tables)
local function transaction_insert(transaction, a, b)
	if b == nil then
		return transaction_action(transaction, function(action)
			local newcols = {}
			for node in pairs(D.mapping) do
				if node.op == "VAR" then
					local name = tostring(node.tab.name)
					local new = a(tab, name)
					if new then
						if not newcols[node.tab] then newcols[node.tab] = {} end
						newcols[node.tab][name] = new
					end
				end
			end
			for tab,cols in pairs(newcols) do
				action(cols, mutate(D.mapping[tab], df_extend))
			end
		end)
	else
		local tab = totablefilter(a)
		if type(b) == "table" then
			b = indexfunc(b)
		end
		return transaction_action(transaction, function(action)
			for _,t in ipairs(D.mapping_tables) do
				if tab(t) then
					local cols = {}
					-- TODO: this should only look at mapped fields
					for _,node in ipairs(t.m3_vars) do
						local name = tostring(node.name)
						cols[name] = b(name)
					end
					if next(cols) then
						action(cols, mutate(D.mapping[t], df_extend))
					end
				end
			end
		end)
	end
end

local function df_clearmask(ptr, mask)
	return ptr:clearmask(mask)
end

-- transaction_delete(func: tab -> expr)
-- transaction_delete("tab", expr)
local function transaction_delete(transaction, a, b)
	if type(a) == "string" then
		local a_ = a
		a = function(name) if a_ == name then return b end end
	end
	return transaction_action(transaction, function(action)
		for _,t in ipairs(D.mapping_tables) do
			local name = tostring(t.name)
			local mask = a(name)
			if mask then
				if type(mask) == "string" then mask = evalexpr(name, mask) end
				action(mask, mutate(D.mapping[t], df_clearmask))
			end
		end
	end)
end

local function transaction_define(transaction, src)
	-- TODO(fhk): scoped define with query-vset variables
	define(src)
	return transaction
end

local function transaction_include(transaction, name)
	-- TODO(fhk): see above
	include(name)
	return transaction
end

local function transaction_bind(transaction, name, value)
	error("TODO(fhk): query-vset variables")
end

local function transaction_read(transaction, ...)
	for _,v in ipairs({...}) do
		transaction_action(transaction, {
			input  = v,
			output = ret()
		})
	end
	return transaction
end

local function transaction_write(transaction, ...)
	for _,v in ipairs({...}) do
		transaction_action(transaction, {
			input  = arg(),
			output = v
		})
	end
	return transaction
end

local function transaction_mutate(transaction, data, f, ...)
	return transaction_action(transaction, {
		input  = splat({...}),
		output = mutate(data, f)
	})
end

local function transaction_set(transaction, output, input)
	return transaction_action(transaction, {
		input  = input,
		output = output
	})
end

local function transaction_call(transaction, f, ...)
	return transaction_action(transaction, {
		input  = splat({...}),
		output = call(f, select("#", ...), D.actions)
	})
end

local function transaction_sql(transaction, stmt, ...)
	return transaction_action(transaction, {
		input  = splat({...}),
		output = dml(stmt, select("#", ...))
	})
end

local function transaction_sql_insert(transaction, tab, values)
	-- TODO: if the table does exist, this should ALTER TABLE instead
	if type(values) == "table" then
		local insert = {sqlite.sql("INSERT", tab)}
		local args = {}
		local dd = buffer.new()
		dd:put("CREATE TABLE IF NOT EXISTS ", tab, "(")
		-- TODO: types could be inferred from fhk here?
		local comma = ""
		for col, val in pairs(values) do
			dd:put(comma, col, " REAL")
			comma = ","
			table.insert(insert, sqlite.sql("VALUES", {col=col, value="?"}))
			table.insert(args, val)
		end
		dd:put(")")
		db.ddl(tostring(dd))
		return transaction_action(transaction, {
			input  = splat(args),
			output = dml(insert, #args)
		})
	elseif type(values) == "function" then
		-- TODO: here the table must already exist, and values(col) returns the value for a column.
		error("TODO")
	else
		error("sql_insert: expected table or function")
	end
end

local function identcol(col)
	return col
end

local function tocolsfunc(col)
	if type(col) == "table" then
		return function(name) return col[name] or col end
	elseif not col then
		return identcol
	else
		return col
	end
end

local autoselect_map = {}

local function autoselect_inspect(tab, f)
	for i=#autoselect_map, 1, -1 do
		local amap = autoselect_map[i]
		local config
		if type(amap) == "table" then
			config = amap[tab]
		else
			config = amap(tab)
		end
		if config then
			if type(config) == "string" then config = {table=config} end
			local value = f(config)
			if value then return value end
		end
	end
end

local function autoselect_rename(tab, col)
	local name
	if col then
		name = autoselect_inspect(tab, function(config)
			if type(config.map) == "table" then
				return config.map[col]
			elseif type(config.map) == "function" then
				return config.map(col)
			end
		end)
	else
		name = autoselect_inspect(tab, function(config) return config.table end)
	end
	return name or col or tab
end

local function haveall(names, cols)
	for _,col in pairs(cols) do
		if not names[col] then return false end
	end
	return true
end

local function autoselect_autowhere(tab, names)
	local schema = db.schema(tab)
	if not schema then return end
	-- try primary key
	local cols = {}
	for name,col in ipairs(schema.columns) do
		if col.pk then cols[name] = string.format("%s_%s", tab, name) end
	end
	if not next(cols) then
		-- table has no primary key, try rowid
		-- TODO: this does not work for WITHOUT ROWID tables. this *should* check that the table
		-- is a rowid table, which is a bit involved because none of the pragmas return that
		-- information
		cols.rowid = string.format("%s_rowid", tab)
	end
	if haveall(names, cols) then return cols end
	-- try foreign keys
	for _,fk in ipairs(schema.foreign_keys) do
		table.clear(cols)
		for from,to in pairs(fk.columns) do
			cols[from] = string.format("%s_%s", fk.table, to)
		end
		if haveall(names, cols) then return cols end
	end
end

local function autoselect_autotask()
	local tab
	for _,t in ipairs(D.mapping_tables) do
		if #t.shape.fields == 0 then
			local tname = autoselect_rename(tostring(o.name))
			if db.schema(tname) then
				if tab then
					error(string.format("autotask conflict (%s, %s)", tab, tname))
				end
				tab = tname
			end
		end
	end
	if tab then
		local sql = {}
		for name,col in pairs(db.schema(tab).columns) do
			if col.pk then
				table.insert(sql, sqlite.sql("SELECT", sqlite.sql("AS", sqlite.escape(name),
					string.format("%s_%s", tab, name))))
			end
		end
		if #sql > 0 then
			table.insert(sql, sqlite.sql("FROM", sqlite.escape(tab)))
			return sql
		else
			return string.format("SELECT rowid AS %s_rowid FROM %s", tab, sqlite.escape(tab))
		end
	else
		return "SELECT 0"
	end
end

-- transaction_autoselect(func: tab -> sqltab, where, bind, cols)
-- transaction_autoselect(task)
-- transaction_autoselect() -> autotask
local function transaction_autoselect(transaction, x)
	if not x then
		local sql = autoselect_autotask()
		transaction_autoselect(transaction, sql)
		return sql
	end
	local selector
	if type(x) == "string" or type(x) == "table" then
		local stmt = db.connection():prepare(x)
		local names = {}
		local ncol = stmt:colcount()
		for i=1, ncol do
			names[stmt:name(i-1)] = i
		end
		selector = function(tab)
			local where = autoselect_inspect(tab, function(config) return config.where end)
			local binds = {}
			if where == nil then
				local auto = autoselect_autowhere(tab, names)
				if auto then
					where = {}
					for col,name in pairs(auto) do
						table.insert(binds, arg(names[name]))
						table.insert(where, string.format("%s = ?%d", sqlite.escape(col), #binds))
					end
					where = sqlite.sql("WHERE", unpack(where))
				end
			elseif where then
				-- this breaks when `?NNN` is embedded in a string/name/whatever, but oh well.
				local idxmap = {}
				where = string.gsub(where, "?(%d+)", function(idx)
					if not idxmap[idx] then
						table.insert(binds, arg(tonumber(idx)))
						idxmap[idx] = string.format("?%d", #binds)
					end
					return idxmap[idx]
				end)
				where = sqlite.sql("WHERE", where)
			end
			return autoselect_rename(tab), where, splat(binds),
				function(col) return autoselect_rename(tab, col) end
		end
	else
		selector = x
	end
	return transaction_action(transaction, function(action)
		for _,t in ipairs(D.mapping_tables) do
			local obj = D.mapping[t]
			local tag = gettag(obj)
			if tag == "struct" or tag == "dataframe" then
				local tab, sql, bind, cols = selector(tostring(t.name))
				if tab then
					local schema = db.schema(tab)
					if schema then
						cols = tocolsfunc(cols)
						local obj = D.mapping[t]
						if tag == "struct" then
							-- only select fields that exist in the schema.
							-- we don't need equivalent logic for dataframes, since they don't support
							-- only writing a subset of the columns.
							-- a partial write for a dataframe will fail at runtime.
							local new = struct()
							for k,v in pairs(obj.fields) do
								if schema.columns[cols(k)] then
									new.fields[k] = v
								end
							end
							obj = new
						end
						action(bind or splat(), autoselect(obj, tab, sql, cols))
					end
				end
			end
		end
	end)
end

local transaction_mt = {
	["m3$transaction"] = true,
	__index = {
		update     = transaction_update,
		insert     = transaction_insert,
		delete     = transaction_delete,
		define     = transaction_define,
		include    = transaction_include,
		bind       = transaction_bind,
		read       = transaction_read,
		write      = transaction_write,
		mutate     = transaction_mutate,
		set        = transaction_set,
		call       = transaction_call,
		sql        = transaction_sql,
		sql_insert = transaction_sql_insert,
		autoselect = transaction_autoselect
	}
}

local function transaction()
	local tx = setmetatable({}, transaction_mt)
	table.insert(D.transactions, tx)
	return tx
end

local function istransaction(x)
	local mt = getmetatable(x)
	return mt and mt["m3$transaction"]
end

--------------------------------------------------------------------------------

return {
	G             = G,
	init          = init,
	memslot       = memslot,
	arg           = arg,
	ret           = ret,
	splat         = splat,
	func          = func,
	commit        = abuf_commit,
	transaction   = transaction,
	istransaction = istransaction,
	define        = define,
	defined       = defined,
	include       = include,
	mappers       = autoselect_map,
}
