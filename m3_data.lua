local array = require "m3_array"
local cdata = require "m3_cdata"
local cdef = require "m3_cdef"
local code = require "m3_code"
local dbg = require "m3_debug"
local environment = require "m3_environment"
local mem = require "m3_mem"
local shutdown = require "m3_shutdown"
local sqlite = require "m3_sqlite"
local fhk = require "fhk"
local buffer = require "string.buffer"
local ffi = require "ffi"
require "table.clear"

local event, enabled = dbg.event, dbg.enabled

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
	return setmetatable({ values = values }, splat_mt)
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
local function autoselect(obj, tab, sql)
	return setmetatable({obj=obj, tab=tab, sql=sql}, autoselect_mt)
end

-- pipe
local pipe_mt = newmeta "pipe"
local function pipe()
	return setmetatable({ sink={} }, pipe_mt)
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
		state = { [0]=0 }
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
	elseif tag == "pipe" then
		return visit(o.sink, f, ...)
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
	local default = G:var(node.tab, "default'$", false, node.name)
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
	for _,node in ipairs(D.mapping_tables) do
		if node.m3_vars then
			table.clear(node.m3_vars)
		end
	end
	table.clear(D.mapping_tables)
	for node in pairs(D.mapping) do
		if node.op == "VAR" then
			if not node.tab.m3_vars then
				node.tab.m3_vars = {}
			end
			table.insert(node.tab.m3_vars, node)
		elseif node.op == "TAB" then
			table.insert(D.mapping_tables, node)
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

local function blockct(size, align)
	return ffi.typeof(string.format([[
		__attribute__((aligned(%d)))
		struct { uint8_t data[%d]; }
	]], align, size))
end

local function heaplayout(slots)
	table.sort(slots, slot_cmp)
	local ptr = 0
	for _,slot in ipairs(slots) do
		local align = ffi.alignof(slot.ctype)
		ptr = bit.band(ptr + align-1, bit.bnot(align-1))
		slot.ofs = ptr
		ptr = ptr + ffi.sizeof(slot.ctype)
	end
	local blocksize = cdef.M3_MEM_BSIZEMIN
	local numblock = math.min(cdef.M3_MEM_HEAPBMAX, math.ceil(ptr/blocksize))
	while numblock*blocksize < ptr do blocksize = blocksize*2 end
	local block_ct = blockct(blocksize, cdef.M3_MEM_BSIZEMIN)
	-- use luajit allocator for the heap so that const heap references become
	-- relative addresses in machine code.
	local heap = ffi.new(ffi.typeof("$[?]", block_ct), numblock)
	mem.setheap(heap, blocksize, numblock)
	D.heap_block = blocksize
	for _, slot in ipairs(slots) do
		slot.ptr = ffi.cast(
			ffi.typeof("$*", slot.ctype),
			ffi.cast("intptr_t", heap) + slot.ofs
		)
	end
end

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
	local region
	if align <= 16 then
		region = malloc(size)
	else
		region = ffi.new(blockct(size, align))
	end
	shutdown(region, "anchor")
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
	heaplayout(rw)
	dummylayout(ro)
	dummylayout(wo)
	for o in pairs(all) do
		if o.init then
			if type(o.init) == "function" then
				if environment.parallel and o.proc_init then
					require("m3_mp").proc_init(function() o.ptr[0] = o.init() end)
				else
					o.ptr[0] = o.init()
				end
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

local function compilegraph()
	makequeries()
	makeresets()
	makemappings()
	G = assert(G:compile())
	-- pre-create instance so that instance creation can assume we always have a non-null instance
	-- available
	D.G_state.ptr.instance = G:newinstance(ffi.C.m3__mem_extalloc, mem.arena)
	D.G_state.ptr.mask = 0
end

---- Action buffers ------------------------------------------------------------

local function abuf_action(f, n)
	local buf = buffer.new()
	buf:put("local f = ...\nreturn function(state, idx)\nf(")
	if n > 0 then
		buf:put("state[idx]")
		for i=2, n do buf:putf(", state[idx+%d]", i-1) end
	end
	buf:putf(") return state[idx+%d](state, idx+%d)\nend\n", n, n+1)
	return load(buf)(f)
end

local function stmt_buffer(stmt, ...)
	return stmt:buffer(...)
end

local abuf_commit = load([[
	local state = ...
	local tail
	local function abuf_stop() end
	return function()
		local h, t = state[0], tail[0]
		if t > h then
			state[0] = t
			state[t+1] = abuf_stop
			state[1](state, 2)
		end
	end
]])(D.actions.state)

---- Transaction compilation ---------------------------------------------------

-- Read --------------------------------

local function newvar(ctx, value)
	local name = ctx:name()
	ctx.buf:putf("local %s = %s\n", name, value)
	return name
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
		ctx.buf:putf("%s%s = %s\n", result, code.index(ctx.uv, k), emitread(ctx, v))
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
	if #splat.values == 0 then
		return "nil"
	end
	local results = {}
	for i,v in ipairs(splat.values) do
		results[i] = emitread(ctx, v)
	end
	return table.concat(results, ", ")
end

function emit_read.literal(ctx, literal)
	return ctx.uv[literal]
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
	if value then
		ctx.buf:putf("if %s ~= nil then %s[0] = %s end\n", value, ptr, value)
	end
	if (not value) or ctype_isstruct(slot.ctype) then
		return ptr
	end
end

function emit_write.struct(ctx, struct, value)
	assert(value, "cannot mutate struct")
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
	if value then
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

local function emitbufcall(ctx, n, trampoline, value)
	ctx.uv.max = math.max
	local tail = ctx.uv[D.actions.tail.ptr]
	local state = ctx.uv[D.actions.state]
	ctx.buf:putf(
		"do local idx = max(%s[0], %s[0]) %s[0]=idx+%d idx=idx-%s[0] %s[idx+1]",
		state, tail, tail, n+1, state, state
	)
	for i=1, n do ctx.buf:putf(", %s[idx+%d]", state, i+1) end
	ctx.buf:putf(" = %s", ctx.uv[trampoline])
	if n > 0 then ctx.buf:putf(", %s", value) end
	ctx.buf:put(" end\n")
end

function emit_write.func(ctx, call, value)
	if call.buf then
		emitbufcall(ctx, call.n, abuf_action(call.func, call.n), value)
	else
		ctx.buf:putf("%s(%s)\n", ctx.uv[call.func], value)
	end
end

-- TODO: vectors should be stored in the actionbuf as vectors, and then expanded inside the action
-- (ie. stmt_buffer)
function emit_write.dml(ctx, dml, value)
	local args = ctx.uv[sqlite.statement(dml.sql)]
	if dml.n > 0 then args = string.format("%s, %s", args, value) end
	emitbufcall(ctx, 1+dml.n, abuf_action(stmt_buffer, 1+dml.n), args)
end

function emit_write.autoselect(ctx, asel, value)
	assert(value, "cannot mutate autoselect")
	local tag = gettag(asel.obj)
	if tag == "struct" then
		local values, cols = {}, {}
		for k,v in pairs(asel.obj.fields) do
			if gettag(v) ~= "size" then
				table.insert(cols, sqlite.escape(k))
				table.insert(values, v)
			end
		end
		if #cols == 0 then return end
		local name = ctx:name()
		local sql = {sqlite.sql("SELECT", unpack(cols)), sqlite.sql("FROM", asel.tab), asel.sql}
		ctx.uv.assert = assert
		ctx.buf:putf(
			"local %s = %s.sqlite3_stmt\n%s:bindargs(%s)\nassert(%s:step(), 'query returned no rows')\n",
			name, ctx.uv[sqlite.statement(sql)],
			name, value,
			name
		)
		for i=1, #cols do
			emitwrite(ctx, values[i], string.format("%s:double(%d)", name, i-1))
		end
		ctx.buf:putf("%s:reset()\n", name)
	elseif tag == "dataframe" then
		local schema = sqlite.schema(asel.tab)
		local cols, names, dummies = {}, {}, {}
		for name,col in pairs(asel.obj.columns) do
			if col.mark then
				if schema.columns[name] then
					table.insert(cols, col)
					table.insert(names, string.format("%s.%s", asel.tab, sqlite.escape(name)))
				elseif col.dummy ~= nil then
					table.insert(dummies, col)
				else
					error(string.format("column `%s.%s' is not in the schema and has no dummy value",
						asel.tab, name))
				end
			end
		end
		if #cols == 0 and #dummies == 0 then return end
		if #names == 0 then
			-- ensure we select at least one value.
			-- note that it would be faster to `SELECT COUNT(*)` here, but this case will almost
			-- never happen in practice so it's not worth optimizing for.
			names[1] = "0"
		end
		local ptr = ctx.uv[asel.obj.slot.ptr]
		local sql = {sqlite.sql("SELECT", unpack(names)), sqlite.sql("FROM", asel.tab), asel.sql}
		ctx.buf:putf(
			"for row in %s:rows(%s) do\nlocal idx = %s:alloc()\n",
			ctx.uv[sqlite.statement(sql)],
			value,
			ptr
		)
		for i,c in ipairs(cols) do
			ctx.buf:putf(
				"%s.%s[idx] = row:%s(%d)\n",
				ptr,
				c.name,
				cdata.isfp(c.ctype) and "double" or "int",
				i-1
			)
		end
		for _,c in ipairs(dummies) do
			ctx.buf:putf("%s.%s[idx] = %s\n", ptr, c.name, ctx.uv[c.dummy])
		end
		ctx.buf:put("end\n")
	else
		error(string.format("cannot autoselect into %s", tag))
	end
end

function emit_write.pipe(ctx, pipe, value)
	assert(value, "cannot mutate pipe")
	if pipe.map_f then
		local v = ctx:name()
		ctx.buf:putf("local %s = %s(%s)\n", v, ctx.uv[pipe.map_f], value)
		value = v
	end
	if pipe.filter_f then
		ctx.buf:putf("if %s(%s) then\n", ctx.uv[pipe.filter_f], value)
	end
	if pipe.channel then
		ctx.buf:putf("%s(%s)\n", ctx.uv[pipe.channel.send], value)
	else
		for _,sink in ipairs(pipe.sink) do
			emitwrite(ctx, sink, value)
		end
	end
	if pipe.filter_f then
		ctx.buf:put("end\n")
	end
end

function emit_write.ret(ctx, ret, value)
	assert(value, "cannot mutate return value")
	local idx = ret.idx or ctx.nret+1
	ctx.nret = math.max(ctx.nret, idx)
	ctx.buf:putf("local ret%d = %s\n", idx, value)
end

function emit_write.mutate(ctx, mut, value)
	ctx.buf:putf("%s(%s", ctx.uv[mut.f], emitwrite(ctx, mut.data))
	if value then
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
	local first = math.floor(ofs / D.heap_block)
	local last = math.floor((ofs+size-1) / D.heap_block)
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
		ctx.uv.mem_setmask = mem.setmask
		ctx.buf:putf("mem_setmask(0x%xull)\n", ctx.mmask)
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
			buf:put(", ret%d", i)
		end
		buf:put("\n")
	end
	buf:put("end\n")
	return load(buf)(unpack(uv))
end

-- TODO: this should take a query mask parameter and only create a new instance if the intersection
-- is nonzero
local function graph_instancefunc()
	return load(string.format([[
		local state, G, C, arena, setmask, iswritable = ...
		return function()
			if not iswritable(state.instance) then
				setmask(0x%xull)
				goto new
			end
			if state.mask == 0 then
				return state.instance
			end
			::new::
			local instance = G:newinstance(C.m3__mem_extalloc, arena, state.instance, state.mask)
			state.instance = instance
			state.mask = 0
			return instance
		end
	]], cdatamask(D.G_state)))(D.G_state.ptr, G, ffi.C, mem.arena, mem.setmask,
		mem.iswritable)
end

local tx_compiled_mt = {
	["m3$transaction"] = true,
	__call = function(self, ...) return self.func(...) end
}

local function compiletransactions()
	local graph_instance = graph_instancefunc()
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

local function init()
	dataflow()
	memlayout()
	compilegraph()
	if enabled("data") then
		event("data", traceobjs())
	end
	compiletransactions()
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

-- TODO: just build copylist directly here (put df_clearmask in m3_array.lua)
-- TODO: remove the mask:get(...) and make the fhk tensor type indexable with []
-- (use an __index function to dispatch indexing/methods, luajit will optimize the check away)
local function df_clearmask(ptr, mask)
	local idx = {}
	for i=0, #mask-1 do
		if mask:get(i) then
			table.insert(idx, i)
		end
	end
	return ptr:clear(idx)
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
		sqlite.datadef(tostring(dd))
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

local function transaction_autoselect(transaction, tab)
	return transaction_action(transaction, function(action)
		for _,t in ipairs(D.mapping_tables) do
			local name = tostring(t.name)
			local sqlt = sqlite.schema(name)
			if sqlt then
				local sql, bind = tab(name, sqlt)
				if sql then
					local obj = D.mapping[t]
					if gettag(obj) == "struct" then
						-- only select fields that exist in the schema.
						-- we don't need equivalent logic for dataframes, since they don't support
						-- only writing a subset of the columns.
						-- a partial write for a dataframe will fail at runtime.
						local new = struct()
						for k,v in pairs(obj.fields) do
							if sqlt.columns[k] then
								new.fields[k] = v
							end
						end
						obj = new
					end
					action(bind or splat(), autoselect(obj, name, sql))
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
		bind       = transaction_bind,
		read       = transaction_read,
		write      = transaction_write,
		mutate     = transaction_mutate,
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

local shared_input, shared_output
if environment.parallel then
	local mp = require "m3_mp"
	local function shpipe(dispatch)
		local source = pipe()
		local sink = pipe()
		sink.sink = source.sink
		source.channel = dispatch:channel(transaction():write(sink))
		return source
	end
	shared_input = function() return shpipe(mp.work) end
	shared_output = function() return shpipe(mp.main) end
else
	shared_input = pipe
	shared_output = pipe
end

local function tosink(x)
	if type(x) == "function" then
		return call(x)
	else
		return x
	end
end

local function connect(source, sink)
	-- source = todataobj(source) TODO?
	sink = tosink(sink)
	local tag = gettag(source)
	if tag == "pipe" then
		table.insert(source.sink, sink)
	else
		-- TODO: allow arbitrary data objects here, eg. if source is an fhk expression,
		-- then make a pipe that outputs a value whenever the expression changes
		error(string.format("TODO connect %s -> %s", gettag(source), gettag(sink)))
	end
	return sink
end

--------------------------------------------------------------------------------

return {
	G             = G,
	init          = init,
	memslot       = memslot,
	arg           = arg,
	ret           = ret,
	func          = func,
	pipe          = pipe,
	commit        = abuf_commit,
	shared_input  = shared_input,
	shared_output = shared_output,
	connect       = connect,
	transaction   = transaction,
	istransaction = istransaction,
	define        = define,
	defined       = defined,
	include       = include,
}
