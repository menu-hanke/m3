local array = require "m3_array"
local cdata = require "m3_cdata"
local cdef = require "m3_cdef"
local de = require "m3_debug"
local environment = require "m3_environment"
local mem = require "m3_mem"
local fhk = require "fhk"
local buffer = require "string.buffer"
local ffi = require "ffi"
require "table.clear"

local G = fhk.newgraph()

local D = {
	transactions   = {},
	mapping        = {}, -- node -> data
	mapping_tables = {}, -- list of table nodes (rebuilt each iteration)
	expr           = {}
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
local function struct()
	return setmetatable({ fields = {}  }, struct_mt)
end

-- dataframe column
local column_mt = newmeta "column"
local function column(df, name, ctype)
	return setmetatable({ df=df, name=name, ctype=ctype }, column_mt)
end

local function dataframe_ctype(df)
	local proto = {}
	for name,col in pairs(df.columns) do
		if col.mark then -- set in memlayout
			col.ctype = ffi.typeof(col.ctype or "double")
			table.insert(proto, {name=name, ctype=col.ctype})
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

local function visit(o, f, ...)
	local tag = gettag(o)
	if tag == "struct" then
		return visit(o.fields, f, ...)
	elseif tag == "dataframe" then
		f(o.slot, ...)
		return visit(o.columns, f, ...)
	elseif tag == "size" then
		f(o.data, ...)
	elseif tag == "splat" then
		return visit(o.values, f, ...)
	elseif tag == "mutate" then
		return visit(o.data, f, ...)
	elseif tag == "pipe" then
		visit(o.sink, f, ...)
	elseif tag == "dynamic" and o.visit then
		error("TODO")
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

D.data = struct()
D.G_state = memslot { ctype = "struct { void *instance; uint64_t mask; }" }

---- Dataflow ------------------------------------------------------------------
-- fixpoint rules:
-- * every node that is an input to a transaction is visited
-- * every parent node of a visited node is visited
-- * `map_table` is called for each visited table
-- * `map_var` is called for each visited variable that has no models

local function map_tab(node)
	local name = tostring(node.name)
	local mapping = D.data.fields[name]
	if not mapping then
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
		D.data.fields[tostring(node.name)] = mapping
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
end

local function islit(x)
	return type(x) == "number" or type(x) == "string"
end

local function resolve(o)
	if type(o) == "string" then
		return expr(o)
	elseif type(o) == "function" then
		return resolve(o())
	elseif type(o) == "table" then
		local tag = gettag(o)
		if tag == "splat" then
			local values = {}
			for i,v in ipairs(o.values) do
				values[i] = resolve(v)
			end
			return splat(values)
		elseif tag then
			return o
		else
			local s = struct()
			for k,v in pairs(o) do
				if not islit(k) then
					k = resolve(k)
				end
				s.fields[k] = resolve(v)
			end
			return s
		end
	else
		return literal(o)
	end
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
			node.m3_default = G:var(node.tab, "default'$", false, node.name)
			table.insert(dcx.backlog, node.m3_default)
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
	mem.anchor(region)
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

local function markslot(o,s)
	local tag = gettag(o)
	if tag == "memslot" then
		s[o] = true
	elseif tag == "column" then
		o.mark = true -- for dataframe_ctype
		markslot(o.df.slot,s)
	end
end

local function memlayout()
	local reads, writes = {}, {}
	reads[D.G_state] = true
	writes[D.G_state] = true
	for _,tx in ipairs(D.transactions) do
		for _,a in ipairs(tx.actions) do
			walk(a.input, markslot, reads)
			walk(a.output, markslot, writes)
		end
	end
	for _,data in pairs(D.mapping) do
		walk(data, markslot, reads)
	end
	local all = {}
	for _,t in ipairs({reads,writes}) do
		for x in pairs(t) do
			all[x] = true
		end
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
		local t
		if reads[o] and (writes[o] or ctype_isstruct(o.ctype)) then
			t = rw
		elseif reads[o] then
			t = ro
		elseif writes[o] then
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
			o.ptr[0] = o.init
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

-- function map_obj.dynamic(obj, tab, var)
-- 	local map = assert(obj.map, "dynamic object doesn't implement map")
-- 	return map(tab, var)
-- end

local function map_default(obj, var)
	local default = var.m3_default
	if not (default and default.m3_models) then return end
	assert(obj.ctype, "default without ctype")
	-- TODO: allow custom dummy values
	local testdummy = cdata.dummy(obj.ctype)
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
	D.G_state.ptr.instance = G:newinstance(ffi.C.m3__mem_extalloc, mem.stack)
	D.G_state.ptr.mask = 0
end

---- Transaction compilation ---------------------------------------------------

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
	local ptr = upvalname(ctx, slot.ptr)
	if value then
		ctx.buf:putf("if %s ~= nil then %s[0] = %s end\n", value, ptr, value)
	end
	if (not value) or ctype_isstruct(slot.ctype) then
		return ptr
	end
end

function emit_write.struct(ctx, struct, value)
	assert(value, "cannot mutate struct")
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
	if value then
		ctx.buf:putf("%s:overwrite('%s', %s)\n", upvalname(ctx, col.df.slot.ptr), col.name, value)
	else
		error("TODO")
	end
end

function emit_write.dataframe(ctx, df, value)
	local ptr = upvalname(ctx, df.slot.ptr)
	if value then
		ctx.buf:putf("if %s ~= nil then %s:settab(%s) end\n", value, ptr, value)
	end
	return ptr
end

function emit_write.splat(ctx, splat, value)
	assert(value, "cannot mutate splat")
	local names = {}
	for i=1, #splat.values do
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
	assert(value, "cannot mutate pipe")
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

function emit_write.ret(ctx, ret, value)
	assert(value, "cannot mutate return value")
	local idx = ret.idx or ctx.nret+1
	ctx.nret = math.max(ctx.nret, idx)
	ctx.buf:putf("local ret%d = %s\n", idx, value)
end

function emit_write.mutate(ctx, mut, value)
	ctx.buf:putf("%s(%s", upvalname(ctx, mut.f), emitwrite(ctx, mut.data))
	if value then
		ctx.buf:putf(", %s", value)
	end
	ctx.buf:put(")\n")
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
		narg   = 0,
		nret   = 0
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
	local ctx = newemit()
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
	for _,a in ipairs(tx.actions) do
		emitwrite(ctx, a.output, emitread(ctx, a.input))
	end
	local buf = buffer.new()
	local uv = emitupvalues(ctx.uv, buf)
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
	]], cdatamask(D.G_state)))(D.G_state.ptr, G, ffi.C, mem.stack, mem.setmask,
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

local function startup()
	dataflow()
	memlayout()
	compilegraph()
	compiletransactions()
	de.event("data", D)
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

local function transaction_call(transaction, f, ...)
	return transaction_action(transaction, {
		input = splat({...}),
		output = func(f)
	})
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

local function transaction_bind(transaction, name, value)
	error("TODO(fhk): query-vset variables")
end

local transaction_mt = {
	["m3$transaction"] = true,
	__index = {
		update = transaction_update,
		insert = transaction_insert,
		delete = transaction_delete,
		define = transaction_define,
		call   = transaction_call,
		bind   = transaction_bind,
		read   = transaction_read,
		write  = transaction_write
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

local function read(...)
	return transaction():read(...)
end

local function write(...)
	return transaction():write(...)
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

local function tosink(x)
	if type(x) == "function" then
		return func(x)
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
	elseif tag == "dynamic" and source.connect then
		return source:connect(sink)
	else
		-- TODO: allow arbitrary data objects here, eg. if source is an fhk expression,
		-- then make a pipe that outputs a value whenever the expression changes
		error(string.format("TODO connect %s -> %s", gettag(source), gettag(sink)))
	end
	return sink
end

local function globals()
	return D.data
end

--------------------------------------------------------------------------------

return {
	startup       = startup,
	memslot       = memslot,
	pipe          = pipe,
	shared        = shared,
	connect       = connect,
	transaction   = transaction,
	istransaction = istransaction,
	read          = read,
	write         = write,
	define        = define,
	defined       = defined,
	include       = include,
	globals       = globals,
}
