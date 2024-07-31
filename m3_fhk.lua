local access = require "m3_access"
local code = require "m3_code"
local data = require "m3_data"
local effect = require "m3_effect"
local mem = require "m3_mem"
local fhk = require "fhk"
local ffi = require "ffi"
local buffer = require "string.buffer"
require "table.clear"
local stack = mem.stack

local function graph_node(graph, id)
	local node = graph.nodes[id]
	if not node then
		node = assert(graph.G:info(id), "index invalid node")
		graph.nodes[id] = node
	end
	return node
end

local graph_analysis_check

local function graph_analysis_visitgfn(graph, tab, col, desc)
	if graph.gfn_visited[desc] then
		return
	end
	graph.gfn_visited[desc] = true
	for name, gfn in pairs(graph.functions) do
		local vname = string.format("%s#`%s`{%s}", tab, name, col)
		local var = graph.G:getv(vname)
		if var then
			graph_analysis_check(graph, var)
			table.insert(gfn.original, desc)
			-- TODO: allow specifying fhk var id here?
			table.insert(gfn.updated, vname)
			effect.change()
		end
	end
end

local function graph_newdata(graph, id, desc)
	local node = graph.nodes[id]
	local tab = graph.nodes[node.table].name
	local default = graph.G:getv(string.format("%s#default{%s}", tab, node.name))
	local name = node.name
	if default and #graph.G:back(default) > 0 then
		name = string.format("data{%s}", name)
	else
		-- only consider defaults that are actually defined.
		default = nil
	end
	-- can't define dummy here yet because the dummy value may be defined later during effects.
	graph.data[desc] = {
		node    = id,
		map     = data.meta(desc).map(desc, tab, name),
		default = default,
		tab     = tab,
		col     = node.name
	}
end

local function graph_relations_index(relations, from)
	local t = {}
	relations[from] = t
	return t
end

local graph_relations_mt = {
	__index = graph_relations_index
}

local function relation_map_(rel)
	if rel.skip then return "" end
	rel.inverse.skip = true
	local from = rel.from
	local to = rel.to
	if data.typeof(from) ~= "struct" then
		from, to = to, from
	end
	if data.typeof(from) == "struct" then
		return string.format([[
			model(%s) @{%s} = ##%s
			model(%s) @{%s} = {0}
			map %s#@{%s} %s#@{%s}
		]],
			from, to, to,
			to, from,
			from, to, to, from
		)
	else
		print(string.format("TODO: %s (%s) <-> %s (%s)", from, data.typeof(from), to, data.typeof(to)))
		error("TODO")
	end
end

local function relation_map(rel)
	return function() return relation_map_(rel) end
end

local relation_mt = {
	data = {
		map = relation_map
	}
}

local function graph_relation(graph, from, to)
	local relations = graph.relations[from]
	local rel = relations[to]
	if not rel then
		rel = setmetatable({from=from, to=to}, relation_mt)
		relations[to] = rel
		rel.inverse = graph_relation(graph, to, from)
	end
	return rel
end

local function graph_analysis_visit(graph, id)
	local ty = fhk.typeof(id)
	local back = graph.G:back(id)
	local node = graph_node(graph, id)
	if #back == 0 and ty == "var" then
		local tab = graph_node(graph, node.table).name
		local desc
		if node.flags.tab then
			desc = data.data(node.name)
		elseif node.name:sub(1,1) == "@" then
			desc = graph_relation(graph, tab, node.name:sub(3,-2))
		else
			desc = data.data(tab)[node.name]
		end
		effect.change()
		graph_newdata(graph, id, desc)
		graph_analysis_visitgfn(graph, tab, node.name, desc)
	else
		local ety
		if ty == "var" then ety = "model" else ety = "var" end
		for _,e in ipairs(back) do
			graph_analysis_check(graph, e[ety])
		end
	end
	graph_analysis_check(graph, node.table)
end

graph_analysis_check = function(graph, id)
	local node = graph_node(graph, id)
	if node.mark then return end
	node.mark = true
	graph_analysis_visit(graph, id)
end

local function graph_analyze(graph)
	for _,id in ipairs(graph.queries) do
		graph_analysis_visit(graph, id)
	end
	for tab, col, desc in data.triples() do
		if (not graph.gfn_visited[desc]) and access.get(desc):match("r") then
			-- only descriptors with r or rw access matter here.
			-- if a descriptor has w but not r access, then no one, including fhk itself,
			-- will ever observe the value, so we don't need the mask/gfns either.
			graph_analysis_visitgfn(graph, tab, col, desc)
		end
	end
end

local function graph_query(graph)
	local id = graph.G:query("global")
	table.insert(graph.queries, id)
	return id
end

local function graph_fn(graph, name)
	local gfn = graph.functions[name]
	if not gfn then
		local original, updated = {}, {}
		gfn = {
			original = original,
			updated  = updated,
			func     = access.forward(access.splat(original), access.splat(updated))
		}
		graph.functions[name] = gfn
	end
	return gfn.func
end

-- return false if `x` is a valid variable name but it has no models.
-- otherwise return true.
local function graph_isexpr(graph, x)
	local ok, id = pcall(graph.G.getv, graph.G, x)
	if (not ok) or (id and #graph.G:back(id) > 0) then
		-- either the parse failed, or it's a variable with defined models
		return true
	end
	-- it's syntactically a valid variable, but it has no models.
	-- check if it has a corresponding default.
	-- TODO(fhk): add an fhk.split(name) function - this match doesn't work when the table name
	-- contains a `#`.
	local tab, col = x:match("^([^#]+)#(.+)$")
	local ok, id = pcall(graph.G.getv, graph.G, string.format("%s#default{%s}", tab, col))
	-- if the default exists, then the original is an expression, otherwise the it can be
	-- forwarded to data.
	return ok and id
end

local function graph_mapping(graph, desc)
	local d = graph.data[desc]
	return d and d.node
end

-- TODO fhk lexer should support streaming
local function graph_readfile(graph, name)
	local fp = assert(io.open(name, "r"))
	graph.G:define(fp:read("*a"))
	fp:close()
end

local function mask_write(mask)
	mask.write = load([[
		local getmask
		return function(mask)
			getmask():set(mask)
		end
	]])()
	access.read(mask.slot)
	return access.use(mask.write, access.write(mask.slot))
end

local mask_mt = {
	data = {
		type  = "graph.mask",
		write = mask_write
	}
}

local function graph_write(graph)
	graph.write = load([[
		local C = require("ffi").C
		local stack = require("m3_mem").stack
		local G, getmask, vmctx = ...
		return function()
			vmctx[0] = G:newstate(C.m3__mem_extalloc, stack, vmctx[0], getmask())
			getmask():clear()
			return vmctx[0]
		end
	]])()
	return access.use(
		access.capture(graph.write),
		access.read(graph.vmctx),
		access.write(graph.mask, graph.vmctx)
	)
end

local function definedummy(buf, desc, info)
	local tab, col = info.tab, info.col
	local dummy = data.dummy(desc)
	if not dummy then
		error(string.format(
			"%s#default{%s} is defined, but %s#%s has no dummy value",
			tab, col, tab, col
		))
	end
	if dummy ~= dummy then
		buf:putf("model(%s) is.dummy{%s} = isnan(data{%s})\n", tab, col, col)
	else
		if type(dummy) == "cdata" then
			dummy = string.format("0x%x", dummy)
		end
		buf:putf("model(%s) is.dummy{%s} = data{%s}=%s\n", tab, col, col, dummy)
	end
	buf:putf([[
		model(%s) %s = default{%s} where is.dummy{%s}
		model(%s) %s = data{%s} where not is.dummy{%s}
	]],
		tab, col, col, col,
		tab, col, col, col
	)
end

local function graph_compile_getmask(graph)
	local slot = graph.mask.slot
	if not slot.ptr then return end
	local ctype = graph.G.maskct
	if ffi.sizeof(ctype) <= ffi.sizeof("int64_t") then
		local ptr = ffi.cast(ffi.typeof("$*", ctype), slot.ptr)
		return load("local ptr = ... return function() return ptr end")(ptr)
	else
		local ptr = ffi.cast(ffi.typeof("$**", ctype), slot.ptr)
		ptr[0] = stack:new(ctype)
		return load(string.format([[
			local ptr, iswritable, stack, ctype, ffi_copy = ...
			return function()
				if not iswritable(ptr[0]) then
					local newmask = stack:new(ctype)
					ffi_copy(newmask, ptr[0], %d)
					ptr[0] = newmask
				end
				return ptr[0]
			end
		]]), ffi.sizeof(ctype))(ptr, mem.iswritable, mem.stack, ctype, ffi.copy)
	end
end

local function graph_compile(graph)
	if not graph.write then
		-- graph is never queried
		return
	end
	local buf = buffer.new()
	for desc, info in pairs(graph.data) do
		if info.default then
			definedummy(buf, desc, info)
		end
		local map = info.map
		if type(map) == "function" then
			map = map()
		end
		buf:put(map, "\n")
	end
	graph.G:define(buf)
	-- TODO: put this behind a verbosity option
	assert(graph.G:compile("g"))
	local getmask = graph_compile_getmask(graph)
	if getmask then
		getmask():clear()
		if graph.mask.write then
			debug.setupvalue(graph.mask.write, 1, getmask)
		end
	end
	code.setupvalues(graph.write, {
		G       = graph.G,
		getmask = getmask,
		vmctx   = ffi.cast(ffi.typeof("$*", graph.G.ctptr), graph.vmctx.ptr)
	})
end

local graph_mt = {
	data = {
		write = graph_write
	},
	__index = {
		query    = graph_query,
		fn       = graph_fn,
		isexpr   = graph_isexpr,
		mapping  = graph_mapping,
		readfile = graph_readfile
	}
}

local function newgraph()
	local graph = setmetatable({
		G           = fhk.newgraph(),
		nodes       = {},
		relations   = setmetatable({}, graph_relations_mt),
		queries     = {},
		functions   = {},
		gfn_visited = {},
		data        = {}
	}, graph_mt)
	graph.vmctx = mem.slot { region=graph, ctype="void *" }
	graph.mask = setmetatable({slot=mem.slot { region=graph, ctype="int64_t" }}, mask_mt)
	effect.effect(function() graph_analyze(graph) end)
	return graph
end

local ctype2fhk = {}
for c,f in pairs {
	uint8_t = "u8",   int8_t = "i8",
	uint16_t = "u16", int16_t = "i16",
	uint32_t = "u32", int32_t = "i32",
	uint64_t = "u64", int64_t = "i64",
	float    = "f32", double  = "f64"
} do ctype2fhk[tonumber(ffi.typeof(c))] = f end

local function typesuffix(ct)
	return ctype2fhk[tonumber(ct)]
end

local graph = newgraph()

local function startup()
	graph_compile(graph)
	table.clear(graph)
end

return {
	graph      = graph,
	typesuffix = typesuffix,
	startup    = startup
}
