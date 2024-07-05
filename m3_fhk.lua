local effect = require "m3_effect"
local data = require "m3_data"
local mem = require "m3_mem"
local buffer = require "string.buffer"
local ffi = require "ffi"
local fhk = require "fhk"
local C = ffi.C
local mem_frame = mem.state.f

local G = fhk.newgraph()
local do_define

-- this exists to make luajit consider state_cdata's address a constant, even if it's only
-- allocated when startup() is called.
-- see startup() for details.
local __state_cdata
local function state() return __state_cdata end

local function emitrelation(rel)
	if rel.skip then return "" end
	rel.inverse.skip = true
	local from = rel.from
	local to = rel.to
	if data.typeof(from) ~= "struct" then
		from, to = to, from
	end
	if data.typeof(from) == "struct" then
		return string.format([[
			model(`%s`) @{`%s`} = ##`%s`
			model(`%s`) @{`%s`} = {0}
			map `%s`#@{`%s`} `%s`#@{`%s`}
		]],
			from, to, to,
			to, from,
			from, to, to, from
		)
	else
		print("TODO", from, "<->", to)
		error("TODO")
	end
end

local relation_mt = {
	["m3$meta"] = {
		graph = emitrelation
	}
}

local function analysis_node_index(nodes, id)
	local node = assert(G:info(id), "index invalid node")
	nodes[id] = node
	return node
end

local function analysis_relation_index(relations, tab)
	local t = {}
	relations[tab] = t
	return t
end

-- keep this around separately from analysis for reflection
local queries = {}

local analysis = {
	nodes     = setmetatable({}, {__index=analysis_node_index}),
	relations = setmetatable({}, {__index=analysis_relation_index}),
	leaf      = {},
	masks     = {},
	graphfn   = {},
}

local function relation(from, to)
	local relations = analysis.relations[from]
	local rel = relations[to]
	if not rel then
		rel = setmetatable({from=from, to=to}, relation_mt)
		relations[to] = rel
		rel.inverse = relation(to, from)
	end
	return rel
end

local analysis_visit

local function analysis_check(id)
	local node = analysis.nodes[id]
	if node.mark then return end
	node.mark = true
	analysis_visit(id)
end

local function analysis_visitleaf(tab, col, desc)
	if analysis.leaf[desc] then
		return
	end
	analysis.leaf[desc] = {tab=tab, col=col}
	for name,gfn in pairs(analysis.graphfn) do
		local vname = string.format("%s#`%s`{%s}", tab, name, col)
		local var = G:getv(vname)
		if var then
			analysis_check(var)
			table.insert(gfn.original, desc)
			-- TODO: allow specifying fhk var id here?
			table.insert(gfn.updated, vname)
			effect.change()
		end
	end
end

analysis_visit = function(id)
	local ty = fhk.typeof(id)
	local back = G:back(id)
	local node = analysis.nodes[id]
	if #back == 0 and ty == "var" then
		local tab = analysis.nodes[node.table].name
		local desc
		if node.flags.tab then
			local var, meta = data.desc(node.name)
			desc = meta.len(var)
		elseif node.name:sub(1,1) == "@" then
			desc = relation(tab, node.name:sub(3,-2))
		else
			desc = data.index(tab, node.name)
		end
		effect.set(desc, "read", true)
		desc.node = id -- used in access to create write masks
		analysis_visitleaf(tab, node.name, desc)
	else
		local ety
		if ty == "var" then ety = "model" else ety = "var" end
		for _,e in ipairs(back) do
			analysis_check(e[ety])
		end
	end
	analysis_check(node.table)
end

local function analysis_define()
	error("attempt to modify graph after simulation start")
end

effect.effect(function()
	do_define = analysis_define
	for _,id in pairs(queries) do
		analysis_visit(id)
	end
	for tab, obj in pairs(data.data()) do
		for col, desc in data.pairs(obj) do
			if desc.read and #col>0 then
				analysis_visitleaf(tab, col, desc)
			end
		end
	end
end)

do_define = function(src)
	return G:define(src)
end

local function define(...)
	return do_define(...)
end

local function readfile(src)
	local fp = assert(io.open(src))
	local def = fp:read("*a")
	fp:close()
	define(def)
end

local function setmask(mask)
	state().mask:set(mask)
end

local function doquery(id)
	-- TODO (fhk): if we have not made a savepoint then we can just mask the current state,
	--             no need to allocate a new one
	-- TODO: check that it was succesful, reraise errors
	-- TODO: udata binding
	local state = state()
	-- TODO: only do this when mask is empty?
	--       or should fhk return back the original vmctx when state is empty?
	if true then
		state.vmctx = G:newstate(C.m3__mem_extalloc, mem_frame, state.vmctx, state.mask)
		state.mask:clear()
	end
	return state.vmctx:query(id)
end

local function query(...)
	-- TODO: group?
	local id = G:query("global")
	for _,f in ipairs({...}) do G:insert(id,f) end
	local trampoline = function() return doquery(id) end
	queries[trampoline] = id
	return trampoline
end

local function insert(query, expr)
	effect.change()
	return G:insert(queries[query], expr)
end

local function info(query)
	return G:info(queries[query])
end

local function mask()
	local id = G:mask()
	analysis.masks[id] = {}
	return id
end

local function insertmask(mask, node)
	table.insert(analysis.masks[mask], node)
end

local function graphfn(name)
	local gfn = analysis.graphfn[name]
	if not gfn then
		assert(do_define ~= analysis_define, "attempt to modify graph after simulation start")
		local original, updated = {}, {}
		gfn = {
			original = original,
			updated = updated,
			-- access required here to work around a circular import
			func = require("m3_access").forward(original, updated)
		}
		analysis.graphfn[name] = gfn
		effect.change()
	end
	return gfn.func
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

local function startup()
	local buf = buffer.new()
	for desc,leaf in pairs(analysis.leaf) do
		buf:put(data.meta(desc).graph(desc, leaf.tab, leaf.col), "\n")
	end
	G:define(buf)
	for mask, nodes in pairs(analysis.masks) do
		-- nodes are variables, insert their model here
		for _,node in ipairs(nodes) do
			local back = G:back(node)
			assert(#back == 1, "graph bug - leaf var should have exactly 1 model")
			G:insert(mask, back[1].model)
		end
	end
	-- TODO: put this behind a verbosity option
	assert(G:compile("g"))
	local st = mem.new(ffi.typeof([[
		struct {
			$ vmctx;
			$ mask;
		}
	]], G.ctptr, G.maskct), "vstack")
	st.vmctx = nil
	st.mask:clear()
	debug.setupvalue(state, 1, st)
	analysis = nil
end

return {
	define     = define,
	readfile   = readfile,
	setmask    = setmask,
	query      = query,
	insert     = insert,
	info       = info,
	mask       = mask,
	insertmask = insertmask,
	graphfn    = graphfn,
	typesuffix = typesuffix,
	startup    = startup
}
