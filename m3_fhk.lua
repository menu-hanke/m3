local effect = require "m3_effect"
local data = require "m3_data"
local mem = require "m3_mem"
local prototype = require "m3_prototype"
local buffer = require "string.buffer"
local ffi = require "ffi"
local fhk = require "fhk"
local C = ffi.C
local mem_frame = mem.state.f

local G
local state

---- analysis ------------------------------------------------------------------

local analysis = {
	nodes     = nil, -- lazy init
	unchecked = {},
	data      = {},
	roots     = {},
	graphfns  = {}
}

local function sim2data()
	for group, objs in pairs(data.data) do
		analysis.data[group] = {}
		for obj in pairs(objs) do
			-- currently all objs that can appear here are C objs with protos,
			-- but note that this may change and eg. lua tables could appear here in the future.
			local proto = assert(prototype.get(obj))
			for name in pairs(proto) do
				local check = {group=group, name=name, obj=obj}
				analysis.data[group][name] = check
				table.insert(analysis.unchecked, check)
			end
		end
	end
	-- need global here even if no data structure is mapped,
	-- since we need to map spaces here.
	if not analysis.data.global then
		analysis.data.global = {}
	end
end

local function nodetype(id)
	if id >= 0 then return "obj" else return "comp" end
end

local function othertype(ty)
	if ty == "obj" then return "comp" else return "obj" end
end

local function graph2data(id)
	local node = analysis.nodes[id]
	if node.mark then return end
	node.mark = true
	local ty = nodetype(id)
	local back = G:back(id)
	if #back == 0 and ty == "obj" then
		local group = analysis.nodes[node.group].name
		local agroup = analysis.data[group]
		if not agroup then
			-- this means a group must be included in the data, but there is no data structure
			-- mapped to it.
			-- note: it's possible to create a hidden data structure for it, and maybe that's
			-- what should be done in the future.
			-- but for now, just error.
			error(string.format("unmapped group required by graph: %s", group))
		end
		local check = agroup[node.name]
		if not check then
			check = {group=group, name=node.name}
			agroup[node.name] = check
			table.insert(analysis.unchecked, check)
			if group == "global" and data.data[node.name] then
				-- special case: this is a group
				check.obj = "group"
			elseif node.name:sub(1,1) == "@" then
				-- special case: this is a default map between groups
				-- TODO: currently a{b} stringifies as ab,
				--       fix the indices here after proper stringify is implemented in fhk.
				check.obj = "map"
				local othergroup = node.name:sub(2,-2)
				check.othergroup = othergroup
				local inverse = G:getobj(string.format("%s#@{%s}", othergroup, node.group))
				local invvar = inverse and analysis.data[othergroup][analysis.nodes[invese].name]
				if invvar and invvar.read then
					-- if we are already mapping the inverse, then don't map this.
					-- mapping the inverse will automatically map both.
					check.read = false
				end
			end
		end
		if check.read == nil then
			check.read = true
		end
	else
		local ety = othertype(ty)
		for _,e in ipairs(back) do
			graph2data(e[ety])
		end
	end
	graph2data(node.group)
end

local function analysis_node_index(nodes, id)
	local node = assert(G:info(id), "index invalid node")
	nodes[id] = node
	return node
end

local function mappingobj(var)
	local objs = data.data[var.group]
	if not next(objs, next(objs)) then
		-- only one object
		return next(objs)
	end
	-- otherwise: TODO
	-- this should probably either require some kind of annotation, or in some cases in can be
	-- deduced, eg. when you have a `frame` struct and a `vstack` struct, all out fields go
	-- in the vstack struct and all non-out fields go in the frame struct.
	-- but, in particular, this can NOT depend on the order, since the mappings aren't ordered
	-- anyway (nor should they be, really).
	-- probably the best way to do this is like:
	--   have annotation?
	--     yes --> use it
	--     no  --> have "natural" target?
	--       yes --> use it
	--       no  --> ambiguous target, throw error
	-- also note that finding the "natural" target can be a bit subtle, eg.
	--   * 1 frame struct, 1 vstack struct  -->  frame struct is the natural target for
	--                                           non-out mappings
	--   * 1 vstack struct  -->  natural target for all mappings
	--   * 2 frame structs, 1 vstack struct  -->  vstack is the natural target for out mappings,
	--                                            no natural target for non-out mappings
	-- the general algorithm goes something like:
	--   * classify each object as "in" or "inout"
	--   * for an out mapping: there must be a unique inout object
	--   * for non-out mapping: either use an unique in object, or a unique inout object
	--     if there are zero in objects
	error("TODO")
end

local function analysis_effect()
	if not analysis.nodes then
		-- first call.
		-- lazy initting nodes specifically doesn't have any particular purpose here,
		-- it's just done to detect the first call.
		-- any other field that isn't used until effects could be used as well.
		analysis.nodes = setmetatable({}, {__index=analysis_node_index})
		sim2data()
		for _,id in ipairs(analysis.roots) do
			graph2data(id)
		end
	end
	-- common part, we go here on every effect iteration.
	while true do
		local uidx = #analysis.unchecked
		if uidx == 0 then break end
		local check = analysis.unchecked[uidx]
		analysis.unchecked[uidx] = nil
		-- check if any graph function outputs this, and if so then recursively include
		-- the dependencies for the updated version.
		for name,gfn in pairs(analysis.graphfns) do
			local vname = string.format("%s#`%s`{%s}", check.group, name, check.name)
			local var = G:getobj(vname)
			if var then
				check.write = true
				-- TODO(fhk): addfield(id, objid) should work
				G:addfield(gfn.query, vname)
				table.insert(gfn.update, check)
				graph2data(var)
			end
		end
		-- map it if unmapped. here it doesn't matter yet if it's read, write, both, or neither.
		-- either it's data and it's already mapped or it's not data and then it must be read.
		-- this must be done inside effect() and not delayed until startup, since other effects
		-- may depend on the changes made here to the mapped data structure.
		if not check.obj then
			local obj = mappingobj(check)
			check.obj = obj
			local proto = prototype.get(obj)
			assert(not proto[check.name]) -- else analysis.data[group][name] = map
			-- TODO: how to select the data type properly?
			-- probably add an api that allows the user to annotate that IF a field is included
			-- in the prototype, THEN it has a specific datatype.
			-- ie. separate the datatype declaration and the part that includes it in the prototype.
			proto { [check.name] = "double" }
		end
	end
end

---- definitions ---------------------------------------------------------------

local function getG()
	if not G then G = fhk.newgraph() end
	return G
end

local function define(src)
	getG():define(src)
end

local function readfile(src)
	local fp = assert(io.open(src))
	local def = fp:read("*a")
	fp:close()
	define(def)
end

local function doquery(id)
	-- TODO: track changes and don't recreate the entire state here.
	--       this is why state is shared among all query functions.
	-- TODO: check that it was succesful, reraise errors
	-- TODO: udata binding
	state[0] = G:newstate(C.m3__mem_extalloc, mem_frame)
	return state[0]:query(id)
end

local function query(...)
	-- TODO: group?
	local G = getG()
	local id = G:query("global")
	for _,f in ipairs({...}) do G:addfield(id,f) end
	table.insert(analysis.roots, id)
	return function() return doquery(id) end
end

local function graphfn(name)
	local gfn = analysis.graphfns[name]
	if not gfn then
		effect.effect(analysis_effect)
		gfn = {
			query = G:query("global"),
			trampoline = load("local f return function() return f() end")(),
			update = {}
		}
		analysis.graphfns[name] = gfn
	end
	return gfn.trampoline
end

---- mapping -------------------------------------------------------------------

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

local function datatype(x)
	local ok, dtype = pcall(function() return x["m3$type"] end)
	if ok then return dtype end
end

local function mapgroup(buf, var)
	-- TODO: check all and make sure the lengths agree.
	-- (this needs support from fhk side to call host lua functions)
	local obj = toref(next(data.data[var.name]))
	local dtype = datatype(obj)
	buf:putf("const(global) `%s` ->", var.name)
	if dtype == "struct" then
		buf:put("1")
	elseif dtype == "dataframe" then
		buf:putf(
			"{..lds.u32(0x%x)}",
			ffi.cast("intptr_t", ffi.cast("void *", obj)) + ffi.offsetof(obj, "num")
		)
	else
		error("TODO")
	end
	buf:put("\n")
end

local function mapmap(buf, var)
	local a, b = var.group, var.othergroup
	-- TODO: the object used here must match mapgroup.
	-- this needs more careful handling when there's multiple mapped objects
	local da, db = datatype(next(data.data[a])), datatype(next(data.data[b]))
	if db == "struct" then
		da, db = db, da
		a, b = b, a
	end
	if da == "struct" then
		buf:putf("const(`%s`) @{`%s`} -> ##`%s`\n", a, b, b)
		buf:putf("const(`%s`) @{`%s`} -> {0}\n", b, a)
		buf:putf("map `%s`#@{`%s`} `%s`#@{`%s`}\n", a, b, b, a)
	else
		error(string.format("TODO map %s-%s", da, db))
	end
end

local function mapfield(buf, var)
	local obj = toref(var.obj)
	local proto = prototype.get(obj)
	if not proto then
		error("TODO") -- reflect?
	end
	local ftype = ctype2fhk[tonumber(proto[var.name].ctype)]
	if not ftype then
		-- TODO: nested structs etc. more complex cases needs reflect.
		return
	end
	local dtype = datatype(obj)
	if dtype == "struct" then
		buf:putf(
			"const(`%s`) `%s` -> lds.%s(0x%x)\n",
			var.group,
			var.name,
			ftype,
			ffi.cast("uintptr_t", ffi.cast("void *", obj)) + ffi.offsetof(obj, var.name)
		)
	elseif dtype == "dataframe" then
		local base = ffi.cast("intptr_t", ffi.cast("void *", obj))
		buf:putf(
			"const(global) `%s`#`%s` -> ldv.%s(0x%x, lds.u32(0x%x))\n",
			var.group,
			var.name,
			ftype,
			base + ffi.offsetof(obj, var.name),
			base + ffi.offsetof(obj, "num")
		)
	else
		error("TODO")
	end
end

local function mapread(buf, var)
	if var.obj == "group" then
		mapgroup(buf, var)
	elseif var.obj == "map" then
		mapmap(buf, var)
	else
		mapfield(buf, var)
	end
end

---- codegen -------------------------------------------------------------------

local function nop() end

local function gfnfunc(gfn)
	if #gfn.update == 0 then return nop end
	local buf = buffer.new()
	local args = {doquery}
	buf:put("local doquery")
	local oname = {}
	for _,u in ipairs(gfn.update) do
		if not oname[u.obj] then
			table.insert(args, u.obj)
			oname[u.obj] = string.format("o%d", #args)
			buf:putf(", %s", oname[u.obj])
		end
	end
	buf:put(" = ...\n")
	buf:put("return function()\n")
	buf:put("local v1")
	for i=2, #gfn.update do buf:putf(", v%d", i) end
	buf:putf("= doquery(%d)\n", gfn.query)
	for i,u in ipairs(gfn.update) do
		local ok, what = pcall(function() return u.obj["m3$type"] end)
		if ok and what == "struct" then
			buf:putf("%s.%s = v%d\n", oname[u.obj], u.name, i)
		elseif ok and what == "dataframe" then
			buf:putf("%s:overwrite('%s', v%d.p)\n", oname[u.obj], u.name, i)
		else
			error("TODO non-cdata")
		end
	end
	buf:put("end")
	return assert(load(buf))(unpack(args))
end

local function compilegfn(gfn)
	debug.setupvalue(gfn.trampoline, 1, gfnfunc(gfn))
end

--------------------------------------------------------------------------------

local function startup()
	if G then
		local buf = buffer.new()
		for _,vars in pairs(analysis.data) do
			for _,var in pairs(vars) do
				if var.read then
					mapread(buf, var)
				end
			end
		end
		define(buf)
		for _,gfn in pairs(analysis.graphfns) do
			compilegfn(gfn)
		end
		-- TODO: put this behind a verbosity option
		local info = assert(G:compile("g"))
		print(info.graph)
		state = mem.new(G.ctype_ptr, "vstack")
		state[0] = nil
	end
	analysis = nil
end

return {
	define   = define,
	readfile = readfile,
	query    = query,
	graphfn  = graphfn,
	startup  = startup
}
