local fhk = require "m3_fhk"
local prototype = require "m3_prototype"
local buffer = require "string.buffer"

-- group -> objects.
local data = {}

local function group(name)
	local group = data[name]
	if not group then
		group = {}
		data[name] = group
	end
	return group
end

local function register(o, g)
	group(g or "global")[o] = true
	return o
end

local function compilesettab(o)
	return load([[
		local o = ...
		return function(v) return o["m3$settab"](o,v) end
	]])(o)
end

local function compilevar(v)
	local ok, what = pcall(function() return v["m3$type"] end)
	if ok and what then
		if what == "dataframe" or what == "struct" then
			return compilesettab(v)
		else
			error(string.format("TODO: cdata %s", what))
		end
	else
		error("TODO non-cdata")
	end
end

local function compilegroup(vars)
	if #vars == 1 then return compilevar(vars[1]) end
	local buf = buffer.new()
	buf:put("local var1 ")
	for i=2, #vars do buf:putf(", var%d", i) end
	buf:put(" = ...\n")
	buf:put("return function(x)\n")
	local funcs = {}
	for i=1, #vars do
		buf:putf("var%d(x)\n", i)
		funcs[i] = compilevar(vars[i])
	end
	buf:put("end")
	return assert(load(buf))(unpack(funcs))
end

local function compile()
	local groups = {}
	for g,v in pairs(data) do
		local vars = {}
		for defs in pairs(v) do table.insert(vars, defs) end
		table.insert(groups, {name=g, vars=vars})
	end
	if #groups == 0 then return function() end end
	local buf = buffer.new()
	buf:put("local reads = ...\n")
	for i=1, #groups do
		buf:putf("local read_%d = reads[%d]\n", i, i)
	end
	buf:put("return function(x)\n")
	-- TODO: allow empty input if no input is required
	local reads = {}
	for i=1, #groups do
		if groups[i].name == "global" then
			buf:putf("read_%d(x.global or x)\n", i)
		else
			buf:putf("read_%d(x[%q])\n", i, groups[i].name)
		end
		reads[i] = compilegroup(groups[i].vars)
	end
	buf:put("end")
	return assert(load(buf))(reads)
end

local read

-- lazy init `read` only if it's used.
local function trampoline(x)
	debug.setupvalue(read, 1, compile())
	return read(x)
end

read = function(x) return trampoline(x) end

-- TODO: use udata for group sizes and set eagerly.
-- this allows verifying that group lenghts match if it consists of etc. multiple parallel
-- dataframes and/or arrays.
local function startup()
	local buf = buffer.new()
	for g,vs in pairs(data) do
		local havegroup
		for o in pairs(vs) do
			local ok, what = pcall(function() return o["m3$type"] end)
			if ok and what then
				if what == "struct" then
					if not havegroup then
						havegroup = true
						if g ~= "global" then
							buf:putf("model(global) `%s` -> {..1}\n", g)
						end
					end
					-- TODO: use udata instead of model when it's implemented in fhk
					-- TODO: when parametrized commands are implemented use them instead here
					local p = prototype.get(o)
					if not p then error("TODO no proto, should reflect") end
					for field, ctype in pairs(p) do
						-- TODO: actually load it
						-- TODO: model(group) : name -> load.ty(addr)
						-- buf:putf("model(`%s`) `%s` -> load.f64(%d
						buf:putf("model(`%s`) `%s` -> 0\n", g, field)
					end
				elseif what == "dataframe" then
					-- TODO.
					-- for now just ignore it.
				else
					error(string.format("TODO: cdata %s", what))
				end
			else
				-- TODO: this is possible to implement but requires both stack swapping
				-- and support from m3_fhk.
				-- (it doesn't necessarily require changes to fhk but it's better to implement
				--  on the fhk side since the lua driver already has a stack swapping
				--  implementation anyway)
				error("TODO non-cdata")
			end
		end
	end
	fhk.define(buf)
end

return {
	register = register,
	read     = read,
	startup  = startup
}
