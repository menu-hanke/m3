local buffer = require "string.buffer"

-- group -> objects.
-- TODO: this will be what is mapped into fhk.
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

return {
	register = register,
	read     = read
}
