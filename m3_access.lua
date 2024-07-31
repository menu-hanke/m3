local data = require "m3_data"
local effect = require "m3_effect"
local event = require "m3_event"
local buffer = require "string.buffer"
local dispatch = event.dispatch

-- circular import workaround
local function getgraph() return require("m3_fhk").graph end

local cache = {
	read             = {},
	write            = {},
	read_trampoline  = {},
	write_trampoline = {},
	read_stmt        = {},
	write_stmt       = {}
}

local function effect_newindex(t, k, v)
	effect.change()
	rawset(t, k, v)
end

local marker_mt = {__index=effect_newindex}

local function ismarker(x,m)
	return getmetatable(x) == marker_mt and (x.m == m or not m)
end

local function patch(x, f)
	return setmetatable({x=x, f=f, m="patch"}, marker_mt)
end

local function defer(f)
	return patch(nil, f)
end

local function capture(f)
	return setmetatable({f=f, m="capture"}, marker_mt)
end

local function splat(x)
	return setmetatable({x=x, m="splat"}, marker_mt)
end

local function sink(values)
	return setmetatable({m="sink", values=values}, marker_mt)
end

local function use(x, ...)
	return setmetatable({m="use", x=x, ...}, marker_mt)
end

local expand

local function read(...)
	local values = {...}
	if #values == 1 and ismarker(values[1], "splat") then
		values = values[1].x
	end
	local v1 = #values == 1 and values[1]
	if type(v1) == "function" then return v1 end
	if cache.read_stmt[v1] then return cache.read_stmt[v1] end
	local trampoline = load("local target return function() return target() end")()
	local stmt = {}
	cache.read_trampoline[trampoline] = effect.effect(function()
		return {stmt=stmt, values=expand("read", values, stmt)}
	end)
	if v1 then cache.read_stmt[v1] = trampoline end
	return trampoline
end

local function write(...)
	local values = {...}
	local splat = #values == 1 and ismarker(values[1], "splat")
	if splat then values = values[1].x end
	local v1 = #values == 1 and values[1]
	if type(v1) == "function" then return v1 end
	if cache.write_stmt[v1] then return cache.write_stmt[v1] end
	local args
	if splat then
		args = "..."
	else
		args = {}
		for i=1, #values do args[i] = string.format("v%d", i) end
		args = table.concat(args, ",")
	end
	local trampoline = load(string.format(
		"local target return function(%s) return target(%s) end",
		args, args
	))()
	local stmt = {use={}}
	cache.write_trampoline[trampoline] = effect.effect(function()
		return {stmt=stmt, values=expand("write", values, stmt)}
	end)
	if v1 then cache.write_stmt[v1] = trampoline end
	return trampoline
end

local function writedesc(x, stmt)
	if ismarker(x, "use") then
		for _,v in ipairs(x) do
			stmt.use[v] = true
		end
	else
		stmt.use[x] = true
	end
	local graph = getgraph()
	if (not cache.write[graph.mask]) and graph:mapping(x) then
		write(graph.mask)
	end
end

local function expandstr(access, s, stmt)
	if access == "read" then
		if not stmt.query_expr then
			stmt.query_expr = {}
		end
		local qexpr = stmt.query_expr[s]
		if qexpr == nil then
			local graph = getgraph()
			if graph:isexpr(s) then
				if not stmt.query then
					stmt.query = graph:query()
					stmt.query_vmctx = write(graph)
				end
				graph.G:insert(stmt.query, s)
				table.insert(stmt.query_expr, s)
				stmt.query_expr[s] = #stmt.query_expr
				return s
			end
			stmt.query_expr[s] = false
			-- fallthrough
		elseif qexpr ~= false then
			return s
		end
	end
	return expand(access, data.data(s), stmt)
end

expand = function(access, x, stmt)
	if access == "write" then writedesc(x, stmt) end
	if ismarker(x, "use") then
		return expand(access, x.x, stmt)
	end
	local v = cache[access][x]
	if v then
		return expand(access, v, stmt)
	end
	if effect.iseffect(x) then
		return expand(access, x(), stmt)
	end
	local meta = data.meta(x)
	if meta then
		local acm = meta[access]
		if not acm then error(string.format("`%s' doesn't support %s access", x, access)) end
		if type(acm) == "function" then
			acm = acm(x)
			cache[access][x] = acm
		end
		effect.change()
		return expand(access, acm, stmt)
	end
	if ismarker(x, "capture") then
		return capture(expand(access, x.f, stmt))
	end
	if ismarker(x, "sink") then
		expand(access, x.values, stmt)
		-- woosh all ur values are gone
		return {}
	end
	if type(x) == "table" and not ismarker(x) then
		local xs = {}
		for k,v in pairs(x) do
			xs[k] = expand(access, v, stmt)
		end
		return xs
	end
	if type(x) == "string" then
		return expandstr(access, x, stmt)
	end
	return x
end

local function get(x)
	x = data.todata(x)
	local r = cache.read[x] or getgraph():mapping(x)
	local w = cache.write[x]
	if r and w then
		return "rw"
	elseif r then
		return "r"
	elseif w then
		return "w"
	else
		return ""
	end
end

local function forward(w, r)
	return load([[
		local write, read = ...
		return function() return write(read()) end
	]])(write(w), read(r))
end

local function connect(source, sink)
	local conn = data.meta(source).connect
	if type(conn) == "function" then
		return conn(source, sink)
	else
		return connect(conn, sink)
	end
end

local function newuv(uv, v)
	if type(v) == "nil" or type(v) == "number" or type(v) == "boolean" then
		return v
	elseif type(v) == "string" then
		return string.format("%q", v)
	else
		local name = string.format("_%p", v)
		uv[name] = v
		return name
	end
end

local function emituv(buf, uv)
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

local function emitpatch(patch)
	if patch.f then
		patch.x = patch.f(patch.x)
		patch.f = nil
	end
	return patch.x
end

local function emitread(stmt, buf, uv, read)
	if type(read) == "function" then
		buf:putf("%s()", newuv(uv, read))
	elseif type(read) == "string" then
		buf:putf("q%d", stmt.query_expr[read])
	elseif ismarker(read, "patch") then
		emitread(stmt, buf, uv, emitpatch(read))
	else
		buf:put("{")
		for k,v in pairs(read) do
			buf:putf("[%s] = ", newuv(uv, k))
			emitread(stmt, buf, uv, v)
			buf:put(",")
		end
		buf:put("}")
	end
end

local function compileread(stmt, values, buf, buf2)
	local uv = {}
	local vmctx = newuv(uv, stmt.query_vmctx)
	for i,v in ipairs(values) do
		if i>1 then buf:put(", ") end
		emitread(stmt, buf, uv, v)
	end
	local uval = emituv(buf2, uv)
	buf2:put("return function()\n")
	if stmt.query then
		buf2:put("local q1")
		for i=2, #stmt.query_expr do
			buf2:putf(", q%d", i)
		end
		buf2:putf(" = %s():query(%d)\n", vmctx, stmt.query)
	end
	buf2:put("return ", buf, "\nend")
	return assert(load(buf2))(unpack(uval))
end

local function emitwrite(buf, uv, value, write, root)
	if ismarker(write, "capture") then
		if root then
			buf:putf("local r%d = ", root)
		end
		emitwrite(buf, uv, value, write.f)
		if root then
			return string.format("r%d", root)
		end
	elseif type(write) == "function" then
		buf:putf("%s(%s)\n", newuv(uv, write), value)
	elseif ismarker(write, "patch") then
		return emitwrite(buf, uv, value, emitpatch(write), root)
	else
		buf:putf("if %s ~= nil then\n", value)
		for k,v in pairs(write) do
			emitwrite(buf, uv, string.format("%s[%s]", value, newuv(uv, k)), v)
		end
		buf:put("end\n")
	end
end

local function writemasks(stmt)
	if stmt.mmask then
		return stmt.mmask, stmt.gnodes
	end
	local mmask, gnodes = 0ull, {}
	local graph = getgraph()
	for w,_ in pairs(stmt.use) do
		if cache.write_trampoline[w] then
			local wmask, wnodes = writemasks(cache.write_trampoline[w].value.stmt)
			mmask = bit.bor(mmask, wmask)
			for node in pairs(wnodes) do gnodes[node] = true end
		else
			if data.typeof(w) == "mem.slot" then
				mmask = bit.bor(mmask, require("m3_mem").slotmask(w))
			end
			local node = graph:mapping(w)
			if node then
				gnodes[node] = true
			end
		end
	end
	if next(gnodes) and cache.write[graph] then
		mmask = bit.bor(mmask, require("m3_mem").slotmask(graph.mask))
	end
	stmt.mmask, stmt.gnodes = mmask, gnodes
	return mmask, gnodes
end

local function compilewrite(stmt, values, buf, buf2)
	local mmask, gnodes = writemasks(stmt)
	local uv, ret = {}, {}
	if mmask ~= 0 then
		uv.mem_setmask = require("m3_mem").setmask
		buf:put("mem_setmask(", require("m3_mem").maskstr(mmask), ")\n")
	end
	local graph = getgraph()
	if next(gnodes) and cache.write[graph] then
		local mask = graph.G:mask()
		for node in pairs(gnodes) do
			graph.G:insert(mask, node)
		end
		uv.fhk_setmask = graph.mask.write
		buf:putf("fhk_setmask(%d)\n", mask)
	end
	for i,v in ipairs(values) do
		local r = emitwrite(buf, uv, string.format("v%d", i), v, i)
		if r then table.insert(ret, r) end
	end
	local uval = emituv(buf2, uv)
	buf2:put("return function(v1")
	for i=2, #values do buf2:putf(", v%d", i) end
	buf2:put(")\n", buf, "\n")
	if #ret > 0 then
		buf2:put("return ", table.concat(ret, ", "), "\n")
	end
	buf2:put("end")
	return assert(load(buf2))(unpack(uval))
end

local function dispatchread(...)
	dispatch("read", ...)
	return ...
end

local function trace_read(f)
	return function() return dispatchread(f()) end
end

local function trace_write(f)
	return function(...)
		dispatch("write", ...)
		return f(...)
	end
end

local function startup()
	local buf, buf2 = buffer.new(), buffer.new()
	for trampoline, fx in pairs(cache.read_trampoline) do
		debug.setupvalue(trampoline, 1, compileread(fx.value.stmt, fx.value.values, buf, buf2))
		buf:reset()
		buf2:reset()
	end
	for trampoline, fx in pairs(cache.write_trampoline) do
		debug.setupvalue(trampoline, 1, compilewrite(fx.value.stmt, fx.value.values, buf, buf2))
		buf:reset()
		buf2:reset()
	end
	if event.listener() then
		for trampoline in pairs(cache.read_trampoline) do
			local _, f = debug.getupvalue(trampoline, 1)
			debug.setupvalue(trampoline, 1, trace_read(f))
		end
		for trampoline in pairs(cache.write_trampoline) do
			local _, f = debug.getupvalue(trampoline, 1)
			debug.setupvalue(trampoline, 1, trace_write(f))
		end
	end
	cache = nil
end

return {
	read    = read,
	write   = write,
	get     = get,
	forward = forward,
	connect = connect,
	patch   = patch,
	defer   = defer,
	capture = capture,
	splat   = splat,
	sink    = sink,
	use     = use,
	startup = startup
}
