local data = require "m3_data"
local data_query = require "m3_data_query"
local effect = require "m3_effect"
local fhk = require "m3_fhk"
local buffer = require "string.buffer"
local ffi = require "ffi"

local reads = {}  --> trampoline => stmt
local writes = {} --> proxy => stmt

local marker_mt = {}

local function ismarker(x)
	return getmetatable(x) == marker_mt
end

local function mark(x,m)
	return setmetatable({data=x, mark=m}, marker_mt)
end

local function direct(x)
	return mark(x, {direct=true})
end

local function unpackdef(stmt, src)
	if effect.iseffect(src) then
		return unpackdef(stmt, src())
	elseif ismarker(src) then
		return mark(unpackdef(stmt, src.data), src.mark)
	elseif type(src) == "string" then
		if stmt.access == "read" then
			if not stmt.query then
				stmt.query = data_query.new()
			end
			return stmt.query[src]
		else
			return unpackdef(stmt, data.data(src))
		end
	elseif type(src) == "function" then
		return src
	else
		local d = data.desc(src)
		if d then
			effect.set(d, stmt.access, true)
			return d
		else
			local proto = {}
			for k,v in pairs(src) do
				proto[k] = unpackdef(stmt, v)
			end
			return proto
		end
	end
end

local function read(...)
	local trampoline = load("local target return function() return target() end")()
	local stmt = { access = "read" }
	local args = {...}
	stmt.def = effect.effect(function() return unpackdef(stmt, args) end)
	reads[trampoline] = stmt
	return trampoline
end

local function write(...)
	local proxy = ffi.new("struct {}")
	local stmt = { access = "write" }
	local args = {...}
	stmt.def = effect.effect(function() return unpackdef(stmt, args) end)
	writes[proxy] = stmt
	return proxy
end

local function forward(w, r)
	return load([[
		local write, read = ...
		return function() return write(read()) end
	]])(write(w), read(direct(r)))
end

local function mutate(o)
	return load([[
		local write, read = ...
		return function(f, ...)
			return write(f(read(), ...))
		end
	]])(write(o), read(o))
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

local function emitread(ctx, value)
	if type(value) == "function" then
		ctx.tail:putf("%s()\n", newuv(ctx.uv, value))
	elseif ismarker(value) then
		local idx = #ctx.markers+1
		ctx.markers[idx] = value.mark
		emitread(ctx, value.data)
		ctx.markers[idx] = nil
	else
		local meta = data.meta(value)
		if meta then
			local v = meta.read(value, ctx)
			if v then emitread(ctx, v) end
		else
			ctx.tail:put("{")
			for k,v in pairs(value) do
				ctx.tail:putf("[%s] = ", newuv(ctx.uv, k))
				emitread(ctx, v)
				ctx.tail:put(",")
			end
			ctx.tail:put("}")
		end
	end
end

local function ctx_checkmark(ctx, f)
	if type(f) == "table" then
		local t = f
		f = function(x)
			for k,v in pairs(t) do
				if x[k] ~= v then
					return false
				end
			end
			return true
		end
	end
	for i=1, #ctx.markers do
		if f(ctx.markers[i]) then
			return true
		end
	end
	return false
end

local ctx_mt = {
	__index = {
		checkmark = ctx_checkmark
	}
}

local function compileread(stmt)
	local ctx = setmetatable({
		head    = buffer.new(),
		tail    = buffer.new(),
		uv      = {},
		markers = {}
	}, ctx_mt)
	for i,v in ipairs(stmt.def.value) do
		if i>1 then ctx.tail:put(", ") end
		emitread(ctx, v)
	end
	local buf = buffer.new()
	local uv = emituv(buf, ctx.uv)
	buf:put("return function()\n")
	buf:put(ctx.head)
	buf:put("return ")
	buf:put(ctx.tail)
	buf:put("\nend")
	return assert(load(buf))(unpack(uv))
end

local function writemask(ctx, obj)
	if type(obj.node) == "number" then
		if not ctx.mask then
			ctx.mask = fhk.mask()
			ctx.uv.setmask = fhk.setmask
		end
		fhk.insertmask(ctx.mask, obj.node)
	end
	local meta = data.meta(obj)
	if meta and meta.pairs then
		for _, o in meta.pairs(obj) do
			writemask(ctx, o)
		end
	end
end

local function emitwrite(ctx, value, consumer)
	if type(consumer) == "function" then
		ctx.buf:putf("%s(%s)\n", newuv(ctx.uv, consumer), value)
		return consumer
	elseif ismarker(consumer) then
		return emitwrite(ctx, value, consumer.data)
	else
		local meta = data.meta(consumer)
		if meta then
			writemask(ctx, consumer)
			local f, name = meta.write(consumer)
			emitwrite(ctx, value, f)
			return f, name
		else
			local t = {}
			ctx.buf:putf("if %s ~= nil then\n", value)
			for k,v in pairs(consumer) do
				local x, name = emitwrite(ctx, string.format("%s[%s]", value, newuv(ctx.uv, k)), v)
				if type(k) ~= "number" or not name then name = k end
				t[name] = x
			end
			ctx.buf:putf("end\n")
			return t
		end
	end
end

local function write_newindex(stmt, key, value)
	stmt[key](value)
end

local function compilewrite(stmt)
	local index = {}
	local ctx = { buf=buffer.new(), uv={} }
	for i,v in ipairs(stmt.def.value) do
		local x, name = emitwrite(ctx, string.format("v%d", i), v)
		if name then
			index[name] = x
		elseif type(x) == "table" then
			for xk,xv in pairs(x) do index[xk] = xv end
		end
	end
	local buf = buffer.new()
	local uv = emituv(buf, ctx.uv)
	buf:put("return function(_")
	for i=1, #stmt.def.value do
		buf:putf(", v%d", i)
	end
	buf:put(")\n")
	buf:put(ctx.buf)
	if ctx.mask then
		buf:putf("setmask(%d\n)", ctx.mask)
	end
	buf:put("end")
	return {
		__index    = index,
		__newindex = write_newindex,
		__call     = assert(load(buf))(unpack(uv))
	}
end

local function startup()
	for trampoline, stmt in pairs(reads) do
		debug.setupvalue(trampoline, 1, compileread(stmt))
	end
	for proxy, stmt in pairs(writes) do
		ffi.metatype(ffi.typeof(proxy), compilewrite(stmt))
	end
	reads = nil
	writes = nil
end

return {
	read    = read,
	write   = write,
	forward = forward,
	mutate  = mutate,
	startup = startup
}
