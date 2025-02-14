local buffer = require "string.buffer"
local event = require("m3_debug").event

local function embedconst(x)
	if type(x) == "string" then
		return string.format("%q", x)
	elseif x == x then
		return tostring(x)
	else
		return "(0/0)"
	end
end

local function uv__index(uv, v)
	if type(v) == "nil" or type(v) == "boolean" or type(v) == "number" or type(v) == "string" then
		return embedconst(v)
	else
		local name = string.format("u%p", v)
		uv[name] = v
		return name
	end
end

local upval_mt = {
	__index = uv__index
}

local function upvalues()
	return setmetatable({}, upval_mt)
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

local function index(uv, k)
	if type(k) == "string" and k:match("^[%a_][%w_]*$") then
		return string.format(".%s", k)
	else
		return string.format("[%s]", uv__index(uv, k))
	end
end

local function code_name(code)
	code.nameid = code.nameid+1
	return string.format("v%d", code.nameid)
end

local code_mt = {
	__index = {
		name = code_name
	}
}

local function new()
	return setmetatable({
		uv     = upvalues(),
		nameid = 0,
		buf    = buffer.new(),
	}, code_mt)
end

local function upvalueidx(f, up)
	for i=1, math.huge do
		local u = debug.getupvalue(f, i)
		if u == up then return i end
		if not u then return end
	end
end

local function setupvalue(f, up, v)
	debug.setupvalue(f, upvalueidx(f, up), v)
end

-- TODO: add descriptive chunk names for each call
local function loadcode(...)
	event("code", ...)
	return assert(load(...))
end

local function chunkname(s)
	return string.format("=m3: %s", s)
end

return {
	embedconst   = embedconst,
	emitupvalues = emitupvalues,
	index        = index,
	new          = new,
	setupvalue   = setupvalue,
	load         = loadcode,
	chunkname    = chunkname
}
