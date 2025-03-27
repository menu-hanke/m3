local data = require "m3_data"
local db = require "m3_db"
local dbg = require "m3_debug"
local ffi = require "ffi"
local buffer = require "string.buffer"
local type = type

local funcs = {}

local function init()
	require "m3_init"
end

-- spicy APIs that shouldn't be exposed directly to scripts go here.
local env = setmetatable({
	m3 = {
		G               = data.G,
		commit          = data.commit,
		schema          = db.schema,
		statement       = db.statement,
		connection_info = db.connection_info,
		settrace        = dbg.settrace,
		init            = init
	}
}, {__index=_G})

local encoder = buffer.new()
local decoder = buffer.new()

local function capture1(n, v, ...)
	if n > 0 then
		encoder:encode(v)
		return capture1(n-1, ...)
	end
end

local function capture(...)
	return capture1(select("#", ...), ...)
end

local function unpackdecode()
	if #decoder > 0 then
		local v = decoder:decode()
		return v, unpackdecode()
	end
end

local function eval(src, ...)
	if type(src) == "number" then
		return funcs[src](...)
	else
		return assert(load(src, "=(eval)", nil, env))(...)
	end
end

env.m3.eval = eval

local function ceval(src, ptr, len)
	encoder:reset()
	decoder:set(ffi.cast("const void *", ptr), len)
	capture(eval(src, unpackdecode()))
	return encoder:ref()
end

funcs[0] = function(src, ...)
	local id = #funcs+1
	funcs[id] = eval(src, ...)
	return id
end

return ceval
