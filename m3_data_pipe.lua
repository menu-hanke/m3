local access = require "m3_access"
local buffer = require "string.buffer"

local function pipe_read(pipe)
	local set, get = load([[
		local value
		return function(x) value = x end, function() return value end
	]])()
	table.insert(pipe, set)
	return get
end

local function nop() end

local function compilewrite(pipe)
	-- TODO: if sink has data.meta & access.get(sink) doesn't have `r`, then the sink
	-- can be skipped
	if #pipe == 0 then
		return nop
	end
	local func
	if #pipe == 1 then
		func = pipe[1]
	else
		local buf = buffer.new()
		buf:put("local pipe = ...\n")
		for i=1, #pipe do
			buf:putf("local sink%d = pipe[%d]\n", i, i)
		end
		buf:put("return function(v)\n")
		for i=1, #pipe do
			buf:putf("sink%d(v)\n", i)
		end
		buf:put("end")
		func = load(buf)(pipe)
	end
	if pipe.transform then
		func = pipe.transform(func)
	end
	return func
end

local function pipe_write(pipe)
	return access.defer(function() return compilewrite(pipe) end)
end

local function pipe_connect(pipe, sink)
	table.insert(pipe, access.write(sink))
end

local pipe_mt

local function pipe_transform(pipe, transform)
	local transformed = setmetatable({transform=transform}, pipe_mt)
	pipe_connect(pipe, transformed)
	return transformed
end

local function pipe_map(pipe, func)
	return pipe_transform(pipe, function(target)
		return load([[
			local target, func = ...
			return function(v)
				return target(func(v))
			end
		]])(target, func)
	end)
end

local function pipe_filter(pipe, func)
	return pipe_transform(pipe, function(target)
		return load([[
			local target, func = ...
			return function(v)
				if func(v) then
					return target(v)
				end
			end
		]])(target, func)
	end)
end

pipe_mt = {
	data = {
		type    = "pipe",
		read    = pipe_read,
		write   = pipe_write,
		connect = pipe_connect
	},
	__index = {
		map     = pipe_map,
		filter  = pipe_filter
	}
}

local function new()
	return setmetatable({}, pipe_mt)
end

local shared_input, shared_output
if require("m3_environment").mode == "mp" then
	local mp = require "m3_mp"

	local function channel_write(pipe)
		return access.defer(function()
			return pipe.dispatch:channel(compilewrite(pipe)).send
		end)
	end

	local shared_mt = {
		data = {
			type    = "pipe.shared",
			read    = pipe_read,
			write   = channel_write,
			connect = pipe_connect
		}
	}

	shared_input = function() return setmetatable({dispatch=mp.work}, shared_mt) end
	shared_output = function() return setmetatable({dispatch=mp.main}, shared_mt) end
else
	shared_input = new
	shared_output = new
end

return {
	new           = new,
	shared_input  = shared_input,
	shared_output = shared_output
}

