local state = require "m3_state"
local channel = state.mode == "mp" and require "m3_channel"
local buffer = require "string.buffer"
local insert = table.insert

---- connection & construction -------------------------------------------------

local pipes = setmetatable({}, {__mode="k"})

local function ispipe(func)
	return (pipes and pipes[func]) ~= nil
end

local function nop() end

local function isempty(func)
	if pipes then
		return #pipes[func] == 0
	else
		for i=1, math.huge do
			local name, value = debug.getupvalue(func, i)
			if not name then return false end
			if name == "target" then
				return value == nop
			end
		end
	end
end

local function checkpipe(func)
	assert(pipes, "attempt to modify pipe after startup")
	if not pipes[func] then
		error(string.format("expected pipe, got %s", func))
	end
	return pipes[func]
end

local function tosink(sink)
	if (type(sink) == "table" or type(sink) == "userdata" or type(sink) == "cdata") and sink.write then
		return function(x) return sink:write(x) end
	elseif type(sink) == "table" then
		return function(x) return insert(sink, x) end
	else
		return sink
	end
end

local function connect(source, sink)
	local info = checkpipe(source)
	if info.fuse then info = checkpipe(info.fuse) end
	insert(info, tosink(sink))
	return sink
end

local function pure(func)
	checkpipe(func).pure = true
	return func
end

local function uncompiled()
	error("attempt to write to pipe before startup", 2)
end

local function new()
	local func = load([[
		local target = ...
		return function(x)
			return target(x)
		end
	]])(uncompiled)
	pipes[func] = {plain=true}
	return func
end

local shared_output, shared_input
if state.mode == "mp" then
	shared_output = function()
		local func = channel.output()
		pipes[func] = {shared=true}
		return func
	end
	shared_input = function()
		local func = channel.input()
		pipes[func] = {shared=true}
		return func
	end
else
	shared_input = new
	shared_output = new
end

local function defer(func)
	local pipe = new()
	pipes[pipe].defer = func
	return pipe
end

local function fuse(sink, source)
	source = source or new()
	assert(not pipes[sink], "attempt to fuse existing pipe")
	pipes[sink] = {fuse=source}
	return sink, source
end

local function map(f)
	local func = load([[
		local target, f = ...
		return function(x)
			return target(f(x))
		end
	]])(uncompiled, f)
	pipes[func] = {pure=true}
	return func
end

local function filter(f)
	local func = load([[
		local target, f = ...
		return function(x)
			if f(x) then
				return target(x)
			end
		end
	]])(uncompiled, f)
	pipes[func] = {pure=true}
	return func
end

---- emit ----------------------------------------------------------------------

local function setupval(func, uv, val)
	for i=1, math.huge do
		local name = debug.getupvalue(func, i)
		if name == uv then
			debug.setupvalue(func, i, val)
			return
		end
	end
end

local function compile(func)
	local info = pipes[func]
	if not info then
		-- not a pipe
		return func
	end
	if info.fuse then
		assert(#info == 0, "fused pipe write head has direct consumers")
		return func
	end
	if info.code then
		if info.code == true or not info.plain then
			-- recursive reference or trampoline required
			return func
		end
		-- no recursion, return target
		return info.code
	end
	info.code = true
	if #info == 0 then
		-- not connected to anything?
		info.code = nop
	elseif #info == 1 then
		-- just a single sink?
		-- don't generate a function and instead just use the sink.
		info.code = compile(info[1])
	else
		-- multiple sinks: generate & redirect target
		local buf = buffer.new()
		local sinks = {}
		buf:put("local sinks = ...\n")
		for i, sink in ipairs(info) do
			buf:putf("local sink%d = sinks[%d]\n", i, i)
			sinks[i] = compile(sink)
		end
		buf:put("return function(x)\n")
		for i=1, #info do
			buf:putf("sink%d(x)\n", i)
		end
		buf:put("end\n")
		info.code = load(buf)(sinks)
	end
	if info.shared and state.mode == "mp" then
		channel.settarget(func, info.code)
	else
		setupval(func, "target", info.code)
	end
	if info.plain then
		return info.code
	else
		return func
	end
end

local function startup()
	-- defer() may create and modify pipes so resolve all defers first.
	while true do
		local new = {}
		for pipe in pairs(pipes) do
			if pipes[pipe].defer then
				insert(new, pipe)
			end
		end
		if #new == 0 then break end
		for _,pipe in ipairs(new) do
			local defer = pipes[pipe].defer()
			for i=1, #pipes[pipe] do
				pipes[defer][i] = pipes[pipe][i]
				pipes[pipe][i] = nil
			end
			pipes[pipe][1] = defer
			pipes[pipe].defer = nil
		end
	end
	-- TODO: optimization. remove all pure pipes without impure sinks.
	for pipe in pairs(pipes) do
		compile(pipe)
	end
	pipes = nil
end

--------------------------------------------------------------------------------

return {
	ispipe        = ispipe,
	isempty       = isempty,
	tosink        = tosink,
	connect       = connect,
	fuse          = fuse,
	pure          = pure,
	new           = new,
	shared_output = shared_output,
	shared_input  = shared_input,
	defer         = defer,
	map           = map,
	filter        = filter,
	startup       = startup
}
