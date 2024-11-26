local m3 = require "m3_api"

local input = coroutine.wrap(function() coroutine.yield({}) end)
local driver = {}
local branch = m3.data.pipe()
local tree = m3.data.pipe()
local write_branch = m3.write(branch)
local write_tree = m3.write(tree)

---- Config environment --------------------------------------------------------

local init         = setmetatable({}, {
	__index = _G,
	__call = function(self, f, ...)
		if type(f) == "string" then f = assert(loadfile(f)) end
		return setfenv(f, self)(...)
	end
})

init.readfile      = init

init.read          = m3.read
init.write         = m3.write
init.apply         = m3.apply
init.define        = m3.data.define
init.include       = m3.data.include
init.connect       = m3.data.connect
init.pipe          = m3.data.pipe

init.nothing       = m3.control.nothing
init.skip          = m3.control.skip
init.call          = m3.control.call
init.all           = m3.control.all
init.any           = m3.control.any
init.optional      = m3.control.optional
init.first         = m3.control.first
init.try           = m3.control.try
init.loop          = m3.control.loop
init.dynamic       = m3.control.dynamic

-- TODO: this always causes a recompilation, maybe cache the instructions.
function init.exec(insn)
	m3.control.exec(m3.control.all { insn, write_branch })
	write_tree()
end

function init.input(x) input = x end
function init.simulate(x) driver = x end

function init.tree()
	local t = m3.data.tree()
	m3.data.connect(branch, m3.write(t.branch))
	local pipe = m3.data.pipe():map(m3.read(t))
	m3.data.connect(tree, pipe)
	return m3.data.dynamic {
		visit   = function(_,f) f(nil, t) f(nil, pipe) end,
		writer  = function() return t:writer() end,
		connect = function(_, sink) return m3.data.connect(pipe, sink) end
	}
end

---- Test driver ---------------------------------------------------------------

local ctest = debug.getregistry()["m3$test"]
local testhooks
if ctest then
	init.test = setmetatable({}, {
		__call = function(_, ...) return ctest(...) end,
		__index = { simulate = function(a,b) return ctest(a, function() init.simulate(b) end) end }
	})
else
	local function disabled() return false end
	init.test = setmetatable({}, {
		__call = disabled,
		__index = { simulate=disabled }
	})
end

---- CLI options ---------------------------------------------------------------

local function unpackiter(f)
	local v = f()
	if not v then return end
	return v, unpackiter(f)
end

local function command(o, v)
	if o == "j" then
		local cmd, opt = v:match("^([^=]+)=(.*)$")
		if cmd then
			opt = opt:gmatch("[^,]+")
		else
			cmd, opt = v, function() end
		end
		if type(jit[cmd]) == "function" then
			jit[cmd](unpackiter(opt))
		else
			require("jit."..cmd).start(unpackiter(opt))
		end
	elseif o == "O" then
		jit.opt.start(unpackiter(v:gmatch("[^,]+")))
	elseif o == "l" then
		require(v)
	elseif o == "i" then
		input = v
	elseif o == "x" then
		error("TODO")
	elseif o == "v" then
		if v == "" then v = true end
		require("m3_debug").settrace(v)
	end
end

---- Host callbacks ------------------------------------------------------------

local function parseurl(url)
	local module, path = url:match("^([^:]+):(.*)$")
	if module then return module, path end
	path = url
	module = path:match("^.*%.([^%.]+)$")
	if module then return module, path end
	error(string.format("cannot infer file type from path: %s", url))
end

local function loadinputmodule(name, ...)
	name = "m3_input_"..name
	local errors = {}
	for _,loader in ipairs(package.loaders) do
		local module = loader(name)
		if type(module) == "function" then
			return module(...)
		elseif type(module) == "string" then
			table.insert(errors, module)
		end
	end
	error(string.format("could not load input module `%s': %s", name, table.concat(errors)))
end

local function loadinput()
	if type(input) == "string" then
		return loadinputmodule(parseurl(input))
	elseif type(input) == "function" then
		return {next=input}
	elseif (input.next or input.read) then
		return input
	else
		return loadinputmodule("data", input)
	end
end

local function loaddriver()
	if type(driver) == "table" then
		return function() return init.exec(driver) end
	else
		return driver
	end
end

-- TODO: this graph function should get special treatment,
-- eg. inputs that are only referenced from this should not be saved in memory etc.
local initfn = m3.apply "init"

local function host()
	local input = loadinput()
	if input.next and not input.read then
		input.read = m3.write(m3.data.globals())
	end
	local driver = loaddriver()
	local work
	if input.next then
		local inputnext = input.next
		local inputread = input.read
		local inpipe = m3.data.shared.input()
		local writeinput = m3.write(inpipe)
		m3.data.connect(inpipe, function(v)
			inputread(v)
			initfn()
			return driver()
		end)
		work = function()
			local v = inputnext()
			if v == nil then return false end
			writeinput(v)
		end
	else
		local inputread = assert(input.read, "input must define at least one of `next' or `read'")
		work = function()
			if inputread() == false then return end
			initfn()
			return driver()
		end
	end
	return {
		work     = work,
		shutdown = input.close
	}
end

--------------------------------------------------------------------------------

return {
	host      = host,
	init      = init,
	testhooks = testhooks,
	command   = command,
}
