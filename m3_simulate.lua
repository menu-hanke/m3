local m3 = require "m3_api"

local host = {}
local input = coroutine.wrap(function() coroutine.yield(true) end)
local driver = {}
local forest = m3.forest()

---- Config environment --------------------------------------------------------

local init         = setmetatable({}, {
	__index = _G,
	__call = function(self, f, ...)
		if type(f) == "string" then f = assert(loadfile(f)) end
		return setfenv(f, self)(...)
	end
})

init.read          = init

-- TODO: this should also handle file sinks
init.connect       = m3.pipe.connect

init.pipe = {
	new            = m3.pipe.new,
	shared         = m3.pipe.shared_output,
	map            = m3.pipe.map,
	filter         = m3.pipe.filter,
	tree           = forest
}

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

init.graph         = m3.fhk.readfile
init.defgraph      = m3.fhk.define

-- TODO: this always causes a recompilation, maybe cache the instructions.
function init.exec(insn)
	m3.control.exec(m3.control.all { insn, forest.branch })
	forest.tree()
end

function init.input(x) input = x end
function init.simulate(x) driver = x end

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
		if v == "" then v = nil end
		require("m3_debug").traceon(v)
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
	else
		return input
	end
end

local function loaddriver()
	if type(driver) == "table" then
		return function() return init.exec(driver) end
	else
		return driver
	end
end

function host.on_data()
	local input = loadinput()
	local driver = loaddriver()
	host.shutdown = input.close
	if input.next then
		local inputnext = input.next
		local inputread = input.read or m3.read
		local inpipe = m3.pipe.shared_input()
		host.work = function()
			local v = inputnext()
			if v == nil then return false end
			inpipe(v)
		end
		m3.pipe.connect(inpipe, function(v)
			inputread(v)
			return driver()
		end)
	else
		local inputread = assert(input.read, "input must define at least one of `next' or `read'")
		host.work = function() return inputread() ~= false and driver() end
	end
end

--------------------------------------------------------------------------------

return {
	host      = host,
	init      = init,
	testhooks = testhooks,
	command   = command,
}
