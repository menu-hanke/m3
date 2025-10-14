local C = ...
local m3 = require "m3"
local sqlite = require "m3.sqlite"
local ffi = require "ffi"

local function help(progname)
	io.stderr:write(
		"Usage: ", progname, " [options]... [--|-T] [script [args]...]\n",
[[Available options are:
  -s path     Set simulator image to use.
  -l name     Require library `name'.
  -j cmd      Perform LuaJIT control command (in worker states).
  -O[opt]     Control LuaJIT optimizations (in worker states).
  -p num      Control parallelization.
  -V          Show version.
  -v[flags]   Verbose output.
  -q          Disable progress indicator (quiet).
  -t          Run in test mode.
  -T          Run in test mode, stop handling options, and treat arguments as additional scripts.
  --          Stop handling options.
]])
end

local function version()
	print("m3 " .. m3.version)
	print("fhk " .. require("m3.fhk").version)
	print(jit.version)
	print("sqlite " .. require("m3.sqlite").version())
end

local function eval_quiet(pool, task)
	return function(...) return pool:eval(task.simulate, ...) end
end

local function updateprogress(submitted, completed, total)
	io.stderr:write(string.format(
		"\r[%-50s] %d (%d) / %d",
		(">"):rep(50*completed/total),
		completed,
		submitted,
		total
	))
	if completed == total then
		io.stderr:write("\n")
	end
	io.stderr:flush()
end

local function eval_progress(pool, task, con)
	local query = { sqlite.sql("SELECT", "COUNT(*)"), sqlite.sql("FROM", task.query) }
	local count = con:row(query)[1]
	if count == 1 then
		return eval_quiet(pool, task)
	end
	local submitted, completed = 0, 0
	return function(...)
		submitted = submitted+1
		updateprogress(submitted, completed, count)
		return pool:eval(task.simulate, ...):oncomplete(function()
			completed = completed+1
			updateprogress(submitted, completed, count)
		end)
	end
end

local function isatty(fd)
	if jit.os == "Windows" then
		-- TODO
		return false
	else
		ffi.cdef [[ int isatty(int); ]]
		return ffi.C.isatty(fd) == 1
	end
end

local function init(env, args)
	if #args.actions > 0 then
		env:eval([[
local function unpackiter(f)
	local v = f()
	if not v then return end
	return v, unpackiter(f)
end
local actions = ...
for _,a in ipairs(actions) do
	local o,v = a.o, a.v
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
	elseif o == "d" then
		error("TODO")
	elseif o == "v" then
		m3.settrace(v == "" and true or v)
	end
end
		]], args.actions)
	end
--	if args.testfunc then
--		env:eval([[
--local ffi = require "ffi"
--local testfunc = ffi.cast("bool (*)(int, const char *)", (...))
--function _G.test(a,b)
--	if a == nil then
--		return true
--	elseif type(a) == "boolean" then
--		if a then
--			testfunc(1, "<skip>")
--			error("<skip>")
--		else
--			testfunc(1, b)
--		end
--	elseif type(a) == "string" then
--		return testfunc(0, a)
--	end
--end
--		]], ffi.cast("uintptr_t", args.testfunc))
--	end
	env:eval(
		"insn = (function(f,...) return assert(loadfile(f))(...) end)(...)",
		args.script, unpack(args.args or {})
	)
	local query, url, ddl = env:eval([[
init = data.transaction()
if data.task then
	data.task = require("sqlite").stringify(data.task)
	init:autoselect(data.task)
else
	data.task = init:autoselect()
end
return data.task, m3.connection_info()
	]])
	env:init()
	local simulate = env:func([[
local init, control_exec, control_load = init, control.exec, control.load
local insn = control.all { insn or control.simulate or control.nothing, m3.commit }
local fp = control.save()
local function xunpack(t, i)
	local v = t[i]
	if v then return v, xunpack(t, i+1) end
end
return function(...)
	control_load(fp)
	init(...)
	control_exec(insn)
end
	]])
	return {
		query    = query,
		url      = url,
		ddl      = ddl,
		simulate = simulate
	}
end

local function driver_simulate(args)
	local pool, task = m3.pool(function(env) return init(env, args) end, args.mode)
	local con = sqlite.open(task.url):gc()
	con:execscript(task.ddl)
	local eval = (args.quiet or not isatty(2))
		and eval_quiet(pool, task)
		or eval_progress(pool, task, con)
	for row in con:rows(task.query) do
		eval(row:unpack())
	end
	m3.wait(pool)
	pool:close()
	con:close()
end

local function test_open(env)
	env:eval([[
test = {}
_G.test = test
function test.error(msg) test.expectfail = msg end
function test.post(check, ...) table.insert(test, {[0]=check, ...}) end
function test.runpostchecks() for i=1, #test do test[i][0](unpack(test[i])) end end
	]])
end

local function test_case(env, args)
	local task = init(env, args)
	if task.query == "SELECT 0" and not task.ddl then
		-- fast path: skip actually opening a database
		env:eval(task.simulate)
	else
		local con = sqlite.open(task.url):gc()
		con:execscript(task.ddl)
		for row in con:rows(task.query) do
			env:eval(task.simulate, row:unpack())
		end
		con:close()
	end
	env:eval("test.runpostchecks()")
end

local function driver_test(args)
	local scripts = args.scripts
	if scripts then
		io.stdout:write("1..", #scripts, "\n")
	else
		scripts = {args.script}
		io.stdout:write("1..1\n")
	end
	for i,script in ipairs(scripts) do
		args.script = script
		local env = m3.new()
		test_open(env)
		local ok, err = xpcall(test_case, debug.traceback, env, args)
		local fail = env:eval("return test.expectfail")
		env:close()
		if (ok and not fail) or (not ok and fail and err:match(fail)) then
			io.stdout:write("ok ", i, " - ", script, "\n")
		else
			io.stdout:write("not ok ", i, " - ", script, "\n")
			if fail then
				if err then
					err = string.format("error does not match expected (`%s'):\n%s", fail, err)
				else
					err = "error expected but none thrown"
				end
			end
			for line in err:gmatch("[^\n]+") do
				io.stdout:write("# ", line, "\n")
			end
		end
	end
end

-- -- TODO: use a bump allocator + disable gc for the worker state, like old m3.
-- local function driver_test(args)
-- 	if not args.mode then
-- 		-- disable parallelization for tests unless explicitly requested.
-- 		-- there's no benefit, it just slows down execution, makes debugging harder,
-- 		-- and test selection doesn't work after forking.
-- 		args.mode = 0
-- 	end
-- 	local todo = args.test
-- 	local completed = {}
-- 	local current, pattern
-- 	local errpattern
-- 	-- testfunc(0, "...") -> test selection
-- 	-- testfunc(1, "...") -> set expected error pattern
-- 	args.testfunc = ffi.cast("bool (*)(int, const char *)", function(what, arg)
-- 		local name = ffi.string(arg)
-- 		if what == 0 then
-- 			if name == current then
-- 				return true
-- 			end
-- 			if completed[name] then
-- 				return false
-- 			end
-- 			if current:match("%*") and name:match(pattern) then
-- 				current = name
-- 				pattern = test_namepattern(name)
-- 				return true
-- 			end
-- 			if name:match("%*") and current:match(test_namepattern(name)) then
-- 				return true
-- 			end
-- 			return false
-- 		elseif what == 1 then
-- 			errpattern = name
-- 			return true
-- 		end
-- 	end)
-- 	local num = 0
-- 	while true do
-- 		current = next(todo)
-- 		if not current then
-- 			-- done.
-- 			io.stdout:write("1.."..num)
-- 			break
-- 		end
-- 		pattern = test_namepattern(current)
-- 		errpattern = nil
-- 		local ok, err = pcall(simulate, args)
-- 		if ok then
-- 			if not current:match("%*") then
-- 				num = num+1
-- 				io.stdout:write("ok ", num, " - ", current, "\n")
-- 			end
-- 		elseif not (errpattern and err:match(errpattern)) then
-- 			num = num+1
-- 			io.stdout:write("not ok ", num, " - ", current, "\n")
-- 			for line in err:gmatch("[^\n]+") do
-- 				io.stdout:write("# ", line, "\n")
-- 			end
-- 		end
-- 		-- flush here or forked subprocesses may flush and cause duplicate output.
-- 		io.stdout:flush()
-- 		todo[current] = nil
-- 		completed[current] = true
-- 	end
-- end

local function parseargs(progname, ...)
	local n = select("#", ...)
	local args = {...}
	local i = 1
	local ret = { actions = {}, driver=driver_simulate }
	while i<=n do
		local a = args[i]
		if a:sub(1,1) ~= "-" then
			break
		end
		local f = a:sub(2,2)
		if f == "-" then
			if a ~= "--" then return help(progname) end
			i = i+1
			break
		elseif f == "V" then
			return version()
		elseif f == "p" or f == "j" or f == "s" then
			if #a == 2 then
				i = i+1
				if i > n then return help(progname) end
				a = args[i]
			else
				a = a:sub(3)
			end
			if f == "p" then
				ret.mode = a
			elseif f == "s" then
				if not ret.image then
					ret.image = a
				else
					ret.image = string.format("%s:%s", ret.image, a)
				end
			else
				table.insert(ret.actions, {o=f, v=a})
			end
		elseif f == "v" or f == "O" then
			table.insert(ret.actions, {o=f, v=a:sub(3)})
		elseif f == "q" then
			ret.quiet = true
		elseif f == "t" then
			ret.driver = driver_test
		elseif f == "T" then
			ret.driver = driver_test
			i = i+1
			ret.scripts = {unpack(args, i)}
			return ret
		else
			return help(progname)
		end
		i = i+1
	end
	if i > n then return help(progname) end
	ret.script = args[i]
	ret.args = {unpack(args, i+1)}
	return ret
end

local function enterimage(args)
	-- we are going to chdir() to the overlay mount, so make sure our script path is not relative
	-- to our current dir
	if args.script and args.script:sub(1,1) ~= "/" then
		local buf = ffi.new("char[4096]")
		ffi.cdef [[ char *getcwd(char *, size_t); ]]
		if ffi.C.getcwd(buf, 4096) ~= nil then
			args.script = string.format("%s/%s", ffi.string(buf), args.script)
		end
	end
	return C.m3_image_enter(C.err, args.image)
end

local function main(...)
	local args = parseargs(...)
	if not args then return 1 end
	if args.image then
		local r = enterimage(args)
		if r == -1 then C.check(r) end
		if r >= 0 then return r end
		-- else: this is the child process, perform work.
	end
	return args:driver()
end

return {
	main = main
}
