local code = require "m3_code"
local data = require "m3_data"
local dbg = require "m3_debug"
local mem = require "m3_mem"
local buffer = require "string.buffer"
local debug_describe = dbg.describe
local istransaction = data.istransaction
local mem_save, mem_load, mem_delete = mem.save, mem.load, mem.delete
local load = code.load
local rawget, rawset = rawget, rawset

---- Opcodes -------------------------------------------------------------------

local function gettag(x)
	local mt = getmetatable(x)
	return mt and mt["m3$ctag"]
end

local function getctr(x)
	local mt = getmetatable(x)
	return mt and mt.__m3_control and mt.__m3_control(x)
end

local function newmeta(tag)
	return { ["m3$ctag"] = tag }
end

local all_mt = newmeta "all"
local any_mt = newmeta "any"
local first_mt = newmeta "first"
local call_mt = newmeta "call"
local check_mt = newmeta "check"
local single_mt = newmeta "single"
local ifelse_mt = newmeta "ifelse"
local loop_mt = newmeta "loop"
local callcc_mt = newmeta "callcc"
local dynamic_mt = newmeta "dynamic"

local function tocontrol(x, tab_mt)
	if gettag(x) then
		return x
	elseif type(x) == "function" then
		return setmetatable({f=x}, call_mt)
	elseif type(x) == "string" then
		return setmetatable({f=data.transaction():read(x)}, check_mt)
	elseif type(x) == "table" then
		if istransaction(x) then
			return setmetatable({f=x}, call_mt)
		else
			local ctr = getctr(x)
			if ctr then
				return tocontrol(ctr, tab_mt)
			end
			if not tab_mt then
				error("plain table not allowed here - use `all' or `any' explicitly")
			end
			local t = setmetatable({}, tab_mt)
			for i,c in ipairs(x) do
				t[i] = tocontrol(c, tab_mt)
			end
			return t
		end
	else
		error(string.format("bad control: %s", x))
	end
end

local function tofunc(f)
	if type(f) == "string" then
		f = data.transaction():read(f)
	end
	return f
end

local function all(xs)
	return tocontrol(xs, all_mt)
end

local function any(xs)
	return tocontrol(xs, any_mt)
end

local function first(xs)
	return tocontrol(xs, first_mt)
end

local nothing = all {}
local skip = any {}

local function optional(node)
	return any { node, nothing }
end

local function try(node)
	return first { node, nothing }
end

local function callcheck(mt, f, ...)
	return setmetatable({f=tofunc(f), n=select("#", ...), ...}, mt)
end

local function call(...)
	return callcheck(call_mt, ...)
end

local function check(...)
	return callcheck(check_mt, ...)
end

local function ifelse(c, t, f)
	return setmetatable({c=tofunc(c), t=tocontrol(t), f=tocontrol(f)}, ifelse_mt)
end

local function single(node)
	return setmetatable({node=node}, single_mt)
end

local function loop(node)
	return setmetatable({node=tocontrol(node)}, loop_mt)
end

local function callcc(f)
	return setmetatable({f=f}, callcc_mt)
end

local function dynamic(f)
	return setmetatable({f=f}, dynamic_mt)
end

local function describe(node)
	if node.__desc then
		return node.__desc
	end
	node.__desc = "<rec>" -- prevent recursion
	local buf = buffer.new()
	local tag = gettag(node)
	buf:put(tag, " ")
	if tag == "all" or tag == "any" or tag == "first" then
		buf:put("{")
		for i=1, #node do
			if i>1 then buf:put(",") end
			buf:put(" ", describe(node[i]))
		end
		buf:put("}")
	elseif tag == "call" or tag == "check" or tag == "callcc" or tag == "dynamic" then
		buf:putf("%s", debug_describe(node.f))
	elseif tag == "single" then
		buf:put(describe(node.node))
	end
	local desc = tostring(buf)
	node.__desc = desc
	return desc
end

---- Optimizer -----------------------------------------------------------------

local optvisit

local function optimize(node)
	local o = node.__opt
	if o then return o end
	node.__opt = node -- prevent recursive calls to same node
	o = optvisit(node)
	o.__opt = o
	node.__opt = o
	return o
end

local function isskip(node)
	return gettag(node) == "any" and #node == 0
end

local function isnop(node)
	return gettag(node) == "all" and #node == 0
end

local function issame(a, b)
	-- TODO: this should be a deep equality check
	return a == b
end

local function concattab(a, b)
	for _,c in ipairs(b) do
		table.insert(a, c)
	end
end

-- TODO: all { any {x, a}, any {x, b} } -> all { x, any {a, b} }
-- TODO: any { all {x, a}, all {x, b} } -> all { x, any {a, b} }
-- TODO: any { x, x } -> x     (should this be allowed? this changes observable behavior)
-- TODO: single(single(x)) -> single(x)
optvisit = function(node)
	local tag = gettag(node)
	if tag == "all" then
		local nodes = {}
		for _,n in ipairs(node) do
			n = optimize(n)
			if gettag(n) == "all" then
				concattab(nodes, n)
			elseif isskip(n) then
				return skip
			else
				table.insert(nodes, n)
			end
		end
		if #nodes == 1 then
			node = nodes[1]
		else
			node = setmetatable(nodes, all_mt)
		end
	elseif tag == "any" then
		local nodes = {}
		for _,n in ipairs(node) do
			n = optimize(n)
			if gettag(n) == "any" then
				concattab(nodes, n)
			else
				table.insert(nodes, n)
			end
		end
		if #nodes == 1 then
			node = nodes[1]
		else
			node = setmetatable(nodes, any_mt)
		end
	elseif tag == "first" then
		local nodes = {}
		for _,n in ipairs(node) do
			n = optimize(n)
			if gettag(n) == "first" then
				concattab(nodes, n)
			elseif isskip(n) then
				-- SKIP.
			else
				table.insert(nodes, n)
				if isnop(n) then
					break -- done. this is guaranteed to create a branch.
				end
			end
		end
		if #nodes == 1 then
			node = nodes[1]
		else
			if #nodes == 2 and gettag(nodes[1]) == "all" and gettag(nodes[1][1]) == "check" then
				-- optimize first(all { check, ...xs }, y)
				--> ifelse(check, first(all { ...xs }, y), y)
				local check = nodes[1][1]
				local xs = {}
				for i=2, #nodes[1] do xs[i-1] = nodes[1][i] end
				return optimize(ifelse(check, first(all(xs), nodes[2]), nodes[2]))
			end
			node = setmetatable(nodes, first_mt)
		end
	elseif tag == "ifelse" then
		local t = optimize(node.t)
		local f = optimize(node.f)
		if issame(t, f) then
			node = t
		else
			node = ifelse(node.c, t, f)
		end
	end
	return node
end

---- Continuation --------------------------------------------------------------

-- note: this function overwrites some slots after dst+n
local function copycont(stack, dst, src, n)
	local a = rawget(stack, src)
	local b = rawget(stack, src+1)
	local c = rawget(stack, src+2)
	local d = rawget(stack, src+3)
	rawset(stack, dst,   a)
	rawset(stack, dst+1, b)
	rawset(stack, dst+2, c)
	rawset(stack, dst+3, d)
	if n>4 then
		return copycont(stack, dst+4, src+4, n-4)
	end
end

local function ctrl_continue(stack, base, top)
	return rawget(stack, top)(stack, base, top-1)
end

local function cont__call(stack, copy)
	local base, top = stack[0], stack[1]
	if copy ~= false then
		copycont(stack, top+1, base, top+1-base)
		base, top = top+1, top+1+top-base
	end
	return ctrl_continue(stack, base, top)
end

local cont_mt = {
	__call = cont__call
}

local function cont_errstack()
	error("unbalanced stack - this function shouldn't be called")
end

local function cont_exit()
	-- NOP.
end

local function newcont()
	return setmetatable({[0]=cont_errstack, [1]=cont_errstack, [2]=cont_exit}, cont_mt), 2, 2
end

---- Emitter -------------------------------------------------------------------

local emit_node = {}

local function emit(node)
	if node.__code then
		if node.__code == true then
			-- trampoline hack for recursive calls
			node.__code = load([[
				local _target
				return function(stack, base, top)
					return _target(stack, base, top)
				end
			]], code.chunkname(string.format("trampoline %s", describe(node))))()
		end
		return node.__code
	end
	node.__code = true
	local o = optimize(node)
	local code = emit_node[gettag(o)](o)
	-- was emit() called recursively?
	if type(node.__code) == "function" then
		debug.setupvalue(node.__code, 1, code)
	end
	node.__code = code
	return code
end

local function buf_header(buf)
	buf:put("return function(stack,base,top)\n")
end

local function buf_end(buf)
	buf:put("end")
end

local function buf_continue(buf)
	buf:put("return rawget(stack,top)(stack,base,top-1)\n")
end

local function buf_tailcall(buf, ctrl)
	buf:putf("return %s(stack,base,top)\n", ctrl)
end

-- push continuation.
local function buf_setcont(buf, cont)
	buf:put("top = top+1\n")
	buf:putf("rawset(stack,top,%s)\n", cont)
end

-- call & check ------------------------

local function buf_argsload(buf, node)
	for i=1, node.n or 0 do
		buf:putf("local arg%d = node[%d]\n", i, i)
	end
end

local function buf_argscall(buf, node)
	if node.n then
		if node.n >= 1 then
			buf:put("arg1")
		end
		for i=2, node.n do
			buf:putf(", arg%d", i)
		end
	end
end

local function callfunc(node)
	local f = node.f
	if istransaction(f) then
		return f.func
	else
		return f
	end
end

local function emitcallcheck(node)
	local buf = buffer.new()
	buf:put("local rawget, f, node = rawget, ...\n")
	buf_argsload(buf, node)
	buf_header(buf)
	buf:put("local r = f(")
	buf_argscall(buf, node)
	buf:put(")\n")
	if gettag(node) == "call" then
		buf:put("if r ~= nil then return r end\n")
	else
		buf:put("if r ~= true then return false end\n")
	end
	buf_continue(buf)
	buf_end(buf)
	return load(buf, code.chunkname(describe(node)))(callfunc(node), node)
end

emit_node.call = emitcallcheck
emit_node.check = emitcallcheck

-- all ---------------------------------

local function emit_chain_func(node, chain)
	local buf = buffer.new()
	buf:put("local rawget, f, node, chain = rawget, ...\n")
	buf_argsload(buf, node)
	buf_header(buf)
	buf:putf("local r = f(")
	buf_argscall(buf, node)
	buf:put(")\n")
	if gettag(node) == "call" then
		buf:put("if r ~= nil then return r end\n")
	else
		buf:put("if r ~= true then return false end\n")
	end
	buf_tailcall(buf, "chain")
	buf_end(buf)
	return load(buf, code.chunkname(describe(node)))(callfunc(node), node, chain)
end

local function emit_chain_ctrl(node, chain)
	local ctrl = emit(node)
	local buf = buffer.new()
	buf:put("local rawset, ctrl, chain = rawset, ...\n")
	buf_header(buf)
	buf_setcont(buf, "chain")
	buf_tailcall(buf, "ctrl")
	buf_end(buf)
	return load(buf, code.chunkname(describe(node)))(ctrl, chain)
end

local function emit_chain(node, chain)
	if not chain then
		return emit(node)
	end
	if gettag(node) == "call" or gettag(node) == "check" then
		return emit_chain_func(node, chain)
	else
		return emit_chain_ctrl(node, chain)
	end
end

function emit_node.all(node)
	local chain = nil
	for i=#node, 1, -1 do
		chain = emit_chain(node[i], chain)
	end
	return chain or ctrl_continue
end

-- any ---------------------------------

function emit_node.any(node)
	local buf = buffer.new()
	buf:put("local copycont, mem_save, mem_load, mem_delete")
	local branches = {}
	for i=1, #node do
		buf:putf(", branch%d", i)
		branches[i] = emit(node[i])
	end
	buf:put(" = ...\n")
	buf_header(buf)
	buf:put("local r\nlocal sp = mem_save()\nlocal top2,base2=top+1+top-base,top+1\n")
	for i=1, #node do
		if i>1 then
			buf:put("mem_load(sp)\n")
		end
		if i == #node then
			buf:putf("mem_delete(sp) do return branch%d(stack, base, top) end\n", i)
		else
			buf:putf("copycont(stack, base2, base, top+1-base) r = branch%d(stack, base2, top2) if r then goto out end\n", i)
		end
	end
	buf:put("::out:: mem_delete(sp) return r end\n")
	return load(buf, code.chunkname(describe(node)))(
		copycont, mem_save, mem_load, mem_delete, unpack(branches))
end

-- first -------------------------------

local function first_aux(stack, base, top)
	stack[stack[top]] = true
	return ctrl_continue(stack, base, top-1)
end

function emit_node.first(node)
	local buf = buffer.new()
	buf:put("local copycont, mem_save, mem_load, mem_delete, first_aux")
	local branches = {}
	for i=1, #node do
		buf:putf(", branch%d", i)
		branches[i] = emit(node[i])
	end
	buf:put(" = ...\n")
	buf_header(buf)
	buf:put("stack[top+1] = false\n") -- top+1 = probe
	buf:put("local top2, base2 = top+2+top-base, top+2\n")
	buf:put("local r\n")
	buf:put("local sp = mem_save()\n")
	for i=1, #node do
		if i>1 then
			buf:put("mem_load(sp)\n")
		end
		if i == #node then
			buf:putf("mem_delete(sp) do return branch%d(stack, base, top) end\n", i)
		else
			buf:put("copycont(stack, base2, base, top+1-base)\n")
			buf:put("stack[top2+1] = top+1\n")
			buf:put("stack[top2+2] = first_aux\n")
			buf:putf("r = branch%d(stack, base2, top2+2) if r or stack[top+1] then goto out end\n", i)
		end
	end
	buf:put("::out:: mem_delete(sp) return r end\n")
	return load(buf, code.chunkname(describe(node)))(
		copycont, mem_save, mem_load, mem_delete, first_aux, unpack(branches))
end

-- single ------------------------------

local function single_aux(stack, base, top)
	local r = stack[top]
	if r.single then
		r.single = false
		return ctrl_continue(stack, base, top-1)
	else
		return "single.skip"
	end
end

local function ctrl_single(stack, base, top, chain)
	-- TODO: come up with a better solution that doesn't require allocation
	-- (editing the stack directly doesn't work because it's copied when branching)
	stack[top+1] = { single=true }
	stack[top+2] = single_aux
	local r = chain(stack, base, top+2)
	if r ~= "single.skip" then return r end
end

function emit_node.single(node)
	return load([[
		local ctrl_single, chain = ...
		return function(stack, base, top)
			return ctrl_single(stack, base, top, chain)
		end
	]], code.chunkname(describe(node)))(ctrl_single, emit(node.node))
end

-- ifelse ------------------------------

function emit_node.ifelse(node)
	local buf = buffer.new()
	buf:put("local c, t, f, node = ...\n")
	buf_argsload(buf, node.c)
	buf_header(buf)
	buf:put("return (c(")
	buf_argscall(buf, node.c)
	buf:put(") and t or f)(stack, base, top)\n")
	buf_end(buf)
	return load(buf, code.chunkname(describe(node)))(callfunc(node.c), emit(node.t), emit(node.f))
end

-- loop --------------------------------

local function emit_loop_call(node)
	return load([[
		local f, ctrl_continue = ...
		local function loop_aux(stack, base, top)
			local r = f()
			if r == nil then return loop_aux(stack, base, top) end
			if r == true then return ctrl_continue(stack, base, top) end
			return r
		end
		return loop_aux
	]], code.chunkname(describe(node)))(callfunc(node.node), ctrl_continue)
end

local function emit_loop(node)
	return load([[
		local loopid = 0
		local ctrl, ctrl_continue = ...
		local function loop_aux(stack, base, top)
			if stack[top-1] == loop_aux and stack[top-2] == stack[top] then
				return "loop.continue"
			end
			stack[top+1] = loop_aux
			local r = ctrl(stack, base, top+1)
			if r == "loop.continue" then
				return loop_aux(stack, base, top-1)
			end
			if r == true then
				return ctrl_continue(stack, base, top-1)
			end
			return r
		end
		return function(stack, base, top)
			stack[top+1] = loopid
			loopid = loopid+1
			return loop_aux(stack, base, top+1)
		end
	]], code.chunkname(describe(node)))(emit(node.node), ctrl_continue)
end

function emit_node.loop(node)
	if gettag(node) == "call" then
		return emit_loop_call(node)
	else
		return emit_loop(node)
	end
end

-- call/cc -----------------------------

local function ctrl_callcc(stack, base, top, func)
	local base0, top0 = stack[0], stack[1]
	stack[0], stack[1] = base, top
	local r = func(stack)
	stack[0], stack[1] = base0, top0
	return r
end

function emit_node.callcc(node)
	return load([[
		local ctrl_callcc, func = ...
		return function(stack, base, top)
			return ctrl_callcc(stack, base, top, func)
		end
	]], code.chunkname(describe(node)))(ctrl_callcc, node.f)
end

-- dynamic -----------------------------

local function ctrl_dynamic(stack, base, top, ctrl, ret)
	if ctrl then
		return emit(tocontrol(ctrl))(stack, base, top)
	else
		return ret
	end
end

function emit_node.dynamic(node)
	return load([[
		local ctrl_dynamic, func = ...
		return function(stack, base, top)
			return ctrl_dynamic(stack, base, top, func())
		end
	]], code.chunkname(describe(node)))(ctrl_dynamic, node.f)
end

---- Entry point ---------------------------------------------------------------

local function exec(node)
	return emit(tocontrol(node))(newcont())
end

--------------------------------------------------------------------------------

return {
	all      = all,
	any      = any,
	first    = first,
	nothing  = nothing,
	skip     = skip,
	optional = optional,
	try      = try,
	call     = call,
	check    = check,
	single   = single,
	loop     = loop,
	callcc   = callcc,
	dynamic  = dynamic,
	exec     = exec
}
