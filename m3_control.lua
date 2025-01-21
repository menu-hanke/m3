-- control library.

local data = require "m3_data"
local mem = require "m3_mem"
local buffer = require "string.buffer"
local rawget, rawset = rawget, rawset

---- opcodes -------------------------------------------------------------------

local function gettag(x)
	return type(x) == "table" and x.tag or nil
end

-- do nothing. NOP.
local nothing = { tag = "nothing" }

-- skip branch.
local skip = { tag = "skip" }

-- function call
local function call(f, ...)
	return {
		tag   = "call",
		f     = f,
		n     = select("#", ...),
		...
	}
end

local function toedges(edges, dest)
	for _,e in ipairs(edges) do
		if type(e) == "table" and not data.istransaction(e) then
			if e.tag then
				table.insert(dest, e)
			else
				toedges(e, dest)
			end
		else
			-- either it's a function or something that has a __call metamethod,
			-- or it will error at runtime.
			table.insert(dest, call(e))
		end
	end
	return dest
end

-- apply sequence edge1 -> edge2 -> ... -> edgeN
local function all(edges)
	return {
		tag   = "all",
		edges = toedges(edges, {})
	}
end

--          /-> edge1
-- branch  * -> edge2
--          \ ...
--          \-> edgeN
local function any(edges)
	return {
		tag   = "any",
		edges = toedges(edges, {})
	}
end

-- special case of `any`: either apply `node` or don't apply it.
local function optional(node)
	return any { node, nothing }
end

-- pick first branch and skip all successive ones.
local function first(edge)
	return {
		tag  = "first",
		edge = edge
	}
end

-- special case: apply function if it leads to a valid state
local function try(node)
	return first(optional(node))
end

-- branching loop:
--  /-> init -> next -> continue
-- * -> next -> continue
--  \ ...
--  \-> next -> continue
--  \-> next returns false, no branch
local function loop(opt)
	return {
		tag  = "loop",
		init = type(opt) == "table" and opt.init,
		next = type(opt) == "table" and opt.next or opt
	}
end

-- dynamic dispatch. user function returns instruction.
local function dynamic(f)
	return {
		tag = "dynamic",
		f   = f
	}
end

---- control flow optimization -------------------------------------------------

-- TODO

---- code emit -----------------------------------------------------------------
-- protocol:
-- all control functions have the signature `function(stack, base, top)`.
-- `stack` is a lua table representing the execution state as a stack of continuations:
--          +------------+
--          |      .     |
--          |      .     |
--          |      .     |
--          +------------+
--          |     ...    | -----> top of the previous frame
--          +------------+
--          | cont       |
--          +------------+ <+-----------------+
--          | arg (n0)   |  | a single frame. |
--          +------------+  +-----------------+
--          | arg (n0-1) |  |
--          +------------|  |
--          |     ...    |  |
--          +------------+  |
--          | arg 2      |  |
--          +------------+  |
--          | link (*)   | ------> points to `base` of previous frame (always arg1 of ret)
--          +------------+  |
-- base --> | ret        | ------> base callback, either setups a new continuation or exits the frame
--          +------------+  | <+-------------------------------------+
--          | arg (n1)   |  |  | the current continuation.           |
--          +------------+  |  | this part is copied when branching. |
--          |     ...    |  |  +-------------------------------------+
--          +------------+  |  |
--          | arg 1      |  |  |
--          +------------+  |  |
--          | cont 1     |  |  |
--          +------------+  |  |
--          |     ...    |  |  |
--          +------------+  |  |
--  top --> |     ...    |  |  |
--          +------------+  |  |
--          | cont n     | ------> currently executing continuation
--          +------------+ <+ <+
--
-- (*) this may be skipped, see `emit_first`

local function chunkname(base)
	return string.format("=m3: insn %s", base)
end

local function debugname(x)
	if type(x) == "table" and x.tag then
		return x.tag
	elseif type(x) == "function" then
		local info = debug.getinfo(x, "S")
		return info.short_src:match("[^/]*$") .. ":" .. info.linedefined
	else
		return tostring(x)
	end
end

local emitfunc = {}

local function emit(node)
	if type(node) == "function" then
		node = call(node)
	end
	if node.__code then
		if node.__code == true then
			-- trampoline hack for recursive calls
			node.__code = load([[
				local _target
				return function(stack, base, top)
					return _target(stack, base, top)
				end
			]], chunkname(string.format("trampoline@%s", debugname(node))))()
		end
		return node.__code
	end
	node.__code = true
	local code = emitfunc[node.tag](node)
	-- was emit() called recursively?
	if type(node.__code) == "function" then
		debug.setupvalue(node.__code, 1, code)
	end
	node.__code = code
	return code
end

-- runtime / auxiliary -----------------

-- note: this will overwrite if n < 4.
local function copyrange(tab, dest, src, n)
	if n <= 4 then
		local a = rawget(tab, src)
		local b = rawget(tab, src+1)
		local c = rawget(tab, src+2)
		local d = rawget(tab, src+3)
		rawset(tab, dest,   a)
		rawset(tab, dest+1, b)
		rawset(tab, dest+2, c)
		rawset(tab, dest+3, d)
	else
		for i=0, n-1 do
			rawset(tab, dest+i, rawget(tab, src+i))
		end
	end
end

-- copy continuation of previous frame to current frame.
local function copyprev(stack, base, narg)
	local link = rawget(stack, base-1)
	-- don't copy ret, link and args (= narg+1)
	local ncopy = base - link - (narg+1)
	copyrange(stack, base+1, link+1, ncopy)
	-- new top is at base+ncopy
	return base+ncopy
end

local function exit() end

local function newstack()
	return {[0]=exit}, 0, 0
end

local function checkstack(stack, base, top)
	if stack then
		return stack, base, top
	else
		return newstack()
	end
end

-- control flow ------------------------

local function buf_header(buf)
	buf:put("return function(stack,base,top)\n")
end

local function buf_end(buf)
	buf:put("end")
end

-- jump to next continuation.
local function buf_continue(buf)
	buf:put("return rawget(stack,top)(stack,base,top-1)\n")
end

-- tailcall with current continuation.
local function buf_tailcall(buf, ctrl)
	buf:putf("return %s(stack,base,top)\n", ctrl)
end

-- call `ret` to exit frame.
local function buf_ret(buf)
	buf:put("return rawget(stack,base)(stack,base,base-1)\n")
end

-- restore `base` and `top` on frame exit.
local function buf_restore(buf, narg)
	buf:put("base = rawget(stack, top)\n")
	buf:putf("top = top-%d\n", narg or 1)
end

-- push continuation.
local function buf_setcont(buf, cont)
	buf:put("top = top+1\n")
	buf:putf("rawset(stack,top,%s)\n", cont)
end

-- special -----------------------------

local function ctrl_continue(stack, base, top)
	return rawget(stack,top)(stack,base,top-1)
end

local function ctrl_skip(stack, base)
	return rawget(stack,base)(stack,base,base-1)
end

function emitfunc.skip()
	return ctrl_skip
end

-- call --------------------------------

local function buf_argsload(buf, node)
	for i=1, node.n do
		buf:putf("local arg%d = node[%d]\n", i, i)
	end
end

local function buf_argscall(buf, node)
	if node.n >= 1 then
		buf:put("arg1")
	end
	for i=2, node.n do
		buf:putf(", arg%d", i)
	end
end

function emitfunc.call(node)
	local buf = buffer.new()
	buf:put("local rawget, f, node = rawget, ...\n")
	buf_argsload(buf, node)
	buf_header(buf)
	buf:put("local r = f(")
	buf_argscall(buf, node)
	buf:put(")\n")
	buf:put("if r == false then\n")
	buf_ret(buf)
	buf:put("end\n")
	buf_continue(buf)
	buf_end(buf)
	local f = node.f
	if data.istransaction(f) then
		f = f.func
	end
	return load(buf, chunkname(string.format("call@%s", debugname(f))))(f, node)
end

-- all ---------------------------------

local function emit_chain_func(node, chain)
	local buf = buffer.new()
	buf:put("local rawget, f, node, chain = rawget, ...\n")
	buf_argsload(buf, node)
	buf_header(buf)
	buf:putf("local r = f(")
	buf_argscall(buf, node)
	buf:putf(")\n")
	buf:put("if r == false then\n")
	buf_ret(buf)
	buf:put("end\n")
	buf_tailcall(buf, "chain")
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("chain call@%s", debugname(f)))
	)(node.f, node, chain)
end

local function emit_chain_ctrl(node, chain)
	local ctrl = emit(node)
	local buf = buffer.new()
	buf:put("local rawset, ctrl, chain = rawset, ...\n")
	buf_header(buf)
	buf_setcont(buf, "chain")
	buf_tailcall(buf, "ctrl")
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("chain ctrl %s", debugname(node)))
	)(ctrl, chain)
end

local function emit_chain(node, chain)
	if not chain then
		return emit(node)
	end
	if gettag(node) == "call" then
		return emit_chain_func(node, chain)
	else
		return emit_chain_ctrl(node, chain)
	end
end

function emitfunc.all(node)
	local chain = nil
	for i=#node.edges, 1, -1 do
		local e = node.edges[i]
		if gettag(e) ~= "nothing" then
			chain = emit_chain(e, chain)
		end
	end
	return chain or ctrl_continue
end

-- any ---------------------------------

local function buf_branch_jump(buf, node)
	if gettag(node) == "nothing" then
		buf_continue(buf)
	else
		buf_tailcall(buf, "ctrl")
	end
end

local function emit_branch_ctrl(node)
	return gettag(node) ~= "nothing" and emit(node)
end

-- arg2: fp
local function emit_branch_tail(node)
	local buf = buffer.new()
	buf:put("local rawget, mem_load, ctrl = rawget, ...\n")
	buf_header(buf)
	buf:put("mem_load(rawget(stack, top-1))\n")
	buf_restore(buf, 2)
	buf_branch_jump(buf, node)
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("any$tail %s", debugname(node)))
	) (mem.load, emit_branch_ctrl(node))
end

-- arg2: fp
local function emit_branch_chain(node, chain)
	local buf = buffer.new()
	buf:put("local rawset, rawget, mem_load, chain, ctrl, copyprev = rawset, rawget, ...\n")
	buf_header(buf)
	buf:put("mem_load(rawget(stack, top-1))\n")
	buf:put("rawset(stack, base, chain)\n")  -- ret
	buf:put("top = copyprev(stack, base, 2)\n")
	buf_branch_jump(buf, node)
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("any$branch %s", debugname(node)))
	) (mem.load, chain, emit_branch_ctrl(node), copyprev)
end

local function emit_branch_head(node, chain)
	local buf = buffer.new()
	buf:put("local rawset, mem_save, chain, ctrl, copyrange = rawset, ...\n")
	buf_header(buf)
	buf:put("rawset(stack, top+1, mem_save())\n") -- arg2 (fp)
	buf:put("rawset(stack, top+2, base)\n")  -- link
	buf:put("rawset(stack, top+3, chain)\n")  -- ret
	buf:put("copyrange(stack, top+4, base+1, top-base)\n")
	buf:put("base, top = top+3, top+3+(top-base)\n")
	buf_branch_jump(buf, node)
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("any$head %s", debugname(node)))
	) (mem.save, chain, emit_branch_ctrl(node), copyrange)
end

function emitfunc.any(node)
	if #node.edges == 0 then
		return ctrl_skip
	end
	if #node.edges == 1 then
		return emit(node.edges[1])
	end
	local chain = emit_branch_tail(node.edges[#node.edges])
	for i=#node.edges-1, 2, -1 do
		chain = emit_branch_chain(node.edges[i], chain)
	end
	return emit_branch_head(node.edges[1], chain)
end

-- first -------------------------------
-- hack: to save a stack slot, we will omit the link.
-- instead our stack layout will look like:
--          +------------+
--          | prev. cont | <--+
--          +------------+    |
-- base --> | ret        |    |  (1)
--          +------------+ <<<-----------------+
--          | prev. base |    |  (2)           |
--          +------------+    |                |
--          | prev. top  |----+  (3)           |  this part is copyable
--          +------------+                     |
--  top --> | jump       |       (4)           |
--          +------------+ <<<-----------------+
-- this works because `jump` can never be popped off the stack,
-- since calling it will exit the frame.
-- this means nothing can pop `base` and `top`, so it's safe for `ret` to assume this layout.

local function ctrl_firstret(stack, base)
	-- this is like in buf_restore() but link is at base+1 instead of top.
	base = rawget(stack, base+1)
	-- return on the base frame: if this function was called it means that something
	-- down in the stack called ret, ie. we did not finish the continuation via jump.
	-- this means the base frame should not finish either.
	return rawget(stack,base)(stack,base,base-1)
end

-- arg2: base
-- arg1: top
local function ctrl_firstjmp(stack, base, top)
	base = rawget(stack, top-1)
	-- unlike ret, this can be called from a copy, so we must actually load top to make the jump.
	top = rawget(stack, top)
	return rawget(stack,top)(stack,base,top-1)
end

function emitfunc.first(node)
	local buf = buffer.new()
	buf:put("local rawset, ctrl, ret, jmp = rawset, ...\n")
	buf_header(buf)
	buf:put("rawset(stack, top+1, ret)\n")
	buf:put("rawset(stack, top+2, base)\n")
	buf:put("rawset(stack, top+3, top)\n")
	buf:put("rawset(stack, top+4, jmp)\n")
	buf:put("base, top = top+1, top+4\n")
	buf_tailcall(buf, "ctrl")
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("first %s", debugname(node.edge)))
	) (emit(node.edge), ctrl_firstret, ctrl_firstjmp)
end

-- loop --------------------------------

-- TODO: move exec to arg3, since we don't need it if initl is not set.
-- TODO: should this load before calling next()? probably yes?

-- arg3: fp       (top-2)
-- arg2: exec     (top-1)
-- arg1: link     (top)
--       ret      (base = top+1)
local function emit_loop_next(initl, nextl)
	local buf = buffer.new()
	buf:put("local rawget, mem_load, copyprev, nextl = rawget, ...\n")
	buf_header(buf)
	if initl then
		buf:put("if nextl(rawget(stack, top-1)) == false then\n")
	else
		buf:put("if nextl() == false then\n")
	end
	-- we must _not_ run the previous frame's continuation any more,
	-- instead we must exit the previous frame.
	buf:put("base = rawget(stack, top)\n")
	-- this will fix top.
	buf_ret(buf)
	buf:put("end\n")
	buf:put("mem_load(rawget(stack, top-2))\n")
	buf:put("top = copyprev(stack, base, 3)")
	buf_continue(buf)
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("loop$next %s", debugname(nextl)))
	) (mem.load, copyprev, nextl)
end

local function emit_loop_init(initl, nextl, ctrl)
	local buf = buffer.new()
	buf:put("local rawget, rawset, mem_save, copyrange, initl, nextl, ctrl = rawget, rawset, ...\n")
	buf_header(buf)
	if initl then
		buf:put("local exec, ok = initl()\n")
		buf:put("if ok ~= false then\n")
	end
	-- must savepoint before nextl since it may modify sim state.
	buf:put("rawset(stack, top+1, mem_save())\n") -- arg3 (fp)
	if initl then
		buf:put("if nextl(exec) ~= false then \n")
		buf:put("rawset(stack, top+2, exec)\n") -- arg2 (exec)
	else
		buf:put("if nextl() ~= false then\n")
	end
	buf:put("rawset(stack, top+3, base)\n") -- link
	buf:put("rawset(stack, top+4, ctrl)\n") -- ret
	buf:put("copyrange(stack, top+5, base+1, top-base)\n")
	buf:put("base, top = top+4, top+4+(top-base)\n")
	buf_continue(buf)
	buf:put("end\n")
	if initl then
		buf:put("end\n")
	end
	buf_ret(buf)
	buf_end(buf)
	return load(
		buf,
		chunkname(string.format("loop$init %s -> %s", initl and debugname(initl), debugname(nextl)))
	) (mem.save, copyrange, initl, nextl, ctrl)
end

function emitfunc.loop(node)
	return emit_loop_init(node.init, node.next, emit_loop_next(node.init, node.next))
end

-- dynamic -----------------------------

local function dynamic_aux(stack, base, top, node)
	if node then
		local ctrl = node.__code
		if ctrl == nil then
			ctrl = emit(node)
		end
		return ctrl(stack, base, top)
	else
		return rawget(stack,top)(stack,base,top-1)
	end
end

function emitfunc.dynamic(node)
	return load([[
			local f, dynamic_aux = ...
			return function(stack, base, top)
				return dynamic_aux(stack, base, top, f())
			end
		]],
		chunkname(string.format("dynamic@%s", debugname(node.f)))
	) (node.f, dynamic_aux)
end

-- entry point -------------------------

local function compile(cfg)
	if cfg.__entry then
		return cfg.__entry
	end
	local entry = load([[
		local ctrl, checkstack = ...
		return function(stack, base, top)
			return ctrl(checkstack(stack, base, top))
		end
	]], chunkname("entry"))(emit(cfg), checkstack)
	cfg.__entry = entry
	return entry
end

local function exec(cfg)
	return compile(cfg)()
end

--------------------------------------------------------------------------------

return {
	all       = all,
	any       = any,
	optional  = optional,
	nothing   = nothing,
	skip      = skip,
	call      = call,
	first     = first,
	try       = try,
	loop      = loop,
	dynamic   = dynamic,
	compile   = compile,
	exec      = exec
}
