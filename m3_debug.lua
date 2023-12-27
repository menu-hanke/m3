local hook = require "m3_hook"
local mem = require "m3_mem"
local pipe = require "m3_pipe"
local state = require "m3_state"
local ffi = require "ffi"
local buffer = require "string.buffer"

local C = ffi.C

local colorterm = (function()
	local term = os.getenv("TERM")
	return (term and term:match("color")) or os.getenv("COLORTERM") ~= nil
end)()

local proccolor = {
	[0]="\x1b[33m", "\x1b[34m", "\x1b[35m", "\x1b[36m",
	"\x1b[1;33m", "\x1b[1;34m", "\x1b[1;35m", "\x1b[1;36m"
}

local function trace_plain(s)
	-- NOTE: keep this as a single write() call to keep it atomic
	-- (assuming unbuffered stderr up to 4KB, ie. don't rely on it),
	-- so debug messages from workers don't get mixed up.
	io.stderr:write(s.."\n")
end

local trace
if state.mode == "mp" then
	if colorterm then
		trace = function(s)
			local id = C.m3__mp_proc_id
			if id > 1 then
				local c = ((id-2)%(#proccolor+1))
				s = string.format("[%s%d\x1b[0m] %s", proccolor[c], id-1, s)
			end
			trace_plain(s)
		end
	else
		trace = function(s)
			local id = C.m3__mp_proc_id
			if id > 1 then
				s = string.format("[%d] %s", id-1, s)
			end
			trace_plain(s)
		end
	end
else
	trace = trace_plain
end

---- trace messages ------------------------------------------------------------

local memstate = mem.state
local memptr = tonumber(ffi.cast("intptr_t", ffi.cast("void *", memstate)))

local function trace_save()
	trace(string.format("SAVE  0x%x (%d bytes)", memstate.fbase-memptr, memptr-memstate.v.cursor))
end

local function trace_load()
	trace(string.format("LOAD  0x%x", memstate.fbase-memptr))
end

local function traceon(flags)
	flags = flags or "s"
	if flags:match("s") then
		pipe.connect(hook.mem_save, trace_save)
		pipe.connect(hook.mem_load, trace_load)
	end
end

---- pretty printing -----------------------------------------------------------

local function isnumkey(k, n)
	return type(k) == "number" and k == math.floor(k) and k >= 1 and k <= n
end

local function isident(k)
	-- returns true for reserved words, but that's ok.
	return type(k) == "string" and #k > 0 and k:match("^%a[%w_]*$")
end

local tokencolor = {
	["nil"]      = "\x1b[31m",
	["true"]     = "\x1b[35m",
	["false"]    = "\x1b[35m",
	string       = "\x1b[32m",
	tabkey       = "\x1b[36m",
	number       = "\x1b[33m",
	curlybracket = "\x1b[1m",
}

local function putcolor_ansi(buf, text, token)
	local color = tokencolor[token]
	if color then
		buf:put(color, text, "\x1b[0m")
	else
		buf:put(text)
	end
end

local function getpretty(x)
	return x["m3$pretty"]
end

local function putpp(x, buf, indent, fmt, rec)
	local putcolor = fmt.putcolor
	if x == nil then
		putcolor(buf, "nil", "nil")
	elseif x == true then
		putcolor(buf, "true", "true")
	elseif x == false then
		putcolor(buf, "false", "false")
	elseif type(x) == "number" then
		putcolor(buf, string.format("%g", x), "number")
	elseif type(x) == "string" then
		-- it may contain embedded quotes but that's fine,
		-- we're not trying to be perfect here.
		putcolor(buf, string.format('"%s"', x), "string")
	elseif pipe.ispipe(x) then
		-- TODO: pipe flags + connections
		buf:putf("<pipe %s>", x)
	elseif type(x) == "table" then
		if rec[x] then
			putcolor(buf, string.format("<recursive table reference %s>", x), "recursive")
			return
		end
		rec[x] = true
		putcolor(buf, "{", "curlybracket")
		local ind = indent..fmt.indent
		local n = #x
		local comma = fmt.newline..ind
		local sep = ", "..fmt.newline..ind
		for i=1, n do
			buf:put(comma)
			comma = sep
			putpp(x[i], buf, ind, fmt, rec)
		end
		for k,v in pairs(x) do
			if not isnumkey(k, n) then
				buf:put(comma)
				comma = sep
				if isident(k) then
					putcolor(buf, k, "tabkey")
				else
					putcolor(buf, "[", "squarebracket")
					putpp(k, buf, "", fmt.tabkey, rec)
					putcolor(buf, "]", "squarebracket")
				end
				buf:put(fmt.space, "=", fmt.space)
				putpp(v, buf, ind, fmt, rec)
			end
		end
		if comma == sep then buf:put(fmt.newline, indent) end
		putcolor(buf, "}", "curlybracket")
		rec[x] = false
	elseif type(x) == "function" then
		local info = debug.getinfo(x, "S")
		buf:putf("<function @ %s:%d>", info.short_src, info.linedefined)
	elseif pcall(getpretty, x) then
		x["m3$pretty"](x, buf, indent, fmt, rec)
	else
		buf:put(tostring(x))
	end
	-- TODO: cdata pretty printing w/ reflect
end

-- c: enable color   C: disable color
-- s: insert spaces
-- n: insert newlines
-- i: indent
local function flags2fmt(flags)
	flags = flags or "s"
	local color
	if flags:match("c") then
		color = putcolor_ansi
	elseif flags:match("C") then
		color = buffer.put
	else
		color = colorterm and putcolor_ansi or buffer.put
	end
	local space = flags:match("s") and " " or ""
	local newline = flags:match("n") and "\n" or ""
	local indent = flags:match("i") and "  " or ""
	local fmt = {
		putcolor = color,
		space    = space,
		newline  = newline,
		indent   = indent,
		tabkey   = {
			putcolor = color,
			space    = "",
			newline  = "",
			indent   = ""
		}
	}
	fmt.tabkey.tabkey = fmt.tabkey
	return fmt
end

local function pretty(x, flags)
	local buf = buffer.new()
	putpp(x, buf, "", flags2fmt(flags), {})
	return tostring(buf)
end

local function pprint(...)
	local n = select("#", ...)
	local fmt = flags2fmt(n > 1 and "s" or "sni")
	local buf = buffer.new()
	for i=1, n do
		local x = select(i, ...)
		-- special case: top-level strings are printed as-is
		if type(x) == "string" then
			buf:put(x)
		else
			putpp(select(i, ...), buf, "", fmt, {})
		end
	end
	trace(buf)
end

--------------------------------------------------------------------------------

return {
	traceon = traceon,
	putpp   = putpp,
	pretty  = pretty,
	pprint  = pprint
}
