local environment = require "m3_environment"
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
if environment.mode == "mp" then
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
	special      = "\x1b[34m",
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

local function cmpkey(a, b)
	if type(a) == "number" then
		if type(b) == "number" then
			return a < b
		else
			return true
		end
	else
		if type(b) == "number" then
			return false
		else
			return tostring(a) < tostring(b)
		end
	end
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
	elseif type(x) == "table" then
		if rec[x] then
			putcolor(buf, string.format("<recursive table reference %s>", x), "recursive")
			return
		end
		rec[x] = true
		local mt = getmetatable(x)
		if mt and mt["m3$pretty"] then
			local o = mt["m3$pretty"](x, buf, indent, fmt, rec)
			if o then putpp(o, buf, indent, fmt, rec) end
			return
		end
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
		local keys = {}
		for k in pairs(x) do table.insert(keys, k) end
		table.sort(keys, cmpkey)
		for _,k in ipairs(keys) do
			local v = x[k]
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

local function vpretty(flags, sep, ...)
	local buf = buffer.new()
	local fmt = flags2fmt(flags)
	for i=1, select("#", ...) do
		if i > 1 and sep then buf:put(sep) end
		local x = select(i, ...)
		-- special case: top-level strings are printed as-is
		if type(x) == "string" then
			buf:put(x)
		else
			putpp(select(i, ...), buf, "", fmt, {})
		end
	end
	return buf
end

local function pprint(...)
	trace(vpretty(select("#", ...) > 1 and "s" or "sni", "\t", ...))
end

---- trace messages ------------------------------------------------------------

local function trace_data(D)
	trace(string.format("DATA  %s", pretty(D.data, "sni")))
end

local function fpfmt(fp)
	return -fp, require("m3_mem").stack.top+fp
end

local function trace_save(fp)
	trace(string.format("SAVE  0x%x (0x%x)", fpfmt(fp)))
end

local function trace_load(fp)
	trace(string.format("LOAD  0x%x (0x%x)", fpfmt(fp)))
end

local function caller(level)
	local info = debug.getinfo(level, "Sl")
	return string.format("%s:%d", info.short_src, info.currentline)
end

local function trace_read(...)
	-- NOTE: caller level here (and trace_write) depends on the wrappers in access
	trace(string.format("READ  %s -> %s", caller(4), vpretty("", "\t", ...)))
end

local function trace_write(...)
	trace(string.format("WRITE %s -> %s", caller(4), vpretty("", "\t", ...)))
end

local function trace_apply(ap)
	local buf = buffer.new()
	buf:put("APPLY ")
	local fmt = flags2fmt("")
	local comma = ""
	for k,v in pairs(ap.sub) do
		buf:put(comma)
		comma = "\t"
		putpp(k, buf, "", fmt, {})
		buf:put("<-")
		putpp(v, buf, "", fmt, {})
	end
	trace(tostring(buf))
end

local function mask2str(mask)
	local idx = {}
	for i=0, 63 do
		if bit.band(mask, bit.lshift(1ull, i)) ~= 0 then
			table.insert(idx, i)
		end
	end
	return table.concat(idx, ",")
end

local function trace_mask(mask)
	trace(string.format("MASK  {%s}", mask2str(mask)))
end

local trace_events = {
	data  = { mask="d", func=trace_data },
	save  = { mask="s", func=trace_save },
	load  = { mask="s", func=trace_load },
	mask  = { mask="s", func=trace_mask },
	read  = { mask="r", func=trace_read },
	write = { mask="w", func=trace_write },
	apply = { mask="a", func=trace_apply }
}

local TRACE_ALL = "dsrwa"
local trace_dispatch = {}

local function trace_off() end

local function trace_on(flags)
	if flags == true then flags = TRACE_ALL end
	local target = setmetatable({}, {__index=function() return trace_off end})
	for event,def in pairs(trace_events) do
		if flags:match(def.mask) then
			target[event] = def.func
		end
	end
	trace_dispatch = ffi.metatype("struct {}", {__index=target})
	return load([[
		local dispatch = ...
		return function(event, ...) return dispatch[event](...) end
	]])(trace_dispatch)
end

local event = load([[
	local target = ...
	return function(...) return target(...) end
]])(trace_off)

local function settrace(flags)
	local target
	if flags then
		target = trace_on(flags)
	else
		target = trace_off
	end
	debug.setupvalue(event, 1, target)
end

local function gettrace(event)
	local f = trace_dispatch[event]
	if f and f ~= trace_off then return f end
end

--------------------------------------------------------------------------------

return {
	settrace = settrace,
	gettrace = gettrace,
	event    = event,
	putpp    = putpp,
	pretty   = pretty,
	pprint   = pprint
}
