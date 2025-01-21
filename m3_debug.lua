local environment = require "m3_environment"
local ffi = require "ffi"
local buffer = require "string.buffer"

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

local trace = trace_plain
if environment.parallel then
	require("m3_mp").proc_init(function(id)
		if colorterm and id>0 then
			trace = function(s)
				local c = id%(#proccolor+1)
				trace_plain(string.format("[%s%d\x1b[0m] %s", proccolor[c], id, s))
			end
		else
			trace = function(s)
				trace_plain(string.format("[%d] %s", id, s))
			end
		end
	end)
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
		if type(x) == "string" and flags:match("t") then
			buf:put(x)
		else
			putpp(select(i, ...), buf, "", fmt, {})
		end
	end
	return buf
end

local function pprint(...)
	trace(vpretty(select("#", ...) > 1 and "st" or "stni", "\t", ...))
end

---- trace messages ------------------------------------------------------------

local function trace_data(objs)
	local buf = buffer.new()
	local fmt = flags2fmt("sni")
	fmt.short = true
	for _,o in ipairs(objs) do
		buf:put("DATA  ")
		putpp(o.obj, buf, "", fmt, {})
		if o.map then
			fmt.putcolor(buf, string.format(" -> %s", o.map), "tabkey")
		end
		trace(buf:get())
	end
end

local function fpfmt(fp)
	return -fp, require("m3_mem").arena.top+fp
end

local function trace_save(fp)
	trace(string.format("SAVE  0x%x (0x%x)", fpfmt(fp)))
end

local function trace_load(fp)
	trace(string.format("LOAD  0x%x (0x%x)", fpfmt(fp)))
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

local function trace_sql(stmt, ...)
	trace(string.format("SQL   %s %s", stmt:sql(), vpretty("s", "\t", ...)))
end

local function trace_code(code)
	trace(string.format("CODE  %s", code))
end

local trace_events = {
	data  = { mask="d", func=trace_data },
	save  = { mask="s", func=trace_save },
	load  = { mask="s", func=trace_load },
	mask  = { mask="s", func=trace_mask },
	sql   = { mask="q", func=trace_sql  },
	code  = { mask="c", func=trace_code },
}

local TRACE_ALL = "dsq"

local function dispatch_on(flags)
	if flags == true then flags = TRACE_ALL end
	local target = setmetatable({}, {__index=function() return false end})
	for event,def in pairs(trace_events) do
		if flags:match(def.mask) then
			target[event] = def.func
		end
	end
	return ffi.metatype("struct {}", {__index=target})
end

local dispatch_off = ffi.metatype("struct {}", { __index = function() return false end })

local event, enabled = load([[
	local dispatch = ...
	return function(event, ...)
		local func = dispatch[event]
		if func then return func(...) end
	end, function(event) return dispatch[event] end
]])(dispatch_off)

local function settrace(flags)
	local target
	if flags then
		target = dispatch_on(flags)
	else
		target = dispatch_off
	end
	debug.setupvalue(event, 1, target)
	debug.setupvalue(enabled, 1, target)
end

--------------------------------------------------------------------------------

return {
	trace    = settrace,
	event    = event,
	enabled  = enabled,
	putpp    = putpp,
	pretty   = pretty,
	pprint   = pprint
}
