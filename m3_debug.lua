local environment = require "m3_environment"
local event = require "m3_event"
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

local function trace_off() end

local function trace_heap(slots)
	error("TODO")
-- 	table.sort(slots, function(a,b)
-- 		return ffi.cast("intptr_t", a.ptr or 0) < ffi.cast("intptr_t", b.ptr or 0)
-- 	end)
-- 	for _,slot in ipairs(slots) do
-- 		if slot.block then
-- 			trace(string.format(
-- 				"%02d.0x%x..+%-3d %s",
-- 				slot.block,
-- 				ffi.cast("intptr_t", slot.ptr),
-- 				ffi.sizeof(slot.ctype),
-- 				slot.ctype
-- 			))
-- 		elseif slot.ptr then
-- 			local acs = access.get(slot)
-- 			if acs == "r" then acs = "r-"
-- 			elseif acs == "w" then acs = "-w"
-- 			else acs = "--" end
-- 			trace(string.format(
-- 				"%s.0x%x..+%-3d %s",
-- 				acs,
-- 				ffi.cast("intptr_t", slot.ptr),
-- 				ffi.sizeof(slot.ctype),
-- 				slot.ctype
-- 			))
-- 		end
-- 	end
end

local function trace_save(fp)
	trace(string.format("SAVE  0x%x", -fp))
end

local function trace_load(fp)
	trace(string.format("LOAD  0x%x", -fp))
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
	heap  = { flags="h", func=trace_heap },
	save  = { flags="s", func=trace_save },
	load  = { flags="s", func=trace_load },
	mask  = { flags="s", func=trace_mask },
	read  = { flags="a", func=trace_read },
	write = { flags="a", func=trace_write }
}

-- trace flags:
--   a    access
--   h    heap layout
--   s    save points
local function traceon(flags)
	flags = flags or "ahs"
	local events = setmetatable({}, {__index=function() return trace_off end})
	for event,def in pairs(trace_events) do
		if flags:match(def.flags) then
			events[event] = def.func
		end
	end
	local ct = ffi.metatype("struct {}", {__index=events})
	event.listener(load([[
		local ct = ...
		return function(event, ...) return ct[event](...) end
	]])(ct))
end

--------------------------------------------------------------------------------

return {
	traceon = traceon,
	putpp   = putpp,
	pretty  = pretty,
	pprint  = pprint
}
