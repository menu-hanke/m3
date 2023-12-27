local de = require "m3_debug"
local mem = require "m3_mem"
local patchptr = require "m3_patchptr"
local prototype = require "m3_prototype"
local state = require "m3_state"
local buffer = require "string.buffer"
local ffi = require "ffi"

local struct_needpatch = {}

local function struct_settabfunc(fields)
	local buf = buffer.new()
	buf:put("return function(struct, tab)\n")
	for _,f in ipairs(fields) do
		buf:putf("struct.%s = tab.%s or 0\n", f, f)
	end
	buf:put("end")
	return assert(load(buf))()
end

local function struct_table(struct)
	local tab = {}
	for _,f in ipairs(struct["m3$fields"]) do
		tab[f] = struct[f]
	end
	return tab
end

local function struct_tostring(struct)
	local fields = struct["m3$fields"]
	local buf = buffer.new()
	buf:put("{")
	for i,f in ipairs(fields) do
		if i > 1 then buf:put(",") end
		buf:putf(" %s=%s", f, struct[f])
	end
	buf:put(" }")
	return tostring(buf)
end

local function struct_pretty(struct, ...)
	return de.putpp(struct_table(struct), ...)
end

local function struct_newct(proto)
	local fields = {}
	for name,info in pairs(proto) do
		table.insert(fields, {
			name  = name,
			ctype = info.ctype,
			align = ffi.alignof(info.ctype)
		})
	end
	table.sort(fields, function(a, b) return b.align < a.align end)
	local ctarg = {}
	local fname = {}
	local ctdef = buffer.new()
	ctdef:put("struct {\n")
	for i,f in ipairs(fields) do
		ctdef:putf("$ %s;\n", f.name)
		ctarg[i] = f.ctype
		fname[i] = f.name
	end
	ctdef:put("}")
	local struct_settab = struct_settabfunc(fname)
	return ffi.metatype(ffi.typeof(tostring(ctdef), unpack(ctarg)), {
		__index = {
			["m3$type"]   = "struct",
			["m3$proto"]  = proto,
			["m3$fields"] = fname,
			["m3$settab"] = struct_settab,
			["m3$pretty"] = struct_pretty
		},
		__tostring = struct_tostring
	})
end

local struct_ctcache = setmetatable({}, {
	__index = function(self, proto)
		self[proto] = struct_newct(proto)
		return self[proto]
	end
})

local function struct_protoct(proto)
	return struct_ctcache[proto]
end

local function struct_new(region, proto)
	proto = prototype.toproto(proto)
	if state.ready then
		return mem.new(struct_protoct(proto), region)
	else
		local ptr = patchptr.new()
		prototype.setpatchptr(ptr, proto)
		struct_needpatch[ptr] = {proto=proto, region=region}
		return ptr
	end
end

local function startup()
	for ptr, info in pairs(struct_needpatch) do
		local cd = mem.new(struct_protoct(info.proto), info.region)
		patchptr.patch(ptr, ffi.typeof(cd), cd)
	end
	struct_needpatch = nil
end

--------------------------------------------------------------------------------

return {
	new     = struct_new,
	startup = startup
}
