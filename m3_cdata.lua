local ffi = require "ffi"
local cdef = require "m3_cdef"

local DUMMY = {
	[tonumber(ffi.typeof "double")]   = 0/0,
	[tonumber(ffi.typeof "float")]    = 0/0,
	[tonumber(ffi.typeof "uint64_t")] = 0xffffffffffffffffull,
	[tonumber(ffi.typeof "int64_t")]  = 0x8000000000000000ll,
	[tonumber(ffi.typeof "uint32_t")] = 0xffffffff,
	[tonumber(ffi.typeof "int32_t")]  = 0x80000000,
	[tonumber(ffi.typeof "uint16_t")] = 0xffff,
	[tonumber(ffi.typeof "int16_t")]  = 0x8000,
	[tonumber(ffi.typeof "uint8_t")]  = 0xff,
	[tonumber(ffi.typeof "int8_t")]   = 0x80,
	[tonumber(ffi.typeof "bool")]     = 0x80
}

local function dummy(ct)
	return ct and DUMMY[tonumber(ffi.typeof(ct))]
end

local function ident(name)
	name = name:gsub("[^%w_]", "_")
	if name:sub(1,1):match("%d") then
		name = "_" .. name
	end
	return name
end

local function isfp(ctype)
	return ffi.istype("double", ctype) or ffi.istype("float", ctype)
end

local function check(e)
	if e ~= 0 then
		error(cdef.errmsg[e], 2)
	end
end

return {
	dummy = dummy,
	ident = ident,
	isfp  = isfp,
	check = check
}
