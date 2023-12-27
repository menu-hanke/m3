local ffi = require "ffi"

-- value and type doesn't matter as long as it's a pointer.
-- the correct type and value will be patched in later.
local function new()
	return ffi.cast("void *", 0)
end

-- NOTE: this is a hack that depends on luajit internals.
-- use it sparingly.
--
-- internally a pointer cdata is laid out in memory like so:
--
--    [GCcdata] [data]
--
-- where GCcdata is defined as:
--
-- struct GCcdata {
--   GCRef nextgc;
--   uint8_t marked;
--   uint8_t gct;
--   uint16_t ctypeid;
-- }
--
-- where GCRef is an uint64_t in LJ_GC64 mode, or uint32_t in non-LJ_GC64.
--
-- when we do
--   string.format("%p", cdata)
-- we get a pointer to [data] above, ie. one past the end of GCcdata,
-- and we use that to modify the ctypeid and data.
local function patch(ptr, ctype, value)
	local cdataptr = ffi.cast("void *", tonumber(string.format("%p", ptr):sub(3), 16))
	-- patch pointer data
	ffi.cast("void **", cdataptr)[0] = value
	-- patch ctypeid
	ffi.cast("uint16_t *", cdataptr)[ffi.abi "gc64" and -3 or -1] = tonumber(ctype)
	-- `ptr` now has the desired type and pointee!
	-- note that luajit assumes the content doesn't change so any jit code that referenced
	-- `ptr` is now invalid.
end

return {
	new   = new,
	patch = patch
}
