local hook = require "m3_hook"
local state = require "m3_state"
local ffi = require "ffi"

local band = bit.band
local tonumber = tonumber
local alignof, cast, sizeof, typeof = ffi.alignof, ffi.cast, ffi.sizeof, ffi.typeof
local intptr_t, voidp = typeof("intptr_t"), typeof("void *")
local C = ffi.C

-- TODO: this needs a commit() on windows
local mem = ffi.cast("m3_MemState *", state.vm.frame.addr)
do
	local vm = state.vm
	mem.x.cursor = vm.scratch.addr + vm.scratch.size
	mem.v.cursor = vm.vstack.addr + vm.vstack.size
	mem.f.cursor = vm.frame.addr + vm.frame.size
	mem.fbase = mem.f.cursor
end

-- NOTE: consider generating save/load at startup and only generate hsave/hload call if
-- they're actually nonempty?
local hsave = hook.mem_save
local hload = hook.mem_load

local function mem_save()
	local fp = C.m3__save(mem)
	if fp < 0 then error("savepoint stack overflow") end
	hsave()
	return fp
end

local function mem_load(fp)
	C.m3__load(mem, fp)
	hload()
end

local function region_meta(region, base)
	return {
		__index = { ptr = region, base = base }
	}
end

-- the point of this hack is to constify the base/cursor addresses for alloc() calls.
local regions = ffi.typeof(
	[[
		struct {
			$ scratch;
			$ vstack;
			$ frame;
		}
	]],
	ffi.metatype("struct {}", region_meta(mem.x, state.vm.scratch.addr)),
	ffi.metatype("struct {}", region_meta(mem.v, state.vm.vstack.addr)),
	ffi.metatype("struct {}", region_meta(mem.f, state.vm.frame.addr))
)()

local function mem_region(region)
	return regions[region].ptr
end

local function mem_base(region)
	return regions[region].base
end

local function mem_alloc(size, align, region)
	local reg = regions[region].ptr
	local p = band(reg.cursor-size, cast(intptr_t, -align))
	if p < regions[region].base then error('region out of memory') end
	reg.cursor = p
	return p
end

local function mem_allocp(size, align, region)
	return (cast(voidp, mem_alloc(size, align, region)))
end

local function mem_reallocp(ptr, oldsize, newsize, align, region)
	return (C.m3__mem_realloc(regions[region].ptr, regions[region].base, ptr, oldsize, newsize,
		align))
end

-- ffi.typeof("$*", ...) isn't compiled so we cache the results instead.
local ctptr = {}

local function typeofp(ctype)
	local ctid = tonumber(ctype)
	local p = ctptr[ctid]
	if not p then
		p = typeof("$*", ctype)
		ctptr[ctid] = p
	end
	return p
end

local function mem_new(ctype, region)
	ctype = typeof(ctype)
	return (cast(typeofp(ctype), mem_alloc(sizeof(ctype), alignof(ctype), region)))
end

local function mem_newarray(ctype, num, region)
	ctype = typeof(ctype)
	return (cast(typeofp(ctype), mem_alloc(num*sizeof(ctype), alignof(ctype), region)))
end

local function intptr(ptr)
	return cast(intptr_t, ptr)
end

local function mem_iswritable(ptr)
	return intptr(ptr) < mem.fbase
end

local xtop = state.vm.scratch.addr + state.vm.scratch.size
local function mem_resetx()
	mem.x.cursor = xtop
end

return {
	state      = mem,
	zeros      = state.zeros,
	save       = mem_save,
	load       = mem_load,
	region     = mem_region,
	base       = mem_base,
	alloc      = mem_alloc,
	allocp     = mem_allocp,
	reallocp   = mem_reallocp,
	new        = mem_new,
	newarray   = mem_newarray,
	iswritable = mem_iswritable,
	resetx     = mem_resetx
}
