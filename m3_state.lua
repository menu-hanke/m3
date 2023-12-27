require "m3_cdef"
local ffi = require "ffi"
local registry = debug.getregistry()

local setup = registry["m3$setup"]
local userdata = registry["m3$userdata"]

local vm = registry["m3$vm"]
local mem = ffi.cast("m3_MemState *", vm.frame.addr)
local zeros = ffi.cast("void *", vm.zeros.addr)

-- these are only set when parallelization is enabled.
local parallel = registry["m3$parallel"]
local fork = registry["m3$fork"]
local shared = parallel and ffi.cast("void *", vm.shared.addr)

return {
	setup    = setup,
	userdata = userdata,
	vm       = vm,
	mem      = mem,
	zeros    = zeros,
	shared   = shared,
	parallel = parallel,
	fork     = fork,
	mode     = parallel and "mp" or "serial",
	ready    = false, -- toggled by m3_startup
}
