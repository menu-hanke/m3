-- vim: ft=lua

local ffi = require "ffi"

local x = 0
local function incx(g)
	x = x+g
	return x
end

local incx_fp = ffi.cast("double (*)(double)", incx)

data.define(string.format([[
model global x0 = call C [0x%x] (g: double): double
model global y0 = call C [0x%x] (g: double): double
# hack: the purpose of the query parameter is to prevent the inlining of x0
model global x = x0 + query.dummy
model global y = y0 + query.dummy
]], ffi.cast("uintptr_t", incx_fp), ffi.cast("uintptr_t", incx_fp)))

-- TODO: transaction():write("g") should (be made to) work
local setg = data.transaction():update("global", {g=data.arg()})
local getx = data.transaction():read("x"):bind("dummy", 0)
local gety = data.transaction():read("y"):bind("dummy", 0)

control.simulate = function()
	setg(1)
	assert(getx() == 1)
	-- should reuse same instance
	assert(getx() == 1)
	local sp = control.save()
	-- should trigger first computation of y
	assert(gety() == 2)
	-- should still reuse same instance until first write after savepoint
	assert(getx() == 1)
	-- should create new instance after write
	setg(2)
	assert(getx() == 4)
	assert(gety() == 6)
	-- should use old instance again
	control.load(sp)
	assert(getx() == 1)
	assert(gety() == 2)
end
