-- vim: ft=lua
local m3 = require "m3_api"

test("struct:write", function()
	local struct = m3.struct()
	local writex = m3.write(struct.x)
	local readx = m3.read(struct.x)
	simulate(function()
		writex(123)
		assert(readx() == 123)
	end)
end)

test("struct:unused", function()
	local struct = m3.struct()
	m3.read(struct.x)
	m3.write(struct.y)
	local _ = struct.z
end)

test("struct:write-save", function()
	local struct = m3.struct()
	local writex = m3.write(struct.x)
	local readx = m3.read(struct.x)
	simulate(function()
		writex(1)
		local fp = m3.save()
		assert(readx() == 1)
		writex(2)
		assert(readx() == 2)
		m3.load(fp)
		assert(readx() == 1)
	end)
end)

test("struct:input", function()
	local struct = m3.data("struct", m3.struct())
	local readxy = m3.read(struct.x, struct.y)
	input {{ struct = { x=123, y=456 } }}
	simulate(function()
		local x, y = readxy()
		assert(x == 123 and y == 456)
	end)
end)
