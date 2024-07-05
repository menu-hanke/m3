-- vim: ft=lua

test("input:struct", function()
	local struct = m3.data("struct", m3.struct())
	local readx = m3.read(struct.x)
	input { {struct={x=123}} }
	simulate(function()
		assert(readx() == 123)
	end)
end)

test("input:dataframe", function()
	local df = m3.data("df", m3.dataframe())
	local readx = m3.read(df.x)
	input { {df={{x=1}}} }
	simulate(function()
		assert(readx()[0] == 1)
	end)
end)

test("input:missing", function()
	local struct = m3.data("struct", m3.struct())
	local readx = m3.read(struct.x)
	local df = m3.data("df", m3.dataframe())
	local dfx = m3.read(df.x)
	input { {struct={x=123}} }
	simulate(function()
		assert(readx() == 123)
		assert(#dfx() == 0)
	end)
end)
