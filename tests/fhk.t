-- vim: ft=lua
local m3 = require "m3_api"

test("fhk:query", function()
	m3.data("struct", m3.struct())
	local writexy = m3.write("struct#x", "struct#y")
	local readxy = m3.read("struct#x + struct#y")
	simulate(function()
		writexy(1, 2)
		assert(readxy() == 3)
	end)
end)

test("fhk:input", function()
	m3.data("struct", m3.struct())
	local readxy = m3.read("struct#x + struct#y")
	input { {struct={x=1,y=2}} }
	simulate(function()
		assert(readxy() == 3)
	end)
end)

test("fhk:writemask", function()
	m3.data("struct", m3.struct())
	local writex = m3.write("struct#x")
	local readxy = m3.read("struct#x + struct#y")
	input { {struct={y=1}} }
	simulate(function()
		writex(1)
		assert(readxy() == 2)
		writex(2)
		assert(readxy() == 3)
	end)
end)

test("fhk:save", function()
	local df1 = m3.data("df1", m3.dataframe())
	local df2 = m3.data("df2", m3.dataframe())
	local mutate1 = m3.mutate(df1)
	local mutate2 = m3.mutate(df2)
	local readxy = m3.read("sum(df1#x) + sum(df2#y)")
	simulate(function()
		mutate1(function(o) o:settab { x={1} } end)
		assert(readxy() == 1)
		local fp1 = m3.save()
		mutate2(function(o) o:settab { y={1,2,3} } end)
		assert(readxy() == 1+1+2+3)
		local fp2 = m3.save()
		mutate2(function(o) o:addrow { y=4 } end)
		assert(readxy() == 1+1+2+3+4)
		m3.load(fp2)
		assert(readxy() == 1+1+2+3)
		m3.load(fp1)
		assert(readxy() == 1)
	end)
end)

test("fhk:automapping", function()
	local struct = m3.data("struct", m3.struct())
	local df = m3.data("df", m3.dataframe())
	defgraph [[
		model(df) x = y + struct#x
	]]
	local query = m3.read("df#x")
	local mutatedf = m3.mutate(df)
	local writesx = m3.write(struct.x)
	simulate(function()
		mutatedf(function(o) o:settab { y={1,2} } end)
		writesx(1)
		local x = query()
		assert(x[0] == 1+1 and x[1] == 1+2)
	end)
end)

test("fhk:mangle", function()
	m3.data("df", m3.dataframe())
	defgraph [[
		model(df) event{a{x}} = a{x}+1
		model(df) x = a{x}
	]]
	local write = m3.write("df")
	local query = m3.read("df#x")
	local event = m3.graphfn "event"
	simulate(function()
		write {{}}
		event()
		assert(query()[0] == 1)
	end)
end)

test("fhk:init", function()
	m3.data("struct", m3.struct())
	defgraph [[
		model(struct) init{x} = y
	]]
	local query = m3.read("struct#x")
	input { {struct = {y=123}} }
	simulate(function()
		assert(query() == 123)
	end)
end)

test("fhk:graphfn", function()
	local struct = m3.data("struct", m3.struct())
	defgraph [[
		model(struct) f{x} = x+1
	]]
	local f = m3.graphfn("f")
	local writex = m3.write(struct.x)
	local readx = m3.read(struct.x)
	simulate(function()
		writex(1)
		f()
		assert(readx() == 2)
	end)
end)
