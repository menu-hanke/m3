-- vim: ft=lua
local m3 = require "m3_api"

local function checkdata(a, n, b, m)
	if n == nil and type(a) == "table" then n = #a end
	if m == nil and type(b) == "table" then m = #b end
	if n ~= m then
		error(string.format("wrong length: %d ~= %d", n, m))
	end
	local i = type(a) == "table" and 1 or 0
	local j = type(b) == "table" and 1 or 0
	for _=1, n do
		if a[i] ~= b[j] then
			error(string.format("wrong data at index a[%d] vs b[%d]: %s ~= %s", i, j, a[i], b[j]))
		end
		i = i+1
		j = j+1
	end
end

local function checkv(a, b)
	checkdata(a, #a, b, #b)
end

test("dataframe:readwrite", function()
	local df = m3.dataframe()
	local mutate = m3.mutate(df)
	local readxy = m3.read(df.x, df.y)
	simulate(function()
		mutate(function(o)
			o:addrow({x=1, y=-1})
			o:addrow({x=2, y=-2})
			o:addrow({x=3, y=-3})
		end)
		local x, y = readxy()
		checkv(x, {1, 2, 3})
		checkv(y, {-1, -2, -3})
	end)
end)

test("dataframe:writeempty", function()
	local df = m3.dataframe()
	local readn = m3.read(#df)
	local write = m3.write(df)
	simulate(function()
		write{}
		assert(readn() == 0)
	end)
end)

test("dataframe:writerows", function()
	local df = m3.dataframe()
	local readxy = m3.read(df.x, df.y)
	local write = m3.write(df)
	simulate(function()
		write {
			{x=1, y=2},
			{x=3, y=4},
			{x=5, y=6}
		}
		local x, y = readxy()
		checkv(x, {1, 3, 5})
		checkv(y, {2, 4, 6})
	end)
end)

test("dataframe:writecols", function()
	local df = m3.dataframe()
	local readxy = m3.read(df.x, df.y)
	local write = m3.write(df)
	simulate(function()
		write {
			x = {1, 2, 3},
			y = {4, 5, 6}
		}
		local x, y = readxy()
		checkv(x, {1, 2, 3})
		checkv(y, {4, 5, 6})
	end)
end)

test("dataframe:overwrite", function()
	local df = m3.dataframe()
	local readxy = m3.read(df.x, df.y)
	local write = m3.write(df)
	simulate(function()
		write { {x=1, y=2} }
		local x, y = readxy()
		checkv(x, {1})
		checkv(y, {2})
		write { x={1,2}, y={3,4} }
		local x, y = readxy()
		checkv(x, {1, 2})
		checkv(y, {3, 4})
	end)
end)

test("dataframe:write-savepoint", function()
	local df = m3.dataframe()
	local readx = m3.read(df.x)
	local write = m3.write(df)
	simulate(function()
		write { x={1,2,3} }
		local fp = m3.save()
		checkv(readx(), {1,2,3})
		write { x={4,5,6} }
		checkv(readx(), {4,5,6})
		m3.load(fp)
		checkv(readx(), {1,2,3})
	end)
end)

test("dataframe:append-savepoint", function()
	local df = m3.dataframe()
	local mutate = m3.mutate(df)
	local readx = m3.read(df.x)
	simulate(function()
		mutate(function(o) o:addrow { x=1 } end)
		local fp = m3.save()
		checkv(readx(), {1})
		mutate(function(o) o:addrow { x=2 } end)
		checkv(readx(), {1, 2})
		m3.load(fp)
		checkv(readx(), {1})
	end)
end)

test("dataframe:delete", function()
	local df = m3.dataframe()
	local mutate = m3.mutate(df)
	local readx = m3.read(df.x)
	local ready = m3.read(df.y)
	simulate(function()
		mutate(function(o)
			o:settab {
				x = {0,1,2,3,4,5,6,7,8,9},
				y = {0,-1,-2,-3,-4,-5,-6,-7,-8,-9}
			}
			o:delete(8)
		end)
		checkv(readx(), {0,1,2,3,4,5,6,7,9})
		checkv(ready(), {0,-1,-2,-3,-4,-5,-6,-7,-9})
		local fp = m3.save()
		mutate(function(o) o:delete{0,1,2,6} end)
		checkv(readx(), {3,4,5,7,9})
		checkv(ready(), {-3,-4,-5,-7,-9})
		m3.load(fp)
		checkv(readx(), {0,1,2,3,4,5,6,7,9})
		checkv(ready(), {0,-1,-2,-3,-4,-5,-6,-7,-9})
	end)
end)
