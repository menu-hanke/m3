-- vim: ft=lua
local m3 = require "m3_api"
local ffi = require "ffi"

local function checkvec(a, b)
	if #a ~= #b then
		error(string.format("wrong length: %d ~= %d", #a, #b))
	end
	local ta, tb = a, b
	if type(ta) ~= "table" then ta = a:table() end
	if type(tb) ~= "table" then tb = b:table() end
	for i=1, #ta do
		if ta[i] ~= tb[i] then
			error(string.format("wrong data at index %d: %s ~= %s", i, ta[i], tb[i]))
		end
	end
end

---- vectors -------------------------------------------------------------------

test.simulate("vec:append", function()
	local vec = m3.vec("double")
	vec:append(1)
	vec:append(2)
	checkvec(vec, {1,2})
	local fp = m3.save()
	vec:append(3)
	checkvec(vec,{1,2,3})
	m3.load(fp)
	checkvec(vec,{1,2})
end)

test.simulate("vec:append-realloc", function()
	local vec = m3.vec("double")
	local tab = {}
	for i=1, 32 do
		vec:append(i)
		tab[i] = i
	end
	checkvec(vec, tab)
end)

test.simulate("vec:alloc", function()
	local vec = m3.vec("double")
	local idx = vec:alloc(3)
	vec.data[idx] = 1
	vec.data[idx+1] = 2
	vec.data[idx+2] = 3
	checkvec(vec, {1,2,3})
end)

test.simulate("vec:mutate", function()
	local vec = m3.vec("double")
	vec:alloc(3)
	m3.save()
	local ptr1 = vec.data
	vec:mutate()
	assert(ffi.cast("intptr_t", vec.data) < ffi.cast("intptr_t", ptr1))
	local ptr2 = vec.data
	vec:mutate()
	assert(ptr2 == vec.data)
end)

test.simulate("vec:extend", function()
	local vec = m3.vec("double")
	vec:extend{1,2,3}
	checkvec(vec, {1,2,3})
	vec:extend(m3.vec("double", {4,5,6}))
	checkvec(vec, {1,2,3,4,5,6})
	vec:extend(m3.vec("int", {7,8,9}))
	checkvec(vec,{1,2,3,4,5,6,7,8,9})
end)

test.simulate("vec:clear", function()
	local vec = m3.vec("double")
	vec:extend{1,2,3}
	vec:clear()
	checkvec(vec,{})
end)

test.simulate("vec:delete", function()
	local vec = m3.vec("double")
	vec:extend{0,1,2,3,4,5,6,7,8,9}
	vec:delete(9)
	checkvec(vec,{0,1,2,3,4,5,6,7,8})
	local fp = m3.save()
	vec:delete{0,1,4,6,7,8}
	checkvec(vec,{2,3,5})
	m3.load(fp)
	checkvec(vec,{0,1,2,3,4,5,6,7,8})
end)

---- data frames ---------------------------------------------------------------

test.simulate("dataframe:alloc", function()
	local df = m3.dataframe{x="double"}
	df:addrow{x=1}
	df:addrow{x=2}
	df:addrow{x=3}
	checkvec(df:table("x"), {1,2,3})
end)

test.simulate("dataframe:setcols", function()
	local df = m3.dataframe{x="double", y="int"}
	df:setcols {
		x = {1,2,3},
		y = m3.vec("int", {4,5,6})
	}
	checkvec(df:table("x"), {1,2,3})
	checkvec(df:table("y"), {4,5,6})
end)

test.simulate("dataframe:setrows", function()
	local df = m3.dataframe{x="double", y="int"}
	df:setrows{{x=1,y=2},{x=3,y=4},{x=5,y=6}}
	checkvec(df:table("x"), {1,3,5})
	checkvec(df:table("y"), {2,4,6})
end)

test.simulate("dataframe:delete", function()
	local df = m3.dataframe{x="double", y="int"}
	df:setcols{
		x = {0,1,2,3,4,5,6,7,8,9},
		y = {0,-1,-2,-3,-4,-5,-6,-7,-8,-9}
	}
	df:delete(8)
	checkvec(df:table("x"), {0,1,2,3,4,5,6,7,9})
	checkvec(df:table("y"), {0,-1,-2,-3,-4,-5,-6,-7,-9})
	local fp = m3.save()
	df:delete{0,1,2,6}
	checkvec(df:table("x"), {3,4,5,7,9})
	checkvec(df:table("y"), {-3,-4,-5,-7,-9})
	m3.load(fp)
	checkvec(df:table("x"), {0,1,2,3,4,5,6,7,9})
	checkvec(df:table("y"), {0,-1,-2,-3,-4,-5,-6,-7,-9})
end)

test.simulate("dataframe:mutate", function()
	local df = m3.dataframe{x="double", y="int"}
	df:alloc(5)
	df.x[0] = 1 df.y[0] = -1
	df.x[1] = 2 df.y[1] = -2
	df.x[2] = 3 df.y[2] = -3
	df.x[3] = 4 df.y[3] = -4
	df.x[4] = 5 df.y[4] = -5
	local fp = m3.save()
	local x, y = df.x, df.y
	df:mutate("y")
	assert(df.x == x)
	assert(df.y ~= y)
	df.y[0] = 100
	checkvec(df:table("x"), {1,2,3,4,5})
	checkvec(df:table("y"), {100,-2,-3,-4,-5})
	m3.load(fp)
	checkvec(df:table("x"), {1,2,3,4,5})
	checkvec(df:table("y"), {-1,-2,-3,-4,-5})
end)
