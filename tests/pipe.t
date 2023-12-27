-- vim: ft=lua

test("pipe:empty", function()
	local empty = pipe.new()
	simulate(empty)
end)

test("pipe:single-consumer", function()
	local tab = {}
	local p = pipe.new()
	connect(p, tab)
	simulate(function()
		p(1) p(2)
		assert(tab[1] == 1 and tab[2] == 2)
	end)
end)

test("pipe:multi-consumer", function()
	local tab1, tab2 = {}, {}
	local p = pipe.new()
	connect(p, tab1)
	connect(p, tab2)
	simulate(function()
		p(1) p(2)
		assert(tab1[1] == 1 and tab1[2] == 2)
		assert(tab2[1] == 1 and tab2[2] == 2)
	end)
end)

test("pipe:map", function()
	local tab = {}
	local p = pipe.new()
	connect(connect(p, pipe.map(function(x) return -x end)), tab)
	simulate(function()
		p(1) p(2)
		assert(tab[1] == -1 and tab[2] == -2)
	end)
end)

test("pipe:filter", function()
	local tab = {}
	local p = pipe.new()
	connect(connect(p, pipe.filter(function(x) return x%2 == 1 end)), tab)
	simulate(function()
		p(1) p(2) p(3)
		assert(tab[1] == 1 and tab[2] == 3)
	end)
end)

test("pipe:recursion", function()
	local tab = {}
	local p1 = pipe.new()
	local p2 = pipe.filter(function(x) return x < 10 end)
	local p3 = pipe.filter(function(x) return x > 5 end)
	local p4 = pipe.map(function(x) return x+1 end)
	connect(p1, p3)
	connect(p3, tab)
	connect(p1, p2)
	connect(p2, p4)
	connect(p4, p1)
	simulate(function()
		p1(1)
		-- TODO: the result is dependent on the connection order.
		-- the pipe optimizer should reorder connections so that recursion doesn't blow up the
		-- call stack.
		assert(
			tab[1] == 6
			and tab[2] == 7
			and tab[3] == 8
			and tab[4] == 9
			and tab[5] == 10
			and tab[6] == nil
		)
	end)
end)
