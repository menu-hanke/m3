-- vim: ft=lua
local m3 = require "m3_api"

local restree = tree()
local result = {}
connect(restree, function(x) table.insert(result, x) end)

local writeres = write(restree)
local function put(x)
	return function() writeres(x) end
end

local function walk(tree, idx, node)
	if idx >= 0 then
		if tree.data[idx] ~= node.data then
			error(string.format("wrong data at index %d: %s ~= %s", idx, node.data, tree.data[idx]))
		end
	end
	local child = idx+1
	for _,x in ipairs(node) do
		if tree.link[child] ~= idx then
			error(string.format("wrong topology at index %d: %d ~= %d", child, tree.link[child], idx))
		end
		child = walk(tree, child, x)
	end
	return child
end

local function check(root)
	local tree = assert(result[1], "no tree")
	local idx = walk(tree, -1, root)
	assert(idx == tree.committed+1, "wrong tree size")
end

--------------------------------------------------------------------------------

test.simulate("control:all:empty", function()
	exec(all {
		all {},
		put(1)
	})
	check { { data=1 } }
end)

test.simulate("control:all:single", function()
	exec(all {
		put(1)
	})
	check { { data=1 } }
end)

test.simulate("control:all:chain", function()
	exec(all {
		put(1),
		put(2)
	})
	check { { data=1, { data=2 } } }
end)

test.simulate("control:any:empty", function()
	exec(all {
		any {},
		put(1)
	})
	check {}
end)

test.simulate("control:any:single", function()
	exec(any {
		put(1)
	})
	check { { data=1 } }
end)

test.simulate("control:any:branch", function()
	exec(all {
		any {
			put(1),
			put(2),
			put(3)
		},
		put(4)
	})
	check {
		{ data=1, {data=4} },
		{ data=2, {data=4} },
		{ data=3, {data=4} }
	}
end)

test.simulate("control:nothing", function()
	exec(all {
		put(1),
		nothing,
		put(2)
	})
	check { {data=1, {data=2}} }
end)

-- TODO: cdata api (?)
-- test("control:optional", function()
-- 	local state = m3.cdata("struct { uint32_t bit; uint32_t value; }")
-- 	local wstate = write(state)
-- 	local rstate = read(state)
-- 	local seen = {}
-- 	local function toggle()
-- 		local s = wstate()
-- 		s.value = s.value+2^s.bit
-- 	end
-- 	local function nextbit()
-- 		local s = wstate()
-- 		s.bit = s.bit + 1
-- 	end
-- 	local function setseen() seen[rstate().value] = true end
-- 	simulate(function()
-- 		exec(all {
-- 			optional(toggle), nextbit,
-- 			optional(toggle), nextbit,
-- 			optional(toggle), nextbit,
-- 			setseen
-- 		})
-- 		for i=0, 7 do assert(seen[i]) end
-- 	end)
-- end)

test.simulate("control:skip", function()
	exec(any {
		all {
			skip,
			put(1)
		},
		put(2)
	})
	check { {data=2} }
end)

test.simulate("control:first:take-branch", function()
	exec(first(any {
		put(1),
		put(2)
	}))
	check { {data=1} }
end)

test.simulate("control:first:skip-branch", function()
	exec(first(any {
		all { skip, put(1) },
		put(2)
	}))
	check { {data=2} }
end)

test.simulate("control:first:nest", function()
	exec(first(any {
		skip,
		first(any {
			put(1),
			put(2)
		}),
		put(3)
	}))
	check { {data=1} }
end)

test.simulate("control:try", function()
	exec(all {
		try(all {
			skip,
			put(1)
		}),
		try(put(2))
	})
	check { {data=2} }
end)

-- see TODO in emit_loop_next
--test("control.loop", function()
--	instructions = all {
--		loop {
--			init = function() return {it=0} end,
--			next = function(state)
--				if state.it < 3 then
--					state.it = state.it + 1
--					tree(state.it)
--				else
--					return false
--				end
--			end
--		},
--		put("end")
--	}
--	sim()
--	check {
--		{ data=1, {data="end"} },
--		{ data=2, {data="end"} },
--		{ data=3, {data="end"} },
--	}
--end)

test.simulate("control:dynamic", function()
	exec(all {
		put(1),
		dynamic(function()
			return all { put(2) }
		end)
	})
	check { { data=1, {data=2} } }
end)

test.simulate("control:callstack-overwrite", function()
	local branch = all {
		any {
			all {},
			all {}
		},
		all {}
	}
	exec(all {
		branch,
		branch,
		put(1)
	})
	check {
		{data=1},
		{data=1},
		{data=1},
		{data=1}
	}
end)

test.simulate("control:deep-callstack", function()
	local num = 0
	local function inc() num = num+1 end
	local branch = all {
		any {
			all {},
			all {}
		},
		all {}
	}
	exec(all {
		all {
			all {
				all {
					all {
						all {
							branch,
							branch,
							branch,
							branch,
							inc
						},
						all {}
					},
					all {}
				},
				all {}
			},
			all {}
		},
		all {}
	})
	assert(num == 2^4)
end)

test.simulate("control:return-false", function()
	exec(any {
		all {
			function() return false end,
			put(1)
		},
		put(2)
	})
	check { {data=2} }
end)

test.simulate("control:recursion", function()
	local n = 0
	local node = all {
		function()
			if n == 10 then
				return false
			end
			n = n+1
		end
	}
	table.insert(node.edges, node)
	exec(node)
	assert(n == 10)
end)
