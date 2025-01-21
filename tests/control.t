-- vim: ft=lua
local m3 = require "m3"

local current = cdata { ctype = "int32_t", init = 1 }
local nodes = { {[0]="(root)"} }

local putnode = transaction()
	:mutate(current, function(current)
		current[0] = math.max(current[0], #nodes)+1
	end)
	:call(function(current, value)
		local idx = #nodes+1
		nodes[idx] = {[0]=value}
		table.insert(nodes[current], idx)
	end, current, arg(1))

local function node(value) return call(putnode, value) end

local function check(result)
	local errors = {}
	for i=1, math.max(#nodes, #result) do
		local have = nodes[i] and table.concat(nodes[i], ",", 0, #nodes[i]) or "nil"
		local want = result[i] and table.concat(result[i], ",", 0, #result[i]) or "nil"
		if have ~= want then
			table.insert(errors, string.format(" * node %d: %s != %s", i, have, want))
		end
	end
	if #errors > 0 then
		error(string.format("computed != true:\n%s", table.concat(errors, "\n")))
	end
end

local function flatten(flat, node)
	local idx = #flat+1
	if type(node) == "table" then
		local n = { [0]=node[1] }
		flat[idx] = n
		for i=2, #node do
			n[i-1] = flatten(flat, node[i])
		end
	else
		flat[idx] = { [0]=node }
	end
	return idx
end

local function tree(branches)
	local flat = {}
	flatten(flat, { "(root)", unpack(branches) })
	return flat
end

local function case(name, insn, result)
	if test(string.format("control:%s", name)) then
		if type(insn) == "function" then
			insn = insn()
		end
		simulate {
			function()
				m3.exec(all { insn, m3.commit })
				check(result)
			end
		}
	end
end

case(
	"all:empty",
	all { all {}, node(1) },
	tree { 1 }
)

case(
	"all:single",
	all { node(1) },
	tree { 1 }
)

case(
	"all:chain",
	all { node(1), node(2) },
	tree { {1, 2} }
)

case(
	"any:single",
	any { node(1) },
	tree { 1 }
)

case(
	"any:branch",
	all { any { node(1), node(2), node(3) }, node(4) },
	tree { {1, 4}, {2, 4}, {3, 4} }
)

case(
	"any:empty-after",
	all { any {}, node(1) },
	tree {}
)

case(
	"any:empty-before",
	any { all { node(1), any {} } },
	tree {}
)

case(
	"any:skip",
	any { node(1), all { node(2), any {} }, node(3) },
	tree { 1, 3 }
)

case(
	"nothing",
	all { node(1), nothing, node(2) },
	tree { {1, 2} }
)

case(
	"optional",
	function()
		local state = cdata { ctype="struct { uint32_t bit; uint32_t value; }" }
		local toggle = transaction():mutate(state, function(s) s.value = s.value+2^s.bit end)
		local nextbit = transaction():mutate(state, function(s) s.bit = s.bit+1 end)
		local getstate = transaction():read(state)
		return all {
			optional(toggle), nextbit,
			optional(toggle), nextbit,
			optional(toggle), nextbit,
			function() putnode(getstate().value) end
		}
	end,
	tree { 0b111, 0b011, 0b101, 0b001, 0b110, 0b010, 0b100, 0b000 }
)

case(
	"skip",
	any { all { skip, node(1) }, node(2) },
	tree { 2 }
)

case(
	"first:take-branch",
	first(any { node(1), node(2) }),
	tree { 1 }
)

case(
	"first:skip-branch",
	first(any { all { skip, node(1) }, node(2) }),
	tree { 2 }
)

case(
	"first:nest",
	first(any { skip, first(any { node(1), node(2) }), node(3) }),
	tree { 1 }
)

case(
	"try",
	all { try(all { skip, node(1) }), try(node(2)) },
	tree { 2 }
)

case(
	"dynamic",
	all { node(1), dynamic(function() return all { node(2) } end) },
	tree { {1, 2} }
)

case(
	"callstack-overwrite",
	function()
		local branch = all {
			any {
				all {},
				all {}
			},
			all {}
		}
		return all {
			branch,
			branch,
			node(1)
		}
	end,
	tree { 1, 1, 1, 1 }
)

case(
	"deep-callstack",
	function()
		local branch = all {
			any {
				all {},
				all {}
			},
			all {}
		}
		return all {
			all {
				all {
					all {
						all {
							all {
								branch,
								branch,
								branch,
								branch,
								node(1)
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
		}
	end,
	tree { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }
)

case(
	"return-false",
	any { all { function() return false end, node(1) }, node(2) },
	tree { 2 }
)

if test "control:recursion" then
	local n = 0
	local insn = all {
		function()
			if n == 10 then return false end
			n = n+1
		end
	}
	table.insert(insn.edges, insn)
	simulate {
		function()
			m3.exec(insn)
			assert(n == 10)
		end
	}
end
