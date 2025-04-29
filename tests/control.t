-- vim: ft=lua
local current = data.cdata { ctype = "int32_t", init = 1 }
local nodes = { {[0]="(root)"} }

local putnode = data.transaction()
	:mutate(current, function(current)
		current[0] = math.max(current[0], #nodes)+1
	end)
	:call(function(current, value)
		local idx = #nodes+1
		nodes[idx] = {[0]=value}
		table.insert(nodes[current], idx)
	end, current, data.arg(1))

local function node(value) return control.call(putnode, value) end

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
		control.simulate = {
			function()
				-- TODO: rewrite this to not use commit directly
				control.exec(control.all { insn, require("m3_data").commit })
				check(result)
			end
		}
	end
end

case(
	"all:empty",
	control.all { control.all {}, node(1) },
	tree { 1 }
)

case(
	"all:single",
	control.all { node(1) },
	tree { 1 }
)

case(
	"all:chain",
	control.all { node(1), node(2) },
	tree { {1, 2} }
)

case(
	"any:single",
	control.any { node(1) },
	tree { 1 }
)

case(
	"any:branch",
	control.all { control.any { node(1), node(2), node(3) }, node(4) },
	tree { {1, 4}, {2, 4}, {3, 4} }
)

case(
	"any:empty-after",
	control.all { control.any {}, node(1) },
	tree {}
)

case(
	"any:empty-before",
	control.any { control.all { node(1), control.any {} } },
	tree {}
)

case(
	"any:skip",
	control.any { node(1), control.all { node(2), control.any {} }, node(3) },
	tree { 1, 3 }
)

case(
	"nothing",
	control.all { node(1), control.nothing, node(2) },
	tree { {1, 2} }
)

case(
	"optional",
	function()
		local state = data.cdata { ctype="struct { uint32_t bit; uint32_t value; }" }
		local toggle = data.transaction():mutate(state, function(s) s.value = s.value+2^s.bit end)
		local nextbit = data.transaction():mutate(state, function(s) s.bit = s.bit+1 end)
		local getstate = data.transaction():read(state)
		return control.all {
			control.optional(toggle), nextbit,
			control.optional(toggle), nextbit,
			control.optional(toggle), nextbit,
			function() putnode(getstate().value) end
		}
	end,
	tree { 0b111, 0b011, 0b101, 0b001, 0b110, 0b010, 0b100, 0b000 }
)

case(
	"skip",
	control.any { control.all { control.skip, node(1) }, node(2) },
	tree { 2 }
)

case(
	"first:take-branch",
	control.first(control.any { node(1), node(2) }),
	tree { 1 }
)

case(
	"first:skip-branch",
	control.first(control.any { control.all { control.skip, node(1) }, node(2) }),
	tree { 2 }
)

case(
	"first:nest",
	control.first(control.any { control.skip, control.first(control.any { node(1), node(2) }), node(3) }),
	tree { 1 }
)

case(
	"try",
	control.all { control.try(control.all { control.skip, node(1) }), control.try(node(2)) },
	tree { 2 }
)

case(
	"dynamic",
	control.all { node(1), control.dynamic(function() return control.all { node(2) } end) },
	tree { {1, 2} }
)

case(
	"calcc",
	control.all {
		node(1),
		control.callcc(function(continue)
			local sp = control.save()
			putnode(2)
			local r = continue()
			if r ~= nil then goto out end
			control.load(sp)
			putnode(3)
			r = continue()
			::out::
			control.delete(sp)
			return r
		end),
		node(4)
	},
	tree { {1, {2, 4}, {3, 4}} }
)

local function nop() end
local nopbarrier = control.all { nop, nop }

case(
	"callstack-overwrite",
	function()
		local branch = control.all {
			control.any {
				nopbarrier,
				nopbarrier,
			},
			nopbarrier,
		}
		return control.all {
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
		local branch = control.all {
			control.any {
				nopbarrier,
				nopbarrier,
			},
			nopbarrier,
		}
		return control.all {
			control.all {
				control.all {
					control.all {
						control.all {
							control.all {
								branch,
								branch,
								branch,
								branch,
								node(1)
							},
							nopbarrier,
						},
						nopbarrier,
					},
					nopbarrier,
				},
				nopbarrier,
			},
			nopbarrier,
		}
	end,
	tree { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }
)

case(
	"return-false",
	control.any { control.all { function() return false end, node(1) }, node(2) },
	tree { 2 }
)

if test "control:recursion" then
	local n = 0
	local insn = control.all {
		function()
			if n == 10 then return false end
			n = n+1
		end
	}
	table.insert(insn, insn)
	control.simulate = {
		function()
			control.exec(insn)
			assert(n == 10)
		end
	}
end

if test "control:loop:call" then
	local n = 0
	local insn = control.loop(function()
		if n == 10 then return true end
		n = n+1
	end)
	control.simulate = {
		function()
			control.exec(insn)
			assert(n == 10)
		end
	}
end

case(
	"control:loop:insn",
	function()
		local state = data.cdata { ctype="struct { uint32_t bit; uint32_t value; }" }
		local toggle = data.transaction():mutate(state, function(s) s.value = s.value+2^s.bit end)
		local nextbit = data.transaction():mutate(state, function(s) s.bit = s.bit+1 end)
		local getstate = data.transaction():read(state)
		return control.all {
			control.loop(control.all {
				function() if getstate().bit >= 3 then return true end end,
				control.optional(toggle),
				nextbit,
			}),
			function() putnode(getstate().value) end
		}
	end,
	tree { 0b111, 0b011, 0b101, 0b001, 0b110, 0b010, 0b100, 0b000 }
)
