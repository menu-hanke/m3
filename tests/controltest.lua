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

local function checkflat(result)
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

local function check(...)
	local flat = {}
	flatten(flat, { "(root)", ... })
	checkflat(flat)
end

return {
	put   = putnode,
	node  = node,
	check = check
}
