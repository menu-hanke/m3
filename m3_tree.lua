local data = require "m3_data"

local function tree_compile(tree)
	if not tree.funcs then
		tree.funcs = load([[
			local max = math.max
			local parent = ...
			local committed = -1
			local data, link = {}, {}
			local function write(x)
				local p = parent[0]
				local idx = max(p, committed)+1
				link[idx] = p
				data[idx] = x
				parent[0] = idx
			end
			local function commit()
				committed = max(committed, parent[0])
			end
			local function flush()
				local v = {data=data, link=link, committed=committed}
				data, link, committed = {}, {}, -1
				return v
			end
			return {write=write, commit=commit, flush=flush}
		]])(tree.parent.ptr)
	end
	return tree.funcs
end

local function tree_read(tree)
	return tree_compile(tree).flush
end

local function tree_write(tree)
	return tree_compile(tree).write
end

local function branch_write(branch)
	return tree_compile(branch.tree).commit
end

local function new()
	local parent = data.memslot("int32_t", -1)
	local tree = data.dynamic {
		visit  = function(_,f) return f(nil, parent) end,
		reader = tree_read,
		writer = tree_write,
		parent = parent
	}
	tree.branch = data.dynamic {
		visit  = function(_,f) return f(nil, tree) end,
		writer = branch_write,
		tree   = tree,
	}
	return tree
end

return {
	new = new
}
