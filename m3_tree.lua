local mem = require "m3_mem"
local pipe = require "m3_pipe"
local max = math.max

local function tree_write(tree, x)
	local parent = tree.parent[0]
	local idx = max(parent, tree.committed) + 1
	tree.link[idx] = parent
	tree.data[idx] = x
	tree.parent[0] = idx
end

local function tree_commit(tree)
	tree.committed = max(tree.committed, tree.parent[0])
end

local function tree_flush(tree)
	local data, link, committed = tree.data, tree.link, tree.committed
	tree.data, tree.link, tree.committed = {}, {}, -1
	return {data=data, link=link, committed=committed}
end

local function tree_new(forest)
	local tree = {
		parent    = mem.new("int32_t", "vstack"),
		committed = -1,
		data      = {},
		link      = {}
	}
	tree.parent[0] = -1
	-- TODO: pipe.sponge: value pipe + action pipe
	local sink, source = pipe.fuse(function(x) return tree_write(tree, x) end)
	-- TODO: sink is pure (source is plain, no explicit mark needed)
	pipe.connect(forest.tree, function() return source(tree_flush(tree)) end)
	pipe.connect(forest.branch, function() return tree_commit(tree) end)
	return sink
end

local forest_mt = {
	__call = tree_new
}

local function forest()
	return setmetatable({
		tree = pipe.new(),
		branch = pipe.new()
	}, forest_mt)
end

return {
	forest = forest
}
