local access = require "m3_access"
local mem = require "m3_mem"

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
	return access.defer(function() return tree_compile(tree).flush end)
end

local function tree_write(tree)
	access.read(tree.parent)
	return access.use(
		access.defer(function() return tree_compile(tree).write end),
		access.write(tree.parent)
	)
end

local tree_mt = {
	data = {
		type  = "tree",
		write = tree_write,
		read  = tree_read
	}
}

local function branch_write(branch)
	access.read(branch.tree.parent)
	return access.defer(function() return tree_compile(branch.tree).commit end)
end

local branch_mt = {
	data = {
		type  = "tree.branch",
		write = branch_write
	}
}

local function new()
	local tree = setmetatable({ parent=mem.slot { ctype="int32_t", init=-1 } }, tree_mt)
	tree.branch = setmetatable({ tree=tree }, branch_mt)
	return tree
end

return {
	new = new
}
