local insert = table.insert

local function table_write(tab)
	local data = tab.data
	return function(x) return insert(data, x) end
end

local function table_read(tab)
	local data = tab.data
	return function() return data end
end

local table_mt = {
	data = {
		type  = "table",
		write = table_write,
		read  = table_read
	}
}

local function new()
	return setmetatable({data={}}, table_mt)
end

return {
	new = new
}
