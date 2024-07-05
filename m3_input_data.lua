local data = ...

if type(data) == "string" then
	data = assert(load("return "..data))()
end

local i = 0
local function data_next()
	i = i+1
	return data[i]
end

return {
	next = data_next
}
