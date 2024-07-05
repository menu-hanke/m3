local function ident(name)
	name = name:gsub("[^%w_]", "_")
	if name:sub(1,1):match("%d") then
		name = "_" .. name
	end
	return name
end

return {
	ident = ident
}
