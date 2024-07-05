local funcs = {}

local function call(func, ...)
	local n = select("#", ...)
	if n > 0 then
		local f, args = func, {...}
		func = function() return f(unpack(args, 1, n)) end
	end
	table.insert(funcs, func)
end

local function startup()
	for _,f in ipairs(funcs) do
		f()
	end
	funcs = nil
end

return {
	call    = call,
	startup = startup
}
