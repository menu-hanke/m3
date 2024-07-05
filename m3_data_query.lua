local fhk = require "m3_fhk"

local function expr_emitread(expr, ctx)
	local query = expr.query["m3$query"]
	local uv = string.format("_%p", query)
	if not ctx.uv[uv] then
		ctx.uv[uv] = query
		ctx.head:putf("local %s_1", uv)
		for i=2, #expr.query do
			ctx.head:putf(", %s_%d", uv, i)
		end
		ctx.head:putf(" = %s()\n", uv)
	end
	ctx.tail:putf("%s_%d", uv, expr.idx)
end

local expr_mt = {
	["m3$meta"] = {
		read = expr_emitread
	}
}

local function query_index(query, expr)
	fhk.insert(query["m3$query"], expr)
	table.insert(query, expr)
	local e = setmetatable({idx=#query, query=query}, expr_mt)
	query[expr] = e
	return e
end

local query_mt = {
	__index = query_index
}

local function new()
	return setmetatable({ ["m3$query"] = fhk.query() }, query_mt)
end

return {
	new = new
}
