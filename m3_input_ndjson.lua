local cjson = require "cjson"

local fp = assert(io.open(...))

local function ndjson_close()
	fp:close()
end

local function ndjson_next()
	local line = fp:read("*l")
	return line and cjson.decode(line)
end

return {
	close = ndjson_close,
	next  = ndjson_next
}
