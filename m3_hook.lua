local pipe = require "m3_pipe"

return {
	mem_save = pipe.new(),
	mem_load = pipe.new()
}
