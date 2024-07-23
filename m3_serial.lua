assert(require("m3_environment").mode == "serial")

local effect = require "m3_effect"
local mem = require "m3_mem"

local function startup()
	local host = require "m3_host"
	local memload = mem.load
	local work = effect.unwrap(host.work)
	local fp = mem.save()
	return function()
		while true do
			if work() == false then return end
			memload(fp)
		end
	end
end

return {
	startup = startup
}
