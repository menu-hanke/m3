local host_shutdown = require("m3_host").shutdown
if host_shutdown then
	host_shutdown()
end

local state = require "m3_state"
if state.mode == "mp" then
	require("m3_mp").shutdown()
end
