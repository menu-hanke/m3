local host_shutdown = require("m3_host").shutdown
if host_shutdown then
	host_shutdown()
end

if require("m3_environment").mode == "mp" then
	require("m3_mp").shutdown()
end
