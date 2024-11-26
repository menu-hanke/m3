local env = debug.getregistry()["m3$environment"]
package.loaded["m3_environment"] = env
env.mode = env.parallel and "mp" or "serial"

_G.m3 = require "m3"

-- load either `m3_mp` or `m3_serial` depending on the mode.
-- this must load before any startups, but must startup last.
local mode = require("m3_"..env.mode)

-- this must go first before any startup() call.
package.loaded["m3_host"] = env.setup(env.userdata)

require("m3_data").startup()

-- must go last
return mode.startup()
