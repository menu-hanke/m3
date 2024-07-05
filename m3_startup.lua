_G.m3 = require "m3"

local state = require "m3_state"

-- load either `m3_mp` or `m3_serial` depending on the mode.
-- this must load before any startups, but must startup last.
local mode = require("m3_"..state.mode)

-- this must go first before any startup() call.
local host = state.setup(state.userdata)
package.loaded["m3_host"] = host

-- must go before anything else (except host).
require("m3_effect").startup()

-- must go after effect, before access
require("m3_layout").startup()

-- must go after layout, before pip
if host.on_data then host.on_data() end

-- must go after host.on_data
require("m3_pipe").startup()

-- must go before fhk
require("m3_access").startup()

-- must go after access
require("m3_fhk").startup()

-- this must go last.
return mode.startup()
