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

require("m3_array").startup()
require("m3_struct").startup()

-- must go after array
require("m3_prototype").startup()

-- must go right after prototype
if host.on_data then host.on_data() end

require("m3_pipe").startup()

-- two reasons to flush() here:
--   (1) startup does some naughty things with pointers (see m3_patchptr.lua), so this makes sure
--       there's no traces referencing the patched pointers.
--   (2) we don't need any of the startup code anymore, we need to jit simulation code now.
--       this saves a trace flush in the forked worker processes.
jit.flush()

state.ready = true

-- this must go last.
return mode.startup()
