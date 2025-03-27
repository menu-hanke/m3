local db = require "m3_db"
local db_disconnect = db.disconnect

_G._m3_shutdown = function()
	db_disconnect(true)
end

require("m3_data").init()

-- TODO: m3_debug uses require, fix that first
-- TODO: mp also does this, unload must come later...
-- at this point no one is going to require "m3_*" modules any more, we can unload them
-- to save some memory.
-- for name in pairs(package.loaded) do
-- 	if name:sub(1,3) == "m3_" then
-- 		package.loaded[name] = "(unloaded)"
-- 	end
-- end

-- now all compiled traces are for initializer functions, and are now useless
jit.flush()

-- collect all the garbage we just created
collectgarbage()
collectgarbage()
