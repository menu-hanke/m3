-- model (m3) API.

local data        = require "m3_data"
local de          = require "m3_debug"

local m3 = {}

m3.read  = data.read
m3.write = data.write
m3.apply = data.apply
m3.print = de.pprint

return m3
