-- model (m3) API.

local access      = require "m3_access"
local data        = require "m3_data"
local data_frame  = require "m3_data_frame"
local data_pipe   = require "m3_data_pipe"
local data_struct = require "m3_data_struct"
local de          = require "m3_debug"
local effect      = require "m3_effect"
local fhk         = require "m3_fhk"
local mem         = require "m3_mem"

local m3 = {}

m3.data           = data.data
m3.read           = access.read
m3.write          = access.write
m3.connect        = access.connect
m3.access         = access.get
m3.cdata          = mem.slot
m3.graphfn        = function(...) return fhk.graph:fn(...) end
m3.effect         = effect.effect
m3.print          = de.pprint
m3.struct         = data_struct.new
m3.dataframe      = data_frame.new
m3.pipe           = data_pipe.new

return m3
