-- model (m3) API.

local access      = require "m3_access"
local data        = require "m3_data"
local data_frame  = require "m3_data_frame"
local data_struct = require "m3_data_struct"
local de          = require "m3_debug"
local effect      = require "m3_effect"
local fhk         = require "m3_fhk"
local mem         = require "m3_mem"
local pipe        = require "m3_pipe"

local m3 = {}

m3.data           = data.data
m3.pairs          = data.pairs
m3.read           = access.read
m3.write          = access.write
m3.mutate         = access.mutate
m3.graphfn        = fhk.graphfn
m3.effect         = effect.effect
m3.print          = de.pprint
m3.new            = mem.new
m3.newarray       = mem.newarray
m3.struct         = data_struct.new
m3.dataframe      = data_frame.new

m3.pipe = {
	connect       = pipe.connect,
	new           = pipe.new,
	map           = pipe.map,
	filter        = pipe.filter
}

return m3
