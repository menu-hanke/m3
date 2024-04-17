-- model (m3) API.

local array     = require "m3_array"
local data      = require "m3_data"
local de        = require "m3_debug"
local effect    = require "m3_effect"
local fhk       = require "m3_fhk"
local mem       = require "m3_mem"
local pipe      = require "m3_pipe"
local prototype = require "m3_prototype"
local struct    = require "m3_struct"

local m3 = {}

m3.dataframe    = array.dataframe
m3.vec          = array.vec
m3.data         = data.register
m3.effect       = effect.effect
m3.print        = de.pprint
m3.new          = mem.new
m3.newarray     = mem.newarray
m3.proto        = prototype.toproto
m3.query        = fhk.query
m3.graphfn      = fhk.graphfn
m3.struct       = struct.new

m3.pipe = {
	connect     = pipe.connect,
	new         = pipe.new,
	map         = pipe.map,
	filter      = pipe.filter
}

return m3
