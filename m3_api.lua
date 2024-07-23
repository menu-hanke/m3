-- m3 embedding api.

local modapi       = require "m3"
local m3           = setmetatable({}, { __index = modapi })

local control      = require "m3_control"
local data         = require "m3_data"
local data_frame   = require "m3_data_frame"
local data_dynamic = require "m3_data_dynamic"
local data_pipe    = require "m3_data_pipe"
local data_table   = require "m3_data_table"
local data_tree    = require "m3_data_tree"
local data_struct  = require "m3_data_struct"
local fhk          = require "m3_fhk"
local mem          = require "m3_mem"

m3.control = {
	nothing        = control.nothing,
	skip           = control.skip,
	call           = control.call,
	all            = control.all,
	any            = control.any,
	optional       = control.optional,
	first          = control.first,
	try            = control.try,
	loop           = control.loop,
	dynamic        = control.dynamic,
	exec           = control.exec,
}

m3.obj = {
	dynamic        = data_dynamic.new,
	dataframe      = data_frame.new,
	pipe           = data_pipe.new,
	table          = data_table.new,
	shared_input   = data_pipe.shared_input,
	shared_output  = data_pipe.shared_output,
	struct         = data_struct.new,
	tree           = data_tree.new
}

m3.fhk = {
	define         = function(...) return fhk.graph.G:define(...) end,
	readfile       = function(...) return fhk.graph:readfile(...) end,
}

m3.save            = mem.save
m3.load            = mem.load

m3.meta            = data.meta
m3.typeof          = data.typeof

return m3
