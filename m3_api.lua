-- m3 embedding api.

local modapi       = require "m3"
local m3           = setmetatable({}, { __index = modapi })

local control      = require "m3_control"
local data         = require "m3_data"
local fhk          = require "m3_fhk"
local mem          = require "m3_mem"
local pipe         = require "m3_pipe"
local tree         = require "m3_tree"
local state        = require "m3_state"

m3.pipe = {
	new            = pipe.new,
	connect        = pipe.connect,
	shared_output  = pipe.shared_output,
	shared_input   = pipe.shared_input,
	defer          = pipe.defer,
	map            = pipe.map,
	filter         = pipe.filter,
}

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

m3.fhk = {
	define         = fhk.define,
	readfile       = fhk.readfile,
}

m3.save            = mem.save
m3.load            = mem.load

m3.data            = data.data
m3.meta            = data.meta
m3.typeof          = data.typeof

m3.forest          = tree.forest
m3.parallel        = state.parallel

return m3
