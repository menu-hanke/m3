-- m3 embedding api.

local modapi       = require "m3"
local m3           = setmetatable({}, { __index = modapi })

local control      = require "m3_control"
local data         = require "m3_data"
local mem          = require "m3_mem"
local tree         = require "m3_tree"

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

m3.data = {
	define         = data.define,
	defined        = data.defined,
	include        = data.include,
	transaction    = data.transaction,
	substitute     = data.substitute,
	delete         = data.delete,
	insert         = data.insert,
	globals        = data.globals,
	pipe           = data.pipe,
	shared         = data.shared,
	connect        = data.connect,
	dynamic        = data.dynamic,
	tree           = tree.new
}

m3.save            = mem.save
m3.load            = mem.load

return m3
