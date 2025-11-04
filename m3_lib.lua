local control      = require "m3_control"
local data         = require "m3_data"
local db           = require "m3_db"
local dbg          = require "m3_debug"
local mem          = require "m3_mem"

local function worker()
	return M3_WORKER_ID or 0
end

_G.control = {
	all            = control.all,
	any            = control.any,
	call           = control.call,
	callcc         = control.callcc,
	check          = control.check,
	dynamic        = control.dynamic,
	exec           = control.exec,
	first          = control.first,
	loop           = control.loop,
	nothing        = control.nothing,
	optional       = control.optional,
	skip           = control.skip,
	single         = control.single,
	try            = control.try,
	delete         = mem.delete,
	load           = mem.load,
	save           = mem.save,
	worker         = worker
}

_G.data = {
	arg            = data.arg,
	cdata          = data.memslot,
	define         = data.define,
	defined        = data.defined,
	func           = data.func,
	include        = data.include,
	mappers        = data.mappers,
	ret            = data.ret,
	splat          = data.splat,
	transaction    = data.transaction,
	attach         = db.attach,
	ddl            = db.ddl,
}

_G.pprint          = dbg.pprint
_G.pretty          = dbg.pretty
