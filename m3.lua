local control      = require "m3_control"
local data         = require "m3_data"
local dbg          = require "m3_debug"
local env          = require "m3_environment"
local mem          = require "m3_mem"
local mp           = env.parallel and require "m3_mp"
local sqlite       = require "m3_sqlite"
local uid          = require "m3_uid"

local m3 = {
	all            = control.all,
	any            = control.any,
	call           = control.call,
	dynamic        = control.dynamic,
	exec           = control.exec,
	first          = control.first,
	loop           = control.loop,
	nothing        = control.nothing,
	optional       = control.optional,
	skip           = control.skip,
	try            = control.try,
	G              = data.G,
	arg            = data.arg,
	cdata          = data.memslot,
	commit         = data.commit,
	connect        = data.connect,
	define         = data.define,
	defined        = data.defined,
	func           = data.func,
	include        = data.include,
	pipe           = data.pipe,
	ret            = data.ret,
	shared_input   = data.shared_input,
	shared_output  = data.shared_output,
	splat          = data.splat,
	transaction    = data.transaction,
	pprint         = dbg.pprint,
	trace          = dbg.trace,
	frame          = mem.frame,
	load           = mem.load,
	save           = mem.save,
	wait           = function() if mp then mp.wait() end end,
	database       = sqlite.database,
	datadef        = sqlite.datadef,
	escapesql      = sqlite.escape,
	schema         = sqlite.schema,
	sql            = sqlite.sql,
	statement      = sqlite.statement,
	uid            = uid.uid
}

-- do this here so that env.init can require "m3"
package.loaded.m3 = m3

local postinit
if type(env.init) == "string" then
	postinit = require(env.init)
else
	postinit = env.init(m3)
end
data.init()
if postinit then
	postinit()
end
if mp then
	mp.init()
end

return m3
