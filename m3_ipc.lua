assert(require("m3_environment").mode == "mp")

local shm = require "m3_shm"
local ffi = require "ffi"
local buffer = require "string.buffer"
require "table.new"
require "table.clear"

local C, cast, copy = ffi.C, ffi.cast, ffi.copy
local heap = shm.heap

---- Message handling ----------------------------------------------------------

local msg_size = ffi.sizeof("m3_Message")
--local msgtab_ct = ffi.typeof("m3_Message *[?]")
local msgtab_ct = ffi.typeof("m3_Message *[20000]")
local msgptr_ct = ffi.typeof("m3_Message *")

-- all messages ever allocated by this process
-- (if we want to get fancier this could hold a separate table per size class)
local allmsg_cap = 20000
local allmsg_num = 0
--local allmsg = ffi.new(msgtab_ct, allmsg_cap)
local allmsg = ffi.new(msgtab_ct)

-- reused encoder & decoder buffers
local encoder = buffer.new(0)
local decoder = buffer.new()

local function message_alloc_new(cls)
	if allmsg_num == allmsg_cap then
		allmsg_cap = 2*allmsg_cap
		local ptr = ffi.new(msgtab_ct, allmsg_cap)
		ffi.copy(ptr, allmsg, allmsg_num*ffi.sizeof("m3_Message *"))
		allmsg = ptr
	end
	local msg = ffi.cast(msgptr_ct, heap:bump_cls(cls))
	msg.cls = cls
	allmsg[allmsg_num] = msg
	allmsg_num = allmsg_num+1
	return msg
end

local function message_alloc_sweep(cls)
	C.m3__mp_msg_sweep(heap, allmsg, allmsg_num)
	local msg = heap:get_free_cls(cls)
	if msg == nil then
		return message_alloc_new(cls)
	else
		msg = cast(msgptr_ct, msg)
	end
	return msg
end

local function message_alloc(len)
	local msg, cls = heap:get_free(msg_size+len)
	if msg == nil then
		msg = message_alloc_sweep(cls)
	else
		msg = cast(msgptr_ct, msg)
	end
	msg.state = 1
	msg.len = len
	msg.cls = cls
	return msg
end

local function message_free(msg)
	msg.state = 2
end

local function encode(chan, msg)
	local data, len = encoder:reset():encode(msg):ref()
	local node = message_alloc(len)
	copy(node.data, data, len)
	node.chan = chan
	return node
end

local function decode(ptr)
	local node = cast(msgptr_ct, ptr)
	local chan = node.chan
	local msg = decoder:set(node.data, node.len):decode()
	message_free(node)
	return chan, msg
end

---- Futures -------------------------------------------------------------------

local function fut_completed(fut)
	return fut.state == -1ULL
end

local function fut_complete(fut)
	fut.state = -1ULL
end

local function fut_wait_sync(fut)
	if not fut_completed(fut) then
		repeat
			C.m3__mp_proc_park(shm.proc())
		until fut_completed(fut)
	end
end

ffi.metatype(
	"m3_Future",
	{
		__index = {
			completed = fut_completed,
			complete  = fut_complete,
			wait_sync = fut_wait_sync
		}
	}
)

---- Queues & channels ---------------------------------------------------------

local function prefork()
	error("channel pipe cannot be used before fork", 2)
end

local function channel_template(chan)
	return load(string.format([[
		local write = ...
		return function(x)
			return write(x, %d)
		end
	]], chan))(prefork)
end

local function dispatch_channel(dispatch, recv)
	local chanid = #dispatch+1
	local chan = {
		send = channel_template(chanid),
		recv = recv
	}
	dispatch[chanid] = chan
	return chan
end

local function dispatch_torecv(dispatch)
	local disp = table.new(#dispatch, 0)
	for id, chan in ipairs(dispatch) do
		disp[id] = chan.recv
	end
	table.clear(dispatch)
	return disp
end

local function dispatch_tosend(dispatch, send)
	for _, chan in ipairs(dispatch) do
		debug.setupvalue(chan.send, 1, send)
	end
	table.clear(dispatch)
end

local dispatch_mt = {
	__index = {
		channel = dispatch_channel,
		torecv  = dispatch_torecv,
		tosend  = dispatch_tosend
	}
}

local function dispatch()
	return setmetatable({}, dispatch_mt)
end

--------------------------------------------------------------------------------

return {
	encode   = encode,
	decode   = decode,
	dispatch = dispatch
}
