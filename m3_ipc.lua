assert(require("m3_state").mode == "mp")

local shm = require "m3_shm"
local ffi = require "ffi"
local buffer = require "string.buffer"

local C, cast, copy = ffi.C, ffi.cast, ffi.copy
local heap = shm.heap

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

return {
	encode = encode,
	decode = decode
}
