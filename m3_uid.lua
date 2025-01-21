-- in (64 bits):  0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x
-- out (32 bits):                                 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
local function deinterleave(x)
	x = bit.band(bit.bor(x, bit.rshift(x, 1)), 0x3333333333333333ull)
	x = bit.band(bit.bor(x, bit.rshift(x, 2)), 0x0f0f0f0f0f0f0f0full)
	x = bit.band(bit.bor(x, bit.rshift(x, 4)), 0x00ff00ff00ff00ffull)
	x = bit.band(bit.bor(x, bit.rshift(x, 8)), 0x0000ffff0000ffffull)
	x = bit.band(bit.bor(x, bit.rshift(x,16)), 0x00000000ffffffffull)
	return tonumber(x)
end

-- in (53 bits):  cccccccccccababababababababababababababababababababab
-- out (53 bits): aaaaaaaaaaaaaaaaaaaaacccccccccccbbbbbbbbbbbbbbbbbbbbb
local function base(seed)
--                             cccccccccccababababababababababababababababababababab
	local a = bit.band(seed, 0b00000000000101010101010101010101010101010101010101010ull)
	local b = bit.band(seed, 0b00000000000010101010101010101010101010101010101010101ull)
	a = deinterleave(bit.lshift(a, 1))
	b = deinterleave(b)
	local c = bit.lshift(seed, 40ull)
	return tonumber(bit.bor(bit.lshift(a, 32), bit.lshift(c, 21), b))
end

-- this could use more precision, but that requires ffi
local global_base = base(bit.lshift(os.time(), 20ull))
local uid = load("local base = ... return function(id) return base + id end")(global_base)

if require("m3_environment").parallel then
	require("m3_mp").proc_init(function(id) debug.setupvalue(uid, 1, global_base + id*2^32) end)
end

return {
	uid = uid
}
