local effects = {}

local version = 0

local function change()
	version = version+1
end

local function effect_get(fx)
	if fx.version < version then
		fx.value = fx.f()
		-- the max is here for effect_once math.huge
		fx.version = math.max(version, fx.version)
	end
	return fx.value
end

local function ident(x)
	return x
end

local function effect_once(fx)
	local f = fx.f
	fx.f = function()
		local v = f()
		fx.version = math.huge
		return v
	end
	fx.once = ident
	return fx
end

local effect_mt = {
	__call = effect_get,
	__index = {
		once = effect_once
	}
}

local function effect(f)
	local fx = effects[f]
	if not fx then
		fx = setmetatable({f=f, version=0}, effect_mt)
		effects[f] = fx
		change()
	end
	return fx
end

local function iseffect(x)
	return getmetatable(x) == effect_mt
end

local function set(tab, k, v)
	if tab[k] ~= v then
		change()
		tab[k] = v
	end
end

local function proxy(tab)
	local proxy = newproxy(true)
	local meta = getmetatable(proxy)
	meta.__index = tab
	meta.__newindex = function(_, k, v) set(tab, k, v) end
	return proxy
end

local function startup()
	for _=1, 1000 do
		local v = version
		for _,fx in pairs(effects) do fx() end
		if v == version then
			effects = nil
			return
		end
	end
	error("no effect fixpoint")
end

return {
	change   = change,
	effect   = effect,
	iseffect = iseffect,
	set      = set,
	proxy    = proxy,
	startup  = startup
}
