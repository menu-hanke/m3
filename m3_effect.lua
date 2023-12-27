local effects = {}

local version = 0

local function effect(f)
	effects[f] = true
end

local function change()
	version = version+1
end

local function startup()
	local v
	repeat
		v = version
		for f in pairs(effects) do f() end
	until version == v
	effects = nil
end

return {
	effect  = effect,
	change  = change,
	startup = startup
}
