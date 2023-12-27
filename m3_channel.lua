assert(require("m3_state").mode == "mp")
require "table.clear"
require "table.new"

local in_chan, out_chan = 0, 0
local inputs, outputs = {}, {}

local function prefork()
	error("channel pipe cannot be used before fork", 2)
end

local function template(chan)
	return load(string.format([[
		local write = ...
		return function(x)
			return write(x, %d)
		end
	]], chan))(prefork)
end

local function input()
	local func = template(in_chan)
	inputs[func] = {chan=in_chan, write=prefork}
	in_chan = in_chan+1
	return func
end

local function output()
	local func = template(out_chan)
	outputs[func] = {chan=out_chan, write=prefork}
	out_chan = out_chan+1
	return func
end

local function settarget(func, target)
	if inputs[func] then
		inputs[func].write = target
	else
		outputs[func].write = target
	end
end

local function setwrite(tab, write)
	for func in pairs(tab) do
		debug.setupvalue(func, 1, write)
	end
	-- we don't need these anymore
	table.clear(tab)
end

local function setinput(input)
	setwrite(inputs, input)
end

local function setoutput(output)
	setwrite(outputs, output)
end

local function getdispatch(tab, chans)
	local disp = table.new(chans, 0)
	for func, info in pairs(tab) do
		debug.setupvalue(func, 1, info.write)
		disp[info.chan] = info.write
	end
	table.clear(tab)
	return disp
end

local function dispinput()
	return getdispatch(inputs, in_chan)
end

local function dispoutput()
	return getdispatch(outputs, out_chan)
end

return {
	input      = input,
	output     = output,
	inputs     = inputs,
	outputs    = outputs,
	settarget  = settarget,
	setinput   = setinput,
	setoutput  = setoutput,
	dispinput  = dispinput,
	dispoutput = dispoutput
}
