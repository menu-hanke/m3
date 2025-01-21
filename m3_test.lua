local completed = {}
local todo = {}
local current, pattern, isglob
local ok, errpattern
local num = 0

local function glob_gsub(char)
	return char == "*" and ".*" or ("%"..char)
end

local function namepattern(name)
	return "^"..name:gsub("[%^%$%(%)%%%.%[%]%*%+%-%+]", glob_gsub).."$"
end

local function set(name)
	current = name
	pattern = namepattern(name)
	isglob = name:match("%*") ~= nil
end

local function settest(name)
	if name == "" then
		name = "*"
	end
	todo[name] = true
end

local function test(name)
	if not ok then
		-- shouldn't normally go here anyway since it asserted
		return false
	end
	if name == current then
		return true
	end
	if completed[name] then
		return false
	end
	if isglob and name:match(pattern) then
		set(name)
		return true
	end
	if name:match("%*") and current:match(namepattern(name)) then
		return true
	end
	return false
end

local function more()
	if current then
		todo[current] = nil
		completed[current] = true
	end
	if ok and not isglob then
		num = num+1
		io.stdout:write("ok ", num, " - ", current, "\n")
	end
	-- this also flushes any fails.
	-- the important part is to flush it between any tests since otherwise
	-- exiting subprocesses may flush their stdout and we get duplicate outputs.
	io.stdout:flush()
	local name = next(todo)
	if not name then
		print("1.."..num)
		return false
	end
	ok = true
	errpattern = nil
	set(name)
	return true
end

local function seterr(pat)
	errpattern = pat
end

local function fail(tb)
	if ok then
		if errpattern and tb:match(errpattern) then return end
		ok = false
		num = num+1
		io.stdout:write("not ok ", num, " - ", current, "\n")
	end
	for line in tb:gmatch("[^\n]+") do
		io.stdout:write("# ", line, "\n")
	end
end

return {
	settest = settest,
	test    = test,
	more    = more,
	seterr  = seterr,
	fail    = fail
}
