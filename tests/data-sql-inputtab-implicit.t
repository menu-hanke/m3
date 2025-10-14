-- vim: ft=lua

data.ddl [[
CREATE TABLE A(x REAL);
INSERT INTO A(x) VALUES (123);
]]

data.define [[
table A
]]

local getx = data.transaction():read("A.x")

control.simulate = function()
	assert(getx() == 123)
end
