-- vim: ft=lua

data.ddl [[
CREATE TABLE A(id INTEGER PRIMARY KEY, x REAL);
INSERT INTO A(x) VALUES (456);
]]

data.define [[
table A
model A default'x = 123
]]

local getx = data.transaction():read("A.x")

control.simulate = function()
	assert(getx() == 456)
end
