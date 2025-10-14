-- vim: ft=lua

data.ddl [[
CREATE TABLE A(id INTEGER PRIMARY KEY, x REAL);
INSERT INTO A(x) VALUES (123);
]]

data.define [[
table B
]]

table.insert(data.mappers, {
	B = {
		table = "A",
		map = { y = "x" }
	}
})

local gety = data.transaction():read("B.y")

control.simulate = function()
	assert(gety() == 123)
end
