-- vim: ft=lua

data.ddl [[
CREATE TABLE A(id INTEGER PRIMARY KEY, x REAL);
INSERT INTO A(id, x) VALUES (1, 123);
CREATE TABLE B(a_id INTEGER REFERENCES A(id), x REAL);
INSERT INTO B(a_id, x) VALUES (1, 1234);
]]

data.define [[
table A
table B[A.N]
model B default'x = A.x
]]

local getx = data.transaction():read("B.x")

control.simulate = function()
	assert(getx()[0] == 1234)
end
