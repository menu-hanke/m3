-- vim: ft=lua

data.ddl [[
CREATE TABLE A(id INTEGER PRIMARY KEY);
CREATE TABLE B(a_id INTEGER REFERENCES A(id));
INSERT INTO A(id) VALUES (1);
]]

data.define [[
table A
table B[A.N]
]]

data.transaction():read("B.x")

test.error "is not in the schema and has no dummy value"
