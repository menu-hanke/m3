-- vim: ft=lua

if test "data:unmapped:memslot" then
	test(false, "read but never written")
	data.define "table A"
	data.transaction():read("A.x")
end

if test "data:unmapped:column" then
	test(false, "is not in the schema and has no dummy value")
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
end

if test "data:default:memslot" then
	data.define [[
		table A
		model A default'x = 123
	]]
	local getx = data.transaction():read("A.x")
	control.simulate = {
		function()
			assert(getx() == 123)
		end
	}
end

if test "data:default:column:*" then
	data.ddl [[
		CREATE TABLE A(id INTEGER PRIMARY KEY, x REAL);
		INSERT INTO A(id, x) VALUES (1, 123);
	]]
	data.define [[
		table A
		table B[A.N]
		model B default'x = A.x
	]]
	local getx = data.transaction():read("B.x")
	local want
	if test "data:default:column:present" then
		data.ddl [[
			CREATE TABLE B(a_id INTEGER REFERENCES A(id));
			INSERT INTO B(a_id) VALUES (1);
		]]
		want = 123
	elseif test "data:default:column:missing" then
		data.ddl [[
			CREATE TABLE B(a_id INTEGER REFERENCES A(id), x REAL);
			INSERT INTO B(a_id, x) VALUES (1, 1234);
		]]
		want = 1234
	else
		test(true)
	end
	control.simulate = {
		function()
			assert(getx():get(0) == want)
		end
	}
end

if test "data:sql:input-tab:*" then
	data.ddl [[
		CREATE TABLE A(x REAL);
		INSERT INTO A(x) VALUES (123);
	]]
	data.define "table A"
	if test "data:sql:input-tab:explicit" then
		data.task = "SELECT rowid AS A_rowid FROM A"
	else
		test "data:sql:input-tab:implicit"
	end
	local getx = data.transaction():read("A.x")
	control.simulate = {
		function()
			assert(getx() == 123)
		end
	}
end

if test "data:sql:input-not-unique" then
	test(false, "cannot determine task")
	data.ddl [[
		CREATE TABLE A(x REAL);
		CREATE TABLE B(x REAL);
	]]
	data.define [[
		table A
		table B
	]]
end

if test "data:sql:datamap" then
	data.ddl [[
		CREATE TABLE A(id INTEGER PRIMARY KEY, x REAL);
		INSERT INTO A(x) VALUES (123);
	]]
	data.define "table B"
	table.insert(data.mappers, {
		B = {
			table = "A",
			map = { y = "x" }
		}
	})
	local gety = data.transaction():read("B.y")
	control.simulate = {
		function()
			assert(gety() == 123)
		end
	}
end

if test "data:sql:null-dummy:*" then
	data.ddl "CREATE TABLE A(id INTEGER PRIMARY KEY, x REAL);"
	data.define [[
		table A
		model A default'x = 123
	]]
	local getx = data.transaction():read("A.x")
	local want
	if test "data:sql:null-dummy:present" then
		data.ddl "INSERT INTO A(x) VALUES (456)"
		want = 456
	elseif test "data:sql:null-dummy:absent" then
		data.ddl "INSERT INTO A(x) VALUES (NULL)"
		want = 123
	else
		test(true)
	end
	control.simulate = {
		function()
			assert(getx() == want)
		end
	}
end
