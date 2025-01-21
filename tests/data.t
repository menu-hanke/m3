-- vim: ft=lua

if test "data:unmapped:memslot" then
	test(false, "read but never written")
	define "table A"
	transaction():read("A.x")
end

if test "data:unmapped:column" then
	test(false, "is not in the schema and has no dummy value")
	datadef [[
		CREATE TABLE A(id INTEGER PRIMARY KEY);
		CREATE TABLE B(a_id INTEGER REFERENCES A(id));
		INSERT INTO A(id) VALUES (1);
	]]
	define [[
		table A
		table B[A.N]
	]]
	transaction():read("B.x")
end

if test "data:default:memslot" then
	define [[
		table A
		model A default'x = 123
	]]
	local getx = transaction():read("A.x")
	simulate {
		function()
			assert(getx() == 123)
		end
	}
end

if test "data:default:column:*" then
	datadef [[
		CREATE TABLE A(id INTEGER PRIMARY KEY, x REAL);
		INSERT INTO A(id, x) VALUES (1, 123);
	]]
	define [[
		table A
		table B[A.N]
		model B default'x = A.x
	]]
	local getx = transaction():read("B.x")
	local want
	if test "data:default:column:present" then
		datadef [[
			CREATE TABLE B(a_id INTEGER REFERENCES A(id));
			INSERT INTO B(a_id) VALUES (1);
		]]
		want = 123
	elseif test "data:default:column:missing" then
		datadef [[
			CREATE TABLE B(a_id INTEGER REFERENCES A(id), x REAL);
			INSERT INTO B(a_id, x) VALUES (1, 1234);
		]]
		want = 1234
	else
		test(true)
	end
	simulate {
		function()
			assert(getx():get(0) == want)
		end
	}
end

if test "data:sql:input-tab:*" then
	datadef [[
		CREATE TABLE A(x REAL);
		INSERT INTO A(x) VALUES (123);
	]]
	define "table A"
	if test "data:sql:input-tab:explicit" then
		input "A"
	else
		test "data:sql:input-tab:implicit"
	end
	local getx = transaction():read("A.x")
	simulate {
		function()
			assert(getx() == 123)
		end
	}
end

if test "data:sql:input-not-unique" then
	test(false, "cannot determine input table")
	datadef [[
		CREATE TABLE A(x REAL);
		CREATE TABLE B(x REAL);
	]]
	define [[
		table A
		table B
	]]
end
