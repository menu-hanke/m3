local shutdown = require "m3_shutdown"
local dbg = require("m3_debug")
local event, enabled = dbg.event, dbg.enabled
require "m3_cdef"
require "table.clear"
local buffer = require "string.buffer"
local ffi = require "ffi"
local C = ffi.C
local select, type = select, type

local SQLITE_TRANSIENT = ffi.cast("void *", -1)
local SQLITE_ROW  = 100
local SQLITE_DONE = 101
local SQLITE_INTEGER = 1
local SQLITE_FLOAT = 2
local SQLITE_TEXT = 3

local MAX_BACKLOG = 1000

local global_connection  -- sqlite3 *
local global_schema -- reflect
local global_maindb = ":memory:"
local global_datadef = {}  -- function(sqlite3 *)
local global_statements = {} -- sql => lazy statement

-- i:     statement
-- i+1:   number of bind parameters
-- i+1+j: j'th bind parameter
local global_backlog = {}
local global_backlogstate = ffi.new [[
	struct {
		int32_t tail;
		int32_t size;
	}
]]

---- Query builder -------------------------------------------------------------

local function sql(fragment, ...)
	return {fragment=fragment, ...}
end

local function sqlvisit(ctx, sql)
	if sql.fragment then
		if not ctx[sql.fragment] then
			ctx[sql.fragment] = {}
		end
		for _,s in ipairs(sql) do
			table.insert(ctx[sql.fragment], s)
		end
	else
		for _,s in ipairs(sql) do
			sqlvisit(ctx, s)
		end
	end
end

local function putlist(buf, values, sep)
	for i,v in ipairs(values) do
		if i>1 then buf:put(sep or ",") end
		buf:put(v)
	end
end

local function sqltostr(sql)
	if type(sql) == "string" then return sql end
	local ctx = {}
	sqlvisit(ctx, sql)
	local buf = buffer.new()
	if ctx.SELECT then
		buf:put("SELECT ")
		for i,v in ipairs(ctx.SELECT) do
			if i>1 then buf:put(",") end
			if type(v) == "string" then
				buf:put(v)
			elseif v.fragment == "AS" then
				buf:put(v[1], " AS ", v[2])
			end
		end
		if ctx.FROM then
			buf:put(" FROM ")
			putlist(buf, ctx.FROM)
		end
		if ctx.WHERE then
			buf:put(" WHERE ")
			putlist(buf, ctx.WHERE, " AND ")
		end
	elseif ctx.INSERT then
		assert(#ctx.INSERT == 1, "trying to INSERT into multiple tables")
		buf:put("INSERT INTO ", ctx.INSERT[1])
		if ctx.VALUES then
			buf:put("(")
			for i,v in ipairs(ctx.VALUES) do
				if i>1 then buf:put(",") end
				buf:put(v.col)
			end
			buf:put(") VALUES (")
			for i,v in ipairs(ctx.VALUES) do
				if i>1 then buf:put(",") end
				buf:put(v.value)
			end
			buf:put(")")
		end
	else
		-- implement other statements when/if needed
		assert(false)
	end
	return tostring(buf)
end

local function escape(str)
	-- TODO
	return str
end

---- Low-level API -------------------------------------------------------------

local function throw(x)
	if ffi.istype("sqlite3_stmt *", x) then
		x = C.sqlite3_db_handle(x)
	end
	error(ffi.string(C.sqlite3_errmsg(x)))
end

local function check(x, r)
	if r ~= 0 then
		throw(x)
	end
end

local function stmt_bind(stmt, i, v)
	local ty = type(v)
	if ty == "nil" then
		check(stmt, C.sqlite3_bind_null(stmt, i))
	elseif ty == "number" then
		check(stmt, C.sqlite3_bind_double(stmt, i, v))
	elseif ty == "string" then
		check(stmt, C.sqlite3_bind_text(stmt, i, v, #v, SQLITE_TRANSIENT))
	elseif ty == "table" then
		for idx,value in pairs(v) do
			if type(idx) == "string" then
				idx = C.sqlite3_bind_parameter_index(stmt, idx)
				if idx == 0 then goto continue end
			end
			stmt_bind(stmt, idx, value)
			::continue::
		end
	else
		error(string.format("can't bind: %s", v))
	end
end

-- *insert cs grad meme*
local function stmt_bindargs(stmt, ...)
	local n = select("#", ...)
	if n == 0 then
	elseif n == 1 then
		local v1 = ...
		stmt_bind(stmt, 1, v1)
	elseif n == 2 then
		local v1, v2 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2)
	elseif n == 3 then
		local v1, v2, v3 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3)
	elseif n == 4 then
		local v1, v2, v3, v4 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
	elseif n == 5 then
		local v1, v2, v3, v4, v5 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5)
	elseif n == 6 then
		local v1, v2, v3, v4, v5, v6 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5) stmt_bind(stmt, 6, v6)
	elseif n == 7 then
		local v1, v2, v3, v4, v5, v6, v7 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5) stmt_bind(stmt, 6, v6) stmt_bind(stmt, 7, v7)
	elseif n == 8 then
		local v1, v2, v3, v4, v5, v6, v7, v8 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5) stmt_bind(stmt, 6, v6) stmt_bind(stmt, 7, v7) stmt_bind(stmt, 8, v8)
	else
		stmt_bind(stmt, nil, {...})
	end
	event("sql", stmt, ...)
end

local function stmt_reset(stmt)
	check(stmt, C.sqlite3_reset(stmt))
end

local function stmt_finalize(stmt)
	check(stmt, C.sqlite3_finalize(stmt))
end

local function stmt_step(stmt)
	local s = C.sqlite3_step(stmt)
	if s == SQLITE_ROW then
		return stmt
	elseif s == SQLITE_DONE then
		stmt_reset(stmt)
	else
		throw(stmt)
	end
end

local function stmt_rows(stmt, ...)
	stmt_bindargs(stmt, ...)
	return stmt_step, stmt
end

local function stmt_exec(stmt, ...)
	stmt_bindargs(stmt, ...)
	if stmt_step(stmt) then
		stmt_reset(stmt)
	end -- else: stmt_step() already called reset
end

local function stmt_text(stmt, i)
	local p = C.sqlite3_column_text(stmt, i)
	if p ~= nil then return ffi.string(p) end
end

local function stmt_col(stmt, i)
	local ty = C.sqlite3_column_type(stmt, i)
	if ty == SQLITE_INTEGER then
		return (C.sqlite3_column_int(stmt, i))
	elseif ty == SQLITE_FLOAT then
		return (C.sqlite3_column_double(stmt, i))
	elseif ty == SQLITE_TEXT then
		return stmt_text(stmt, i)
	end
end

local function stmt_name(stmt, i)
	local name = C.sqlite3_column_name(stmt, i)
	if name ~= nil then
		return ffi.string(name)
	end
end

local function stmt_sql(stmt)
	return ffi.string(C.sqlite3_sql(stmt))
end

ffi.metatype("sqlite3_stmt", {
	__index = {
		bind       = stmt_bind,
		bindargs   = stmt_bindargs,
		paramcount = C.sqlite3_bind_parameter_count,
		colcount   = C.sqlite3_column_count,
		reset      = stmt_reset,
		finalize   = stmt_finalize,
		step       = stmt_step,
		rows       = stmt_rows,
		exec       = stmt_exec,
		double     = C.sqlite3_column_double,
		int        = C.sqlite3_column_int,
		text       = stmt_text,
		col        = stmt_col,
		name       = stmt_name,
		sql        = stmt_sql
	}
})

local function sqlite3_open(url)
	local buf = ffi.new("sqlite3 *[1]")
	local err = C.sqlite3_open(url, buf)
	if err ~= 0 then
		error(ffi.string(C.sqlite3_errstr(err)))
	end
	return buf[0]
end

local function sqlite3_close(conn)
	check(conn, C.sqlite3_close_v2(conn))
end

local function sqlite3_prepare(conn, sql)
	local buf = ffi.new("sqlite3_stmt *[1]")
	check(conn, C.sqlite3_prepare_v2(conn, sql, #sql, buf, nil))
	return buf[0]
end

local function sqlite3_rows(conn, sql, ...)
	return ffi.gc(sqlite3_prepare(conn, sql), stmt_finalize):rows(...)
end

local function sqlite3_exec(conn, sql, ...)
	local stmt = ffi.gc(sqlite3_prepare(conn, sql), stmt_finalize)
	stmt:exec(...)
	ffi.gc(stmt, nil)
	stmt_finalize(stmt)
end

local function sqlite3_execscript(conn, sql)
	check(conn, C.sqlite3_exec(conn, sql, nil, nil, nil))
end

ffi.metatype("sqlite3", {
	__index = {
		close      = sqlite3_close,
		prepare    = sqlite3_prepare,
		rows       = sqlite3_rows,
		exec       = sqlite3_exec,
		execscript = sqlite3_execscript
	}
})

---- Reflection ----------------------------------------------------------------

local function reflect__index(self, key)
	local field = getmetatable(self).fields[key]
	if field then
		local value = field(self, key)
		rawset(self, key, value)
		return value
	end
end

local function reftab_columns(tab)
	local columns = {}
	for row in tab.conn:rows(string.format("PRAGMA table_xinfo(%s)", tab.name)) do
		columns[row:text(1)] = {
			nullable = row:int(3) == 0,
			pk = row:int(5) == 1
		}
	end
	return columns
end

local function reftab_fks(tab)
	local fks = {}
	local cur
	for row in tab.conn:rows(string.format("PRAGMA foreign_key_list(%s)", tab.name)) do
		if row:int(1) == 0 then
			table.insert(fks, cur)
			cur = { table=row:text(2), columns={} }
		end
		cur.columns[row:text(3)] = row:text(4)
	end
	table.insert(fks, cur)
	return fks
end

local reftab_mt = {
	fields = {
		columns      = reftab_columns,
		foreign_keys = reftab_fks
	},
	__index = reflect__index
}

local function reflect_tables(refl)
	local tables = {}
	for row in refl.conn:rows("PRAGMA table_list") do
		-- prefer first occurrence if the same table name appears in multiple schemas.
		-- this matches sqlite behavior.
		local name = row:text(1)
		if not tables[name] then
			tables[name] = setmetatable({conn=refl.conn, name=name}, reftab_mt)
		end
	end
	return tables
end

local function reflect_databases(refl)
	local databases = {}
	for row in refl.conn:rows("PRAGMA database_list") do
		local db = {}
		local file = row:text(2)
		if file ~= "" then db.file = file end
		databases[row:text(1)] = db
	end
	return databases
end

local reflect_mt = {
	fields = {
		tables = reflect_tables,
		databases = reflect_databases
	},
	__index = reflect__index
}

local function reflect(conn)
	return setmetatable({conn=conn}, reflect_mt)
end

---- Database management -------------------------------------------------------

local function datadef(sql, ...)
	if select("#", ...) > 0 then
		local params = {...}
		table.insert(global_datadef, function(conn) conn:exec(sql, unpack(params)) end)
	else
		table.insert(global_datadef, function(conn) conn:execscript(sql) end)
	end
end

local function database(url, name)
	if name == "main" or not name then
		global_maindb = url
	else
		datadef("ATTACH DATABASE ? AS ?", url, name)
	end
end

local function connection()
	if not global_connection then
		global_connection = sqlite3_open(global_maindb)
		for _,dd in ipairs(global_datadef) do
			dd(global_connection)
		end
	end
	return global_connection
end

-- schema() -> refl
-- schema(tab) -> refl[tab]
local function schema(tab)
	if not global_schema then
		global_schema = reflect(connection())
	end
	if tab then
		return global_schema.tables[tab]
	else
		return global_schema
	end
end

---- Backlog -------------------------------------------------------------------

local function backlog_flush()
	local tail = global_backlogstate.tail
	local backlog = global_backlog
	global_backlogstate.tail = 0
	global_backlogstate.size = 0
	local idx = 0
	global_statements.BEGIN:exec()
	while idx < tail do
		local stmt = backlog[idx]
		local n = backlog[idx+1]
		for i=1, n do
			stmt_bind(stmt, i, backlog[idx+1+i])
		end
		if enabled("sql") then
			event("sql", stmt, unpack(backlog, idx+2, idx+1+n))
		end
		if stmt_step(stmt) then
			stmt_reset(stmt)
		end
		idx = idx+2+n
	end
	global_statements.COMMIT:exec()
	table.clear(backlog)
end

local function backlog_check()
	if global_backlogstate.size >= MAX_BACKLOG then
		backlog_flush()
	end
end

local backlog_func = setmetatable({}, {
	__index = function(self, n)
		local buf = buffer.new()
		buf:put("local backlog, backlogstate, backlog_check = ... return function(stmt")
		for i=1, n do buf:putf(", p%d", i) end
		buf:put(")\n")
		buf:putf("local idx = backlogstate.tail backlogstate.tail = idx+%d\n", n+2)
		buf:put("backlogstate.size = backlogstate.size+1\n")
		buf:putf("backlog[idx] = stmt.sqlite3_stmt backlog[idx+1] = %d\n", n)
		for i=1, n do buf:putf("backlog[idx+%d] = p%d\n", i+1, i) end
		buf:put("backlog_check()\nend\n")
		local func = load(buf)(global_backlog, global_backlogstate, backlog_check)
		self[n] = func
		return func
	end
})

---- Lazy statements -----------------------------------------------------------

local compiled_mt = {
	__index = {
		step = function(self) return stmt_step(self.sqlite3_stmt) end,
		exec = function(self, ...) return stmt_exec(self.sqlite3_stmt, ...) end,
		rows = function(self, ...) return stmt_rows(self.sqlite3_stmt, ...) end,
	}
}

local uncompiled_mt = {
	__index = function(self, field)
		self.sqlite3_stmt = connection():prepare(self.sql)
		self.buffer = backlog_func[self.sqlite3_stmt:paramcount()]
		setmetatable(self, compiled_mt)
		return self[field]
	end
}

local function iscompiled(stmt)
	return getmetatable(stmt) == compiled_mt
end

local function newstatement(sql)
	return setmetatable({sql=sql}, uncompiled_mt)
end

local function statement(sql)
	sql = sqltostr(sql)
	local stmt = global_statements[sql]
	if not stmt then
		stmt = newstatement(sql)
		global_statements[sql] = stmt
	end
	return stmt
end

-- backlog_flush() assumes these exist
statement "BEGIN"
statement "COMMIT"

local function ismemory()
	for _,db in pairs(schema().databases) do
		if db.file then
			return false
		end
	end
	return true
end

local function disconnect(force)
	if global_connection then
		if global_backlogstate.size > 0 then
			backlog_flush()
		end
		if force == false and ismemory() then return end
		for _,stmt in pairs(global_statements) do
			if iscompiled(stmt) then
				stmt_finalize(stmt.sqlite3_stmt)
				stmt.sqlite3_stmt = nil
				setmetatable(stmt, uncompiled_mt)
			end
		end
		global_connection:close()
		global_connection = nil
		global_schema = nil
	end
end

shutdown(disconnect)

--------------------------------------------------------------------------------

return {
	sql        = sql,
	escape     = escape,
	database   = database,
	datadef    = datadef,
	schema     = schema,
	disconnect = disconnect,
	statement  = statement,
}
