local dbg = require "m3_debug"
local buffer = require "string.buffer"
local ffi = require "ffi"
local sqlite = require "sqlite"
local sqlite_escape, sqlite_open, sqlite_reflect = sqlite.escape, sqlite.open, sqlite.reflect
local enabled, event = dbg.enabled, dbg.event

-- TODO make these configurable
local MAX_BACKLOG = 1000
local BUSY_TIMEOUT = 5000

local global_connection  -- sqlite3 *
local global_schema -- reflect
local global_maindb = ":memory:"
local global_datadef = {} -- list of DDL
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

---- Database management -------------------------------------------------------

local function ddl(sql)
	if sql then
		table.insert(global_datadef, sql)
	end
end

local function attach(url, name)
	if name == "main" or not name then
		global_maindb = url
	else
		ddl(string.format("ATTACH DATABASE '%s' AS %s", sqlite_escape(url), sqlite_escape(name)))
	end
end

local function connection_info()
	return global_maindb, table.concat(global_datadef, ";\n")
end

local function connection()
	if not global_connection then
		local url, dd = connection_info()
		global_connection = sqlite_open(url)
		global_connection:execscript(dd)
		if BUSY_TIMEOUT then
			global_connection:execscript(string.format("PRAGMA busy_timeout=%d", BUSY_TIMEOUT))
		end
	end
	return global_connection
end

-- schema() -> refl
-- schema(tab) -> refl[tab]
local function schema(tab)
	if not global_schema then
		global_schema = sqlite_reflect(connection())
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
			stmt:bind(i, backlog[idx+1+i])
		end
		if enabled("sql") then
			event("sql", stmt, unpack(backlog, idx+2, idx+1+n))
		end
		if stmt:step() then
			stmt:reset()
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
		step = function(self) return self.sqlite3_stmt:step() end,
		exec = function(self, ...) return self.sqlite3_stmt:exec(...) end,
		rows = function(self, ...) return self.sqlite3_stmt:rows(...) end,
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
	sql = sqlite.stringify(sql)
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
				stmt.sqlite3_stmt:finalize()
				stmt.sqlite3_stmt = nil
				setmetatable(stmt, uncompiled_mt)
			end
		end
		global_connection:close()
		global_connection = nil
		global_schema = nil
	end
end

--------------------------------------------------------------------------------

return {
	connection      = connection,
	connection_info = connection_info,
	attach          = attach,
	ddl             = ddl,
	schema          = schema,
	disconnect      = disconnect,
	statement       = statement,
}
