local dbg = require "m3_debug"
local buffer = require "string.buffer"
local ffi = require "ffi"
local sqlite = require "sqlite"
local type = type
local sqlite_escape, sqlite_open, sqlite_reflect = sqlite.escape, sqlite.open, sqlite.reflect
local enabled, event = dbg.enabled, dbg.event

-- TODO make these configurable
local MAX_BACKLOG = 10000
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
local global_backlogstate = ffi.new [[ struct { int32_t tail; } ]]

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

-- scalar statement or vector statement head:
--   [base]              sqlite3_stmt
--   [base+1]            ncol (number of arguments to the statement)
--   [base+1+i]          i'th bind arg  (i=1,...,narg)
-- vector statement tail:
--   [base]              nvcol (number of vector arguments)
--   [base+1]            nvrow (number of vector rows)
--   [base+1+i]          i'th vector argument index  (i=1,...,narg)
--   [base+1+nvcol*j+i]  i'th vector arg of j'th row

local function backlog_flush()
	local tail = global_backlogstate.tail
	local backlog = global_backlog
	global_backlogstate.tail = 0
	local idx = 0
	global_statements.BEGIN.sqlite3_stmt:exec()
	local stmt
	while idx < tail do
		local v = backlog[idx]
		if type(v) == "cdata" then
			-- it's a new statement
			stmt = v
			local narg = backlog[idx+1]
			for i=1, narg do
				stmt:bind(i, backlog[idx+1+i])
			end
			if enabled("sql") then
				event("sql", stmt, unpack(backlog, idx+2, idx+1+narg))
			end
			if stmt:step() then
				stmt:reset()
			end
			idx = idx+2+narg
		else
			-- it's the tail part of a vectorized insert
			local nvcol = v
			local nvrow = backlog[idx+1]
			for j=1, nvrow do
				for i=1, nvcol do
					stmt:bind(backlog[idx+1+i], backlog[idx+1+nvcol*j+i])
				end
				-- TODO: sql event (need scalar values from previous statement)
				if stmt:step() then
					stmt:reset()
				end
			end
			idx = idx+2+(nvrow+1)*nvcol
		end
	end
	global_statements.COMMIT.sqlite3_stmt:exec()
end

local function backlog_check()
	if global_backlogstate.tail >= MAX_BACKLOG then
		backlog_flush()
	end
end

local function backlog_expandvec(base)
	local backlog = global_backlog
	local narg = backlog[base+1]
	local vbase = base+2+narg
	local nvcol = 0
	local nrow
	for i=1, narg do
		local v = backlog[base+1+i]
		if type(v) == "table" or type(v) == "cdata" then
			nvcol = nvcol+1
			backlog[vbase+1+nvcol] = i
			if not nrow then
				nrow = #v
			else
				if #v ~= nrow then
					error(string.format("vector arguments lengths don't match (%d != %d)", nrow, #v))
				end
			end
		end
	end
	local nvrow = nrow-1
	backlog[vbase] = nvcol
	backlog[vbase+1] = nvrow
	for i=1, nvcol do
		local argi = backlog[vbase+1+i]
		local argv = backlog[base+1+argi]
		local arg0 = type(argv) == "table" and 1 or 0
		backlog[base+1+argi] = argv[arg0]
		for j=1, nvrow do
			backlog[vbase+1+nvcol*j+i] = argv[arg0+j]
		end
	end
	global_backlogstate.tail = vbase+2+(nvrow+1)*nvcol
end

local backlog_func = setmetatable({}, {
	__index = function(self, n)
		local buf = buffer.new()
		buf:put("local type, backlog, backlogstate, backlog_check, backlog_expandvec = type, ...\n")
		buf:put("return function(stmt")
		for i=1, n do buf:putf(", p%d", i) end
		buf:put(")\n")
		buf:putf("local idx = backlogstate.tail backlogstate.tail = idx+%d\n", n+2)
		buf:putf("backlog[idx] = stmt.sqlite3_stmt backlog[idx+1] = %d\n", n)
		for i=1, n do buf:putf("backlog[idx+%d] = p%d\n", i+1, i) end
		if n>0 then
			buf:put("if ")
			for i=1, n do
				if i>1 then buf:put(" or ") end
				buf:putf("type(p%d) == 'table' or type(p%d) == 'cdata'", i, i)
			end
			buf:put("then backlog_expandvec(idx) end\n")
		end
		buf:put("backlog_check()\nend\n")
		local func = load(buf)(global_backlog, global_backlogstate, backlog_check, backlog_expandvec)
		self[n] = func
		return func
	end
})

---- Lazy statements -----------------------------------------------------------

local function stmt_buffer(stmt, ...)
	return stmt:buffer(...)
end

local compiled_mt = {
	__call = stmt_buffer
}

local function stmt_compile(stmt)
	stmt.sqlite3_stmt = connection():prepare(stmt.sql)
	stmt.buffer = backlog_func[stmt.sqlite3_stmt:paramcount()]
	return setmetatable(stmt, compiled_mt)
end

local uncompiled_mt = {
	__index = function(self, field) return stmt_compile(self)[field] end,
	__call = function(self, ...) return stmt_compile(self)(...) end
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
		if global_backlogstate.tail > 0 then
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
