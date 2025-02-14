local m3 = require "m3"

local global_insn = {}
local global_data = {}
local global_task

local globals = {
	"all", "any", "call", "callcc", "dynamic", "exec", "first", "loop", "nothing", "optional", "skip", "try",
	"arg", "cdata", "connect", "define", "defined", "func", "include", "pipe", "ret", "transaction",
	"save", "delete",
	"pprint", "pretty",
	"database", "datadef",
	"uid"
}
for _,func in ipairs(globals) do _G[func] = m3[func] end
_G.sload = m3.load -- rename so we don't overwrite the builtin `load`

if not test then
	function test() return false end
end -- else: m3 is in test mode, test is a C funtion

function data(def)
	if type(def) == "string" then
		global_task = def
	elseif type(def) == "table" then
		table.insert(global_data, function(tab) return def[tab] end)
	else
		table.insert(global_data, def)
	end
end

local function tabdata(tab, f)
	for i=#global_data, 1, -1 do
		local config = global_data[i](tab)
		if config then
			if type(config) == "string" then config = {table=config} end
			local value = f(config)
			if value then return value end
		end
	end
end

local function rename(tab, col)
	local name
	if col then
		name = tabdata(tab, function(config)
			if type(config.map) == "table" then
				return config.map[col]
			elseif type(config.map) == "function" then
				return config.map(col)
			end
		end)
	else
		name = tabdata(tab, function(config) return config.table end)
	end
	return name or col or tab
end

function simulate(insn)
	table.insert(global_insn, m3.all(insn))
end

local function unpackiter(f)
	local v = f()
	if not v then return end
	return v, unpackiter(f)
end

local function cmd(o, v)
	if o == "j" then
		local cmd, opt = v:match("^([^=]+)=(.*)$")
		if cmd then
			opt = opt:gmatch("[^,]+")
		else
			cmd, opt = v, function() end
		end
		if type(jit[cmd]) == "function" then
			jit[cmd](unpackiter(opt))
		else
			require("jit."..cmd).start(unpackiter(opt))
		end
	elseif o == "O" then
		jit.opt.start(unpackiter(v:gmatch("[^,]+")))
	elseif o == "l" then
		require(v)
	elseif o == "d" then
		error("TODO")
	elseif o == "v" then
		m3.trace(v == "" and true or v)
	end
end

local function autotask()
	local tab
	for o in m3.G:objects() do
		if o.op == "TAB" and #o.shape.fields == 0 then
			local tname = rename(tostring(o.name))
			if m3.schema(tname) then
				if tab then
					error(string.format(
						"cannot determine task (`%s' or `%s'). Use `data(task)' to set it explicitly.",
						tab, o.name
					))
				end
				tab = tname
			end
		end
	end
	if tab then
		local sql = {}
		for name,col in pairs(m3.schema(tab).columns) do
			if col.pk then
				table.insert(sql, m3.sql("SELECT", m3.sql("AS", m3.escapesql(name),
					string.format("%s_%s", tab, name))))
			end
		end
		if #sql > 0 then
			table.insert(sql, m3.sql("FROM", m3.escapesql(tab)))
			return sql
		else
			return string.format("SELECT rowid AS %s_rowid FROM %s", tab, m3.escapesql(tab))
		end
	else
		return "SELECT 0"
	end
end

local function haveall(names, cols)
	for _,col in pairs(cols) do
		if not names[col] then return false end
	end
	return true
end

local function autowhere(tab, names)
	local schema = m3.schema(tab)
	if not schema then return end
	-- try primary key
	local cols = {}
	for name,col in ipairs(schema.columns) do
		if col.pk then cols[name] = string.format("%s_%s", tab, name) end
	end
	if not next(cols) then
		-- table has no primary key, try rowid
		-- TODO: this does not work for WITHOUT ROWID tables. this *should* check that the table
		-- is a rowid table, which is a bit involved because none of the pragmas return that
		-- information
		cols.rowid = string.format("%s_rowid", tab)
	end
	if haveall(names, cols) then return cols end
	-- try foreign keys
	for _,fk in ipairs(schema.foreign_keys) do
		table.clear(cols)
		for from,to in pairs(fk.columns) do
			cols[from] = string.format("%s_%s", fk.table, to)
		end
		if haveall(names, cols) then return cols end
	end
end

local function initselect(tab, names)
	local tname = rename(tab)
	local where = tabdata(tab, function(config) return config.where end)
	local binds = {}
	if where == nil then
		local auto = autowhere(tab, names)
		if auto then
			where = {}
			for col,name in pairs(auto) do
				table.insert(binds, m3.arg(names[name]))
				table.insert(where, string.format("%s = ?%d", m3.escapesql(col), #binds))
			end
			where = m3.sql("WHERE", unpack(where))
		end
	elseif where then
		-- this breaks when `?NNN` is embedded in a string/name/whatever, but oh well.
		local idxmap = {}
		where = string.gsub(where, "?(%d+)", function(idx)
			if not idxmap[idx] then
				table.insert(binds, m3.arg(tonumber(idx)))
				idxmap[idx] = string.format("?%d", #binds)
			end
			return idxmap[idx]
		end)
		where = m3.sql("WHERE", where)
	end
	return tname, where, m3.splat(binds), function(col) return rename(tab, col) end
end

-- this is equivalent to:
--   for i=1, n do t[i] = r:col(i) end
-- but can be inlined by the jit compiler
local function packrow(t, r, i, n)
	if i <= n then
		t[i] = r:col(i)
		return packrow(t, r, i+1, n)
	end
end

-- this is equivalent to `unpack` but does not cause a trace abort
local function unpackrow(t, i, n)
	if i <= n then
		return t[i], unpackrow(t, i+1, n)
	end
end

local workqueue = m3.shared_input()
local putwork = m3.transaction():write(workqueue)

local function build()
	local query = m3.statement(global_task or autotask())
	local nc = query.sqlite3_stmt:colcount()
	local names = {}
	for i=1, nc do names[query.sqlite3_stmt:name(i-1)] = i end
	local insn = m3.all { m3.any(global_insn), m3.commit }
	local init = m3.transaction():autoselect(function(tab) return initselect(tab, names) end)
	local fp
	m3.connect(workqueue, function(task)
		m3.load(fp)
		init(unpackrow(task, 0, nc-1))
		m3.exec(insn)
	end)
	package.loaded.m3_simulate.run = function()
		local t = {}
		for row in query:rows() do
			packrow(t, row, 0, nc-1)
			putwork(t)
		end
	end
	return function()
		fp = m3.save()
	end
end

return {
	cmd   = cmd,
	build = build
}
