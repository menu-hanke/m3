local m3 = require "m3"

local global_config = {
	insn  = {},
	tab   = nil,
	col   = "rowid",
	query = nil
}

local globals = {
	"all", "any", "call", "dynamic", "exec", "first", "loop", "nothing", "optional", "skip", "try",
	"arg", "cdata", "connect", "define", "defined", "func", "include", "pipe", "ret", "transaction",
	"pprint",
	"database", "datadef", "datamap",
	"uid"
}
for _,func in ipairs(globals) do _G[func] = m3[func] end

if not test then
	function test() return false end
end -- else: m3 is in test mode, test is a C funtion

-- TODO make this more general:
--   * allow specifying which column(s) `query` iterates over
--   * allow specifying multiple tables
--   * allow specifying how other tables are joined
function input(tab, query)
	global_config.tab = m3.rename(tab)
	global_config.query = query
end

function simulate(insn)
	table.insert(global_config.insn, m3.all(insn))
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

local init = m3.transaction():autoselect(function(tab)
	if not global_config.tab then return end
	local main = global_config.tab
	local sql = {m3.sql("WHERE", string.format("%s.%s=?", main, global_config.col))}
	if tab.name ~= global_config.tab then
		-- TODO: `input` should allow specifying the join condition here
		table.insert(sql, m3.sql("FROM", m3.escapesql(main)))
		for _,fk in ipairs(tab.foreign_keys) do
			if fk.table == global_config.tab then
				for _,f in ipairs(fk.fields) do
					table.insert(sql, m3.sql("WHERE", string.format("%s.%s=%s.%s",
						m3.escapesql(main), m3.escapesql(f.to), m3.escapesql(tab.name), m3.escapesql(f.from))))
				end
				break
			end
		end
	end
	return sql, m3.arg(1)
end)

local function autotab()
	local tab
	for o in m3.G:objects() do
		if o.op == "TAB" and #o.shape.fields == 0 then
			local oname = m3.rename(tostring(o.name))
			if m3.schema(oname) then
				if tab then
					error(string.format(
						"cannot determine input table (`%s' and `%s' both exist in db). Use `input' to set it explicitly.",
						tab, o.name
					))
				end
				tab = oname
			end
		end
	end
	return tab
end

local function autoquery(tab)
	if tab then
		return {m3.sql("SELECT", "rowid"), m3.sql("FROM", tab)}
	else
		return "SELECT 0"
	end
end

local workqueue = m3.shared_input()
local putwork = m3.transaction():write(workqueue)

local function build()
	if global_config.tab == nil then global_config.tab = autotab() end
	local query = m3.statement(global_config.query or autoquery(global_config.tab))
	local insn = m3.all { m3.any(global_config.insn), m3.commit }
	local fp
	m3.connect(workqueue, function(task)
		m3.load(fp)
		init(task)
		m3.exec(insn)
	end)
	package.loaded.m3_simulate.run = function()
		for row in query:rows() do
			putwork(row:int(0))
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
