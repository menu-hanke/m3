-- vim: ft=lua

data.define [[
table A
]]

data.transaction():read("A.x")

test.error "read but never written"
