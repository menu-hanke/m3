#pragma once

#include "def.h"

#include "LuaJIT/src/lua.h"

M3_FUNC void m3_bc_load(lua_State *L, const char *name);
M3_FUNC void m3_bc_open(lua_State *L);
