#include "LuaJIT/src/lua.h"
#include "LuaJIT/src/lauxlib.h"
#include "LuaJIT/src/lualib.h"

#include <stdio.h>
#include <stdlib.h>

int luaopen_m3(lua_State *);

static int traceback(lua_State *L)
{
	if (!lua_isstring(L, 1)) {
		if (lua_isnoneornil(L, 1) ||
			!luaL_callmeta(L, 1, "__tostring") ||
			!lua_isstring(L, -1))
			return 1;
		lua_remove(L, 1);
	}
	luaL_traceback(L, L, lua_tostring(L, 1), 1);
	return 1;
}

static int pmain(lua_State *L)
{
	char **argv = lua_touserdata(L, 1);
	lua_gc(L, LUA_GCSTOP, 0);
	luaL_openlibs(L);
	luaL_findtable(L, LUA_REGISTRYINDEX, "_LOADED", 16);
	luaopen_m3(L);
	lua_setfield(L, -2, "m3");
	lua_gc(L, LUA_GCRESTART, -1);
	lua_pop(L, 1);
	lua_getglobal(L, "require");
	lua_pushliteral(L, "m3.cli");
	lua_call(L, 1, 1);
	lua_getfield(L, -1, "main");
	int argc = 0;
	for (; argv[argc]; argc++)
		lua_pushstring(L, argv[argc]);
	lua_call(L, argc, 1);
	return 1;
}

int main(int argc, char **argv)
{
	(void)argc;
	lua_State *L = luaL_newstate();
	if (!L) {
		fputs("cannot create state: not enough memory\n", stderr);
		return EXIT_FAILURE;
	}
	lua_settop(L, 0);
	lua_pushcfunction(L, traceback);
	lua_pushcfunction(L, pmain);
	lua_pushlightuserdata(L, argv);
	int r = lua_pcall(L, 1, 1, 1);
	int exit;
	if (r) {
		fputs(lua_tostring(L, -1), stderr);
		fputc('\n', stderr);
		exit = EXIT_FAILURE;
	} else {
		exit = lua_tonumber(L, -1);
	}
	lua_close(L);
	return exit;
}
