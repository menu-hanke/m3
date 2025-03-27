#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include "bc.h"
#include "cdef.h"

static int host_cf_bcload(lua_State *L)
{
	m3_bc_load(L, lua_tostring(L, 1));
	return 1;
}

int luaopen_fhk(lua_State *); // from libfhk.a

int luaopen_m3(lua_State *L)
{
	// re-export our version of fhk for the host, and also for m3_cli version check
	luaL_findtable(L, LUA_REGISTRYINDEX, "_PRELOAD", 4);
	lua_pushcfunction(L, luaopen_fhk);
	lua_setfield(L, -2, "m3.fhk");
	lua_pop(L, 1);
	// return bcload("m3_host")(bcload("m3_cdef")(&M3_CDEF))
	m3_bc_load(L, "m3_host");
	m3_bc_load(L, "m3_cdef");
	lua_pushlightuserdata(L, &M3_CDEF);
	lua_call(L, 1, 1);
	lua_pushcfunction(L, host_cf_bcload);
	lua_call(L, 2, 1);
	return 1;
}
