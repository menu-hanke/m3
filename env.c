#include "bc.h"
#include "cdef.h"
#include "def.h"
#include "err.h"

#include "LuaJIT/src/lua.h"
#include "LuaJIT/src/lualib.h"
#include "LuaJIT/src/lauxlib.h"

#include <assert.h>

#if M3_LUADEF
CDEF typedef struct m3_State m3_State;
#else
#define m3_State lua_State
#endif

#define ENV_STACK_TRACEBACK    1
#define ENV_STACK_EVAL         2
#define ENV_REG_TMP            "m3$tmp"

#define ENV_SERIAL_REG_FLUSH   "m3$serial.flush"

CDEF typedef struct m3_Buf {
	const void *ptr;
	size_t len;
} m3_Buf;

static int traceback(lua_State *L)
{
	luaL_traceback(L, L, lua_tostring(L, 1), 1);
	return 1;
}

#ifdef NDEBUG
#define env_call lua_call
#else
#include <stdio.h>
static void env_call(lua_State *L, int nargs, int nres)
{
	if (lua_pcall(L, nargs, nres, ENV_STACK_TRACEBACK)) {
		fputs(lua_tostring(L, -1), stderr);
		fputc('\n', stderr);
		assert(0);
	}
}
#endif

int luaopen_fhk(lua_State *); // from libfhk.a

CFUNC m3_State *m3_env_newstate(void)
{
	lua_State *L = luaL_newstate();
	if (!L) return NULL;
	luaL_openlibs(L);
	// set up stack for the rest of the initialization.
	lua_settop(L, 0);
	lua_pushcfunction(L, traceback);    // ENV_STACK_TRACEBACK
	// install require handler
	m3_bc_open(L);
	luaL_findtable(L, LUA_REGISTRYINDEX, "_LOADED", 16);
	// _LOADED.fhk = luaopen_fhk()
	luaopen_fhk(L);
	lua_setfield(L, -2, "fhk");
	// _LOADED.m3_C = bcload("m3_cdef")(&M3_CDEF)
	// _LOADED.sqlite = bcload("sqlite")(m3_C)
	m3_bc_load(L, "m3_cdef");
	lua_pushlightuserdata(L, &M3_CDEF);
	env_call(L, 1, 1);
	m3_bc_load(L, "sqlite");
	lua_pushvalue(L, -2);
	env_call(L, 1, 1);
	lua_setfield(L, -3, "sqlite");
	lua_setfield(L, -2, "m3_C");
	// pop _LOADED
	lua_pop(L, 1);
	// set up globals
	m3_bc_load(L, "m3_lib");
	env_call(L, 0, 0);
	// eval = bcload("m3_eval")()
	m3_bc_load(L, "m3_eval");
	env_call(L, 0, 1);                  // ENV_STACK_EVAL
	assert(lua_gettop(L) == ENV_STACK_EVAL);
	return L;
}

CFUNC void m3_env_close(m3_State *L)
{
	lua_close(L);
}

static int env_pcall(lua_State *L, int nargs, int nres, m3_Buf *response)
{
	int r = lua_pcall(L, nargs, nres, ENV_STACK_TRACEBACK);
	if (UNLIKELY(r)) {
		response->ptr = lua_tolstring(L, -1, &response->len);
		// make sure the error string doesn't get gc'd.
		lua_setfield(L, LUA_REGISTRYINDEX, ENV_REG_TMP);
	}
	return r;
}

static void env_getref(lua_State *L, int idx, m3_Buf *buf)
{
	buf->ptr = *(const void **) lua_topointer(L, idx);
	buf->len = lua_tointeger(L, idx+1);
}

static int env_eval(lua_State *L, const void *args, size_t len, m3_Buf *response)
{
	lua_pushvalue(L, ENV_STACK_EVAL);
	lua_insert(L, -2);
	lua_pushlightuserdata(L, (void *) args);
	lua_pushinteger(L, len);
	int r;
	if (UNLIKELY((r = env_pcall(L, 3, 2, response)))) {
		return r;
	} else {
		env_getref(L, -2, response);
		lua_pop(L, 2);
		return 0;
	}
}

CFUNC int m3_env_eval(m3_State *L, const char *src, const void *args, size_t len, m3_Buf *response)
{
	lua_pushstring(L, src);
	return env_eval(L, args, len, response);
}

CFUNC int m3_env_exec(m3_State *L, int func, const void *args, size_t len, m3_Buf *response)
{
	lua_pushinteger(L, func);
	return env_eval(L, args, len, response);
}

// /* ---- Serial environments ------------------------------------------------- */
// 
// CFUNC int m3_env_serial_flush(m3_State *L, const void *buf, size_t len, m3_Buf *response)
// {
// 	lua_getfield(L, LUA_REGISTRYINDEX, ENV_SERIAL_REG_FLUSH);
// 	lua_pushlightuserdata(L, (void *) buf);
// 	lua_pushinteger(L, len);
// 	int r;
// 	if (UNLIKELY((r = env_pcall(L, 2, 2, response)))) {
// 		return r;
// 	} else {
// 		env_getref(L, -2, response);
// 		lua_pop(L, 2);
// 		return 0;
// 	}
// }
// 
// /* ---- Environments -------------------------------------------------------- */
// 
// static int traceback(lua_State *L)
// {
// 	luaL_traceback(L, L, lua_tostring(L, -1), 1);
// 	lua_pushvalue(L, -1);
// 	lua_setfield(L, LUA_REGISTRYINDEX, ENV_REG_ERROR);
// 	return 1;
// }
// 
// static int traceback_pcall(lua_State *L, int narg, int nret)
// {
// 	int base = lua_gettop(L) - narg;
// 	lua_pushcfunction(L, traceback);
// 	lua_insert(L, base);
// 	int r = lua_pcall(L, narg, nret, base);
// 	lua_remove(L, base);
// 	return r;
// }
// 
// int luaopen_fhk(lua_State *); // from libfhk.a
// 
// static lua_State *env_newstate(void)
// {
// 	lua_State *L = luaL_newstate();
// 	if (!L) return NULL;
// 	luaL_openlibs(L);
// 	luaL_findtable(L, LUA_REGISTRYINDEX, "_LOADED", 16);
// 	luaopen_fhk(L);
// 	lua_setfield(L, -2, "fhk");
// 	m3_bc_open(L);
// 	m3_bc_load(L, "m3_cdef");
// 	lua_pushlightuserdata(L, &M3_CDEF);
// 	lua_call(L, 1, 1);
// 	lua_setfield(L, -2, "m3_C");
// 	lua_pop(L, 1); // pop _LOADED
// 	return L;
// }
// 
// /* ---- Serial environments ------------------------------------------------- */
// 
// #define SerialEnv LVOID(lua_State)
// 
// CFUNC int m3_env_serial_new(const void *init, size_t len, SerialEnv **envp)
// {
// 	lua_State *L = env_newstate();
// 	if (!L) return M3_ERR_LSTATE;
// 	m3_bc_load(L, "m3_init");
// 	lua_pushliteral(L, "serial");
// 	lua_pushlstring(L, init, len);
// 	int err = traceback_pcall(L, 1, 0);
// 	lua_settop(L, 0);
// 	*envp = L;
// 	return err ? M3_ERR_LINIT : M3_OK;
// }
// 
// CFUNC const char *m3_env_serial_err(SerialEnv *L)
// {
// 	lua_getfield(L, LUA_REGISTRYINDEX, ENV_REG_ERROR);
// 	const char *err = lua_tostring(L, -1);
// 	// string is still in registry, so popping cannot free it
// 	lua_pop(L, 1);
// 	return err;
// }
// 
// CFUNC int m3_env_serial_send(SerialEnv *L, int chan, const void *buf, size_t len)
// {
// 	// TODO
// 	return 0;
// }
// 
// CFUNC int m3_env_serial_shutdown(SerialEnv *L)
// {
// 	// TODO
// 	return 0;
// }
// 
// CFUNC void m3_env_serial_close(SerialEnv *L)
// {
// 	lua_close(L);
// }
// 
// /* ---- Thread environments ------------------------------------------------- */
// 
// /* ---- Fork environments --------------------------------------------------- */
