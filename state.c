#define _GNU_SOURCE  /* for sched_getaffinity */

#include "def.h"
#include "m3.h"
#include "mp.h"
#include "target.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#if M3_LINUX

#include <sched.h>

static int sys_num_cpus(void)
{
	cpu_set_t set;
	CPU_ZERO(&set);
	if (sched_getaffinity(0, sizeof(set), &set))
		return 1;
	return CPU_COUNT(&set);
}

#endif

static void checkopt(m3_Init *opt)
{
#if M3_LINUX
	if (opt->parallel == M3_PARALLEL_NCPU) opt->parallel = sys_num_cpus();
#endif
	opt->vmsize = opt->vmsize ? (opt->vmsize & -M3_PAGE_SIZE) : VMSIZE_DEFAULT;
}

// from libfhk.a
int luaopen_fhk(lua_State *L);

static void openlibs(lua_State *L)
{
	luaL_openlibs(L);
	lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
	lua_pushcfunction(L, luaopen_fhk);
	lua_call(L, 0, 1);
	lua_setfield(L, -2, "fhk");
	lua_pop(L, 1);
}

static void pushenv(lua_State *L, m3_Init *opt)
{
	lua_newtable(L);
	lua_pushcfunction(L, opt->setup);
	lua_setfield(L, -2, "setup");
	lua_pushlightuserdata(L, opt->ud);
	lua_setfield(L, -2, "userdata");
	lua_pushinteger(L, VMSIZE_DEFAULT);
	lua_setfield(L, -2, "stack");
#if M3_LINUX
	if (opt->parallel) {
		lua_pushinteger(L, opt->parallel);
		lua_setfield(L, -2, "parallel");
		lua_pushcfunction(L, m3_mp_fork);
		lua_setfield(L, -2, "fork");
	}
#endif
}

lua_State *m3_newstate(m3_Init *opt)
{
	checkopt(opt);
	lua_State *L = opt->alloc ? lua_newstate(opt->alloc, opt->ud) : luaL_newstate();
	openlibs(L);
	pushenv(L, opt);
	lua_setfield(L, LUA_REGISTRYINDEX, "m3$environment");
	int eh;
	if (opt->err) {
		lua_pushcfunction(L, opt->err);
		lua_pushvalue(L, -1);
		lua_setfield(L, LUA_REGISTRYINDEX, "m3$errhandler");
		eh = lua_gettop(L);
	} else {
		// TODO: use builtin?
		eh = 0;
	}
	lua_getglobal(L, "require");
	lua_pushstring(L, "m3_startup");
	if (lua_pcall(L, 1, 1, eh)) {
		lua_close(L);
		return NULL;
	} else {
		lua_setfield(L, LUA_REGISTRYINDEX, "m3$run");
		lua_settop(L, 0);
		return L;
	}
}

void m3_close(lua_State *L)
{
	lua_getfield(L, LUA_REGISTRYINDEX, "m3$errhandler");
	int eh = lua_isnil(L, -1) ? 0 : lua_gettop(L);
	lua_getglobal(L, "require");
	lua_pushstring(L, "m3_shutdown");
	// don't care about the return value.
	// what are we gonna do about it anyway?
	lua_pcall(L, 1, 0, eh);
	// this also destroys memory maps etc.
	lua_close(L);
}
