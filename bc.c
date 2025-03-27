// for dladdr():
#define _GNU_SOURCE

#include "def.h"
#include "bc.h"
#include "target.h"
#if !defined(M3_LUADEF) && !defined(M3_MAKEDEP)
#include "bcode.h"
#endif

#include "LuaJIT/src/lua.h"
#include "LuaJIT/src/lauxlib.h"

#include <stdio.h>
#include <string.h>

#define BC_LOAD_OK       0
#define BC_LOAD_NOTFOUND 1
#define BC_LOAD_ERROR    2

static const void *bc_load_builtin(const char *name)
{
	ssize_t left = 0;
	ssize_t right = M3_BCODE_NUM-1;
	while (left <= right) {
		ssize_t mid = (left+right) >> 1;
		void *data = (void *)M3_BCODE_DATA + M3_BCODE_OFS[mid];
		int cmp = strcmp(name, data);
		if (!cmp) return data + strlen(name) + 1;
		right = cmp < 0 ? (mid-1) : right;
		left = cmp < 0 ? left : (mid+1);
	}
	return NULL;
}

#if M3_LOADLUA

#if M3_LINUX
#include <dlfcn.h>  // dladdr
#include <stdlib.h> // realpath
#include <limits.h> // PATH_MAX
#define BC_PATH_MAX PATH_MAX // must be at least PATH_MAX for realpath, but can also be larger
#else
#define BC_PATH_MAX 4096
#endif

static int bc_load_file(lua_State *L, const char *name)
{
	char path[BC_PATH_MAX];
#if M3_LINUX
	static const char *dli_fname = (const char *) ~(uintptr_t)0;
	if (dli_fname == (const char *) ~(uintptr_t)0) {
		Dl_info dli;
		dladdr(&bc_load_file, &dli);
		dli_fname = dli.dli_fname;
		if (dli_fname)
			dli_fname = realpath(dli_fname, NULL);
	}
	if (!dli_fname)
		return BC_LOAD_NOTFOUND;
	ssize_t n = strlen(dli_fname);
	if (n >= BC_PATH_MAX)
		return BC_LOAD_NOTFOUND;
	memcpy(path, dli_fname, n+1);
#else
	ssize_t n = 0;
#endif
	while (n > 0 && path[n-1] != '/')
		n--;
	size_t nname = strlen(name);
	if (n+nname+5 > BC_PATH_MAX)
		return BC_LOAD_NOTFOUND;
	memcpy(path+n, name, nname);
	path[n+nname  ] = '.';
	path[n+nname+1] = 'l';
	path[n+nname+2] = 'u';
	path[n+nname+3] = 'a';
	path[n+nname+4] = 0;
	switch (luaL_loadfile(L, path)) {
		case LUA_OK: return BC_LOAD_OK;
		case LUA_ERRFILE: return BC_LOAD_NOTFOUND;
		default: return BC_LOAD_ERROR;
	}
}

#endif

static int bc_tryload(lua_State *L, const char *name)
{
	const void *bcode = bc_load_builtin(name);
	if (bcode)
		return luaL_loadbuffer(L, bcode, ~(size_t)0, name) ? BC_LOAD_ERROR : BC_LOAD_OK;
#if M3_LOADLUA
	return bc_load_file(L, name);
#else
	return BC_LOAD_NOTFOUND;
#endif
}

static int bc_load(lua_State *L, const char *name, int loader)
{
	switch (bc_tryload(L, name)) {
		case BC_LOAD_OK:
			return 1;
		case BC_LOAD_NOTFOUND:
			if (loader) return 0;
			lua_pushfstring(L, "bytecode not found for module `%s'", name);
			// fallthrough
		default:
			// BC_LOAD_ERROR
			lua_error(L);
			return 0; // unreachable
	}
}

void m3_bc_load(lua_State *L, const char *name)
{
	bc_load(L, name, 0);
}

static int bc_cf_loader(lua_State *L)
{
	return bc_load(L, lua_tostring(L, 1), 1);
}

void m3_bc_open(lua_State *L)
{
	lua_getglobal(L, "package");
	lua_getfield(L, -1, "loaders");
	size_t n = lua_objlen(L, -1);
	lua_pushcfunction(L, bc_cf_loader);
	lua_rawseti(L, -2, n+1);
	lua_pop(L, 2);
}
