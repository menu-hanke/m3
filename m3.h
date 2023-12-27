/* m3 public embedding API. */

#pragma once

#include <stddef.h>

#ifndef M3_API
#define M3_API
#endif

#define M3_PARALLEL_NCPU     (-1)
#define M3_PARALLEL_OFF      0

typedef struct lua_State lua_State;

typedef int (*lua_CFunction)(lua_State *);
typedef void *(*lua_Alloc)(void *, void *, size_t, size_t);

typedef struct m3_Init {
	lua_CFunction setup;
	void *ud;
	lua_CFunction err;
	lua_Alloc alloc;
	size_t vmsize;
	int parallel;
} m3_Init;

M3_API lua_State *m3_newstate(m3_Init *opt);
M3_API void m3_close(lua_State *L);
#define m3_pushrun(L) lua_getfield((L), LUA_REGISTRYINDEX, "m3$run")
