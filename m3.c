#include "m3.h"
#include "target.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <luajit.h>

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OPT_SCRIPT      '@'
#define OPT_SCRIPTARG   '-'

typedef struct {
	char option;
	const char *strv;
} Arg;

typedef struct {
	lua_State *T;
	intptr_t alloc_base;
	intptr_t alloc_end;
	intptr_t alloc_ptr;
} TestState;

typedef struct {
	int argn;
	Arg *ap;
	TestState *test;
} SetupData;

static void help(const char *progname)
{
	fprintf(stderr, "Usage: %s [option|script]... [-- [scriptargs]...]\n", progname);
	fputs(
			"    -O[opt]            Control LuaJIT optimizations.\n"
			"    -j cmd             Perform LuaJIT control command.\n"
			"    -p num             Control parallelization (default: number of CPUs).\n"
			"    -l module          Load module.\n"
			"    -i input           Set input.\n"
			"    -v[flags]          Verbose output.\n"
			"    -m[region] num     Set max memory per region.\n"
			"    -t[tests]          Test the simulator.\n"
			"    -V                 Show version.\n"
			"    --                 Stop handling options. Remaining arguments are passed to scripts.\n",
			stderr
	 );
}

static void version(void)
{
	puts(
			"m3 "
#ifdef M3_GITVER
			M3_GITVER
#else
			"(unknown version)"
#endif
			" ["
#if M3_SP64
			"sp64"
#else
			"sp32"
#endif
#if M3_WINDOWS
			" win"
#endif
#if M3_LINUX
			" linux"
#endif
#if M3_MMAP
			" mmap"
#endif
			"]"
	);
	puts(
			LUAJIT_VERSION
#ifdef LJ_GITVER
			" "
			LJ_GITVER
#endif
	);
}

static void parseargs(char **argv, SetupData *sd, m3_Init *init)
{
	Arg *ap = sd->ap;
#define STRARG(o,v) do { ap->option = (o); ap->strv = (v); ap++; } while(0)
	const char *progname = *argv++;
	if (!*argv) {
help:
		help(progname);
		exit(0);
	}
	for (; *argv; argv++) {
		if (argv[0][0] != '-') {
			STRARG(OPT_SCRIPT, *argv);
			continue;
		}
		char f = argv[0][1];
		switch (f) {
			case '-':
				for (argv++; *argv; argv++)
					STRARG(OPT_SCRIPTARG, *argv);
				goto out;
			case 'h':
				goto help;
			case 'V':
				version();
				exit(0);
			case 'O': case 'v': case 't':
				STRARG(f, argv[0]+2);
				break;
			case 'j': case 'p': case 'l': case 'i':
			{
				const char *v = argv[0] + 2;
				if (!*v) {
					v = *++argv;
					if (!v) {
						fprintf(stderr, "-%c: value required\n", f);
						exit(-1);
					}
				}
				if (f == 'p') {
					init->parallel = strtoul(v, NULL, 10);
				} else {
					STRARG(f, v);
				}
				break;
			}
			case 'm':
				assert(!"TODO: -m parsing");
			default:
				fprintf(stderr, "unrecognized option: -%c\n", f);
				help(progname);
				exit(-1);
		}
	}
out:
	sd->argn = ap - sd->ap;
#undef STRARG
}

static int locals(lua_State *L, int level)
{
	lua_Debug ar;
	if (!lua_getstack(L, level, &ar))
		return 0;
	lua_getinfo(L, "S", &ar);
	if(ar.what[0] == 'C' && ar.what[1] == 0) {
		// skip C functions (error & assert)
		if (!lua_getstack(L, level+1, &ar))
			return 0;
	}
	luaL_Buffer B;
	luaL_buffinit(L, &B);
	if (lua_gettop(L)) {
		luaL_addvalue(&B);
		luaL_addchar(&B, '\n');
	}
	luaL_addstring(&B, "locals:");
	for(int i=1;;i++) {
		const char *lname = lua_getlocal(L, &ar, i);
		if(!lname) break;
		lua_pop(L, 1);
		luaL_addstring(&B, "\n\t");
		luaL_addstring(&B, lname);
		luaL_addstring(&B, ": ");
		lua_getlocal(L, &ar, i);
		if(!lua_isstring(L, -1)) {
			lua_getglobal(L, "tostring");
			lua_insert(L, -2);
			if(lua_pcall(L, 1, 1, 0)) {
				lua_pop(L, 1);
				lua_pushstring(L, "(error in tostring)");
			}
		}
		luaL_addvalue(&B);
	}
	luaL_pushresult(&B);
	return 1;
}

/* yoinked from luajit.c */
static int traceback(lua_State *L)
{
	if (!lua_isstring(L, 1)) {
		if (lua_isnoneornil(L, 1) ||
				!luaL_callmeta(L, 1, "__tostring") ||
				!lua_isstring(L, -1))
			return 1;
		lua_remove(L, 1);
	}
	lua_getfield(L, LUA_REGISTRYINDEX, "m3$T");
	lua_State *T = lua_touserdata(L, -1);
	lua_pop(L, 1);
	if (T && !getenv("M3_NOLOCALS")) locals(L, 1);
	luaL_traceback(L, L, lua_tostring(L, 1), 1);
	return 1;
}

static int pcalltb(lua_State *L, int narg, int nret)
{
	int base = lua_gettop(L) - narg;
	lua_pushcfunction(L, traceback);
	lua_insert(L, base);
	int r = lua_pcall(L, narg, nret, base);
	lua_remove(L, base);
	return r;
}

static int report(lua_State *L, int r)
{
	if (r) {
		const char *err = lua_tostring(L, -1);
		if (err) {
			fputs(err, stderr);
			fputc('\n', stderr);
		}
	}
	return r;
}

static int errhandler(lua_State *L)
{
	traceback(L);
	return report(L, 1);
}

static int require(lua_State *L, const char *name)
{
	lua_getglobal(L, "require");
	lua_pushstring(L, name);
	return pcalltb(L, 1, 1);
}

static void *testalloc(SetupData *sd, void *ptr, size_t osize, size_t nsize)
{
	if (!nsize)
		// free: nop.
		return NULL;
	if (nsize <= osize)
		// smaller realloc.
		return ptr;
	TestState *ts = sd->test;
	intptr_t p = (ts->alloc_ptr + 7) & -8;
	intptr_t np = p + nsize;
	intptr_t end = ts->alloc_end;
	if (np < end) {
		ts->alloc_ptr = np;
	} else {
		// alloc doesn't fit - it's time perform a pro gamer move: leak the old allocation.
		// this is actually fine - we double the allocation size so in total we have leaked
		// at most half of our memory, and we will exit after running all the tests anyway.
		intptr_t newsz = 2*(end-ts->alloc_base);
		p = (intptr_t) malloc(newsz);
		if (!p)
			return NULL;
		ts->alloc_base = p;
		ts->alloc_end = p + newsz;
		ts->alloc_ptr = p + nsize;
	}
	if (osize > 0) {
		// realloc
		memcpy((void *)p, ptr, osize);
	}
	return (void *) p;
}

static int testfunc(lua_State *L)
{
	if (!lua_gettop(L)) {
		/* test() => true */
		lua_pushboolean(L, 1);
	} else {
		/* L: test(string) => T: test(string) */
		lua_getfield(L, LUA_REGISTRYINDEX, "m3$T");
		lua_State *T = lua_touserdata(L, -1);
		lua_pop(L, 1);
		lua_getfield(T, LUA_REGISTRYINDEX, "test");
		lua_pushstring(T, luaL_checkstring(L, 1));
		lua_call(T, 1, 1);
		int enabled = lua_toboolean(T, -1);
		lua_pop(T, 1);
		/* L: call second arg */
		if (enabled && lua_gettop(L) >= 2)
			lua_call(L, 0, 0);
		lua_pushboolean(L, enabled);
	}
	return 1;
}

static int setup(lua_State *L)
{
	SetupData *setup = lua_touserdata(L, 1);
	lua_pop(L, 1);
	if (setup->test) {
		// don't bother gcing, state will be thrown away soon anyway.
		lua_gc(L, LUA_GCSTOP, 0);
		// TODO: further speedup here: hook `require` and cache compiled bytecode.
		lua_pushlightuserdata(L, setup->test->T);
		lua_setfield(L, LUA_REGISTRYINDEX, "m3$T");
		lua_pushcfunction(L, testfunc);
		lua_setfield(L, LUA_REGISTRYINDEX, "m3$test");
	}
	if (require(L, "m3_api") || require(L, "m3_simulate"))
		lua_error(L);
	/* stack:
	 *   1: m3 api
	 *   2: simulate */
	Arg *ap = setup->ap;
	for (int i=0; i<setup->argn; i++, ap++) {
		switch (ap->option) {
			case OPT_SCRIPT:
			{
				lua_getfield(L, 2, "init");
				lua_pushstring(L, ap->strv);
				int narg = 1;
				for (int j=i+1; j<setup->argn; j++) {
					Arg *aj = &setup->ap[j];
					if (aj->option == OPT_SCRIPTARG) {
						lua_pushstring(L, aj->strv);
						narg++;
					}
				}
				lua_call(L, narg, 0);
				break;
			}
			case OPT_SCRIPTARG:
				return 0;
			default:
				lua_getfield(L, 2, "command");
				lua_pushlstring(L, &ap->option, 1);
				lua_pushstring(L, ap->strv);
				lua_call(L, 2, 0);
				break;
		}
	}
	lua_getfield(L, 2, "host");
	return 1;
}

static int runtests(m3_Init *init)
{
	init->alloc = (lua_Alloc) testalloc;
	lua_State *T = luaL_newstate();
	if (!T) return -1;
	luaL_openlibs(T);
	lua_settop(T, 0);
	if (report(T, require(T, "m3_test")))
		return -1;
	SetupData *sd = init->ud;
	for (int i=0; i<sd->argn; i++) {
		Arg *ap = &sd->ap[i];
		if (ap->option == 't') {
			lua_getfield(T, 1, "settest");
			lua_pushstring(T, ap->strv);
			lua_call(T, 1, 0);
		}
	}
	lua_getfield(T, 1, "more");
	lua_setfield(T, LUA_REGISTRYINDEX, "more");
	lua_getfield(T, 1, "test");
	lua_setfield(T, LUA_REGISTRYINDEX, "test");
	lua_getfield(T, 1, "fail");
	lua_setfield(T, LUA_REGISTRYINDEX, "fail");
	lua_pop(T, 1);
	TestState test;
	test.T = T;
	test.alloc_base = (intptr_t) malloc(1024*1024);
	test.alloc_end = test.alloc_base + 1024*1024;
	sd->test = &test;
	for (;;) {
		lua_getfield(T, LUA_REGISTRYINDEX, "more");
		lua_call(T, 0, 1);
		if (!lua_toboolean(T, -1)) {
			lua_close(T);
			return 0;
		}
		test.alloc_ptr = test.alloc_base;
		lua_State *L = m3_newstate(init);
		if (!L) return -1;
		m3_pushrun(L);
		if (pcalltb(L, 0, 0)) {
			lua_getfield(T, LUA_REGISTRYINDEX, "fail");
			lua_pushstring(T, lua_tostring(L, -1));
			lua_call(T, 1, 0);
		}
		m3_close(L);
	}
}

int main(int argc, char **argv)
{
	SetupData sd = {0};
	m3_Init init = {
		.setup = setup,
		.ud = &sd,
		.err = errhandler,
		.parallel = -1
	};
	Arg ap[argc];
	sd.ap = ap;
	parseargs(argv, &sd, &init);
	for (int i=0; i<sd.argn; i++) {
		if (sd.ap[i].option == 't')
			 return runtests(&init);
	}
	lua_State *L = m3_newstate(&init);
	if (!L) return -1;
	m3_pushrun(L);
	if (report(L, pcalltb(L, 0, 0))) return -1;
	m3_close(L);
	return 0;
}
