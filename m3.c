#include "target.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <luajit.h>
#include <sqlite3.h>

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// from libfhk.a
int luaopen_fhk(lua_State *L);
extern const char *fhk_VERSION;

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
	int parallel;
	int mem;
	Arg *ap;
} SetupData;

static void help(const char *progname)
{
	fprintf(stderr, "Usage: %s [option|script]... [-- [scriptargs]...]\n", progname);
	fputs(
		"    -O[opt]            Control LuaJIT optimizations.\n"
		"    -j cmd             Perform LuaJIT control command.\n"
#if M3_LINUX
		"    -p num             Set number of worker processes (default: number of CPUs).\n"
#endif
		"    -l module          Load module.\n"
		"    -d file[=name]     Attach database.\n"
		"    -v[flags]          Verbose output (use -vh to list options).\n"
		"    -t[tests]          Test the simulator.\n"
		"    -V                 Show version.\n"
		"    --                 Stop handling options. Remaining arguments are passed to scripts.\n",
		stderr
	 );
}

static void verbosehelp(void)
{
	fputs(
		"Verbose flags (you can set multiple):\n"
		"    d      Show inferred data model.\n"
		"    s      Show save and load events.\n"
		"    q      Show SQL queries.\n"
		"    c      Show generated Lua code.\n"
		"    a      Show memory allocations.\n"
		"    g      Show generated fhk mappings.\n",
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
#if M3_LINUX
			" linux"
#endif
#if M3_WINDOWS
			" windows"
#endif
#if M3_MMAP
			" mmap"
#endif
#if M3_VIRTUALALLOC
			" VirtualAlloc"
#endif
			" ]"
	);
	printf("fhk %s\n", fhk_VERSION);
	puts(
			LUAJIT_VERSION
#ifdef LJ_GITVER
			" ("
			LJ_GITVER
			")"
#endif
	);
	puts("SQLite " SQLITE_VERSION " (" SQLITE_SOURCE_ID ")");
}

static void parseargs(char **argv, SetupData *sd)
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
			case 'v':
				if (strchr(argv[0], 'h')) {
					verbosehelp();
					exit(0);
				}
				// fallthrough
			case 'O': case 't':
				STRARG(f, argv[0]+2);
				break;
			case 'j': case 'p': case 'l': case 'd':
			{
				const char *v = argv[0] + 2;
				if (!*v) {
					v = *++argv;
					if (!v) {
						fprintf(stderr, "-%c: value required\n", f);
						exit(-1);
					}
				}
				if (M3_LINUX && f == 'p') {
					sd->parallel = strtoul(v, NULL, 10);
				} else {
					STRARG(f, v);
				}
				break;
			}
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
		if (!strcmp(lname, "(*temporary)")) continue;
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

static int require(lua_State *L, const char *name)
{
	lua_getglobal(L, "require");
	lua_pushstring(L, name);
	return pcalltb(L, 1, 1);
}

static void *testalloc(TestState *ts, void *ptr, size_t osize, size_t nsize)
{
	if (!nsize)
		// free: nop.
		return NULL;
	if (nsize <= osize)
		// smaller realloc.
		return ptr;
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
		// test() => true
		lua_pushboolean(L, 1);
	} else if (lua_isboolean(L, 1)) {
		lua_State *T = lua_touserdata(L, lua_upvalueindex(1));
		lua_getfield(T, LUA_REGISTRYINDEX, "seterr");
		if (!lua_toboolean(L, 1)) {
			// L: test(false, string) => T: set error pattern
			lua_pushstring(T, luaL_checkstring(L, 2));
			lua_call(T, 1, 0);
			lua_pushboolean(L, 1);
		} else {
			// L: test(true) => skip
			lua_pushliteral(T, "<skip>");
			lua_call(T, 1, 0);
			lua_pushliteral(L, "<skip>");
			lua_error(L);
		}
	} else {
		// L: test(string) => T: test(string)
		lua_State *T = lua_touserdata(L, lua_upvalueindex(1));
		lua_getfield(T, LUA_REGISTRYINDEX, "test");
		lua_pushstring(T, luaL_checkstring(L, 1));
		lua_call(T, 1, 1);
		int on = lua_toboolean(T, -1);
		lua_pop(T, 1);
		lua_pushboolean(L, on);
	}
	return 1;
}

static int setup(lua_State *L)
{
	SetupData *setup = lua_touserdata(L, 1);
	if (require(L, "m3_simulate"))
		lua_error(L);
	Arg *ap = setup->ap;
	for (int i=0; i<setup->argn; i++, ap++) {
		switch (ap->option) {
			case OPT_SCRIPT:
			{
				if (luaL_loadfile(L, ap->strv))
					lua_error(L);
				int narg = 0;
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
				break;
			default:
				lua_getfield(L, 2, "cmd");
				lua_pushlstring(L, &ap->option, 1);
				lua_pushstring(L, ap->strv);
				lua_call(L, 2, 0);
				break;
		}
	}
	lua_getfield(L, 2, "build");
	lua_call(L, 0, 1);
	return 1;
}

static int setupstate(lua_State *L, SetupData *sd)
{
	luaL_openlibs(L);
	luaL_loadstring(L,
		"local luaopen_fhk, setup, udata, parallel = ...\n"
		"package.loaded.fhk = luaopen_fhk()\n"
		"package.loaded.m3_environment = {\n"
		  "init = function() return setup(udata) end,\n"
#if M3_LINUX
		  "parallel = parallel == -1 and 'auto' or (parallel > 0 and parallel),\n"
#endif
		"}\n"
		"require 'm3'\n"
		"return require('m3_simulate').run\n"
	);
	lua_pushcfunction(L, luaopen_fhk);
	lua_pushcfunction(L, setup);
	lua_pushlightuserdata(L, sd);
	lua_pushinteger(L, sd->parallel);
	return pcalltb(L, 4, 1);
}

static int waitworkers(lua_State *L)
{
	int r;
	if (((r = require(L, "m3"))) == LUA_OK) {
		lua_getfield(L, -1, "wait");
		r = pcalltb(L, 0, 0);
	}
	return r;
}

static const char *TESTREG[] = { "more", "seterr", "test", "fail" };

static int runtests(SetupData *sd)
{
	lua_State *T = luaL_newstate();
	if (!T) return -1;
	luaL_openlibs(T);
	lua_settop(T, 0);
	if (report(T, require(T, "m3_test")))
		return -1;
	for (int i=0; i<sd->argn; i++) {
		Arg *ap = &sd->ap[i];
		if (ap->option == 't') {
			lua_getfield(T, 1, "settest");
			lua_pushstring(T, ap->strv);
			lua_call(T, 1, 0);
		}
	}
	for (size_t i=0; i<sizeof(TESTREG)/sizeof(TESTREG[0]); i++) {
		lua_getfield(T, 1, TESTREG[i]);
		lua_setfield(T, LUA_REGISTRYINDEX, TESTREG[i]);
	}
	lua_pop(T, 1);
	TestState test;
	test.T = T;
	test.alloc_base = (intptr_t) malloc(1024*1024);
	test.alloc_end = test.alloc_base + 1024*1024;
	for (;;) {
		lua_getfield(T, LUA_REGISTRYINDEX, "more");
		lua_call(T, 0, 1);
		if (!lua_toboolean(T, -1)) {
			lua_close(T);
			return EXIT_SUCCESS;
		}
		test.alloc_ptr = test.alloc_base;
		lua_State *L = lua_newstate((lua_Alloc) testalloc, &test);
		if (!L) return EXIT_FAILURE;
		lua_gc(L, LUA_GCSTOP, 0);
		lua_pushlightuserdata(L, T);
		lua_pushcclosure(L, testfunc, 1);
		lua_setglobal(L, "test");
		if (setupstate(L, sd) || pcalltb(L, 0, 0) || waitworkers(L)) {
			lua_getfield(T, LUA_REGISTRYINDEX, "fail");
			lua_pushstring(T, lua_tostring(L, -1));
			lua_call(T, 1, 0);
		}
		lua_close(L);
	}
}

int main(int argc, char **argv)
{
	SetupData sd = {
		.argn = 0,
		.parallel = -1,
		.mem = -1,
	};
	Arg ap[argc];
	sd.ap = ap;
	parseargs(argv, &sd);
	for (int i=0; i<sd.argn; i++) {
		if (sd.ap[i].option == 't')
			 return runtests(&sd);
	}
	lua_State *L = luaL_newstate();
	if (!L) return EXIT_FAILURE;
	int r = report(L, setupstate(L, &sd))
		|| report(L, pcalltb(L, 0, 0))
		|| report(L, waitworkers(L));
	lua_close(L);
	return r;
}
