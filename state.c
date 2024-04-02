#define _GNU_SOURCE  /* for sched_getaffinity */

#include "def.h"
#include "m3.h"
#include "mp.h"
#include "target.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#if M3_WINDOWS
#error "TODO: windows: sys_num_cpus"
#else

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

#if M3_MMAP

#include <sys/mman.h>

typedef struct {
	void *private_addr;
	size_t private_size;
	void *shared_addr;
	size_t shared_size;
} MapInfo;

static void ls_pushmap(lua_State *L, void *addr, size_t size)
{
	lua_createtable(L, 0, 2);
	lua_pushnumber(L, (uintptr_t)addr);
	lua_setfield(L, -2, "addr");
	lua_pushnumber(L, size);
	lua_setfield(L, -2, "size");
}

static void ls_tomap(lua_State *L, void **addr, size_t *size)
{
	if (lua_isnil(L, -1)) {
		*addr = NULL;
		*size = 0;
	} else {
		lua_getfield(L, -1, "addr");
		lua_getfield(L, -2, "size");
		*addr = (void *) (uintptr_t) lua_tonumber(L, -2);
		*size = (size_t) lua_tonumber(L, -1);
		lua_pop(L, 2);
	}
}

static int vm_map(lua_State *L, m3_Init *opt)
{
	void *private = mmap(NULL, 3*opt->vmsize+VMSIZE_HUGE, PROT_READ,
		MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0);
	if (UNLIKELY(private == MAP_FAILED)) return 0;
	if (UNLIKELY(mprotect(private, 3*opt->vmsize, PROT_READ|PROT_WRITE))) return 0;
	madvise(private, 3*opt->vmsize+VMSIZE_HUGE, MADV_DONTDUMP);
	// main proc + shared area + worker heaps + alignment
	void *shared = opt->parallel
		? mmap(NULL, VMSIZE_PROC*(opt->parallel+3), PROT_READ|PROT_WRITE,
			MAP_SHARED|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0)
		: NULL;
	if (UNLIKELY(shared == MAP_FAILED)) {
		munmap(private, 3*opt->vmsize+VMSIZE_HUGE);
		return 0;
	}
	madvise(shared, VMSIZE_PROC*(opt->parallel+3), MADV_DONTDUMP);
	// addresses have 47 bits, doubles fit 52 bit integers so we're fine.
	lua_createtable(L, 0, 4);
	ls_pushmap(L, private, opt->vmsize);
	lua_setfield(L, -2, "scratch");
	ls_pushmap(L, private+opt->vmsize, opt->vmsize);
	lua_setfield(L, -2, "vstack");
	ls_pushmap(L, private+2*opt->vmsize, opt->vmsize);
	lua_setfield(L, -2, "frame");
	ls_pushmap(L, private+3*opt->vmsize, VMSIZE_HUGE);
	lua_setfield(L, -2, "zeros");
	ls_pushmap(L, private, 3*opt->vmsize+VMSIZE_HUGE);
	lua_setfield(L, -2, "private");
	if (shared) {
		ls_pushmap(L, shared, VMSIZE_PROC*(opt->parallel+3));
		lua_setfield(L, -2, "shared");
	}
	lua_setfield(L, LUA_REGISTRYINDEX, "m3$vm");
	return 1;
}

static void vm_getmap(lua_State *L, MapInfo *info)
{
	lua_getfield(L, LUA_REGISTRYINDEX, "m3$vm");
	lua_getfield(L, -1, "private");
	ls_tomap(L, &info->private_addr, &info->private_size);
	lua_getfield(L, -2, "shared");
	ls_tomap(L, &info->shared_addr, &info->shared_size);
	lua_pop(L, 3);
}

static void vm_unmap(MapInfo *map)
{
	munmap(map->private_addr, map->private_size);
	if (map->shared_addr)
		munmap(map->shared_addr, map->shared_size);
}

#else
#error "TODO: windows: vm_map_*"
#endif

static void checkopt(m3_Init *opt)
{
	if (opt->parallel == M3_PARALLEL_NCPU) opt->parallel = sys_num_cpus();
	opt->vmsize = opt->vmsize ? (opt->vmsize & -M3_PAGE_SIZE) : VMSIZE_DEFAULT;
}

// from libfhk.a
int luaopen_fhk(lua_State *L);

lua_State *m3_newstate(m3_Init *opt)
{
	checkopt(opt);
	lua_State *L = opt->alloc ? lua_newstate(opt->alloc, opt->ud) : luaL_newstate();
	if (UNLIKELY(!L)) return NULL;
	if (UNLIKELY(!vm_map(L, opt))) goto fail_map;
	lua_pushcfunction(L, opt->setup);
	lua_setfield(L, LUA_REGISTRYINDEX, "m3$setup");
	lua_pushlightuserdata(L, opt->ud);
	lua_setfield(L, LUA_REGISTRYINDEX, "m3$userdata");
	if (opt->parallel) {
		lua_pushnumber(L, opt->parallel);
		lua_setfield(L, LUA_REGISTRYINDEX, "m3$parallel");
		lua_pushcfunction(L, m3_mp_fork);
		lua_setfield(L, LUA_REGISTRYINDEX, "m3$fork");
	}
	luaL_openlibs(L);
	lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
	lua_pushcfunction(L, luaopen_fhk);
	lua_call(L, 0, 1);
	lua_setfield(L, -2, "fhk");
	lua_pop(L, 1);
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
	int err = lua_pcall(L, 1, 1, eh);
	if (err) goto fail_startup;
	lua_setfield(L, LUA_REGISTRYINDEX, "m3$run");
	lua_settop(L, 0);
	return L;
fail_startup:
	{
		MapInfo map;
		vm_getmap(L, &map);
		vm_unmap(&map);
	}
fail_map:
	lua_close(L);
	return NULL;
}

void m3_close(lua_State *L)
{
	lua_getfield(L, LUA_REGISTRYINDEX, "m3$errhandler");
	int eh = lua_isnil(L, -1) ? 0 : lua_gettop(L);
	lua_getglobal(L, "require");
	lua_pushstring(L, "m3_shutdown");
	// don't care about the result.
	// what are we gonna do about it anyway?
	lua_pcall(L, 1, 0, eh);
	// unmapping must be done in this order because finalizers may reference the mappings.
	MapInfo map;
	vm_getmap(L, &map);
	lua_close(L);
	vm_unmap(&map);
}
