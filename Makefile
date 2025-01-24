# ---- Configuration ------------------------------------------------------------------

# programs
CROSS          =
CC             = $(CROSS)gcc
AR             = $(CROSS)gcc-ar
STRIP          = $(CROSS)strip
CARGO          = cargo
GIT            = git
LUAJIT         = $(LUAJIT_JITPATH) $(LUAJIT_EXE)

# embed lua bytecode? [y]/n
EMBEDLUA       = y

# embed jit.* lua bytecode? [y]/n
EMBEDJIT       = y

# target system (for cross-compiler)
TARGET_SYS     = $(HOST_SYS)

# ---- Compiler & Linker options -----------------------------------------------

CCOPT          = -O2
CCWARN         = -Wall -Wextra
CCDEBUG        =

CCOPTIONS      = $(CCOPT) $(CCWARN) $(CFLAGS) $(XCFLAGS)
LDOPTIONS      = -Wl,--gc-sections $(LDFLAGS)

ifneq (,$(CCDEBUG))
STRIP          = :
endif

# ---- Host & target detection -------------------------------------------------

ifneq (,$(findstring Windows,$(OS)))
HOST_SYS       = Windows
else
HOST_SYS       = $(shell uname -s)
endif

TARGET_MACHINE = $(shell $(CC) -dumpmachine)
ifneq (,$(findstring x86_64,$(TARGET_MACHINE)))
TARGET_ARCH    = x64
endif # else: add as needed

# ---- Files and paths ---------------------------------------------------------

ifeq (Windows,$(TARGET_SYS))
TARGET_EXE     = .exe
TARGET_SO      = .dll
else
TARGET_SO      = .so
endif

M3_A           = m3.a
M3_EXE         = m3$(TARGET_EXE)
M3_SO          = m3$(TARGET_SO)

M3EXE_O        = libm3.o m3.o
M3SO_O         = libm3.pic.o
M3A_O          = libm3.o
M3GENLUA       = m3_cdef.lua

ifeq (y,$(EMBEDLUA))
M3LUA_O        = m3.lua.o m3_array.lua.o m3_cdata.lua.o m3_cdef.lua.o m3_code.lua.o \
				 m3_constify.lua.o m3_control.lua.o m3_data.lua.o m3_debug.lua.o m3_loop.lua.o \
				 m3_mem.lua.o m3_mp.lua.o m3_mp_main.lua.o m3_mp_worker.lua.o m3_shutdown.lua.o \
				 m3_sqlite.lua.o m3_uid.lua.o
M3EXELUA_O     = m3_simulate.lua.o m3_test.lua.o
endif

ifeq (y,$(EMBEDJIT))
ifneq (,$(TARGET_ARCH))
JITDIS_O       = jit_dis_$(TARGET_ARCH).lua.o
ifeq (x64,$(TARGET_JITARCH))
JITDIS_O      += jit_dis_x86.lua.o
endif
endif
JITLUA_O       = jit_bc.lua.o jit_dump.lua.o jit_p.lua.o jit_v.lua.o jit_vmdef.lua.o jit_zone.lua.o \
				 $(JITDIS_O)
endif

ifneq (Windows,$(TARGET_SYS))
M3EXE_SYMS     = -Wl,--dynamic-list=dynamic.list
endif

# ---- fhk ---------------------------------------------------------------------

FHK_ROOT       = fhk5
FHK_A          = $(FHK_ROOT)/target/release/libfhk.a
ifeq (Windows,$(TARGET_SYS))
FHK_SO         = $(FHK_ROOT)/target/release/fhk.dll
M3EXE_FHK      = $(FHK_SO)
else
FHK_SO         = $(FHK_ROOT)/target/release/libfhk.so
M3EXE_FHK      = -Wl,--whole-archive $(FHK_A) -Wl,--no-whole-archive
endif
M3SO_FHK       = $(FHK_SO)

# ---- LuaJIT ------------------------------------------------------------------

LUAJIT_ROOT    = LuaJIT
LUAJIT_SRC     = $(LUAJIT_ROOT)/src
LUAJIT_A       = $(LUAJIT_SRC)/libluajit.a
LUAJIT_EXE     = $(LUAJIT_SRC)/luajit$(TARGET_EXE)
LUAJIT_PKGPATH = $(shell $(LUAJIT_EXE) -e print\(package.path\));$(LUAJIT_SRC)/?.lua
LUAJIT_JITPATH = LUA_PATH="$(LUAJIT_PKGPATH)"
LUAJIT_INCLUDE = -I$(LUAJIT_SRC)
ifeq (Windows,$(TARGET_SYS))
LUAJIT_SO      = $(LUAJIT_SRC)/lua51.dll
M3EXE_LUAJIT   = $(LUAJIT_SO)
else
LUAJIT_SO      = $(LUAJIT_SRC)/libluajit.so
M3EXE_LUAJIT   = $(LUAJIT_A) -lm -ldl
endif
M3SO_LUAJIT    = $(LUAJIT_SO)

# ---- SQLite ------------------------------------------------------------------

SQLITE_ROOT    = sqlite
SQLITE_O       = $(SQLITE_ROOT)/sqlite3.o
SQLITE_PICO    = $(SQLITE_ROOT)/sqlite3.pic.o
SQLITE_INCLUDE = -I$(SQLITE_ROOT)
M3EXE_SQLITE   = $(SQLITE_O)
M3SO_SQLITE    = $(SQLITE_PICO)
SQLITE_CFLAGS  = -DSQLITE_CUSTOM_INCLUDE=../sql.h

# ---- Targets -----------------------------------------------------------------

$(M3_EXE): $(M3EXE_O) $(M3LUA_O) $(M3EXELUA_O) $(JITLUA_O) $(M3GENLUA)
	$(CC) $(LDOPTIONS) $(M3EXE_O) $(M3LUA_O) $(M3EXELUA_O) $(JITLUA_O)  $(M3EXE_FHK) \
		$(M3EXE_LUAJIT) $(M3EXE_SQLITE) $(M3EXE_SYMS) -o $@
	$(STRIP) $@

$(M3_SO): $(M3SO_O) $(M3LUA_O) $(M3GENLUA)
	$(CC) $(LDOPTIONS) $(M3SO_O) $(M3LUA_O) $(M3GENLUA_O) $(M3SO_FHK) $(M3SO_LUAJIT) \
		$(M3SO_SQLITE) -shared -o $@
	$(STRIP) $@

$(M3_A): $(M3A_O) $(M3LUA_O) $(M3GENLUA)
	$(AR) rcs $@ $(M3A_O) $(M3LUA_O)

# ---- Rules -------------------------------------------------------------------

%.o: %.c
	$(CC) $(CCOPTIONS) -c $< -o $@

%.pic.o: %.c
	$(CC) $(CCOPTIONS) -fPIC -c $< -o $@

%.lua.c: %.lua
	$(LUAJIT) -b -n $(subst _,.,$(notdir $*)) $< $@

jit_%.lua.c: $(LUAJIT_SRC)/jit/%.lua
	$(LUAJIT) -b -n jit.$* $< $@

m3_cdef.lua: libm3.o
	$(CC) -P -E -nostdinc -DM3_LUADEF libm3.c 2>/dev/null | $(LUAJIT) luadef.lua > $@

CCGITVER = $(shell GITVER=$$($(GIT) describe) && echo -DM3_GITVER='\"'$$(echo $$GITVER)'\"')
CCLJVER  = $(shell GITVER=$$(cd $(LUAJIT_ROOT) && $(GIT) describe) && echo -DLJ_GITVER='\"'$$(echo $$GITVER)'\"')
m3.o: XCFLAGS += $(CCGITVER) $(CCLJVER) $(LUAJIT_INCLUDE) $(SQLITE_INCLUDE)
$(SQLITE_O) $(SQLITE_PICO): XCFLAGS += $(SQLITE_CFLAGS)

# ---- Dependencies ------------------------------------------------------------

deps-fhk:
	cd $(FHK_ROOT) && cargo build --release

deps-luajit:
	$(MAKE) -C $(LUAJIT_ROOT) amalg

deps-sqlite:
	cd $(SQLITE_ROOT) && ./configure && $(MAKE) sqlite3.c
	$(MAKE) $(SQLITE_O) $(SQLITE_PICO)

deps: deps-fhk deps-luajit deps-sqlite
.PHONY: deps deps-fhk deps-luajit deps-sqlite

# ---- Auxiliary ------------------------------------------------------------------

depend:
	$(MAKE) clean
	$(CC) -MM *.c > Makefile.dep

clean:
	$(RM) $(M3_EXE) $(M3GENLUA) *.o *.so *.dll *.a *.lua.c

.PHONY: depend clean

-include Makefile.dep

.SUFFIXES:
