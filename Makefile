#-------------------------------------------------------------------------------#
# m3 makefile.                                                                  #
#-------------------------------------------------------------------------------#

# programs
CC             = $(CROSS)gcc
AR             = $(CROSS)gcc-ar
STRIP          = $(CROSS)strip
LUAJIT         = $(LJLUAPATH) $(LJBINARY)

# embed lua bytecode? [y]/n
LINKLUA        = y

# embed jit.* lua bytecode? [y]/n
LINKJIT        = y

# enable debugging? y/[n]
DEBUG          = n

# ---- Platform & cross-compiler -----------------------------------------------

TARGET         = $(HOST)
CROSS          =

ifneq (,$(findstring Windows,$(OS)))
HOST           = Windows
else
HOST           = $(shell uname -s)
endif

M3_A           = libm3.a
ifeq (Windows,$(TARGET))
M3_EXE         = m3.exe
M3_SO          = m3.dll
else
M3_EXE         = m3
M3_SO          = libm3.so
endif

ifeq (Windows,$(HOST))
HOST_EXE       = .exe
endif

# note: this works only when TARGET=HOST.
TARGET_JITARCH = $(shell $(LUAJIT) -e print\(jit.arch\))

# ---- fhk ---------------------------------------------------------------------

FHKPATH        = fhk5
FHKA           = fhk5/target/release/libfhk.a
FHKSO          = fhk5/target/release/libfhk.so
M3EXE_FHK      = -Wl,--whole-archive $(FHKA) -Wl,--no-whole-archive

# ---- LuaJIT ------------------------------------------------------------------

LJSRC          = LuaJIT/src
LJBINARY       = $(LJSRC)/luajit$(HOST_EXE)
LJPKGPATH      = $(shell $(LJBINARY) -e print\(package.path\));$(LJSRC)/?.lua
LJLUAPATH      = LUA_PATH="$(LJPKGPATH)"
LJINCLUDE      = -ILuaJIT/src
LJA            = $(LJSRC)/libluajit.a
ifeq (Windows,$(TARGET))
LJDLL          = $(LJSRC)/lua51.dll
M3EXE_LUAJIT   = $(LJDLL)
M3SO_LUAJIT    = $(LJDLL)
else
LJDLL          = $(LJSRC)/libluajit.so
M3EXE_LUAJIT   = $(LJA) -lm -ldl
M3SO_LUAJIT    = $(LJDLL)
endif

# ---- Compiler & Linker options -----------------------------------------------

ifneq (y,$(DEBUG))
XCFLAGS        = -Wall -Wextra -O2 -DNDEBUG
else
XCFLAGS        = -Wall -Wextra -O0 -g3 -DM3_DEBUG
endif

ifeq (y,$(DEBUG))
STRIP          = :
endif

ifneq (Windows,$(TARGET))
M3EXE_SYMS     = -Wl,--dynamic-list=dynamic.list
endif

CCOPTIONS      = $(XCFLAGS) $(LJINCLUDE) $(CFLAGS)
LDOPTIONS      = -Wl,--gc-sections $(LDFLAGS)

# ---- Objects -----------------------------------------------------------------

M3EXE_O        = libm3.o m3.o
M3SO_O         = libm3.pic.o
M3A_O          = libm3.o
M3GENLUA       = m3_cdef.lua

ifeq (y,$(LINKLUA))
M3LUA_O        = m3.lua.o m3_access.lua.o m3_api.lua.o m3_array.lua.o m3_cdata.lua.o m3_cdef.lua.o \
				 m3_channel.lua.o m3_control.lua.o m3_data.lua.o m3_data_frame.lua.o \
				 m3_data_query.lua.o m3_data_struct.lua.o m3_debug.lua.o m3_effect.lua.o \
				 m3_fhk.lua.o m3_hook.lua.o m3_ipc.lua.o m3_layout.lua.o m3_loop.lua.o \
				 m3_mem.lua.o m3_mp.lua.o m3_mp_main.lua.o m3_mp_worker.lua.o m3_pipe.lua.o \
				 m3_serial.lua.o m3_shm.lua.o m3_shutdown.lua.o m3_startup.lua.o m3_state.lua.o \
				 m3_tree.lua.o
M3EXELUA_O     = m3_input_data.lua.o m3_input_ndjson.lua.o m3_simulate.lua.o m3_test.lua.o
endif

ifeq (y,$(LINKJIT))
JITLUA_O       = jit_bc.lua.o jit_dump.lua.o jit_p.lua.o jit_v.lua.o jit_vmdef.lua.o jit_zone.lua.o \
				 jit_dis_$(TARGET_JITARCH).lua.o
ifeq (x64,$(TARGET_JITARCH))
JITLUA_O      += jit_dis_x86.lua.o
endif
endif

# ---- Targets -----------------------------------------------------------------

$(M3_EXE): $(M3EXE_O) $(M3LUA_O) $(M3EXELUA_O) $(JITLUA_O) $(M3GENLUA)
	$(CC) $(LDOPTIONS) $(M3EXE_O) $(M3LUA_O) $(M3EXELUA_O) $(JITLUA_O) \
		$(M3EXE_LUAJIT) $(M3EXE_FHK) $(M3EXE_SYMS) \
		-o $@
	$(STRIP) $@

$(M3_SO): $(M3SO_O) $(M3LUA_O) $(M3GENLUA)
	$(CC) $(LDOPTIONS) $(M3SO_O) $(M3LUA_O) $(M3SO_LUAJIT) -shared -o $@
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

jit_%.lua.c: $(LJSRC)/jit/%.lua
	$(LUAJIT) -b -n jit.$* $< $@

m3_cdef.lua: libm3.o
	$(CC) -P -E -nostdinc -DM3_LUADEF libm3.c 2>/dev/null | $(LUAJIT) luadef.lua > $@

loader.c:
	$(BCLOADER) -o $(TARGET) -n m3 -c m3_api -L > $@

CCGITVER = $(shell GITVER=$$(git rev-parse --short HEAD) && echo -DM3_GITVER='\"'$$(echo $$GITVER)'\"')
CCLJVER  = $(shell GITVER=$$(cd $(LJSRC) && git describe) && echo -DLJ_GITVER='\"'$$(echo $$GITVER)'\"')
m3.o: XCFLAGS += $(CCGITVER) $(CCLJVER)

# ---- Auxiliary ------------------------------------------------------------------

.PHONY: dep
dep:
	$(MAKE) clean
	$(CC) -MM *.c > Makefile.dep

.PHONY: clean
clean:
	$(RM) $(M3_EXE) $(M3GENLUA) *.o *.so *.dll *.a *.lua.c

-include Makefile.dep

.SUFFIXES:
