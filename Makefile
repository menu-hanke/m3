# ---- Configuration ------------------------------------------------------------------

# programs
CROSS          =
CC             = $(CROSS)gcc
AR             = $(CROSS)gcc-ar
STRIP          = $(CROSS)strip
CARGO          = cargo
GIT            = git
LUAJIT         = $(LUAJIT_EXE)

# embed lua bytecode? [y]/n
EMBEDLUA       = y

# embed jit.* lua bytecode? [y]/n
EMBEDJIT       = y

# target system (for cross-compiler)
TARGET_SYS     = $(HOST_SYS)

# ---- Host & target detection -------------------------------------------------

ifneq (,$(findstring Windows,$(OS)))
HOST_SYS       = Windows
else
HOST_SYS       = $(shell uname -s)
endif

TARGET_MACHINE = $(shell $(CC) -dumpmachine)

# ---- Compiler & Linker options -----------------------------------------------

CCOPT          = -O2
CCWARN         = -Wall -Wextra
CCDEBUG        =
M3_CFLAGS      = $(M3_CFLAGS_)
M3_LDFLAGS     = -Wl,--gc-sections

ifeq (,$(CCDEBUG))
M3_CFLAGS     += -DNDEBUG
M3_DEBUG       = 0
else
STRIP          = :
M3_DEBUG       = 1
endif

ifneq (y,$(EMBEDLUA))
M3_CFLAGS     += -DM3_LOADLUA
endif

ifneq (Windows,$(TARGET_SYS))
M3_LDFLAGS    += -Wl,--dynamic-list=dynamic.list
endif

CCOPTIONS      = $(CCOPT) $(CCWARN) $(CCDEBUG) $(LUAJIT_INCLUDE) $(CFLAGS) $(M3_CFLAGS)
LDOPTIONS      = $(M3_LDFLAGS) $(LDFLAGS)

# ---- Files and paths ---------------------------------------------------------

M3_CMOD        = amalg $(SQLITE_ROOT)/sqlite3
M3_LMOD        = m3_array m3_cdata m3_cdef m3_cli m3_code m3_control m3_data m3_db m3_debug m3_eval \
				 m3_host m3_init m3_lib m3_mem
M3_GEN         = bcode.h cdef.c m3_cdef.lua
M3_COBJ        = $(addsuffix $(M3_CEXT).o, $(M3_CMOD))
# top-level make sets:
# M3_CEXT

ifeq (Windows,$(TARGET_SYS))
TARGET_EXE     = .exe
TARGET_DLL     = .dll
else
TARGET_DLL     = .so
endif

M3_EXE         = m3$(TARGET_EXE)
M3_DLL         = m3$(TARGET_DLL)

M3_BCODE       = sqlite=sqlite-lua/sqlite.lua
ifeq (y,$(EMBEDLUA))
M3_BCODE_LUA   = $(addsuffix .lua, $(M3_LMOD))
M3_BCODE      += $(M3_BCODE_LUA)
endif
ifeq (y,$(EMBEDJIT))
M3_BCODE      += jit=$(LUAJIT_SRC)
endif

# ---- fhk ---------------------------------------------------------------------

FHK_ROOT       = fhk5
FHK_A          = $(FHK_ROOT)/target/release/libfhk.a
ifeq (Windows,$(TARGET_SYS))
FHK_DLL        = $(FHK_ROOT)/target/release/fhk.dll
M3EXE_FHK      = $(FHK_DLL)
else
FHK_DLL        = $(FHK_ROOT)/target/release/libfhk.so
M3EXE_FHK      = -Wl,--whole-archive $(FHK_A) -Wl,--no-whole-archive
endif

M3DLL_FHK      = $(M3EXE_FHK)

# ---- LuaJIT ------------------------------------------------------------------

LUAJIT_ROOT    = LuaJIT
LUAJIT_SRC     = $(LUAJIT_ROOT)/src
LUAJIT_A       = $(LUAJIT_SRC)/libluajit.a
LUAJIT_EXE     = $(LUAJIT_SRC)/luajit$(TARGET_EXE)
LUAJIT_INCLUDE = -I$(LUAJIT_SRC)
ifeq (Windows,$(TARGET_SYS))
LUAJIT_DLL     = $(LUAJIT_SRC)/lua51.dll
M3EXE_LUAJIT   = $(LUAJIT_DLL)
M3DLL_LUAJIT   = $(LUAJIT_DLL)
else
LUAJIT_DLL     = $(LUAJIT_SRC)/libluajit.so
M3EXE_LUAJIT   = $(LUAJIT_A) -lm -ldl
M3DLL_LUAJIT   =
endif

# ---- SQLite ------------------------------------------------------------------

SQLITE_ROOT    = sqlite
SQLITE_INCLUDE = -I$(SQLITE_ROOT)
SQLITE_CFLAGS  = -DSQLITE_CUSTOM_INCLUDE=../sql.h

# ---- Targets -----------------------------------------------------------------

all: exe lib
exe: $(M3_EXE)
lib: $(M3_DLL)

.PHONY: amalg all exe lib

# ---- Rules -------------------------------------------------------------------

ifeq (1,$(M3_MAKEREC))
$(M3_EXE): $(M3_COBJ) $(M3_GEN)
	$(CC) $(LDOPTIONS) $(M3_COBJ) $(M3EXE_FHK) $(M3EXE_LUAJIT) -o $@
	$(STRIP) $@
$(M3_DLL): $(M3_COBJ) $(M3_GEN)
	$(CC) $(LDOPTIONS) $(M3_COBJ) $(M3DLL_FHK) $(M3DLL_LUAJIT) -shared -o $@
	$(STRIP) $@
else
$(M3_EXE): $(M3_GEN)
	$(MAKE) $@ M3_MAKEREC=1 M3_CMOD="$(M3_CMOD) m3"
$(M3_DLL): $(M3_GEN)
	$(MAKE) $@ M3_MAKEREC=1 M3_CEXT=_dll M3_CFLAGS_=-fPIC
.PHONY: $(M3_EXE) $(M3_DLL)
endif

%$(M3_CEXT).o: %.c
	$(CC) $(CCOPTIONS) -c $< -o $@

$(SQLITE_ROOT)/sqlite3$(M3_CEXT).o: M3_CFLAGS += $(SQLITE_CFLAGS)

bcode.h: $(M3_BCODE_LUA)
	$(LUAJIT) build.lua bcode $(TARGET_MACHINE) $(M3_DEBUG) $(M3_BCODE) > $@

# these are here because cc -MM doesn't like missing files
amalg$(M3_CEXT).o bc$(M3_CEXT).o: bcode.h

M3_GITVER = $(shell $(GIT) describe)
m3_cdef.lua cdef.c &:
	$(CC) -P -E -nostdinc -DM3_LUADEF amalg.c 2>/dev/null | $(LUAJIT) build.lua cdef - $(M3_GITVER)

# ---- Dependencies ------------------------------------------------------------

deps-fhk:
	cd $(FHK_ROOT) && cargo build --release

deps-luajit:
	$(MAKE) -C $(LUAJIT_ROOT) amalg

deps-sqlite:
	cd $(SQLITE_ROOT) && ./configure && $(MAKE) sqlite3.c

deps: deps-fhk deps-luajit deps-sqlite
.PHONY: deps deps-fhk deps-luajit deps-sqlite

# ---- Auxiliary ------------------------------------------------------------------

depend:
	$(MAKE) clean
	$(CC) -DM3_MAKEDEP -MM *.c | sed 's/^amalg.o/m3_cdef.lua cdef.c amalg.o/; s/\.o:/$$(M3_CEXT).o:/' > Makefile.dep

clean:
	$(RM) $(M3_EXE) $(M3_GEN) *.o *.so *.dll *.a

.PHONY: depend clean

-include Makefile.dep

.SUFFIXES:
