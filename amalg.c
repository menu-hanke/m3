// for sched_getaffinity:
#define _GNU_SOURCE

#define M3_AMALG 1

#include "def.h"

#include "array.c"
#include "bc.c"
#include "env.c"
#include "err.c"
#include "mem.c"
#include "sys.c"
#include "host.c"
#include "mp.c"

#include "sql.h"

#ifdef M3_LUADEF

LDEF(local global_err = ffi.gc(ffi.new("m3_Err"), _.m3_err_clear))
LDEF(_.err = global_err)
LDEF(function _.check(x) if x ~= 0 then error(global_err.ep ~= nil and ffi.string(global_err.ep) or nil) end end)

#include "target.h"
#include "config.h"

LDEF(_.CONFIG_MP_PROC_MEMORY = M3_MP_PROC_MEMORY)
LDEF(_.CONFIG_MEM_BLOCKSIZE_MIN = M3_MEM_BLOCKSIZE_MIN)
LDEF(_.TARGET_CACHELINE_SIZE = M3_CACHELINE_SIZE)

#endif

#if !defined(M3_LUADEF) && !defined(M3_MAKEDEP)
#include "cdef.c"
#endif
