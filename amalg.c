// for sched_getaffinity:
#define _GNU_SOURCE

#define M3_AMALG 1

#include "def.h"

#include "array.c"
#include "bc.c"
#include "env.c"
#include "mem.c"
#include "sys.c"
#include "host.c"
#include "mp.c"

#include "sql.h"

#ifdef M3_LUADEF

LDEF(local errmsg = {)
#define ERRDEF(_, msg) LDEF(msg),
#include "errmsg.h"
#undef ERRDEF
LDEF(})
LDEF(_.errmsg = errmsg)
LDEF(function _.check(x) if x ~= 0 then error(errmsg[x], 2) end end)

#include "target.h"
#include "config.h"

LDEF(_.CONFIG_MP_PROC_MEMORY = M3_MP_PROC_MEMORY)
LDEF(_.CONFIG_MEM_BLOCKSIZE_MIN = M3_MEM_BLOCKSIZE_MIN)
LDEF(_.TARGET_CACHELINE_SIZE = M3_CACHELINE_SIZE)

#endif

#if !defined(M3_LUADEF) && !defined(M3_MAKEDEP)
#include "cdef.c"
#endif
