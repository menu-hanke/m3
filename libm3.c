#define _GNU_SOURCE /* for sched_getaffinity() */

#define NOAPI static

#include "def.h"
#include "array.c"
#include "mem.c"
#include "mp.c"
#include "sql.h"

#if M3_LUADEF

CDEF void *malloc(size_t);
CDEF void free(void *);

LUADEF(cdef.errmsg = {)
#define ERRDEF(_, msg) LUADEF(msg),
#include "errmsg.h"
#undef ERRDEF
LUADEF(})

#include "config.h"
#include "target.h"
LUADEF(cdef.CONFIG_MP_PROC_MEMORY = M3_MP_PROC_MEMORY)
LUADEF(cdef.CONFIG_MEM_BLOCKSIZE_MIN = M3_MEM_BLOCKSIZE_MIN)
LUADEF(cdef.TARGET_CACHELINE_SIZE = M3_CACHELINE_SIZE)

#endif
