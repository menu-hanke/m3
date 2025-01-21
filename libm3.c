#define _GNU_SOURCE /* for sched_getaffinity() */

#define NOAPI static

#include "def.h"
#include "mem.c"
#include "mp.c"
#include "sql.h"

#if M3_LUADEF
CDEF void *malloc(size_t);
CDEF void free(void *);
#endif
