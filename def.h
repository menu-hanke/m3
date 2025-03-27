#pragma once

#include <stddef.h>
#include <stdint.h>

#include "target.h"

#define LIKELY(x)            __builtin_expect(!!(x), 1)
#define UNLIKELY(x)          __builtin_expect(!!(x), 0)
#define AINLINE              __attribute__((always_inline)) inline
#define NOINLINE             __attribute__((noinline))
#define COLD                 __attribute__((cold))
#define NORETURN             __attribute__((noreturn))

#if M3_AMALG
#define M3_HIDDEN            static
#define M3_NOAPI             M3_HIDDEN
#else
#if __ELF__
#define M3_HIDDEN            __attribute__((visibility("hidden")))
#else
#define M3_HIDDEN
#endif
#define M3_NOAPI             M3_HIDDEN extern
#endif


#define M3_FUNC              M3_NOAPI
#define M3_DATA              M3_NOAPI
#if M3_AMALG
#define M3_DATADEF           static
#else
#define M3_DATADEF
#endif

#ifdef M3_LUADEF
#define CDEF                 @cdef@
#define CFUNC                @cfunc@
#define LDEF(...)            @lua@ __VA_ARGS__
#define LVOID(x)             void
#else
#define CDEF
#define CFUNC                M3_FUNC
#define LDEF(...)
#define LVOID(x)             x
#endif
