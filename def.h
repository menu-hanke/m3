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

#define MIN(a, b)            ({ typeof(a) _a = (a); typeof(a) _b = (b); _a < _b ? _a : _b; })
#define MAX(a, b)            ({ typeof(a) _a = (a); typeof(a) _b = (b); _a > _b ? _a : _b; })

#ifndef NOAPI
#if __ELF__
#define NOAPI                __attribute__((visibility("hidden"))) extern
#else
#define NOAPI
#endif
#endif

#if M3_WINDOWS
#define LUAFUNC              __declspec(dllexport)
#else
#define LUAFUNC
#endif

#ifdef M3_LUADEF
#define CDEF                 @cdef@
#define LUADEF(...)          @lua@ __VA_ARGS__
#define LUAVOID(x)           void
#define CDEFFUNC             CDEF
#else
#define CDEF
#define LUADEF(...)
#define LUAVOID(x)           x
#define CDEFFUNC             LUAFUNC
#endif

CDEF typedef int32_t m3_MRef32;
CDEF typedef intptr_t m3_MRef;
typedef m3_MRef32 MRef32;
typedef m3_MRef MRef;
#define mrefp(b,r) (((void*)(b)) + (ptrdiff_t)(r))  // mem ref to pointer
#define pmref(b,p) ((intptr_t)(p) - (intptr_t)(b))  // pointer to mem ref

#define VMSIZE_PROC 0x100000000ull
LUADEF(cdef.M3_VMSIZE_DEFAULT = 0x80000000) // must fit in 32 bits
LUADEF(cdef.M3_VMSIZE_PROC = VMSIZE_PROC)

#if M3_VIRTUALALLOC
LUADEF(cdef.M3_MEM_VIRTUALALLOC = true)
#endif
