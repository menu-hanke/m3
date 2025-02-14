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
