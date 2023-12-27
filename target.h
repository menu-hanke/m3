#pragma once

#ifdef __x86_64__
#define M3_x86               1
#else
#error "TODO: non-x64"
#endif

#if defined(_WIN32) || defined(_CYGWIN)
#define M3_WINDOWS           1
#else
#define M3_MMAP              1
#endif

#ifdef __linux__
#define M3_LINUX             1
#endif

#define M3_SP64              0
#define M3_CACHELINE_SIZE    64
#define M3_PAGE_SIZE         4096
