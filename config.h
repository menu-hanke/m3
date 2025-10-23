#pragma once

#include "target.h"

// enable threading support?
// this causes sqlite to be with threading enabled which builds a slightly larger and slower binary.
#ifndef M3_USE_THREADS
#define M3_USE_THREADS                 0
#endif

// size of shared virtual memory mapping in multiprocess mode (per process).
// this must be a power of two.
// virtual memory is committed lazily, so you can put a huge number here.
#define M3_MP_PROC_MEMORY              0x100000000ull

// size of (all but last) work blocks.
// this should be chosen to balance the copying overhead per save/load, and the
// frequency of m3_mem_write() calls from lua.
// an aligned copy of the default 512 bytes takes 8 cycles on zen 5, or 16 on zen 4 and earlier.
// smaller or larger values might work better depending on your specific workload and cpu.
// in practice, you're unlikely to notice any difference unless
// (a) you're creating hundreds of millions of savepoints per second; or
// (b) your work memory size overflows 64 blocks (64*512 = 32KB, or 4K double/pointer variables)
// #define M3_CONFIG_BLOCKSIZE            512
#define M3_CONFIG_BLOCKSIZE            64

// size of initial frame memory chunk.
// this should be a multiple of page size, otherwise you waste some memory.
#define M3_CONFIG_CHUNKSIZE            M3_PAGE_SIZE
