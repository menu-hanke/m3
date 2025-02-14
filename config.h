#pragma once

#include "target.h"

// size of shared virtual memory mapping in multiprocess mode (per process).
// this must be a power of two.
// virtual memory is committed lazily, so you can put a huge number here.
#define M3_MP_PROC_MEMORY              0x100000000ull

// minimum work memory block size.
// the actual block size is the smallest integer multiple of this number that makes the whole
// work memory fit into 64 blocks.
// this should be a reasonably small multiple of cache line size (eg. <1024)
// increasing this makes each savepoint write do more work, but results in fewer writes overall.
#define M3_MEM_BLOCKSIZE_MIN           M3_CACHELINE_SIZE

// minimum memory chunk size.
// increasing this may save some allocations, but also may increase memory usage.
// this should be a multiple of page size.
#define M3_MEM_CHUNKSIZE_MIN           M3_PAGE_SIZE

// attempt to free unused chunks every `n` allocations
#define M3_MEM_SWEEP_INTERVAL          10
