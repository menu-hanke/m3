#include "def.h"
#include "target.h"

#include <assert.h>
#include <stdalign.h>
#include <stddef.h>
#include <stdint.h>

#if M3_x86
#include <x86intrin.h>
#endif

#ifdef __AVX__
#define MEM_ALIGN_COPY 32
#define MEM_SIMD       1
#elifdef __SSE__
#define MEM_ALIGN_COPY 16
#define MEM_SIMD       1
#else
#define MEM_ALIGN_COPY 1
#define MEM_SIMD       0
#endif

/* must match mem_fastcpy & mem_fastzero */
#if MEM_SIMD
#define MEM_EXTRA      63
#else
#define MEM_EXTRA      0
#endif

#if M3_SP64
CDEF typedef int64_t m3_Savepoint;
#else
CDEF typedef int32_t m3_Savepoint;
#endif

// NOTE: use a signed `intptr_t` here: bit.band(p, -mask) = 0 if p is unsigned.
CDEF typedef struct m3_RegionState {
	intptr_t cursor;         // smallest allocated address
#if M3_WINDOWS
	intptr_t commit;         // smallest committed address
#endif
} m3_RegionState;

/*
 * [ vstack memory ] . [ frame memory ]
 *                   ^
 *                   |
 *                   m3_MemState *
 */
CDEF typedef struct m3_MemState {
	m3_RegionState v;        // vstack
	m3_RegionState f;        // frame
	m3_RegionState x;        // scratch memory
	intptr_t fbase;          // current frame start
} m3_MemState;

#define MEM_ALIGN_SP   MAX(MEM_ALIGN_COPY, alignof(m3_Savepoint))

/* ---- Memory copies ------------------------------------------------------- */

/*
 * backwards overwriting memcpy.
 * NOTE: always check the assembly of m3__save/m3__load if you modify this function.
 */
AINLINE
static void mem_fastcpy_inline(void *dst, void *src, void *dstend)
{
#ifdef __AVX__
	do {
		__m256 ymm0 = _mm256_loadu_ps(src-32);
		__m256 ymm1 = _mm256_loadu_ps(src-64);
		_mm256_storeu_ps(dst-32, ymm0);
		_mm256_storeu_ps(dst-64, ymm1);
		dst -= 64;
		src -= 64;
	} while((intptr_t)dst > (intptr_t)dstend);
#elifdef __SSE__
	do {
		__m128 xmm0 = _mm_loadu_ps(src-16);
		__m128 xmm1 = _mm_loadu_ps(src-32);
		_mm_storeu_ps(dst-16, xmm0);
		_mm_storeu_ps(dst-32, xmm1);
		__m128 xmm2 = _mm_loadu_ps(src-48);
		__m128 xmm3 = _mm_loadu_ps(src-64);
		_mm_storeu_ps(dst-48, xmm2);
		_mm_storeu_ps(dst-64, xmm3);
		dst -= 64;
		src -= 64;
	} while((intptr_t)dst > (intptr_t)dstend);
#else
	size_t size = (intptr_t)dst - (intptr_t)dstend;
	memcpy(dstend, src-size, size);
#endif
}

static void mem_fastcpy(void *dst, void *src, void *dstend)
{
	mem_fastcpy_inline(dst, src, dstend);
}

static void mem_fastcpyAA(void *dst, void *src, void *dstend)
{
	mem_fastcpy_inline(
			__builtin_assume_aligned(dst, MEM_ALIGN_COPY),
			__builtin_assume_aligned(src, MEM_ALIGN_COPY),
			dstend
	);
}

/* backwards overwriting memset to zero */
static void mem_fastzero(void *ptr, void *end)
{
#ifdef __AVX__
	__m256 ymm0 = _mm256_setzero_ps();
	do {
		_mm256_storeu_ps(ptr-32, ymm0);
		_mm256_storeu_ps(ptr-64, ymm0);
		ptr -= 64;
	} while((intptr_t)ptr > (intptr_t)end);
#elifdef __SSE__
	__m128 xmm0 = _mm_setzero_ps();
	do {
		_mm_storeu_ps(ptr-16, xmm0);
		_mm_storeu_ps(ptr-32, xmm0);
		_mm_storeu_ps(ptr-48, xmm0);
		_mm_storeu_ps(ptr-64, xmm0);
		ptr -= 64;
	} while((intptr_t)ptr > (intptr_t)end);
#else
	memset(end, 0, (intptr_t)ptr - (intptr_t)end);
#endif
}

/* ---- Memory (re-)allocation ---------------------------------------------- */

CDEFFUNC void *m3__mem_realloc(m3_RegionState *reg, uintptr_t base, void *src,
	size_t oldsize, size_t newsize, size_t align)
{
	uintptr_t p = reg->cursor;
	p -= newsize;
	p &= -align;
	if (UNLIKELY(p < base))
		return NULL;
	reg->cursor = p;
	mem_fastcpy((void *) (p+oldsize), src+oldsize, (void *) p);
	return (void *) p;
}

CDEFFUNC uint32_t *m3__mem_skiplist_build(uint32_t *tx, size_t nskip, size_t num)
{
	assert(nskip > 0);
	uint64_t *bitmap = (uint64_t *) (((intptr_t)tx - ((num+63)>>3)) & -8);
	mem_fastzero(tx, bitmap);
	size_t i = 0;
	do {
		uint32_t idx = *tx++;
		bitmap[idx>>6] |= 1ULL << (idx & 63);
	} while(++i < nskip);
	uint32_t *iv = (uint32_t *) bitmap;
	*--iv = 0;
	uint64_t c = 0;
	uint64_t off = 0;
	uint32_t nsk = 0;
	do {
		uint64_t w = *bitmap++;
		nsk += __builtin_popcountll(w);
		uint64_t s = (int64_t)w < 0;
		w ^= w << 1;
		w ^= c;
		c = s;
		while(w) {
			*--iv = off + __builtin_ctzll(w);
			w &= w-1;
		}
		off += 64;
	} while(off < num);
	if(UNLIKELY(c || *iv >= num))
		iv++;
	else
		*--iv = num;
	*--iv = num-nsk;
	return iv;
}

CDEFFUNC void *m3__mem_skiplist_realloc(m3_MemState *mem, void *src,
		size_t elsize, size_t num, size_t align, uint32_t *iv)
{
	uint32_t ncp = *iv++;
	intptr_t p = mem->f.cursor;
	p -= num*elsize;
	p &= -align;
	if (UNLIKELY(p < (intptr_t)(mem+1)))
		return NULL;
	mem->f.cursor = p;
	void *d = (void*)p + ncp*elsize;
	for(;;) {
		uint32_t end = *iv++;
		if(!end) break;
		uint32_t start = *iv++;
		size_t size = (end-start)*elsize;
		mem_fastcpy(d, src+end*elsize, d-size);
		if(!start) break;
		d -= size;
	}
	return (void *) p;
}

/* ---- External allocator -------------------------------------------------- */

CDEFFUNC void* m3__mem_extalloc(m3_RegionState *reg, size_t size, size_t align)
{
	intptr_t p = reg->cursor;
	p -= size;
	p &= -align;
	// TODO: check if p is still in the region here.
	// (can include base in the userdata struct etc.
	//  or use guard pages.
	//  or compute it from the address.)
	reg->cursor = p;
	return (void *) p;
}

/* ---- Savepoint handling -------------------------------------------------- */

CDEFFUNC m3_Savepoint m3__save(m3_MemState *mem)
{
	intptr_t f = mem->f.cursor;
	intptr_t v = mem->v.cursor;
	f -= sizeof(m3_Savepoint);
	f &= -MEM_ALIGN_SP;
	intptr_t ofs = (intptr_t)mem - (intptr_t)v;
	intptr_t fofs = f - ofs;
	if (UNLIKELY(fofs < (intptr_t)(mem+1)+64)) {
		// TODO: set error
		return -1;
	}
	*(m3_Savepoint *)f = M3_SP64 ? v : ofs;
	mem->f.cursor = fofs;
	mem->fbase = fofs;
	mem_fastcpyAA((void *) f, mem, (void *) fofs);
	return M3_SP64 ? f : (f - (intptr_t)mem);
}

CDEFFUNC void m3__load(m3_MemState *mem, m3_Savepoint sp)
{
#if M3_SP64
	intptr_t f = sp;
	intptr_t v = *(m3_Savepoint *)f;
	intptr_t ofs = (intptr_t)exec - v;
#else
	intptr_t f = (intptr_t)mem + sp;
	intptr_t ofs = *(m3_Savepoint *)f;
	intptr_t v = (intptr_t)mem - ofs;
#endif
	intptr_t fofs = f - ofs;
	mem->v.cursor = v;
	mem->f.cursor = fofs;
	mem->fbase = fofs;
	mem_fastcpyAA(mem, (void *)f, (void *)v);
}
