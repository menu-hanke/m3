#include "def.h"
#include "target.h"

#include <stdlib.h>
#include <string.h>

/* ---- Virtual memory ------------------------------------------------------ */

#if M3_LINUX

#include <sys/mman.h>

CDEFFUNC void *m3__mem_map_shared(size_t size)
{
	return mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0);
}

CDEFFUNC void *m3__mem_map_stack(size_t size)
{
	void *map = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE,
		-1, 0);
	madvise(map, size, MADV_DONTDUMP);
	return map;
}

CDEFFUNC void m3__mem_unmap(void *base, size_t size)
{
	munmap(base, size);
}

#endif

/* ---- Stack/arena allocation ---------------------------------------------- */

CDEF typedef struct m3_Stack {
	intptr_t cursor;
	intptr_t base;
	intptr_t top;
#if M3_VIRTUALALLOC
	intptr_t bottom;
#endif
} m3_Stack;

AINLINE static void mem_align(m3_Stack *stack, size_t align)
{
	stack->cursor &= -align;
}

AINLINE static void mem_bump(m3_Stack *stack, size_t size)
{
	stack->cursor -= size;
}

AINLINE static int mem_check(m3_Stack *stack)
{
	if (UNLIKELY(stack->cursor < stack->base)) {
#if M3_VIRTUALALLOC
#error "TODO"
#endif
		return 1;
	} else {
		return 0;
	}
}

CDEFFUNC void *m3__mem_extalloc(m3_Stack *stack, size_t size, size_t align)
{
	intptr_t p = stack->cursor;
	mem_bump(stack, size);
	mem_align(stack, align);
	if (UNLIKELY(mem_check(stack))) {
		stack->cursor = p;
		return NULL;
	} else {
		return (void *) stack->cursor;
	}
}

/* ---- Savepoint handling -------------------------------------------------- */

/*
 * Stack layout:
 *
 *                 +----------------------+       ss->base
 *--------+        |                      |          |
 *        |        v                      |          v
 *-----+--|---+------+-----------+-----+--|---+------+-----------+-----+
 * ... | link | mask | heap save | ... | link | mask | heap save | ... |
 *-----+------+------+-----------+-----+------+------+-----------+-----+
 *                   ^                               ^                 ^
 *                   |                               |                 |
 *                savepoint                       savepoint           stack
 *
 * each mask lists CLEAN blocks, ie. a bit corresponding to a heap block is SET
 * if the block is NOT written, and UNSET if the block IS written.
 *
 */

// maximum number of blocks (1-64)
#define MEM_HEAPBMAX 64

// minimum block size (power of 2)
#define MEM_BSIZEMIN 64

LUADEF(cdef.M3_MEM_HEAPBMAX = MEM_HEAPBMAX);
LUADEF(cdef.M3_MEM_BSIZEMIN = MEM_BSIZEMIN);

CDEF typedef struct m3_SaveState {
	void *heap;
	intptr_t base;
	uint64_t mask; // same as *((uint64_t *)base-1)
	uint32_t blocksize;
	m3_Stack stack;
} m3_SaveState;

static void mem_copyblock(void *dst, void *src, void *end)
{
	do {
		memcpy(dst, src, MEM_BSIZEMIN);
		dst += MEM_BSIZEMIN;
		src += MEM_BSIZEMIN;
	} while (src < end);
}

CDEFFUNC void m3__mem_setmask(m3_SaveState *ss, uint64_t mask)
{
	void *heap = ss->heap;
	void *base = (void *) ss->base;
	ptrdiff_t blocksize = ss->blocksize;
	ss->mask &= ~mask;
	for (;;) {
		uint64_t fmask = *((uint64_t *)base-1);
		uint64_t need = fmask & mask;
		if (!need) return;
		*((uint64_t *)base-1) &= ~mask;
		do {
			ptrdiff_t idx = __builtin_ctzll(need);
			need &= need-1;
			ptrdiff_t ofs = idx*blocksize;
			mem_copyblock(base + ofs, heap + ofs, heap + ofs + blocksize);
		} while(need);
		base = *((void **)base-2);
	}
}

CDEFFUNC void m3__mem_load(m3_SaveState *ss, intptr_t base)
{
	ss->base = base;
	ss->stack.cursor = (intptr_t)base-16;
	uint64_t mask = *((uint64_t *)base-1);
	ss->mask = mask;
	ptrdiff_t blocksize = ss->blocksize;
	void *heap = ss->heap;
	mask = ~mask;
	while (mask) {
		ptrdiff_t idx = __builtin_ctzll(mask);
		mask &= mask-1;
		ptrdiff_t ofs = idx*blocksize;
		mem_copyblock(heap + ofs, (void *)base + ofs, (void *)base + ofs + blocksize);
	}
}

/* ---- Vector manipulation ------------------------------------------------- */

// keep in sync with m3_array.lua
typedef struct {
	uint32_t ofs;
	uint32_t num;
} CopySpan;

CDEFFUNC int32_t m3__mem_buildcopylist(m3_Stack *stack, size_t num)
{
	uint64_t *bitmap = (uint64_t *) stack->cursor;
	// mark one past end as skip so that the tail is also handled by the loop
	bitmap[num>>6] |= 1ULL << (num & 0x3f);
	uint32_t start = 0;
	uint32_t newnum = 0;
	for (uint32_t base=0; base<num; base+=64) {
		uint64_t word = *bitmap++;
		uint32_t ofs = base;
		while (word) {
			uint32_t bit = __builtin_ctzll(word);
			if (LIKELY(ofs+bit > start)) {
				mem_bump(stack, sizeof(CopySpan));
				if (UNLIKELY(mem_check(stack))) return -1;
				CopySpan *c = (CopySpan *) stack->cursor;
				c->ofs = start;
				c->num = ofs+bit - start;
				newnum += c->num;
			}
			// shift by +1 here so that ~word is guaranteed to be nonzero
			word >>= bit + 1;
			uint32_t skip = __builtin_ctzll(~word);
			ofs += bit+1+skip;
			start = ofs;
			word >>= skip;
		}
	}
	return newnum;
}

CDEF typedef struct m3_DfProto {
	uint16_t num;
	uint8_t align;
	uint8_t size[];
} m3_DfProto;

// the point of this union is just to ensure that the VLA can fit (at least) one element.
typedef union {
	uint32_t u32;
	m3_DfProto proto;
} DfProto1;

// keep in sync with m3_array.lua
typedef struct {
	uint32_t num;
	uint32_t cap;
	void *col[];
} DfData;

CDEFFUNC int m3__mem_copy_list(
	m3_Stack *stack,
	intptr_t clist,
	size_t ncopy,
	m3_DfProto *proto,
	LUAVOID(DfData) *data
) {
	mem_align(stack, proto->align);
	size_t num = proto->num;
	CopySpan *cs = (CopySpan *) clist + ncopy;
	size_t cap = data->cap;
	for (size_t i=0; i<num; i++) {
		size_t size = proto->size[i];
		mem_bump(stack, cap*size);
		if (UNLIKELY(mem_check(stack)))
			return -1;
		void *dt = data->col[i];
		void *ptr = (void *) stack->cursor;
		data->col[i] = ptr;
		for (ssize_t j=-1; j>=-(ssize_t)ncopy; j--) {
			size_t n = size*cs[j].num;
			memcpy(ptr, dt + size*cs[j].ofs, n);
			ptr += n;
		}
	}
	return 0;
}

CDEFFUNC int m3__mem_copy_list1(
	m3_Stack *stack,
	intptr_t clist,
	size_t ncopy,
	size_t size,
	size_t align,
	LUAVOID(DfData) *data
) {
	DfProto1 p;
	p.proto.num = 1;
	p.proto.align = align;
	p.proto.size[0] = size;
	return m3__mem_copy_list(stack, clist, ncopy, &p.proto, data);
}
