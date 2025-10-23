#include "def.h"
#include "err.h"
#include "mem.h"

#include <assert.h>
#include <string.h>

#define ARRAY_CAP0 4

CDEF typedef struct m3_DfProto {
	uint16_t num;
	uint8_t align;
#if M3_LUADEF
	uint8_t size[?];
#else
	uint8_t size[];
#endif
} m3_DfProto;

// keep in sync with m3_array.lua
typedef struct {
	uint32_t num;
	uint32_t cap;
	void *col[];
} DfData;

CDEF typedef struct m3_Span {
	uint32_t ofs;
	uint32_t num;
} m3_Span;

static int array_retain_spans(m3_Mem *mem, m3_DfProto *proto, DfData *data,
	m3_Span *spans, uint32_t nspan, uint32_t nremain)
{
	mem->curtmp = 0;
	if (!nspan || !nremain) {
		data->num = data->cap = 0;
		return 0;
	}
	data->num = nremain;
	while ((data->cap>>1) >= nremain)
		data->cap >>= 1;
	size_t cap = data->cap;
	size_t pnum = proto->num;
	for (size_t i=0; i<pnum; i++) {
		size_t size = proto->size[i];
		void *ptr = m3_mem_allocf(mem, cap*size, size);
		if (UNLIKELY(!ptr))
			return -1;
		void *old = data->col[i];
		data->col[i] = ptr;
		for (size_t j=0; j<nspan; j++) {
			size_t n = size*spans[j].num;
			memcpy(ptr, old + size*spans[j].ofs, n);
			ptr += n;
		}
	}
	return 0;
}

// spans must be allocated at the start of the scratch buffer
CFUNC int m3_array_retain_spans(m3_Mem *mem, m3_DfProto *proto, LVOID(DfData) *data,
	uint32_t nremain)
{
	return array_retain_spans(mem, proto, data, mem->tmp, mem->curtmp/sizeof(m3_Span), nremain);
}

// delete bitmap must be allocated at the start of the scratch buffer, with at least one extra
// bit at the end.
CFUNC int m3_array_delete_bitmap(m3_Mem *mem, m3_DfProto *proto, LVOID(DfData) *data)
{
	uint32_t ofs = mem->curtmp;
	assert(!(ofs & -4));
	uint32_t word = 0, bit = 0;
	uint32_t num = data->num;
	uint32_t lastword = (num+1) >> 6;
	uint64_t *delete = mem->tmp;
	delete[lastword] |= (-1ULL) << ((num+1) & 0x3f); // mark tail as deleted
	int64_t w = *delete; // must be signed
	uint32_t j;
	if (w & 1) goto ones;
	for (;;) {
		assert((w & 1) == 0);
		uint32_t start = 64*word + bit;
		while (w == 0) {
			bit = 0;
			w = ((uint64_t *)mem->tmp)[++word];
		}
		j = __builtin_ctzll(w);
		w >>= j;
		bit += j;
		uint32_t n = 64*word + bit - start;
		num -= n;
		m3_Span *span = m3_mem_tmp(mem, sizeof(m3_Span));
		span->ofs = start;
		span->num = n;
ones:
		assert((w & 1) == 1);
		while (w == -1) {
			if (word == lastword) {
				uint32_t nspan = (mem->curtmp - ofs) / sizeof(m3_Span);
				return array_retain_spans(mem, proto, data, mem->tmp + ofs, nspan, num);
			}
			bit = 0;
			w = ((uint64_t *)mem->tmp)[++word];
		}
		j = __builtin_ctzll(~w);
		w >>= j;
		bit += j;
	}
}

static void *array_realloc(m3_Mem *mem, void *ptr, uint32_t oldsize, uint32_t newsize,
	uint32_t align)
{
	void *p = m3_mem_allocf(mem, newsize, align);
	if (p && oldsize)
		memcpy(p, ptr, oldsize);
	return p;
}

CFUNC int m3_array_grow(m3_Mem *mem, m3_DfProto *proto, LVOID(DfData) *data, uint32_t n)
{
	if (!data->cap)
		data->cap = ARRAY_CAP0;
	uint32_t num = data->num;
	data->num += n;
	while (data->cap < data->num)
		data->cap <<= 1;
	size_t cap = data->cap;
	size_t ncol = proto->num;
	uint32_t align = proto->align;
	for (size_t i=0; i<ncol; i++) {
		uint32_t size = proto->size[i];
		void *p = array_realloc(mem, data->col[i], num*size, cap*size, align);
		if (UNLIKELY(!p))
			return -1;
		data->col[i] = p;
	}
	return 0;
}

CFUNC int m3_array_mutate(m3_Mem *mem, m3_DfProto *proto, LVOID(DfData) *data)
{
	size_t num = data->num;
	size_t cap = data->cap;
	size_t ncol = proto->num;
	uint32_t align = proto->align;
	for (size_t i=0; i<ncol; i++) {
		if (!m3_mem_inchunk(mem->framealloc, data->col[i])) {
			uint32_t size = proto->size[i];
			void *p = array_realloc(mem, data->col[i], num*size, cap*size, align);
			if (UNLIKELY(!p))
				return -1;
			data->col[i] = p;
		}
	}
	return 0;
}
