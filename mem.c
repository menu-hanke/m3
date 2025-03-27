#include "config.h"
#include "def.h"
#include "err.h"
#include "mem.h"
#include "target.h"

#include <assert.h>
#include <stdalign.h>
#include <stdlib.h>
#include <string.h>

#define MEM_VECSIZE0 16
#define FRAME_OBJS   1
#define FRAME_ALIVE  2
#define FRAME_CHILD  4

typedef struct ChunkMetadata {
	struct ChunkMetadata *prev;
	uint32_t size;
} ChunkMetadata;

#define chunk_base(meta) ((void *)(meta) + sizeof(ChunkMetadata) - (meta)->size)

static void *mem_vec_grow(m3_Vec *vec, uint32_t size)
{
	uint32_t cap = vec->cap;
	if (!cap) cap = MEM_VECSIZE0;
	while (cap < vec->len+size) cap <<= 1;
	vec->data = vec->cap ? realloc(vec->data, cap) : malloc(cap);
	vec->cap = cap;
	void *ptr = vec->data + vec->len;
	vec->len += size;
	return ptr;
}

void *m3_mem_vec_alloc(m3_Vec *vec, uint32_t size)
{
	if (UNLIKELY(vec->len+size > vec->cap))
		return mem_vec_grow(vec, size);
	void *ptr = vec->data + vec->len;
	vec->len += size;
	return ptr;
}

#if M3_MMAP

#include <sys/mman.h>

static int mem_mmap(void **map, size_t size, int flags)
{
	void *p = mmap(NULL, size, PROT_READ|PROT_WRITE, flags, -1, 0);
	if (p == MAP_FAILED) {
		return M3_ERR_MMAP;
	} else {
		*map = p;
		madvise(p, size, MADV_DONTDUMP);
		return M3_OK;
	}
}

CFUNC int m3_mem_map_shared(size_t size, void **map)
{
	return mem_mmap(map, size, MAP_SHARED|MAP_ANONYMOUS|MAP_NORESERVE);
}

CFUNC void m3_mem_unmap(void *base, size_t size)
{
	munmap(base, size);
}

static int mem_chunk_map(void **base, size_t size)
{
	return mem_mmap(base, size, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE);
}

static void mem_chunk_unmap(ChunkMetadata *meta)
{
	munmap(chunk_base(meta), meta->size);
}

#elif M3_VIRTUALALLOC

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

static int mem_chunk_map(void **base, size_t size)
{
	void *p = VirtualAlloc(NULL, size, MEM_RESERVE|MEM_COMMIT, PAGE_READWRITE);
	if (p) {
		*base = p;
		return M3_OK;
	} else {
		return M3_ERR_MMAP;
	}
}

static void mem_chunk_unmap(ChunkMetadata *meta)
{
	VirtualFree(chunk_base(meta), 0, MEM_RELEASE);
}

#endif

static void mem_chunk_unmap_chain(ChunkMetadata *meta)
{
	while (meta) {
		ChunkMetadata *prev = meta->prev;
		mem_chunk_unmap(meta);
		meta = prev;
	}
}

static void mem_chunk_sweep(m3_Mem *mem)
{
	(void)mem;
	// TODO: check every frame, dead frames can be swept immediately, for non-dead frames put
	// non-primary chunks in a work list and store current frame->chunk, these can be freed
	// when frame->chunk changes.
}

NOINLINE
CFUNC int m3_mem_chunk_new(m3_Mem *mem, uint32_t need)
{
	if (UNLIKELY(!mem->sweep)) {
		mem_chunk_sweep(mem);
		mem->sweep = M3_MEM_SWEEP_INTERVAL;
	} else {
		mem->sweep--;
	}
	ChunkMetadata *prev = mem->chunk ? (mem->chunk+mem->chunktop) : NULL;
	size_t size = prev ? (prev->size<<1) : M3_MEM_CHUNKSIZE_MIN;
	while (size < need+sizeof(ChunkMetadata))
		size <<= 1;
	int err;
	if (UNLIKELY((err = mem_chunk_map(&mem->chunk, size))))
		return err;
	mem->chunktop = size - sizeof(ChunkMetadata);
	mem->cursor = mem->chunktop;
	ChunkMetadata *meta = mem->chunk + mem->chunktop;
	meta->prev = prev;
	meta->size = size;
	return M3_OK;
}

CFUNC void *m3_mem_alloc(m3_Mem *mem, size_t size, size_t align)
{
	if (UNLIKELY(size > mem->cursor)) {
		if (UNLIKELY(m3_mem_chunk_new(mem, size)))
			return NULL;
	}
	mem->cursor -= size;
	mem->cursor &= -align;
	return mem->chunk + mem->cursor;
}

int m3_mem_alloc_bump(m3_Mem *mem, uint32_t size)
{
	if (UNLIKELY(size > mem->cursor)) {
		int err;
		if (UNLIKELY((err = m3_mem_chunk_new(mem, size))))
			return err;
	}
	mem->cursor -= size;
	return M3_OK;
}

static void mem_ftab_grow(m3_Mem *mem)
{
	FrameId maxframe = mem->maxframe ? (mem->maxframe<<1) : MEM_VECSIZE0;
	mem->ftab = realloc(mem->ftab, maxframe*sizeof(*mem->ftab));
	memset(mem->ftab + mem->maxframe, 0, (maxframe-mem->maxframe)*sizeof(*mem->ftab));
	mem->fblock = realloc(mem->fblock, maxframe*mem->wnum*sizeof(*mem->fblock));
	mem->fobj = realloc(mem->fobj, maxframe*sizeof(*mem->fobj));
	void *fwork0 = malloc(maxframe*mem->wnum*mem->bsize + M3_CACHELINE_SIZE-1);
	void *fwork = (void *) (((intptr_t)fwork0 + M3_CACHELINE_SIZE-1) & -M3_CACHELINE_SIZE);
	if (mem->fwork)
		memcpy(fwork, mem->fwork, mem->maxframe*mem->wnum*mem->bsize);
	free(mem->fwork0);
	mem->fwork0 = fwork0;
	mem->fwork = fwork;
	mem->maxframe = maxframe;
}

static void mem_assert_fresh_frame_invariants(m3_Mem *mem)
{
	(void)mem;
	assert(mem->unsaved == ~mem->ftab[mem->frame].save);
	assert((uint32_t)mem->lfreen == mem->lfree.len);
	assert(mem->cursor == mem->chunktop);
	assert(mem->diff == 0);
}

static void mem_save_objlist(m3_Mem *mem, size_t fp)
{
	ObjList *old = mem->fobj[fp];
	size_t lfree0 = mem->lfreen;
	size_t lfreen = mem->lfree.len;
	if (lfree0 < lfreen) {
		size_t size = lfreen - lfree0;
		mem->lfree.len = lfree0;
		// this alloc could technically fail, and if it does, just ignore it.
		// the worst that can happen is we just leak more memory (the object ids).
		ObjList *objs = m3_mem_alloc(mem, sizeof(*objs)+size, alignof(*objs));
		if (LIKELY(objs)) {
			mem->fobj[fp] = objs;
			objs->size = size;
			memcpy(&objs->id, mem->lfree.data+lfree0, size);
		}
	}
	if (mem->ftab[fp].state) {
		assert(mem->ftab[fp].state == FRAME_OBJS);
		void *dest = m3_mem_vec_alloc(&mem->lfree, old->size);
		mem->lfreen = mem->lfree.len;
		memcpy(dest, &old->id, old->size);
	}
}

CFUNC int m3_mem_save(m3_Mem *mem)
{
	size_t id = mem->frame+1;
	FrameState newstate = (uint32_t)mem->lfreen < mem->lfree.len;
	for (;;id++) {
		FrameState state;
		if (UNLIKELY(id >= mem->maxframe)) {
			mem_ftab_grow(mem);
			state = 0;
			goto found;
		}
		state = mem->ftab[id].state;
		if (LIKELY(state < FRAME_ALIVE)) {
found:
			if (UNLIKELY(state|newstate))
				mem_save_objlist(mem, id);
			break;
		}
	}
	FrameId prev = mem->frame;
	mem->ftab[prev].state += FRAME_CHILD;
	m3_Frame *frame = &mem->ftab[id];
	void *chunk = frame->chunk;
	uint32_t chunktop = frame->chunktop;
	frame->state = FRAME_ALIVE | newstate;
	frame->chunk = mem->chunk;
	frame->chunktop = mem->chunktop;
	frame->diff = mem->diff;
	frame->prev = prev;
	frame->save = 0;
	mem->chunk = chunk;
	mem->chunktop = chunktop;
	mem->cursor = chunktop;
	mem->frame = id;
	mem->diff = 0;
	mem->unsaved = ~0ULL;
	// TODO: should this eagerly copy (save & ~diff) from previous frame?
	// this would reduce calls to m3_mem_write() with the expense of an extra loop here
	mem_assert_fresh_frame_invariants(mem);
	return id;
}

AINLINE static void mem_copyblock(void *dst, void *src, size_t bsize)
{
	assert(!((intptr_t)dst & (M3_CACHELINE_SIZE-1)));
	assert(!((intptr_t)src & (M3_CACHELINE_SIZE-1)));
	struct __attribute__((aligned(M3_CACHELINE_SIZE))) {
		uint8_t _[M3_MEM_BLOCKSIZE_MIN];
	} *d = dst, *s = src;
	size_t n = 0;
	for (;;) {
		*d++ = *s++;
		n += M3_MEM_BLOCKSIZE_MIN;
		if (n >= bsize) break;
	}
}

AINLINE static void mem_restore(m3_Mem *mem, FrameId *bfp, Mask mask)
{
	size_t bsize = mem->bsize;
	void *work = mem->work;
	size_t wsize = mem->wnum*bsize;
	void *fwork = mem->fwork;
	for (; mask; mask&=mask-1) {
		size_t idx = __builtin_ctzll(mask);
		assert(mem->ftab[bfp[idx]].save & (1 << idx));
		size_t ofs = idx*bsize;
		mem_copyblock(work + ofs, fwork + bfp[idx]*wsize + ofs, bsize);
	}
}

static void mem_copyframeptr(FrameId *dest, FrameId *src, Mask mask)
{
	for (; mask; mask&=mask-1) {
		size_t idx = __builtin_ctzll(mask);
		dest[idx] = src[idx];
	}
}

static void mem_frame_store(m3_Mem *mem, FrameId fp, Mask mask)
{
	m3_Frame *ftab = mem->ftab;
	assert(mask && !(mask & ftab[fp].save));
	ftab[fp].save |= mask;
	Mask diff = mask & ftab[fp].diff;
	size_t wnum = mem->wnum;
	FrameId *fblock = mem->fblock + wnum*fp;
	// maintain invariant: (child.save ∪  child.diff) ⊂  parent.save,
	// however, we *don't* maintain child.diff ⊂  child.save here. that is done in the slow path
	// of m3_mem_load.
	// for each block `b` in `mask`, we have two cases:
	//   (1) b ∈ diff: by the invariant, we have b ∈ parent.save, so we create a new copy in this
	//       frame
	//   (2) b ∉ diff: we don't know if b ∈ parent.save, but we haven't modified b, so we ensure
	//       a copy exists in the parent frame, and copy the pointer
	if (diff) {
		size_t bsize = mem->bsize;
		void *fwork = mem->fwork + wnum*fp*bsize;
		void *work = mem->work;
		for (Mask m=diff; m; m&=m-1) {
			size_t idx = __builtin_ctzll(m);
			fblock[idx] = fp;
			size_t ofs = idx*bsize;
			mem_copyblock(fwork + ofs, work + ofs, bsize);
		}
		if (mask == diff)
			return;
		mask &= ~diff;
	}
	FrameId fp1 = ftab[fp].prev;
	Mask propagate = mask & ~ftab[fp1].save;
	if (UNLIKELY(propagate))
		mem_frame_store(mem, fp1, propagate);
	mem_copyframeptr(fblock, fblock + (fp1-fp)*wnum, mask);
}

CFUNC void m3_mem_write(m3_Mem *mem, Mask mask)
{
	Mask unsaved = mem->unsaved;
	assert(unsaved == ~mem->ftab[mem->frame].save);
	mem->unsaved &= ~mask;
	mem_frame_store(mem, mem->frame, mask & unsaved);
}

static void mem_load_slow(m3_Mem *mem, size_t fp)
{
	assert(mem->scratch.len == 0);
	size_t frame = mem->frame;
	mem->frame = fp;
	m3_Frame *ftab = mem->ftab;
	Mask restore = mem->diff;
	FrameId bfp[64];
	size_t r = 0;
	mem->unsaved = ~ftab[fp].save;
	for (;;) {
		if (frame > fp) {
			Mask diff = ftab[frame].diff;
			restore |= diff;
			if (UNLIKELY(ftab[frame].state & FRAME_ALIVE)) {
				Mask need = diff & ~ftab[frame].save;
				if (UNLIKELY(need))
					mem_frame_store(mem, frame, need);
			}
			frame = ftab[frame].prev;
		} else if (fp > frame) {
			*mem_vec_allocT(&mem->scratch, FrameId) = fp;
			fp = ftab[fp].prev;
			r++;
		} else {
			break;
		}
	}
	FrameId *fblock = mem->fblock;
	size_t wnum = mem->wnum;
	mem_copyframeptr(bfp, fblock + wnum*frame, restore);
	if (r > 0) {
		mem->scratch.len = 0;
		FrameId *right = mem->scratch.data;
		do {
			FrameId f = right[r];
			Mask diff = ftab[f].diff;
			restore |= diff;
			mem_copyframeptr(bfp, fblock + wnum*f, diff);
		} while (r-- > 0);
	}
	mem_restore(mem, bfp, restore);
}

CFUNC void m3_mem_load(m3_Mem *mem, int fpx)
{
	size_t fp = fpx;
	assert(mem->ftab[fp].state & FRAME_ALIVE);
	if (LIKELY(fp == mem->frame)) {
		mem_restore(mem, mem->fblock + mem->wnum*fp, mem->diff);
	} else {
		mem_load_slow(mem, fp);
	}
	mem->cursor = mem->chunktop;
	mem->lfreen = mem->lfree.len;
	mem->diff = 0;
	mem_assert_fresh_frame_invariants(mem);
}

CFUNC int m3_mem_newobjref(m3_Mem *mem)
{
	ObjId oref = mem->lrefmax++;
	*mem_vec_allocT(&mem->lfree, ObjId) = oref;
	return oref;
}

CFUNC void m3_mem_init(m3_Mem *mem)
{
	// objref zero is always nil
	mem->lrefmax = 1;
	// frame zero always contains a valid pseudo-savepoint to avoid special cases in save/load
	mem_ftab_grow(mem);
	m3_Frame *frame = &mem->ftab[0];
	frame->state = FRAME_ALIVE;
	frame->diff = ~0ULL;
	mem->unsaved = ~0ULL;
	mem_assert_fresh_frame_invariants(mem);
}

CFUNC void m3_mem_destroy(m3_Mem *mem)
{
	m3_Frame *ftab = mem->ftab;
	for (size_t fp=1; fp<mem->maxframe; fp++) {
		void *chunk = ftab[fp].chunk;
		if (chunk)
			mem_chunk_unmap_chain(chunk + ftab[fp].chunktop);
	}
	if (mem->chunk)
		mem_chunk_unmap_chain(mem->chunk + mem->chunktop);
	free(mem->ftab);
	free(mem->fblock);
	free(mem->fobj);
	free(mem->fwork0);
	free(mem->scratch.data);
	free(mem->lfree.data);
}
