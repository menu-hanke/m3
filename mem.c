#include "config.h"
#include "mem.h"

#include <assert.h>
#include <string.h>
#include <stdlib.h>

#define FRAME_ACTIVE   1
#define FRAME_ALIVE    2
#define FRAME_CHILD    4

typedef struct ChunkMetadata {
	struct ChunkMetadata *prev;
	uint32_t size;
} ChunkMetadata;

#define chunk_base(meta) ((void *)(meta) + sizeof(ChunkMetadata) - (meta)->size)

#if M3_MMAP

#include <sys/mman.h>

static int mem_mmap(m3_Err *err, void **map, size_t size, int flags)
{
	void *p = mmap(NULL, size, PROT_READ|PROT_WRITE, flags, -1, 0);
	if (p == MAP_FAILED) {
		return m3_err_sys(err, M3_ERR_MMAP);
	} else {
		*map = p;
		madvise(p, size, MADV_DONTDUMP);
		return 0;
	}
}

CFUNC int m3_mem_map_shared(m3_Err *err, size_t size, void **map)
{
	return mem_mmap(err, map, size, MAP_SHARED|MAP_ANONYMOUS|MAP_NORESERVE);
}

CFUNC void m3_mem_unmap(void *base, size_t size)
{
	munmap(base, size);
}

static int mem_chunk_map(m3_Err *err, void **base, size_t size)
{
	return mem_mmap(err, base, size, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE);
}

static void mem_chunk_unmap(ChunkMetadata *meta)
{
	munmap(chunk_base(meta), meta->size);
}

#elif M3_VIRTUALALLOC

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

static int mem_chunk_map(m3_Err *err, void **base, size_t size)
{
	void *p = VirtualAlloc(NULL, size, MEM_RESERVE|MEM_COMMIT, PAGE_READWRITE);
	if (p) {
		*base = p;
		return 0;
	} else {
		return m3_err_set(err, M3_ERR_MMAP);
	}
}

static void mem_chunk_unmap(ChunkMetadata *meta)
{
	VirtualFree(chunk_base(meta), 0, MEM_RELEASE);
}

#endif

static void *mem_alloc_grow(m3_Err *err, m3_Alloc *alloc, size_t size, size_t align)
{
	ChunkMetadata *prev = alloc->chunk ? (alloc->chunk+alloc->chunktop) : NULL;
	size_t chunksize = prev ? (prev->size<<1) : M3_CONFIG_CHUNKSIZE;
	while (chunksize < size+sizeof(ChunkMetadata))
		chunksize <<= 1;
	if (UNLIKELY(mem_chunk_map(err, &alloc->chunk, chunksize)))
		return NULL;
	size_t cursor = chunksize - sizeof(ChunkMetadata);
	alloc->chunktop = cursor;
	cursor -= size;
	cursor &= -align;
	alloc->cursor = cursor;
	alloc->needsweep = 1;
	ChunkMetadata *meta = alloc->chunk + alloc->chunktop;
	meta->prev = prev;
	meta->size = chunksize;
	return alloc->chunk + cursor;
}

void *m3_mem_alloc(m3_Err *err, m3_Alloc *alloc, size_t size, size_t align)
{
	if (LIKELY(size < alloc->cursor)) {
		alloc->cursor -= size;
		alloc->cursor &= -align;
		return alloc->chunk + alloc->cursor;
	}
	return mem_alloc_grow(err, alloc, size, align);
}

// for fhk custom allocator callback
void *m3_mem_allocx(m3_Alloc *alloc, size_t size, size_t align)
{
	return m3_mem_alloc(NULL, alloc, size, align);
}

#define mem_alloclist(e,a,p,n) (p = m3_mem_alloc((e), (a), (n)*sizeof(*(p)), alignof(*(p))))

void *m3_mem_tmp(m3_Mem *mem, size_t size)
{
	if (UNLIKELY(mem->curtmp+size >= mem->sizetmp))
		m3_mem_growvec(mem->tmp, mem->sizetmp, mem->curtmp+size);
	void *p = mem->tmp + mem->curtmp;
	mem->curtmp += size;
	return p;
}

static void mem_alloc_sweep(m3_Alloc *alloc)
{
	if (!alloc->chunk)
		return;
	ChunkMetadata *meta = alloc->chunk + alloc->chunktop;
	ChunkMetadata *m = meta->prev;
	meta->prev = NULL;
	while (m) {
		ChunkMetadata *prev = m->prev;
		mem_chunk_unmap(m);
		m = prev;
	}
}

static void mem_alloc_destroy(m3_Alloc *alloc)
{
	mem_alloc_sweep(alloc);
	if (alloc->chunk)
		mem_chunk_unmap((ChunkMetadata *) (alloc->chunk + alloc->chunktop));
}

static void mem_alloc_init(m3_Alloc *alloc)
{
	memset(alloc, 0, sizeof(*alloc));
}

static m3_Alloc *mem_alloc_new(m3_Mem *mem)
{
	m3_Alloc *alloc = m3_mem_alloc(mem->err, &mem->alloc, sizeof(*alloc), alignof(*alloc));
	mem_alloc_init(alloc);
	return alloc;
}

void *m3_mem_grow(void *p, size_t *sz, size_t esz, size_t need)
{
	size_t s = *sz;
	s = s ? (s<<1) : 16;
	while (s < need)
		s <<= 1;
	*sz = s;
	return realloc(p, s*esz);
}

static void mem_grow_ftab(m3_Mem *mem)
{
	FrameId sizeftab = mem->sizeftab ? (mem->sizeftab<<1) : 8;
	mem->ftab = realloc(mem->ftab, sizeftab*sizeof(*mem->ftab));
	memset(mem->ftab+mem->sizeftab, 0, (sizeftab-mem->sizeftab)*sizeof(*mem->ftab));
	for (size_t i=mem->sizeftab; i<sizeftab; i++)
		mem->ftab[i].alloc = mem_alloc_new(mem);
	void *fsave_base = malloc(sizeftab*mem->sizework + M3_CACHELINE_SIZE-1);
	void *fsave = (void *) (((intptr_t)fsave_base + M3_CACHELINE_SIZE-1) & -M3_CACHELINE_SIZE);
	memcpy(fsave, mem->fsave, mem->sizeftab*mem->sizework);
	free(mem->fsave_base);
	mem->fsave_base = fsave_base;
	mem->fsave = fsave;
	mem->sizeftab = sizeftab;
}

CFUNC int m3_mem_save(m3_Mem *mem)
{
	// find a free child frame id. invariant: child id > parent id
	size_t id = mem->parent + 1;
	mem->ftab[mem->parent].state += FRAME_CHILD;
	for (;; id++) {
		// need to grow?
		if (UNLIKELY(id >= mem->sizeftab)) {
			mem_grow_ftab(mem);
			break;
		}
		// find a free frame?
		if (LIKELY(!mem->ftab[id].state))
			break;
	}
	// commit new frame
	m3_Frame *frame = &mem->ftab[id];
	frame->diff = mem->diff;
	frame->save = 0;
	frame->parent = mem->parent;
	frame->state = FRAME_ACTIVE | FRAME_ALIVE;
	// swap allocators
	m3_Alloc *alloc = frame->alloc;
	frame->alloc = mem->framealloc;
	mem->framealloc = alloc;
	// commit pending object handles and return old object handles to free pool
	if (UNLIKELY(frame->nobj || (mem->nfreeobj != mem->framefreeobj))) {
		size_t nobj = mem->framefreeobj - mem->nfreeobj;
		size_t fnobj = frame->nobj;
		ObjId *fobjref = frame->objref;
		frame->nobj = nobj;
		if (nobj) {
			mem_alloclist(mem->err, frame->alloc, frame->objref, nobj);
			memcpy(frame->objref, mem->freeobj+mem->nfreeobj, nobj*sizeof(*frame->objref));
		}
		if (fnobj) {
			if (UNLIKELY(mem->nfreeobj+fnobj >= mem->sizefreeobj))
				m3_mem_growvec(mem->freeobj, mem->sizefreeobj, mem->nfreeobj+fnobj);
			memcpy(mem->freeobj+mem->nfreeobj, fobjref, fnobj*sizeof(*fobjref));
			mem->nfreeobj += fnobj;
		}
		mem->framefreeobj = mem->nfreeobj;
	}
	// reset pending frame
	mem->parent = id;
	mem->diff = 0;
	mem->unsaved = ~0ULL;
	alloc->cursor = alloc->chunktop;
	if (UNLIKELY(alloc->needsweep))
		mem_alloc_sweep(alloc);
	return id;
}

static void mem_copyblocks(void *dst, void *src, BlockMask mask, size_t sizewmem)
{
	if (UNLIKELY((int64_t)mask < 0)) {
		// copy tail
		assert(sizewmem > 63*M3_CONFIG_BLOCKSIZE);
		memcpy(dst + 63*M3_CONFIG_BLOCKSIZE, src + 63*M3_CONFIG_BLOCKSIZE,
			sizewmem - 63*M3_CONFIG_BLOCKSIZE);
		mask &= ~(1ULL << 63);
	}
	for (; mask; mask &= mask-1) {
		size_t i = __builtin_ctzll(mask);
		memcpy(dst + i*M3_CONFIG_BLOCKSIZE, src + i*M3_CONFIG_BLOCKSIZE, M3_CONFIG_BLOCKSIZE);
	}
}

static void mem_load_walk(m3_Mem *mem, FrameId fp)
{
	assert(mem->curtmp == 0);
	m3_Frame *ftab = mem->ftab;
	void *fsave = mem->fsave;
	size_t frame = mem->parent;
	BlockMask restore = mem->diff;
	size_t sizework = mem->sizework;
	void *work = mem->work;
	mem->parent = fp;
	mem->unsaved = ~ftab[fp].save;
	size_t curtmp = 0;
	for (;;) {
		if (frame > fp) {
			// walk up from source frame
			m3_Frame *f = &ftab[frame];
			assert(f->state & FRAME_ACTIVE);
			f->state &= ~FRAME_ACTIVE;
			BlockMask diff = f->diff;
			restore |= diff;
			// did we walk past a frame we may later return to?
			if (UNLIKELY(f->state)) {
				// then ensure it saves its own diff.
				// no propagation needed here because the invariant child.diff ⊂ parent.save
				// guarantees that what we save here is already saved in the parent
				assert((diff & ftab[f->parent].save) == diff);
				BlockMask need = diff & ~f->save;
				if (UNLIKELY(need)) {
					f->save |= need;
					mem_copyblocks(fsave + frame*sizework, work, need, sizework);
				}
			}
			frame = f->parent;
		} else if (UNLIKELY(fp > frame)) {
			// walk up from target frame (slow path: target is not an ancestor of source)
			if (UNLIKELY(curtmp+sizeof(FrameId) >= mem->sizetmp))
				m3_mem_growvec(mem->tmp, mem->sizetmp, curtmp+sizeof(FrameId));
			*(FrameId *)(mem->tmp + curtmp) = fp;
			curtmp += sizeof(FrameId);
			fp = ftab[fp].parent;
		} else {
			break;
		}
	}
	// rollback to common ancestor
	mem_copyblocks(work, fsave + frame*sizework, restore, sizework);
	// if target was not an ancestor, apply the other branch in reverse order
	if (UNLIKELY(curtmp)) {
		FrameId *base = mem->tmp;
		FrameId *fid = mem->tmp + curtmp;
		do {
			FrameId fi = *--fid;
			m3_Frame *f = &ftab[fi];
			assert(!(f->state & FRAME_ACTIVE));
			f->state |= FRAME_ACTIVE;
			mem_copyblocks(mem->work, fsave + fi*sizework, f->diff, sizework);
		} while (fid > base);
	}
}

CFUNC void m3_mem_load(m3_Mem *mem, FrameId fp)
{
	assert((mem->diff & mem->unsaved) == 0);
	if (LIKELY(fp == mem->parent)) {
		// reset pending frame
		mem_copyblocks(mem->work, mem->fsave + fp*mem->sizework, mem->diff, mem->sizework);
	} else {
		// walk the savepoint tree to the target frame
		mem_load_walk(mem, fp);
	}
	mem->diff = 0;
	mem->framealloc->cursor = mem->framealloc->chunktop;
	mem->nfreeobj = mem->framefreeobj;
}

CFUNC void m3_mem_write(m3_Mem *mem, BlockMask mask)
{
	// maintain invariants:
	// (a) child.diff ⊂ parent.save
	// (b) child.save ⊂ parent.save
	// i.e. propagate mask to all parents
	mem->unsaved &= ~mask;
	size_t frame = mem->parent;
	for (;;) {
		m3_Frame *f = &mem->ftab[frame];
		if ((mask & f->save) == mask)
			break;
		mem_copyblocks(mem->fsave + frame*mem->sizework, mem->work, mask & ~f->save, mem->sizework);
		f->save |= mask;
		frame = f->parent;
	}
}

CFUNC int m3_mem_newobjref(m3_Mem *mem)
{
	ObjId oref = mem->objh++;
	if (UNLIKELY(mem->framefreeobj == mem->sizefreeobj))
		m3_mem_growvec(mem->freeobj, mem->sizefreeobj, 0);
	mem->freeobj[mem->framefreeobj++] = oref;
	return oref;
}

// note: this function assumes mem is already zero-initialized (by ffi.new)
CFUNC void m3_mem_init(m3_Mem *mem)
{
	// objref zero is always nil
	mem->objh = 1;
	// frame zero always contains a valid pseudo-savepoint to avoid special cases in save/load
	mem_grow_ftab(mem);
	m3_Frame *frame = &mem->ftab[0];
	frame->state = FRAME_ACTIVE | FRAME_ALIVE;
	frame->diff = ~0ULL;
	mem->unsaved = ~0ULL;
	mem->framealloc = mem_alloc_new(mem);
}

CFUNC void m3_mem_destroy(m3_Mem *mem)
{
	if (!mem->ftab)
		return; // m3_mem_init was never called
	m3_Frame *ftab = mem->ftab;
	for (size_t fp=0; fp<mem->sizeftab; fp++)
		mem_alloc_destroy(ftab[fp].alloc);
	mem_alloc_destroy(mem->framealloc);
	mem_alloc_destroy(&mem->alloc);
	free(mem->ftab);
	free(mem->fsave_base);
	free(mem->freeobj);
	free(mem->tmp);
}
