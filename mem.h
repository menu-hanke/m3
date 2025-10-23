#pragma once

#include "def.h"
#include "err.h"

#if M3_LUADEF
#define FrameId   uint32_t
#define ObjId     uint32_t
#define BlockMask uint64_t
#else
typedef uint32_t FrameId;
typedef uint32_t ObjId;
typedef uint64_t BlockMask;
#endif

CDEF typedef struct m3_Alloc {
	void *chunk;             // base address of last chunk owned by this allocator
	uint32_t chunktop;       // end of chunk, just before ChunkMetadata
	uint32_t cursor;         // current allocation position in chunk (0 <= cursor <= chunktop)
	uint8_t needsweep;       // allocator has multiple chunks and old chunks should be freed
} m3_Alloc;

// invariant: parent id < child id
// invariant: child.save ⊂ parent.save, child.diff ⊂ parent.save
CDEF typedef struct m3_Frame {
	BlockMask diff;          // blocks that changed from the parent frame
	BlockMask save;          // blocks this frame's fsave contains a copy of
	m3_Alloc *alloc;         // frame memory (owned by this frame)
	ObjId *objref;           // lua object handles owned by this frame
	FrameId parent;          // parent frame
	uint32_t state;          // (child count << 2) | alive | active
	uint32_t nobj;           // number of lua object handles owned by this frame
	uint8_t _unused[4];
} m3_Frame;

CDEF typedef struct m3_Mem {
	m3_Alloc alloc;          // general allocator for non-moving stuff (e.g. other allocators)
	m3_Alloc *framealloc;    // pending frame memory (owned by mem, swapped on savepoint creation)
	m3_Frame *ftab;          // frame table [sizeftab]
	void *fsave;             // work save memory [sizeftab * sizework]
	void *fsave_base;        // fsave base allocation (unaligned)
	ObjId *freeobj;          // free lua object handles
	void *work;              // work memory [sizework], owned by lua
	void *tmp;               // temporary buffer
	BlockMask diff;          // blocks that have changed since the last savepoint
	BlockMask unsaved;       // blocks that are NOT saved in the last savepoint (~ftab[parent].save)
	FrameId sizeftab;        // frame table size
	FrameId parent;          // last savepoint
	ObjId objh;              // next unallocated object handle
	uint32_t sizework;       // work memory size in bytes
	uint32_t nfreeobj;       // number of free lua object handles
	uint32_t framefreeobj;   // number of free handles at frame start
	uint32_t sizefreeobj;    // free handle list size
	uint32_t curtmp;         // temporary buffer cursor
	uint32_t sizetmp;        // temporary buffer size
	m3_Err *err;             // global error pointer
} m3_Mem;

CFUNC void *m3_mem_alloc(m3_Err *err, m3_Alloc *alloc, size_t size, size_t align);
CFUNC void *m3_mem_allocx(m3_Alloc *alloc, size_t size, size_t align);
CFUNC void *m3_mem_tmp(m3_Mem *mem, size_t size);
M3_FUNC void *m3_mem_grow(void *p, size_t *sz, size_t esz, size_t need);

#define m3_mem_allocf(mem, size, align) m3_mem_alloc((mem)->err, (mem)->framealloc, (size), (align))

#define m3_mem_growvec(p,sz,need) do { \
	size_t ss = (sz); \
	p = m3_mem_grow((p), &ss, sizeof(*(p)), (need)); \
	sz = ss; \
} while (0)

// is `p` in the current chunk of allocator `a`?
#define m3_mem_inchunk(a,p) (((uintptr_t)(p) - (uintptr_t)(a)->chunk) <= (a)->chunktop)
