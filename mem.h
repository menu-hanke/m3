#pragma once

#include "def.h"
#include "err.h"

#if M3_LUADEF
#define FrameId    uint16_t
#define Mask       uint64_t
#else
typedef uint16_t   FrameId;
typedef uint64_t   Mask;
#endif
// +----------+-------+------+
// |   15..2  |   1   |   0  |
// +----------+-------+------+
// | children | alive | objs |
// +----------+-------+------+
#define FrameState uint16_t
#define ObjId      uint32_t

CDEF typedef struct m3_Frame {
	void *chunk;        // last chunk base address
	Mask diff;          // modified work blocks since previous savepoint
	Mask save;          // saved work blocks (in fblock, not necessarily in this frame's fwork)
	FrameId prev;       // previous savepoint
	FrameState state;   // savepoint state
	uint32_t chunktop;  // end of last chunk, just before ChunkMetadata
} m3_Frame;

CDEF typedef struct m3_Vec {
#if M3_LUADEF
	uint8_t *data;
#else
	void *data;
#endif
	uint32_t len, cap;  // in bytes
} m3_Vec;

typedef struct {
	uint32_t size; // in bytes
	ObjId id[];
} ObjList;

CDEF typedef struct m3_Mem {
	void *chunk;             // current chunk base address
	uint32_t cursor;         // current chunk alloc offset
	uint32_t chunktop;       // end of current chunk, just before ChunkMetadata
	void *work;              // work memory (wnum × bsize)
	Mask diff;               // modified work blocks since previous savepoint
	Mask unsaved;            // ~ftab[frame].save
	m3_Vec scratch;          // scratch memory
	m3_Frame *ftab;          // savepoint data (maxframe)
	FrameId *fblock;         // work memory save pointers (maxframe × wnum)
	void *fwork;             // work memory save data (maxframe × wnum × bsize)
	LVOID(ObjList) **fobj;   // lua objects referenced by frame (maxframe)
	m3_Vec lfree;            // free lua object references (ObjId)
	int32_t lfreen;          // available lfree.len, reset on new frame (signed for lua)
	ObjId lrefmax;           // next unallocated lua object reference
	FrameId frame;           // previous savepoint
	FrameId maxframe;        // savepoint list size
	uint32_t bsize;          // work memory block size: must be a positive multiple of MEM_BLOCKSIZE0
	uint8_t wnum;            // number of work memory blocks (1 <= wnum <= 64)
	uint8_t sweep;           // sweep counter for chunk allocator
	void *fwork0;            // fwork allocation (not aligned)
	m3_Err *err;             // error info
} m3_Mem;

CFUNC void *m3_mem_vec_alloc(m3_Vec *vec, uint32_t size);
#define mem_vec_allocTn(v,t,n) ((t*)m3_mem_vec_alloc((v),sizeof(t)*(n)))
#define mem_vec_allocT(v,t)    mem_vec_allocTn((v),t,1)
M3_NOAPI int m3_mem_alloc_bump(m3_Mem *mem, uint32_t size);
#define mem_iswritable(m,p) (((uintptr_t)(p) - (uintptr_t)(m)->chunk) < (m)->chunktop)
