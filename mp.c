#include "target.h"

#if M3_LINUX

#define _GNU_SOURCE /* for sched_getaffinity */

#include "config.h"
#include "def.h"
#include "mem.h"

#include <lua.h>

#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <sched.h>
#include <stdlib.h>
#include <stdio.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>

#if M3_x86
#include <immintrin.h>
#define spin_pause          _mm_pause
#else
#define spin_pause()        ((void)0)
#endif

// memory allocator settings
#define MP_HEAP_NUMCLS       28
#define MP_HEAP_MINCLS       4 // 2^4 = 16 = sizeof(m3_Future)

#define MP_PARK_PARKED       ((uint32_t)(-1))
#define MP_PARK_EMPTY        0
#define MP_PARK_NOTIFIED     1

#define FUT_COMPLETED        (-1ull)

#define MUTEX_UNLOCKED       0
#define MUTEX_LOCKED         1
#define MUTEX_CONTENDED      2

CDEF typedef uint32_t m3_Futex;

CDEF typedef struct m3_Mutex {
	m3_Futex state;
} m3_Mutex;

// NOTE: order of fields matters: `data` must be readable even when the future is
// in the heap freelist
CDEF typedef struct m3_Future {
	uint64_t state; // -1: completed, otherwise: pending
	uint64_t data;  // must be unsigned for Lua
} m3_Future;

CDEF typedef struct m3_Heap {
	uintptr_t cursor;
	uintptr_t freelist[MP_HEAP_NUMCLS];
} m3_Heap;

CDEF typedef struct m3_Shared {
	m3_Mutex lock;
	m3_Heap heap;
} m3_Shared;

#define MSG_FREE 0
#define MSG_REF  1
#define MSG_DEAD 2

// `state` must be first, because when this block is in the freelist, we have
// state=MSG_FREE automatically due to alignment.
CDEF typedef struct m3_Message {
	uint8_t state;
	uint8_t cls;
	uint16_t chan;
	uint32_t len;
	uint8_t data[];
} m3_Message;

CDEF typedef struct m3_Proc {
	m3_Futex park;
} m3_Proc;

CDEF typedef struct m3_ProcPrivate {
	m3_Heap heap;     // this proc's shared memory heap
	m3_Message **msg; // all messages ever allocated by this proc
	uint32_t nmsg, sizemsg;
} m3_ProcPrivate;

/* ---- Shared memory layout ------------------------------------------------ */

AINLINE static m3_Proc *mp_owner(void *ptr)
{
	return (m3_Proc *) ((uintptr_t)ptr & -M3_MP_PROC_MEMORY);
}

/* ---- Error "handling" ---------------------------------------------------- */

NORETURN COLD static void m3_panic(const char *msg)
{
	fputs("m3 panic: ", stderr);
	fputs(msg, stderr);
	fputc('\n', stderr);
	exit(EXIT_FAILURE);
}

/* ---- Futex --------------------------------------------------------------- */

static int mp_futex_wait(m3_Futex *futex, uint32_t v, struct timespec *to)
{
	long r = syscall(SYS_futex, futex, FUTEX_WAIT, v, to);
	if (!r || errno == EAGAIN) return 0;
	if (errno == ETIMEDOUT) return 1;
	m3_panic("futex_wait");
}

static void mp_futex_wake(m3_Futex *futex, uint32_t num)
{
	if (syscall(SYS_futex, futex, FUTEX_WAKE, num) >= 0)
		return;
	m3_panic("futex_wake");
}

#define mp_futex_wake1(futex)    mp_futex_wake((futex), 1)
#define mp_futex_wake_all(futex) mp_futex_wake((futex), INT_MAX)

/* ---- Parking ------------------------------------------------------------- */

// parking yoinked from Rust stdlib:
// https://doc.rust-lang.org/src/std/sys_common/thread_parking/futex.rs.html

static uint64_t now_ns()
{
	struct timespec tp;
	clock_gettime(CLOCK_REALTIME, &tp);
	return tp.tv_sec*1000000000ULL + tp.tv_nsec;
}

NOINLINE static int mp_proc_park_wait(m3_Proc *proc, uint64_t timeout)
{
	assert(proc->park != MP_PARK_EMPTY);
	uint64_t deadline = timeout ? now_ns() + timeout : 0;
	for (;;) {
		if (deadline) {
			int64_t left = deadline - now_ns();
			if (left <= 0) goto timeout;
			struct timespec to = {
				.tv_sec = left / 1000000000ULL,
				.tv_nsec = left % 1000000000ULL
			};
			if (mp_futex_wait(&proc->park, MP_PARK_PARKED, &to)) {
				m3_Futex parked;
timeout:
				parked = MP_PARK_PARKED;
				if (__atomic_compare_exchange_n(&proc->park, &parked, MP_PARK_EMPTY, 1,
						__ATOMIC_RELAXED, __ATOMIC_RELAXED))
					return 1;
			}
		} else {
			mp_futex_wait(&proc->park, MP_PARK_PARKED, NULL);
		}
		m3_Futex notified = MP_PARK_NOTIFIED;
		if (__atomic_compare_exchange_n(&proc->park, &notified, MP_PARK_EMPTY, 1,
				__ATOMIC_RELAXED, __ATOMIC_RELAXED))
			return 0;
	}
}

CFUNC void m3_mp_proc_park(m3_Proc *proc)
{
	if (LIKELY(__atomic_fetch_sub(&proc->park, 1, __ATOMIC_ACQUIRE) == MP_PARK_NOTIFIED)) return;
	mp_proc_park_wait(proc, 0);
}

CFUNC int m3_mp_proc_park_timeout(m3_Proc *proc, uint64_t timeout)
{
	if (LIKELY(__atomic_fetch_sub(&proc->park, 1, __ATOMIC_ACQUIRE) == MP_PARK_NOTIFIED)) return 0;
	return mp_proc_park_wait(proc, timeout);
}

static void mp_proc_unpark(m3_Proc *proc)
{
	if (__atomic_exchange_n(&proc->park, MP_PARK_NOTIFIED, __ATOMIC_RELEASE) == MP_PARK_PARKED)
		mp_futex_wake1(&proc->park);
}

/* ---- Mutex --------------------------------------------------------------- */

// mutex yoinked from Rust stdlib:
// https://doc.rust-lang.org/src/std/sys/unix/locks/futex_mutex.rs.html

COLD static void mp_mutex_lock_contended(m3_Mutex *mutex)
{
	m3_Futex value;
	for (int i=0; i<100; i++) {
		value = __atomic_load_n(&mutex->state, __ATOMIC_RELAXED);
		if (value != MUTEX_LOCKED) break;
		spin_pause();
	}
	if (value == 0 && __atomic_compare_exchange_n(&mutex->state, &value, MUTEX_LOCKED, 1,
				__ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
		return;
	for (;;) {
		if (__atomic_exchange_n(&mutex->state, MUTEX_CONTENDED, __ATOMIC_ACQUIRE) == 0)
			return;
		mp_futex_wait(&mutex->state, MUTEX_CONTENDED, NULL);
	}
}

static void mp_mutex_lock(m3_Mutex *mutex)
{
	m3_Futex v = MUTEX_UNLOCKED;
	if (LIKELY(__atomic_compare_exchange_n(&mutex->state, &v, MUTEX_LOCKED, 1,
					__ATOMIC_ACQUIRE, __ATOMIC_RELAXED)))
		return;
	mp_mutex_lock_contended(mutex);
}

static void mp_mutex_unlock(m3_Mutex *mutex)
{
	if (UNLIKELY(__atomic_exchange_n(&mutex->state, MUTEX_UNLOCKED, __ATOMIC_RELEASE)
				== MUTEX_CONTENDED))
		mp_futex_wake1(&mutex->state);
}

/* ---- Memory allocation --------------------------------------------------- */

AINLINE static size_t mp_sizecls(size_t size)
{
	size = (size-1) >> MP_HEAP_MINCLS;
	return size ? (64-__builtin_clzll(size)) : 0;
}

AINLINE static size_t mp_clssize(size_t cls)
{
	return 1ULL << (cls+MP_HEAP_MINCLS);
}

static void *mp_heap_bump(m3_Heap *heap, size_t size)
{
	assert(!(size & (mp_clssize(0)-1)));
	void *ptr = (void *) heap->cursor;
	heap->cursor += size;
	// ensure heap->cursor is always at a cache line boundary
	uint64_t boundary = (heap->cursor + M3_CACHELINE_SIZE - 1) & -M3_CACHELINE_SIZE;
	uint64_t slack = boundary - heap->cursor;
	if (UNLIKELY(slack)) {
		uintptr_t cursor = heap->cursor;
		heap->cursor = boundary;
		while (slack) {
			uint64_t bit = __builtin_ctzll(slack);
			uint64_t cls = bit - MP_HEAP_MINCLS;
			*(uintptr_t *)cursor = heap->freelist[cls];
			heap->freelist[cls] = cursor;
			cursor += 1ULL << bit;
			slack -= 1ULL << bit;
		}
	}
	return ptr;
}

static void *mp_heap_bump_cls(m3_Heap *heap, size_t cls)
{
	return mp_heap_bump(heap, mp_clssize(cls));
}

static void *mp_heap_get_free(m3_Heap *heap, size_t *size)
{
	size_t cls = mp_sizecls(*size);
	*size = cls;
	uintptr_t ptr = heap->freelist[cls];
	if (LIKELY(ptr))
		heap->freelist[cls] = *(uintptr_t *) ptr;
	return (void *) ptr;
}

static void *mp_heap_get_free_cls(m3_Heap *heap, size_t cls)
{
	uintptr_t ptr = heap->freelist[cls];
	if (LIKELY(ptr))
		heap->freelist[cls] = *(uintptr_t *) ptr;
	return (void *) ptr;
}

static void mp_heap_free_cls(m3_Heap *heap, uintptr_t ptr, size_t cls)
{
	*(uintptr_t *)ptr = heap->freelist[cls];
	heap->freelist[cls] = ptr;
}

CFUNC void *m3_mp_heap_alloc(m3_Heap *heap, size_t size)
{
	void *ptr = mp_heap_get_free(heap, &size);
	if (LIKELY(ptr)) {
		return ptr;
	} else {
		return mp_heap_bump_cls(heap, size);
	}
}

/* ---- Message management -------------------------------------------------- */

static void mp_proc_sweep(m3_ProcPrivate *pp)
{
	m3_Message **messages = pp->msg;
	size_t nmes = pp->nmsg;
	size_t i = 0;
	while (i < nmes) {
		m3_Message *mes = messages[i];
		if (mes->state == MSG_DEAD) {
			mp_heap_free_cls(&pp->heap, (uintptr_t) mes, mes->cls);
			messages[i] = messages[--nmes];
		} else {
			i++;
		}
	}
	pp->nmsg = i;
}

CFUNC m3_Message *m3_mp_proc_alloc_message(m3_ProcPrivate *pp, uint16_t chan, size_t size)
{
	size_t len = size;
	size += sizeof(m3_Message);
	m3_Message *msg = mp_heap_get_free(&pp->heap, &size);
	if (UNLIKELY(!msg)) {
		mp_proc_sweep(pp);
		msg = mp_heap_get_free_cls(&pp->heap, size);
		if (UNLIKELY(!msg))
			msg = mp_heap_bump_cls(&pp->heap, size);
	}
	if (UNLIKELY(pp->nmsg >= pp->sizemsg))
		m3_mem_growvec(pp->msg, pp->sizemsg, 0);
	pp->msg[pp->nmsg++] = msg;
	msg->state = MSG_REF;
	msg->len = len;
	msg->cls = size;
	msg->chan = chan;
	return msg;
}

/* ---- Futures ------------------------------------------------------------- */

// this load synchronizes with stores to `fut->state` that also store to `fut->data`.
// only writes that both:
//   (1) store to `fut->data`,
//   (2) wake up our process (ie. originate from a different process)
// use an atomic (release) store. other writes don't use atomic stores.
// therefore, after this function returns true, the *only* assumption you may make
// is that you can read `fut->data`.
CFUNC int m3_mp_future_completed(m3_Future *fut)
{
	return __atomic_load_n(&fut->state, __ATOMIC_ACQUIRE) == FUT_COMPLETED;
}

/* ---- Events -------------------------------------------------------------- */

CDEF typedef struct m3_Event {
	m3_Future *waiters;
	m3_Mutex lock;
	uint32_t flag;
} m3_Event;

CFUNC void m3_mp_event_wait(m3_Event *event, uint32_t value, m3_Future *fut)
{
	uint32_t flag = event->flag;
	if (flag != value) {
complete:
		fut->state = FUT_COMPLETED;
		fut->data = flag;
		return;
	}
	mp_mutex_lock(&event->lock);
	flag = event->flag;
	if (flag != value) {
		mp_mutex_unlock(&event->lock);
		goto complete;
	}
	fut->state = (uintptr_t) event->waiters;
	event->waiters = fut;
	mp_mutex_unlock(&event->lock);
}

CFUNC void m3_mp_event_set(m3_Event *event, uint32_t flag)
{
	if (event->flag == flag) return;
	mp_mutex_lock(&event->lock);
	event->flag = flag;
	m3_Future *fut = event->waiters;
	event->waiters = NULL;
	mp_mutex_unlock(&event->lock);
	while (fut) {
		uintptr_t info = fut->state;
		fut->data = flag;
		__atomic_store_n(&fut->state, FUT_COMPLETED, __ATOMIC_RELEASE);
		mp_proc_unpark(mp_owner(fut));
		fut = (m3_Future *) info;
	}
}

/* ---- Queues -------------------------------------------------------------- */

// most of the implementation is yoinked from:
// http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
struct m3_Queue {
	struct {
		uint64_t read;         // read pointer (next read)
		uint64_t rmask;        // queue size - 1
		m3_Future *wfut;
		m3_Mutex wfut_lock;
	} __attribute__((aligned(M3_CACHELINE_SIZE)));
	struct {
		uint64_t write;        // write pointer (next write)
		uint64_t wmask;        // queue size - 1
		m3_Future *rfut;
		m3_Mutex rfut_lock;
	} __attribute__((aligned(M3_CACHELINE_SIZE)));
	struct {
		// stamp=write:  slot is writable
		// stamp=read+1: slot is readable
		uint64_t stamp;
		uintptr_t data;
	} slots[];
};

CDEF typedef struct m3_Queue m3_Queue;

CFUNC m3_Queue *m3_mp_queue_new(m3_Heap *heap, size_t size)
{
	if (size & (size-1))
		size = 1ULL << (64 - __builtin_clzll(size));
	m3_Queue *queue = mp_heap_bump(heap, sizeof(*queue)+size*sizeof(*queue->slots));
	for (size_t i=0; i<size; i++)
		queue->slots[i].stamp = i;
	queue->rmask = size-1;
	queue->wmask = size-1;
	return queue;
}

CFUNC void m3_mp_queue_write(m3_Queue *queue, uintptr_t data, m3_Future *fut)
{
	uint64_t mask = queue->wmask;
again:
	for (;;) {
		uint64_t write = __atomic_load_n(&queue->write, __ATOMIC_RELAXED);
		uint64_t idx = write & mask;
		uint64_t stamp = __atomic_load_n(&queue->slots[idx].stamp, __ATOMIC_ACQUIRE);
		if (LIKELY(stamp == write)) {
			// reader is done, this slot is free, attempt to write.
			if (__atomic_compare_exchange_n(&queue->write, &write, write+1, 1,
					__ATOMIC_SEQ_CST, __ATOMIC_RELAXED)) {
				// no read can proceed past this slot until we complete the write
				assert(__atomic_load_n(&queue->read, __ATOMIC_RELAXED)  <= write);
				// we may proceed to write
				fut->state = FUT_COMPLETED;
				// is a read waiting to be completed?
				// if so, then either:
				//   (1) read < write: there are (either pending or finished) writes between
				//       our write and the read pointer. either the reader will withdraw the future
				//       it submitted or an earlier write will complete it.
				//   (2) read = write: there are no pending or finished writes between our write
				//       and the read pointer. neither a read or write operation may move the
				//       read pointer until we commit our stamp.
				// in case (1) we spin until either read=write or the pending future is gone.
				// in case (2) we forward the write to the future and increment read pointer.
				for (;;) {
					if (LIKELY(!__atomic_load_n(&queue->rfut, __ATOMIC_SEQ_CST))) break;
					// is this case (1) or (2)?
					uint64_t read = __atomic_load_n(&queue->read, __ATOMIC_RELAXED);
					if (UNLIKELY(read < write)) {
						// it's case (1)
						spin_pause();
						continue;
					}
					// it's case (2): we now have full control over the read pointer;
					// no read may proceed until we finish this write.
					// therefore we may forward the read from the waiting future.
					mp_mutex_lock(&queue->rfut_lock);
					m3_Future *rfut = queue->rfut;
					if (rfut) queue->rfut = (m3_Future *) rfut->state;
					mp_mutex_unlock(&queue->rfut_lock);
					if (!rfut) {
						// the pending future was cleared by the a previous write before we
						// loaded the read pointer.
						break;
					}
					// otherwise: this is currently the only process that may modify `queue->read`
					queue->read = write+1;
					// no atomic needed here: the future is synchronized by unpark.
					queue->slots[idx].stamp = write+mask+1;
					// forward the value to `rfut`.
					rfut->data = data;
					__atomic_store_n(&rfut->state, FUT_COMPLETED, __ATOMIC_RELEASE);
					mp_proc_unpark(mp_owner(rfut));
					return;
				}
				// no reader was waiting.
				queue->slots[idx].data = data;
				__atomic_store_n(&queue->slots[idx].stamp, write+1, __ATOMIC_RELEASE);
				return;
			} else {
				// we lost the race to another process
				continue;
			}
		} else if (stamp < write) {
			// the queue is full.
			fut->data = data;
			mp_mutex_lock(&queue->wfut_lock);
			fut->state = (uintptr_t) queue->wfut;
			__atomic_store_n(&queue->wfut, fut, __ATOMIC_SEQ_CST);
			mp_mutex_unlock(&queue->wfut_lock);
			uint64_t read = __atomic_load_n(&queue->read, __ATOMIC_SEQ_CST);
			if (write-read == mask+1) {
				// the queue is definitely still full.
				// any process that increments `queue->read` will now see our updated `wfut`.
				return;
			} else {
				// `read` has been incremented at some point and the queue may not be full anymore.
				// now either:
				//   (1) `fut` is in the pending write list and we should remove it and retry; or
				//   (2) `fut` is not in the pending write list, which means a read operation
				//       has forwarded it in which case we may return.
				mp_mutex_lock(&queue->wfut_lock);
				for (m3_Future **wf=&queue->wfut; *wf; wf=(m3_Future**)&(*wf)->state) {
					if (*wf == fut) {
						// case (1): unlink `fut` and try again
						*wf = (m3_Future *) fut->state;
						mp_mutex_unlock(&queue->wfut_lock);
						goto again;
					}
				}
				// case (2): our write was forwarded and we can leave.
				mp_mutex_unlock(&queue->wfut_lock);
				return;
			}
		} else {
			// another process overwrote the stamp before we loaded it
			continue;
		}
	}
}

CFUNC void m3_mp_queue_read(m3_Queue *queue, m3_Future *fut)
{
	uint64_t mask = queue->rmask;
again:
	for (;;) {
		uint64_t read = __atomic_load_n(&queue->read, __ATOMIC_RELAXED);
		uint64_t idx = read & mask;
		uint64_t stamp = __atomic_load_n(&queue->slots[idx].stamp, __ATOMIC_ACQUIRE);
		if (LIKELY(stamp == read+1)) {
			// we cannot read past the write pointer
			assert(read < __atomic_load_n(&queue->write, __ATOMIC_RELAXED));
			if (__atomic_compare_exchange_n(&queue->read, &read, read+1, 1,
					__ATOMIC_SEQ_CST, __ATOMIC_RELAXED)) {
				// we may proceed to read
				fut->state = FUT_COMPLETED;
				fut->data = queue->slots[idx].data;
				// was a writer waiting for space?
				// if so, then either:
				//   (1) read+mask+1 > write: there are (either pending or finished) reads between
				//       the write pointer and our read. either the writer will withdraw the future
				//       it submitted or an earlier read will complete it.
				//   (2) read+mask+1 = write: there are no pending or finished reads between our
				//       the write pointer and our read. neither a read or write operation may move
				//       the write pointer until we commit our stamp.
				// in case (1) we spin until either read+mask+1=write or the pending future is gone.
				// in case (2) we forward the empty slot to the future and increment write pointer.
				for (;;) {
					if (LIKELY(!__atomic_load_n(&queue->wfut, __ATOMIC_SEQ_CST))) break;
					// is this case (1) or (2)?
					uint64_t write = __atomic_load_n(&queue->write, __ATOMIC_RELAXED);
					if (UNLIKELY(read+mask+1 > write)) {
						// it's case (1)
						spin_pause();
						continue;
					}
					// it's case (2): we now have full control over the write pointer;
					// no write may proceed until we finish this read.
					// therefore we may forward the write from the waiting future.
					mp_mutex_lock(&queue->wfut_lock);
					m3_Future *wfut = queue->wfut;
					if (wfut) queue->wfut = (m3_Future *) wfut->state;
					mp_mutex_unlock(&queue->wfut_lock);
					if (!wfut) {
						// the pending future was cleared by a previous read before we
						// loaded the write pointer.
						break;
					}
					// otherwise: this is currently the only process that may modify `queue->write`
					queue->slots[idx].data = wfut->data;
					queue->write = read+mask+2;
					// unlike the corresponding store in write(), this needs a release
					// because we need to synchronize queue->slots[idx].data
					__atomic_store_n(&queue->slots[idx].stamp, read+mask+2, __ATOMIC_RELEASE);
					// forward the empty slot to `wfut`.
					// this does not need release, or even an atomic store, because there is no
					// data in the future that the other process might read.
					wfut->state = FUT_COMPLETED;
					mp_proc_unpark(mp_owner(wfut));
					return;
				}
				// no writer was waiting.
				__atomic_store_n(&queue->slots[idx].stamp, read+mask+1, __ATOMIC_RELEASE);
				return;
			} else {
				// we lost the race
				continue;
			}
		} else if (stamp < read+1) {
			// the queue is empty.
			mp_mutex_lock(&queue->rfut_lock);
			fut->state = (uintptr_t) queue->rfut;
			__atomic_store_n(&queue->rfut, fut, __ATOMIC_SEQ_CST);
			mp_mutex_unlock(&queue->rfut_lock);
			uint64_t write = __atomic_load_n(&queue->write, __ATOMIC_SEQ_CST);
			if (write == read) {
				// the queue is definitely still empty.
				// any process that increments `queue->write` will now see our updated `rfut`.
				return;
			} else {
				// `write` has been increment at some point and the queue may not be empty anymore.
				// now either:
				//   (1) `fut` is in the pending read list and we should remove it and retry; or
				//   (2) `fut` is not in the pending read list, which means a write operation
				//       has forwarded it in which case we may return.
				mp_mutex_lock(&queue->rfut_lock);
				for (m3_Future **rf=&queue->rfut; *rf; rf=(m3_Future**)&(*rf)->state) {
					if (*rf == fut) {
						// case (1): unlink `fut` and try again.
						*rf = (m3_Future *) fut->state;
						mp_mutex_unlock(&queue->rfut_lock);
						goto again;
					}
				}
				// case (2): our read was forwarded and we can leave.
				mp_mutex_unlock(&queue->rfut_lock);
				return;
			}
			return;
		} else {
			// another process overwrote the stamp
			continue;
		}
	}
}

#endif
