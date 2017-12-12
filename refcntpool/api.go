package refcntpool

// refcntpool provides interfaces and objects to implement pools of reference
// counted items, where the item is returned to the pool when its reference
// count drops to zero (upon a call to object.Release()).
//
// The pool interface can be extended to perform more complicated actions when
// the reference count drops to zero, e.g. an inode cache where releasing the
// item returns it to the free list of the inode cache.
//
// There are two ways to use reference counted items: 1) the first is to embed a
// RefCntItem object in the object you want reference counted and use the
// generic RefCntItemPool object with a custom New() routine that creates
// objects of the desired type; or 2) embed a RefCntItem object in the object
// you want reference counted and write your own pool that supports the
// RefCntItemPooler interface.  The second approach allows more flexible actions
// to be taken when objects are released and reallocated.
//
// An implementation of reference counted memory buffers is also provided, which
// also serves as an example.  Use RefCntBufPoolMake(bufSz uint64) to create a
// pool of reference counted memory buffers of size bufSz.

import (
	"fmt"
	"sync"
)

// A object implementing the RefCntItemer interface is acquired from a
// RefCntItemPooler.  Hold() increments the reference count and Release()
// decrements it.  Upon final release when the reference count drops to zero) it
// is returned the pool from whence it came.
//
// An object returned by Get() starts with one hold.  When all the holds are
// released the object must not be accessed.
//
// Init() is invoked by the pool before the item is returned via Get().  It
// should only be called by the RefCntItemPooler.  It is called with a pointer
// to the pool and a pointer to the reference counted item that its embedded in.
//
type RefCntItemer interface {
	Init(RefCntItemPooler, interface{}) // invoked by RefCntItemPooler.Get() before the item is returned
	Hold()                              // get an additional hold on the item
	Release()                           // release a hold on the item
}

// The RefCntItemPooler interface defines Get() and put() methods for objects
// that support the RefCntItemer interface.
//
// While Get() is called to get a new object, put() should only be called via
// the object's Release() method and not called directly.
//
type RefCntItemPooler interface {
	// Return an object of the type held by the pool which also supports
	// the RefCntItem methods (Hold() and Release())
	Get() interface{}

	// Put an object of the type held by the pool back in the pool.
	put(interface{})
}

// RefCntItem is an object that implements the RefCntItemer interface.  It can
// be embedded in other objects to allow them to be reference counted.
//
// The reference counted object is typically acquired from a RefCntItemPool
// object, or other object implementing the RefCntItemPooler interface.
//
type RefCntItem struct {
	pool    RefCntItemPooler
	cntItem interface{} // the acutal item this is embedded in
	refCnt  int32       // updated atomically
	_       sync.Mutex  // insure a RefCntItem is not copied
}

// RefCntItemPool is an object that implements a pool of reference counted items.
// The items must support the RefCntItemer interface.  Items are "allocated" by
// calling Get() on the pool.
//
// The items are returned to the pool when the reference count drops to zero
// (upon the final call to Release()).
//
// Like sync.pool, the user must supply a New() routine to allocate new objects.
//
type RefCntItemPool struct {
	itemPool sync.Pool
	_        sync.Mutex // insure a RefCntItemPool is not copied

	New func() interface{}
}

// A reference counted memory buffer implementing Hold() and Release().
//
type RefCntBuf struct {
	RefCntItem        // track reference count; provides Hold() and Release()
	origBuf    []byte // original buffer allocation
	Buf        []byte // current buffer
}

// A pool of reference counted memory buffers, where bufers are acquired by
// calling Get() and returned on the final Relase().
//
// Call RefCntBufPoolMake() to return a pool for memory buffers of the desired
// size.
//
type RefCntBufPool struct {
	bufPool sync.Pool  // buffer pool
	bufSz   uint64     // all buffers in this pool are bufSz bytes
	_       sync.Mutex // insure a RefCntBufPool is not copied
}

// Create and return a pool of reference counted memory buffers with the
// specified bufSz.
//
func RefCntBufPoolMake(bufSz uint64) (poolp *RefCntBufPool) {
	poolp = &RefCntBufPool{}

	poolp.bufPool.New = func() interface{} {

		// Make a new RefCntBuf
		bufp := &RefCntBuf{
			origBuf: make([]byte, 0, bufSz),
		}
		return bufp
	}

	poolp.bufSz = bufSz
	return
}

// A set of reference counted memory buffer pools of various sizes.
//
// The GetRefCntBuf() method returns smallest buffer large enough to hold the
// requested allocation size.
//
type RefCntBufPoolSet struct {
	memBufPools     []*RefCntBufPool
	bufferPoolSizes []uint64
}

// Initialize an array of reference counted memory buffer pools.  The size of
// each pool must be specified (in ascending order).
//
// Init() must be called exactly once and before any allocations are requested.
//
func (slabs *RefCntBufPoolSet) Init(sizes []uint64) {
	if len(slabs.memBufPools) != 0 {
		panic(fmt.Sprintf("(*memBufPools).Init() called more than once for RefCntBufPoolSet at %p", slabs))
	}
	slabs.memBufPools = make([]*RefCntBufPool, len(sizes))
	for i, sz := range sizes {
		slabs.memBufPools[i] = RefCntBufPoolMake(sz)

		if i > 0 && sizes[i-1] >= sz {
			panic(fmt.Sprintf("(*memBufPools).Init() size not increasing: size[%d] %d  size[%d] %d",
				i-1, sizes[i-1], i, sz))
		}
	}
	slabs.bufferPoolSizes = sizes
	return
}

// Get a reference counted memory buffer that's large enough to hold the
// requested size.
//
// It is a fatal error to get a rqeuest for buffer larger then the largest pool.
//
func (slabs *RefCntBufPoolSet) GetRefCntBuf(bufSz uint64) (bufp *RefCntBuf) {

	sizeCnt := len(slabs.bufferPoolSizes)

	// binary search for the right buffer pool
	low := 0
	high := sizeCnt
	idx := sizeCnt / 2
	for idx < sizeCnt {
		// if this buffer pool is too small, search forward
		if slabs.bufferPoolSizes[idx] < bufSz {
			low = idx
			idx += (high - idx + 1) / 2
			continue
		}

		// else check if this buffer pool is big enough -- if this is
		// the first pool or the next smaller pool is too small, then
		// we've found the right pool
		if idx == 0 || slabs.bufferPoolSizes[idx-1] < bufSz {
			bufp = slabs.memBufPools[idx].Get().(*RefCntBuf)
			return
		}

		// otherwise check the smaller pools
		high = idx + 1
		idx -= (idx - low + 1) / 2
	}

	// there's no joy in Mudville; panic with an explanation of the problem
	if sizeCnt == 0 {
		panic(fmt.Sprintf("GetRefCntBuf(): no pools have been allocated for RefCntBufPoolSet at %p",
			slabs))
	}
	panic(fmt.Sprintf("GetRefCntBuf(): requested buf size %d is larger then the largest pool size %d",
		bufSz, slabs.bufferPoolSizes[sizeCnt-1]))

	// Unreachable
}
