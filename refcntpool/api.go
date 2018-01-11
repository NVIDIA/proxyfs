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
	AssertIsHeld()                      // will panic if the item is not held
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
	bufSz   int        // all buffers in this pool are bufSz bytes
	_       sync.Mutex // insure a RefCntBufPool is not copied
}

// Create and return a pool of reference counted memory buffers with the
// specified bufSz.
//
func RefCntBufPoolMake(bufSz int) (poolp *RefCntBufPool) {
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
	bufferPoolSizes []int
}

// Initialize an array of reference counted memory buffer pools.  The size of
// each pool must be specified (in ascending order).
//
// Init() must be called exactly once and before any allocations are requested.
//
func (slabs *RefCntBufPoolSet) Init(sizes []int) {
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
func (slabs *RefCntBufPoolSet) GetRefCntBuf(bufSz int) (bufp *RefCntBuf) {

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

	// "but there is no joy in Mudville -- mighty Casey has struck out"
	// (panic with an explanation of the problem)
	if sizeCnt == 0 {
		panic(fmt.Sprintf("GetRefCntBuf(): no pools have been allocated for RefCntBufPoolSet at %p",
			slabs))
	}
	panic(fmt.Sprintf("GetRefCntBuf(): requested buf size %d is larger then the largest pool size %d",
		bufSz, slabs.bufferPoolSizes[sizeCnt-1]))

	// Unreachable
}

// A reference counted list (array) of reference counted memory buffers,
//
// The RefCntBufList itself is reference counted, with Hold() and Release()
// methods.  Upon final release the associated RefCntBuf are Released.
//
// Buf is an array of slices, one per RefCntBuf.  Each slice may represent the
// entire Buf slice of the underlying RefCntBuf or may be a subset.  Changes to
// this Buf slice do not affect the associated RefCntBuf Buf slice and vice
// versa.
//
// RefCntBuf can only be added to the list using the Append() method.  Adding
// or deleting slices to the Buf array is not allowed.
//
// RefCntBufList must come from a RefCntBufListPool type object which supplies
// an empty RefCntBufList.
//
// RefCntBufList is mostly useful for scatter/gather i/o.
//
type RefCntBufList struct {
	RefCntItem
	Bufs       [][]byte
	RefCntBufs []*RefCntBuf
	_          sync.Mutex // insure a RefCntBufList is not copied
}

// Append a reference counted buffer to the list.
//
// This calls Hold() on refCntBuf.  Release() is called on each RefCntBuf on
// the list on the final release of this RefCntBufList.
//
func (bufList *RefCntBufList) AppendRefCntBuf(refCntBuf *RefCntBuf) {
	refCntBuf.Hold()
	if len(bufList.RefCntBufs) != len(bufList.Bufs) {
		panic(fmt.Sprintf(
			"(*RefCntBufList).AppendRefCntBuf(): len(list.RefCntBufs) != len(list.Bufs) (%d != %d) at %p",
			len(bufList.RefCntBufs), len(bufList.Bufs), bufList))
	}
	bufList.RefCntBufs = append(bufList.RefCntBufs, refCntBuf)
	bufList.Bufs = append(bufList.Bufs, refCntBuf.Buf)
}

// Return the sum of the len() of each RefCntBuf
//
func (bufList *RefCntBufList) Length() (length int) {
	bufList.RefCntItem.AssertIsHeld()
	for i := 0; i < len(bufList.Bufs); i++ {
		length += len(bufList.Bufs[i])
	}
	return
}

// Given a reference counted list of RefCntBuf (essentially a scatter/gather
// buffer), a starting offset in that buffer and a length generate a slice of
// byte slices that maps the requested region of the buffer.
//
// Returns:
// bufSlices a slices of byte slices mapping the buffer
// byteCnt   number of bytes mapped
// firstIdx  index of the first RefCntBuf in the buffer list that appears in bufSlices
//
// cnt may be less then length, or even 0, if the scatter/gather buffer contains
// less then offset+length bytes in buffers
//
func (bufList *RefCntBufList) BufListToSlices(offset int, length int) (bufSlices [][]byte, byteCnt int, firstIdx int) {
	var (
		idx       int // current buffer index
		curOffset int // sum of buffer lengths so far
	)

	if offset < 0 || length < 0 {
		panic(fmt.Sprintf("(*RefCntBufList) BufListToSlices(): offset %d or length %d is less then 0",
			offset, length))
	}

	// find the first interesting buffer
	for idx = 0; idx < len(bufList.Bufs); idx++ {
		if offset < curOffset+len(bufList.Bufs[idx]) {

			// offset falls within this buffer; its the first
			bufSlices = make([][]byte, 0, len(bufList.Bufs)-idx)
			firstIdx = idx
			break
		}

		curOffset += len(bufList.Bufs[idx])
	}

	// offset falls within bufList.Bufs[idx] or we've run out of buffers
	// (which means this loop won't be entered)
	byteCnt = 0
	for ; idx < len(bufList.Bufs) && curOffset < offset+length; idx++ {
		slice := bufList.Bufs[idx]

		// if the request starts in the middle then trim the slice
		if offset > curOffset {
			slice = slice[offset-curOffset:]
		}
		// if the slice is too big, trim the tail (and this is the last buf)
		if offset+length < curOffset+len(bufList.Bufs[idx]) {
			slice = slice[0 : length-byteCnt]
		}
		bufSlices = append(bufSlices, slice)
		byteCnt += len(slice)

		curOffset += len(bufList.Bufs[idx])
	}

	return
}

// Append the reference counted buffer(s) on the passed list to this list.
//
// This calls Hold() on each refCntBuf referenced, but not on the list itself.
// Release() is called for each RefCntBuf on this list on the final release of
// this list.
//
func (bufList *RefCntBufList) AppendRefCntBufList(newList *RefCntBufList, offset int, length int) (byteCnt int) {

	if len(newList.RefCntBufs) != len(newList.Bufs) {
		panic(fmt.Sprintf(
			"(*RefCntBufList).AppendRefCntBufList(): len(new.RefCntBufs) != len(new.Bufs) (%d != %d) at %p",
			len(newList.RefCntBufs), len(newList.Bufs), newList))
	}
	if len(bufList.RefCntBufs) != len(bufList.Bufs) {
		panic(fmt.Sprintf(
			"(*RefCntBufList).AppendRefCntBufList(): len(new.RefCntBufs) != len(new.Bufs) (%d != %d) at %p",
			len(bufList.RefCntBufs), len(bufList.Bufs), bufList))
	}

	var (
		bufSlices [][]byte // slice of buffers
		idx       int      // current buffer in bufSlices
		firstIdx  int      // first RefCntBuf in newList to reference
		tgtIdx    int      // newest RefCntBuf in bufList
	)

	// let BufListToSlices() do the hard work, then reference the relevant
	// buffers in newList and copy the slices returned
	bufSlices, byteCnt, firstIdx = newList.BufListToSlices(offset, length)

	tgtIdx = len(bufList.Bufs)
	for idx = 0; idx < len(bufSlices); idx += 1 {
		bufList.AppendRefCntBuf(newList.RefCntBufs[firstIdx+idx])
		bufList.Bufs[tgtIdx+idx] = bufSlices[idx]

	}
	return
}

// Copy bytes out of the buffer list to the target slice, buf and return the
// number of bytes copied.
//
// Copying starts at offset and continues until the RefCntBufList is exhausted
// or buf is full.
//
func (bufList *RefCntBufList) CopyOut(buf []byte, offset int) (cnt int) {
	var (
		bufSlices [][]byte // slice of buffers
		slice     []byte   // current slice to copy
		curOffset int      // sum of buffer lengths so far
	)

	// let BufListToSlices() do the hard work
	bufSlices, cnt, _ = bufList.BufListToSlices(offset, len(buf))

	for _, slice = range bufSlices {
		copy(buf[curOffset:], slice)
		curOffset += len(slice)
	}
	if curOffset != cnt {
		panic(fmt.Sprintf("CopyOut(): logic error: curOffset %d != cnt %d", curOffset, cnt))
	}
	return
}

// A pool of reference counted lists of reference counted buffers.
//
type RefCntBufListPool struct {
	realPool sync.Pool
	_        sync.Mutex // insure a RefCntBufListPool is not copied
}

// Get a pointer to empty RefCntBufList from the pool and return it.
//
// It has a reference count of 1 and should be released with a call to Release().
//
func (listPool *RefCntBufListPool) GetRefCntBufList() *RefCntBufList {
	return listPool.Get().(*RefCntBufList)
}

// Make a pool of lists of reference counted buffers.
//
func RefCntBufListPoolMake() (listPoolp *RefCntBufListPool) {
	listPoolp = &RefCntBufListPool{}

	listPoolp.realPool.New = func() interface{} {
		// Make a new RefCntBufList
		return &RefCntBufList{}
	}

	return
}
