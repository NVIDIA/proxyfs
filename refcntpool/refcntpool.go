package refcntpool

// refcntpool provides functions and interfaces to implement pools of reference
// counted objects, where the object is returned to the pool when its reference
// count drops to zero.
//
// Users of refcountpool must supply an object implementing the RefCntItemPool
// interface which makes objects of the desired type and maintains a pool
// for holding inactive objects of that type.
//
// An implementation of pools of reference counted memory buffers is also
// provided, and also serves as an example.  Use RefCntBufPoolMake(bufSz uint64)
// to create a pool of reference counted memory buffers of size bufSz.

import (
	"fmt"
	"sync/atomic"
)

// RefCntItem implementation
//
func (item *RefCntItem) Hold() {
	newCnt := atomic.AddInt32(&item.refCnt, 1)
	if newCnt < 2 {
		panic(fmt.Sprintf("RefCntItem.Hold(): item %T at %p was not held when called: newCnt %d",
			item, item, newCnt))
	}
}

func (item *RefCntItem) Release() {
	// Decrement cnt by 1.  Even if two threads do this concurrently,
	// only one will have newcnt == 0
	newCnt := atomic.AddInt32(&item.refCnt, -1)

	if newCnt == 0 {
		item.pool.put(item.cntItem)
	} else if newCnt < 0 {
		panic(fmt.Sprintf("RefCntItem.Release(): item was not held when called: newCnt %d", newCnt))
	}
}

func (item *RefCntItem) AssertIsHeld() {
	// Decrement cnt by 1.  Even if two threads do this concurrently,
	// only one will have newcnt == 0
	refCnt := atomic.LoadInt32(&item.refCnt)
	if refCnt < 1 {
		panic(fmt.Sprintf("(*RefCntItem).AssertIsHeld(): refCnt %d < 1 for RefCntItem at %p",
			refCnt, item))
	}
}

// This Init() routine is awkward because it cannot be sub-classed.  In other
// words, if your object needs custom initialization it cannot supply its own
// Init() routine and then call this one to initialize item.refCnt.
//
// Instead, there are two choices: implement your own RefCntItemPooler type
// object which does the initialization (this is the approach taken by
// RefCntBufPool) or implement your own RefCntItemer type object with its own
// Init() routine.
//
func (item *RefCntItem) Init(pool RefCntItemPooler, cntItem interface{}) {
	newCnt := atomic.AddInt32(&item.refCnt, 1)
	if newCnt != 1 {
		panic(fmt.Sprintf("RefCntItem.Init(): item %T at %p in pool %T at %p was not free: newCnt %d",
			item, item, item.pool, item.pool, newCnt))
	}
	item.pool = pool
	item.cntItem = cntItem
}

// RefCntPool implementation
//
func (refCntPool *RefCntItemPool) Get() (item interface{}) {
	item = refCntPool.itemPool.Get()
	if item == nil {
		item = refCntPool.New()
	}

	refCntItem := item.(RefCntItemer)
	refCntItem.Init(refCntPool, item)

	return
}

func (refCntPool *RefCntItemPool) put(item interface{}) {
	refCntPool.itemPool.Put(item)
}

// Get a RefCntBuf from the pool.
//
// The caller must use a type assertion like (*refCntBufPool).Get().(*RefCntBuf)
// to get a pointer to the memory buffer.
//
func (poolp *RefCntBufPool) Get() (item interface{}) {

	// get a buffer
	item = poolp.bufPool.Get()

	// reinitialize the buffer
	bufp := item.(*RefCntBuf)
	bufp.Init(poolp, bufp)
	bufp.Buf = bufp.origBuf

	return
}

func (poolp *RefCntBufPool) put(item interface{}) {

	// clear bufP.Buf just in case it points to a different buffer (to speed
	// up garbage collection)
	bufp := item.(*RefCntBuf)
	bufp.Buf = nil

	poolp.bufPool.Put(item)
	return
}

// Get a RefCntBufList from the pool.
//
// The caller must use a type assertion like (*refCntBufPool).Get().(*RefCntBufList)
// to get a pointer to the list of RefCntBuf.
//
func (listPoolp *RefCntBufListPool) Get() (item interface{}) {

	// get a buffer
	item = listPoolp.realPool.Get()

	// (re)initialize the list of buffers
	bufListp := item.(*RefCntBufList)

	bufListp.Init(listPoolp, bufListp)
	bufListp.Bufs = bufListp.Bufs[0:0]
	bufListp.RefCntBufs = bufListp.RefCntBufs[0:0]

	return
}

func (listPoolp *RefCntBufListPool) put(item interface{}) {

	bufListp := item.(*RefCntBufList)
	if len(bufListp.RefCntBufs) != len(bufListp.Bufs) {
		panic(fmt.Sprintf(
			"(*RefCntBufListPool).put(): len(bufListp.RefCntBufs) != len(bufListp.Buf) (%d != %d) at %p",
			len(bufListp.RefCntBufs), len(bufListp.Bufs), bufListp))
	}

	for i := 0; i < len(bufListp.RefCntBufs); i++ {
		bufListp.RefCntBufs[i].Release()

		// get rid of references to enhance garbage collection of unused
		// buffers
		bufListp.RefCntBufs[i] = nil
		bufListp.Bufs[i] = bufListp.Bufs[i][0:0]
	}
	bufListp.RefCntBufs = bufListp.RefCntBufs[0:0]

	listPoolp.realPool.Put(item)
	return
}
