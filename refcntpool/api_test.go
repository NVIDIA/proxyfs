package refcntpool

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// Invoke the passed function, testFunc, which is typically a closure and
// return the the panic value if it panics or nil if it does not.
//
// testFunc is typcially a closure with that calls another function with the
// specified arguments.
//
func catchPanic(testFunc func()) (panicErr interface{}) {

	// set a trap to catch panics
	defer func() {
		panicErr = recover()
		if panicErr != nil {
			return
		}
	}()

	// invoke the function to be tested
	testFunc()

	// apparently it didn't panic, so return nil
	return
}

// basic tests of RefCntItem/RefCntPool
//
func TestRefCntItem(t *testing.T) {
	var (
		refCntItemp                        *RefCntItem
		refCntPool                         *RefCntItemPool
		itemIface0, itemIface1, itemIface2 RefCntItemer
	)
	refCntPool = &RefCntItemPool{
		New: func() interface{} {
			itemp := &RefCntItem{}
			return itemp
		},
	}
	refCntItemp = refCntPool.Get().(*RefCntItem)
	itemIface0 = refCntPool.Get().(RefCntItemer)
	itemIface1 = refCntPool.Get().(RefCntItemer)
	itemIface2 = refCntPool.Get().(RefCntItemer)

	// hold and release don't panic
	refCntItemp.Hold()
	refCntItemp.Hold()
	refCntItemp.Release()
	refCntItemp.Release()

	// final release
	refCntItemp.Release()

	// Items must be different!
	if itemIface0 == itemIface1 || itemIface0 == itemIface2 || itemIface1 == itemIface2 {
		t.Errorf("refCntPool.Get() returned two items with the same pointer!")
	}

	// just release the other items
	itemIface0.Release()
	itemIface1.Release()
	itemIface2.Release()

	// if we get another item, odds are it is one of the ones released
	newItemp := refCntPool.Get().(*RefCntItem)
	if newItemp != itemIface0 && newItemp != itemIface1 && newItemp != itemIface2 && newItemp != refCntItemp {
		t.Errorf("expected refCntPool.Get() to return one of the items that was released")
		t.Logf("newItemp %p  itemIface0 %p  itemIface1 %p  itemIface2 %p  refCntItemp %p",
			newItemp, itemIface0, itemIface1, itemIface2, refCntItemp)
		t.Logf("This test may fail occasional due to varagries of garbage collection.")
	}

	// types are convertible and refer to the same object (second Release()
	// doesn't panic becaue we did a Hold() on the interface)
	itemIface0 = newItemp
	refCntItemp = itemIface0.(*RefCntItem)
	refCntItemp.Hold()
	itemIface0.Release()
	newItemp.Release()

	// test error cases that should cause a panic
	//
	var (
		testFunc func()
		panicErr interface{}
		panicStr string
	)

	// releasing an item once too often should panic (throw in an extra hold
	// for good measure)
	itemIface0 = refCntPool.Get().(RefCntItemer)
	itemIface0.Hold()

	// a test function to release the item
	testFunc = func() {
		itemIface0.Release()
	}

	// the first two releases should be fine
	panicErr = catchPanic(testFunc)
	if panicErr != nil {
		t.Errorf("itemIface0.Release() paniced for no good reason")
	}
	panicErr = catchPanic(testFunc)
	if panicErr != nil {
		t.Errorf("itemIface0.Release() paniced for no good reason")
	}

	// but the third release is the panic-inducing charm
	panicErr = catchPanic(testFunc)
	if panicErr == nil {
		t.Errorf("itemIface0.Release() failed to panic when over-released")
	}
	panicStr = panicErr.(string)
	if !strings.Contains(panicStr, "RefCntItem.Release(): item was not held when called") {
		t.Errorf("itemIface0.Release() failed to panic with correct error when over-released")
	}

	// similarly, we can't hold a fully released item
	testFunc = func() {
		itemIface0.Hold()
	}

	panicErr = catchPanic(testFunc)
	if panicErr == nil {
		t.Errorf("itemIface0.Hold() failed to panic for unreferenced item")
	}
	panicStr = panicErr.(string)
	if !strings.Contains(panicStr, "was not held when called") {
		t.Errorf("itemIface0.Hold() failed to panic with correct error for undeserved Hold()")
	}
}

// Test RefCntPooler, i.e. creating our own pool of objects with different
// Get()/put() behavior
//
// Define a reference counted pool of "inodes" (used for the above tests).  This
// really just tests that the API works to let us define new pool types.
//
type Inode struct {
	RefCntItem
	iNum     int64
	permBits uint32
}

type InodePool struct {
	pool sync.Pool
}

func (inodePool *InodePool) Get() interface{} {
	item := inodePool.pool.Get()
	inodep := item.(*Inode)

	// init the RefCntItem stuff
	inodep.Init(inodePool, inodep)

	// init the inode identity (currently all inodes have the same iNum)
	inodep.iNum = 963
	inodep.permBits = 0644
	return inodep
}

func (inodePool *InodePool) put(item interface{}) {
	ifacep := item
	inodep := ifacep.(*Inode)

	// we could put the inode on a freelist and let it keeps its identity
	// but that's a lot of code for a unit test.  instead, wipe its identity
	// and put it in the pool for reuse.
	inodep.iNum = 0xffffffff
	inodePool.pool.Put(inodep)
}

func TestRefCntPooler(t *testing.T) {

	inodePool := &InodePool{
		pool: sync.Pool{
			New: func() interface{} {
				inodep := &Inode{
					iNum:     0,
					permBits: 0,
				}
				return inodep
			},
		},
	}

	// get 3 inodes and then put them back
	inode1p := inodePool.Get().(*Inode)
	inode2p := inodePool.Get().(*Inode)
	inode3p := inodePool.Get().(*Inode)

	inode1p.Release()
	inode1p = nil
	inode2p.Release()
	inode2p = nil
	inode3p.Release()
	inode3p = nil

}

// Test Reference Counted Memory Buffers (RefCntBuf and RefCntBufPool).
//
func TestRefCntBuf(t *testing.T) {

	var (
		bufPool64k *RefCntBufPool = RefCntBufPoolMake(65536)
		bufPool8k  *RefCntBufPool = RefCntBufPoolMake(8192)
		bufp       *RefCntBuf
	)
	bufp = bufPool64k.Get().(*RefCntBuf)
	if len(bufp.Buf) != 0 {
		t.Errorf("RefCntBufPool64k().Get returned a buffer with len() %d != 0",
			len(bufp.Buf))
	}
	if cap(bufp.Buf) != 65536 {
		t.Errorf("RefCntBufPool64k().Get returned a buffer with cap() %d != 65536",
			cap(bufp.Buf))
	}
	bufp.Release()

	bufp = bufPool8k.Get().(*RefCntBuf)
	if len(bufp.Buf) != 0 {
		t.Errorf("RefCntBufPool64k().Get returned a buffer with len() %d != 0",
			len(bufp.Buf))
	}
	if cap(bufp.Buf) != 8192 {
		t.Errorf("RefCntBufPool64k().Get returned a buffer with cap() %d != 8192",
			cap(bufp.Buf))
	}
	bufp.Release()

	// Get 20 buffers, record their addresses, release them, then get them
	// and release them again
	bufs := make(map[*RefCntBuf]int)
	for i := 0; i < 20; i++ {
		bufp = bufPool64k.Get().(*RefCntBuf)

		_, ok := bufs[bufp]
		if ok {
			t.Errorf("bufPool64k.Get(): returned the same buffer twice: %T %v", bufp, bufp)
		}
		bufs[bufp] = 1
	}

	for bufp, _ = range bufs {
		bufs[bufp] -= 1
		bufp.Release()
	}

	// get them again and keep a count of how many were seen previously
	reuseCnt := 0
	for i := 0; i < 20; i++ {
		bufp = bufPool64k.Get().(*RefCntBuf)

		cnt, ok := bufs[bufp]
		if ok && cnt > 0 {
			t.Errorf("bufPool64k.Get() 2nd try: returned the same buffer twice: %T %v", bufp, bufp)
		}
		if ok {
			reuseCnt += 1
			bufs[bufp] += 1
		} else {
			bufs[bufp] = 1
		}
	}

	if reuseCnt != 20 {
		t.Errorf("bufPool64k.Get() only reused %d of 20 buffers released to it", reuseCnt)
		t.Logf("This test may fail occasional due to varagries of garbage collection.")
	}
}

// Test Reference Counted Memory Buffer Pool Sets (RefCntBufPoolSet).
//
func TestRefCntBufPoolSet(t *testing.T) {
	var (
		bufPoolSet     *RefCntBufPoolSet
		bufp           *RefCntBuf
		reqSize        uint64
		bufSize        uint64
		poolSizes      []uint64
		bufToPoolSizes map[uint64]uint64
	)

	// Create a set of refCntBufPool holding 1, 8, 64, and 128 Kibyte
	// buffer pools and that buffers of sundry sizes are allocated from
	// correct pool (basically testing the binary search algorithm)
	//
	poolSizes = []uint64{1024, 8 * 1024, 64 * 1024, 128 * 1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	bufToPoolSizes = map[uint64]uint64{
		0:          1024,
		512:        1024,
		1024:       1024,
		1025:       8192,
		2000:       8192,
		4097:       8192,
		8192:       8192,
		8193:       64 * 1024,
		16388:      64 * 1024,
		32777:      64 * 1024,
		65535:      64 * 1024,
		65536:      64 * 1024,
		65537:      128 * 1024,
		99999:      128 * 1024,
		128000:     128 * 1024,
		128 * 1024: 128 * 1024,
	}
	for reqSize, bufSize = range bufToPoolSizes {
		bufp = bufPoolSet.GetRefCntBuf(reqSize)
		if len(bufp.Buf) != 0 {
			t.Errorf("bufPoolSet.GetRefCntBuf(%d) returned a buffer with len() %d != 0",
				reqSize, len(bufp.Buf))
		}
		if cap(bufp.Buf) != int(bufSize) {
			t.Errorf("bufPoolSet.GetRefCntBuf(%d) returned a buffer with cap() %d != %d",
				reqSize, cap(bufp.Buf), bufSize)
		}
		bufp.Release()
	}

	// Test search algorithm when there's only one buffer size
	//
	poolSizes = []uint64{1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	bufToPoolSizes = map[uint64]uint64{
		0:    1024,
		512:  1024,
		1024: 1024,
	}
	for reqSize, bufSize = range bufToPoolSizes {
		bufp = bufPoolSet.GetRefCntBuf(reqSize)
		if len(bufp.Buf) != 0 {
			t.Errorf("bufPoolSet.GetRefCntBuf(%d) returned a buffer with len() %d != 0",
				reqSize, len(bufp.Buf))
		}
		if cap(bufp.Buf) != int(bufSize) {
			t.Errorf("bufPoolSet.GetRefCntBuf(%d) returned a buffer with cap() %d != %d",
				reqSize, cap(bufp.Buf), bufSize)
		}
		bufp.Release()
	}

	// Test search algorithm when there's an odd number of sizes (seven)
	//
	poolSizes = []uint64{1024, 2 * 1024, 3 * 1024, 4 * 1024, 5 * 1024, 6 * 1024, 7 * 1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	bufToPoolSizes = map[uint64]uint64{
		0:    1024,
		512:  1024,
		1024: 1024,
		1025: 2048,
		2000: 2048,
		2047: 2048,
		2048: 2048,
		2049: 3072,
		3000: 3072,
		3071: 3072,
		3072: 3072,
		3073: 4096,
		4096: 4096,
		4097: 5120,
		5000: 5120,
		5120: 5120,
		5121: 6144,
		5122: 6144,
		6000: 6144,
		6140: 6144,
		6144: 6144,
		6145: 7168,
		7000: 7168,
		7168: 7168,
	}
	for reqSize, bufSize = range bufToPoolSizes {
		bufp = bufPoolSet.GetRefCntBuf(reqSize)
		if len(bufp.Buf) != 0 {
			t.Errorf("bufPoolSet.GetRefCntBuf(%d) returned a buffer with len() %d != 0",
				reqSize, len(bufp.Buf))
		}
		if cap(bufp.Buf) != int(bufSize) {
			t.Errorf("bufPoolSet.GetRefCntBuf(%d) returned a buffer with cap() %d != %d",
				reqSize, cap(bufp.Buf), bufSize)
		}
		bufp.Release()
	}

	// Random tests of the search algorithm varying the number of
	// RefCntBufPool, their sizes, and the request sizes
	//
	rand.Seed(int64(syscall.Getpid()))
	fmt.Printf("TestRefCntBufPoolSet(): using %d as random seed\n", syscall.Getpid())

	for numPool := 1; numPool <= 23; numPool++ {
		poolSizes = make([]uint64, numPool, numPool)
		poolSizes[0] = (rand.Uint64() % 32) + 1

		// bufPoolSize grows by 1.5 times (on average)
		for i := 1; i < numPool; i++ {
			incr := (rand.Uint64() % poolSizes[i-1]) + 1
			poolSizes[i] = poolSizes[i-1] + incr
		}
		maxBufPoolSize := poolSizes[numPool-1]

		bufPoolSet = &RefCntBufPoolSet{}
		bufPoolSet.Init(poolSizes)

		// allocate 10,000 random buffers
		bufs := make(map[*RefCntBuf]int)
		for i := 1; i < 10000; i++ {

			reqSize = rand.Uint64() % (maxBufPoolSize + 1)
			bufp = bufPoolSet.GetRefCntBuf(reqSize)

			// did we get the same buffer twice?
			_, ok := bufs[bufp]
			if ok {
				t.Errorf("TestRefCntBufPoolSet(randalloc): returned the same buffer twice: %T %v",
					bufp, bufp)
			}
			bufs[bufp] = 1

			// check the buffer came from the correct pool
			for pool := 0; pool < numPool; pool++ {
				if reqSize <= poolSizes[pool] {
					if cap(bufp.Buf) != int(poolSizes[pool]) {
						t.Errorf("bufPoolSet.GetRefCntBuf(randalloc) returned a buffer"+
							" for reqSize %d with cap() %d != %d",
							reqSize, cap(bufp.Buf), poolSizes[pool])
					}
					break
				}
			}
		}

		// instead of releasing the buffers we could just let them be
		// garbage collected, but let's release them anyway ...
		for bufp, _ = range bufs {
			bufp.Release()
		}
	}

	// test some panic scenarios
	var (
		testFunc func()
		panicErr interface{}
		panicStr string
	)

	// must Init() a RefCntBufPoolSet before using it
	bufPoolSet = &RefCntBufPoolSet{}
	testFunc = func() {
		_ = bufPoolSet.GetRefCntBuf(reqSize)
	}

	panicErr = catchPanic(testFunc)
	if panicErr == nil {
		t.Errorf("bufPoolSet.GetRefCntBuf() failed to panic when unitialized")
	}
	panicStr = panicErr.(string)
	if !strings.Contains(panicStr, "no pools have been allocated for RefCntBufPoolSet") {
		t.Errorf("bufPoolSet.GetRefCntBuf() failed to panic with correct error for unitialized")
	}

	// don't Init() a RefCntBufPoolSet twice
	poolSizes = []uint64{1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)
	testFunc = func() {
		bufPoolSet.Init(poolSizes)
	}

	panicErr = catchPanic(testFunc)
	if panicErr == nil {
		t.Errorf("bufPoolSet.Init() failed to panic when itialized twice")
	}
	panicStr = panicErr.(string)
	if !strings.Contains(panicStr, "called more than once") {
		t.Errorf("bufPoolSet.Init() failed to panic with correct error when called twice")
	}

	// don't buffer pool sizes must be monotonically increasing
	poolSizes = []uint64{1024, 1024}
	bufPoolSet = &RefCntBufPoolSet{}
	testFunc = func() {
		bufPoolSet.Init(poolSizes)
	}

	panicErr = catchPanic(testFunc)
	if panicErr == nil {
		t.Errorf("bufPoolSet.Init() failed to panic when buffer pool size repeates")
	}
	panicStr = panicErr.(string)
	if !strings.Contains(panicStr, "size not increasing") {
		t.Errorf("bufPoolSet.Init() failed to panic with correct error when pool size repeats")
	}

	// don't buffer pool sizes must be monotonically increasing
	poolSizes = []uint64{2048, 1024}
	bufPoolSet = &RefCntBufPoolSet{}
	testFunc = func() {
		bufPoolSet.Init(poolSizes)
	}

	panicErr = catchPanic(testFunc)
	if panicErr == nil {
		t.Errorf("bufPoolSet.Init() failed to panic when buffer pool size decreases")
	}
	panicStr = panicErr.(string)
	if !strings.Contains(panicStr, "size not increasing") {
		t.Errorf("bufPoolSet.Init() failed to panic with correct error when pool size decreases")
	}

	// ask for a buf larger then the largest size when the RefCntBufPoolSet
	// as 1, 2, or 3 pool sizes
	poolSizesArray := [][]uint64{
		{1024},
		{512, 1024},
		{256, 512, 1024},
	}
	for _, poolSizes = range poolSizesArray {

		bufPoolSet = &RefCntBufPoolSet{}
		bufPoolSet.Init(poolSizes)
		testFunc = func() {
			bufPoolSet.GetRefCntBuf(1025)
		}

		panicErr = catchPanic(testFunc)
		if panicErr == nil {
			t.Errorf("bufPoolSet.GetRefCntBuf(1025) poolSize %v failed to panic when request too big",
				poolSizes)
		}
		panicStr = panicErr.(string)
		if !strings.Contains(panicStr, "larger then the largest pool size") {
			t.Errorf("bufPoolSet.GetRefCntBuf(1025) poolSize %v failed to panic with correct error",
				poolSizes)
		}
	}
}
