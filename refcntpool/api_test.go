package refcntpool

import (
	"bytes"
	// "fmt"
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
		reqSize        int
		bufSize        int
		poolSizes      []int
		bufToPoolSizes map[int]int
	)

	// Create a set of refCntBufPool holding 1, 8, 64, and 128 Kibyte
	// buffer pools and that buffers of sundry sizes are allocated from
	// correct pool (basically testing the binary search algorithm)
	//
	poolSizes = []int{1024, 8 * 1024, 64 * 1024, 128 * 1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	bufToPoolSizes = map[int]int{
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
	poolSizes = []int{1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	bufToPoolSizes = map[int]int{
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
	poolSizes = []int{1024, 2 * 1024, 3 * 1024, 4 * 1024, 5 * 1024, 6 * 1024, 7 * 1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	bufToPoolSizes = map[int]int{
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
	t.Logf("TestRefCntBufPoolSet(): using %d as random seed\n", syscall.Getpid())

	for numPool := 1; numPool <= 17; numPool++ {
		poolSizes = make([]int, numPool, numPool)
		poolSizes[0] = (rand.Int() % 32) + 1

		// bufPoolSize grows by 1.5 times (on average)
		for i := 1; i < numPool; i++ {
			incr := (rand.Int() % poolSizes[i-1]) + 1
			poolSizes[i] = poolSizes[i-1] + incr
		}
		maxBufPoolSize := poolSizes[numPool-1]

		bufPoolSet = &RefCntBufPoolSet{}
		bufPoolSet.Init(poolSizes)

		// allocate 100 random buffers
		bufs := make(map[*RefCntBuf]int)
		for i := 1; i < 100; i++ {

			reqSize = rand.Int() % (maxBufPoolSize + 1)
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
	poolSizes = []int{1024}
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
	poolSizes = []int{1024, 1024}
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
	poolSizes = []int{2048, 1024}
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
	poolSizesArray := [][]int{
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

// Test reference counted list of Reference Counted Memory Buffers
//
func TestRefCntBufList(t *testing.T) {
	var (
		bufList     *RefCntBufList
		bufListPool *RefCntBufListPool
		bufp        *RefCntBuf
		bufPoolSet  *RefCntBufPoolSet
		bufSlice    []*RefCntBuf
	)

	// This test uses psuedo-random numbers, so log the set of random
	// numbers in use.
	//
	rand.Seed(int64(syscall.Getpid()))
	t.Logf("TestRefCntBufList(): using %d as random seed\n", syscall.Getpid())

	// Create a pool of reference counted lists of reference counted buffers
	bufListPool = RefCntBufListPoolMake()

	// Get and put a RefCntBufList
	bufList = bufListPool.GetRefCntBufList()
	bufList.Release()

	bufList = bufListPool.GetRefCntBufList()
	bufList.Hold()
	bufList.Release()
	bufList.Release()
	if bufList.RefCntItem.refCnt != 0 {
		t.Errorf("bufList still active: after two releases: %v", bufList)
	}

	// Create a set of refCntBufPool holding 1, 8, 64, and 128 Kibyte buffer
	// pools
	poolSizes := []int{1024, 8 * 1024, 64 * 1024}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	// put RefCntBuf onto a RefCntBufList using AppendRefCntBuf() and/or
	// AppendRefCntList()
	// all RefCntBuf are 1 byte long
	bufSlice = make([]*RefCntBuf, 0)
	for testCnt := 0; testCnt < 1024; testCnt += 1 {

		bufSlice = bufSlice[0:0]
		bufList = bufListPool.GetRefCntBufList()

		// upto 13 calls to AppendRefCntBuf() or AppendRefCntBufList()
		bufCnt := 0
		for i := 0; i < rand.Intn(14); i++ {

			if rand.Intn(4) < 3 {

				// 3/4 of the time simply append a buf
				bufp = bufPoolSet.GetRefCntBuf(rand.Intn(64 * 1024))
				bufp.Buf = bufp.Buf[0:1]
				bufSlice = append(bufSlice, bufp)
				bufList.AppendRefCntBuf(bufp)

				bufp.Release()
				bufCnt += 1
				continue
			}

			// 1/4 of the time append a buf list
			bufList2 := bufListPool.GetRefCntBufList()
			bufCnt2 := rand.Intn(6)
			bufSlice2 := populateBufList(bufList2, bufPoolSet, bufCnt2)

			bufSlice = append(bufSlice, bufSlice2...)
			bufList.AppendRefCntBufList(bufList2, 0, bufList2.Length())
			bufCnt += bufCnt2

			bufList2.Release()
		}

		// make sure we have the right number of buffers
		if len(bufSlice) != bufCnt || len(bufList.Bufs) != bufCnt {
			t.Errorf("bufCnt mismatch: bufCnt %d != len(bufSlice) %d || != len(bufList.Bufs) %d\n",
				bufCnt, len(bufSlice), len(bufList.Bufs))
		}

		// verify the buffers are there, in order
		for i := 0; i < bufCnt; i++ {
			if bufSlice[i] != bufList.RefCntBufs[i] {
				t.Errorf("bufp mismatch: bufSlice[%d] = %p != bufList.RefCntBufs[%d] = %p  bufCnt %d\n",
					i, bufSlice[i], i, bufList.RefCntBufs[i], bufCnt)
			}
		}

		// release the buffer list and all the buffers, then verify they are released
		bufList.Release()
		if bufList.RefCntItem.refCnt != 0 {
			t.Errorf("bufList still active: testCnt %d reference count %d\n",
				testCnt, bufList.RefCntItem.refCnt)
		}
		for i := 0; i < bufCnt; i++ {
			if bufSlice[i].RefCntItem.refCnt != 0 {
				t.Errorf("bufp still active: testCnt %d  bufCnt %d  bufSlice[%d]RefCntItem.refCnt %d\n",
					testCnt, bufCnt, i, bufSlice[i].RefCntItem.refCnt)
			}
		}
	}

	testCopyOut(t)
}

// Allocate cnt randomly sized RefCntBuf (upto 64 K) from the passed
// RefCntBufPoolSet and append them the passed RefCntBufList.
//
// Return a slice with pointers to all of the bufs allocated.
//
func populateBufList(bufList *RefCntBufList, bufPoolSet *RefCntBufPoolSet, bufCnt int) (bufSlice []*RefCntBuf) {

	var bufp *RefCntBuf

	bufSlice = make([]*RefCntBuf, 0, bufCnt)
	for i := 0; i < bufCnt; i++ {
		bufp = bufPoolSet.GetRefCntBuf(rand.Intn(64 * 1024))
		bufp.Buf = bufp.Buf[0:1]
		bufSlice = append(bufSlice, bufp)
		bufList.AppendRefCntBuf(bufp)
		bufp.Release()
	}
	return
}

// Test (*RefCntBufList).CopyOut().
//
// The real goal here is to test BufListToSlices() and AppendRefCntList() by
// running through all of their corner cases.  Not to mention CopyOut().
//
// Try and hit lots of interesting patterns including empty buffer lists and buffers.
//
func testCopyOut(t *testing.T) {
	var (
		bufList          *RefCntBufList
		bufListPool      *RefCntBufListPool
		bufp             *RefCntBuf
		bufPoolSet       *RefCntBufPoolSet
		refSlice         []byte
		refSliceMaxBytes int
		verifySlice      []byte
		padBytes         int
	)

	// reserve padBytes before and after so we can can make RefCntBuf larger
	// then needed and then trim them
	padBytes = 4096

	// there's a reference slice and a verify slice
	refSliceMaxBytes = 128*1024 + 2*padBytes
	refSlice = make([]byte, 0, refSliceMaxBytes)
	verifySlice = make([]byte, 0, refSliceMaxBytes)

	// Create a pool of reference counted lists of reference counted buffers
	bufListPool = RefCntBufListPoolMake()

	// Create a set of refCntBufPool holding 1, 8, 32, 64, and 128 Kibyte buffer
	// pools
	poolSizes := []int{1024, 8 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, refSliceMaxBytes}
	bufPoolSet = &RefCntBufPoolSet{}
	bufPoolSet.Init(poolSizes)

	// fill the reference slice with random bytes
	refSlice = refSlice[0:refSliceMaxBytes]
	for i := 0; i < refSliceMaxBytes; i += 1 {
		refSlice[i] = byte(rand.Intn(256))
	}

	for testCnt := 0; testCnt < 1005; testCnt += 1 {

		// testOff -- start at a random offset in refSlice (with at least extra padBytes before)
		// testSize -- size of test slice: range 0 .. len(refSlice) (with padBytes reserved)
		// testSlice is the result
		testOff := rand.Intn(len(refSlice)/2) + padBytes
		testSize := rand.Intn(len(refSlice) - testOff - padBytes + 1)

		testSlice := refSlice[testOff-padBytes : testOff+testSize+padBytes]

		// create the test list of buffers
		bufList = bufListPool.GetRefCntBufList()

		// number of buffers or buffer lists to use: 0 .. 15, heavily
		// biased toward the middle (chance of 0 or 15 is 1 in 216)
		bufCnt := rand.Intn(6) + rand.Intn(6) + rand.Intn(6)

		copyByteCnt := 0
		for bufIdx := 0; bufIdx < bufCnt; bufIdx += 1 {

			// choose the size of the buffer or buffer list to add:
			// range 0 .. space remaining (which may be 0);
			// average is 1/2 space remaining
			size := rand.Intn(testSize - copyByteCnt + 1)

			// 50% of the time append buffer, 50% apend a buffer list;
			// add some extra bytes to the buffer or buffer list so we
			// can verify that trimming them off works
			if rand.Intn(2) == 0 {
				headPadBytes := rand.Intn(padBytes + 1)
				tailPadBytes := rand.Intn(padBytes + 1)
				paddedSize := size + headPadBytes + tailPadBytes

				bufp = bufPoolSet.GetRefCntBuf(paddedSize)
				bufp.Buf = bufp.Buf[0:paddedSize]
				copy(bufp.Buf, testSlice[padBytes-headPadBytes+copyByteCnt:])
				bufList.AppendRefCntBuf(bufp)
				bufp.Release()

				// trim the slice (in the list) to the size required
				bufList.Bufs[len(bufList.Bufs)-1] = bufp.Buf[headPadBytes : headPadBytes+size]
				copyByteCnt += size

			} else {
				bufList2 := bufListPool.GetRefCntBufList()

				randBufCnt := rand.Intn(4) + 1
				copySliceToBuflist(t, bufList2, bufPoolSet, randBufCnt, 2*padBytes,
					testSlice[copyByteCnt:copyByteCnt+size+2*padBytes])

				byteCnt := bufList.AppendRefCntBufList(bufList2, padBytes, size)
				bufList2.Release()
				copyByteCnt += byteCnt
			}
		}
		if copyByteCnt > testSize {
			t.Errorf("testCopyOut(): logic error: copyByteCnt %d > testSize %d", copyByteCnt, testSize)
		}

		// verify bufList length two ways
		byteCnt := bufList.Length()
		if copyByteCnt != byteCnt {
			t.Errorf("testCopyOut(): bufList length %d != copyByteCnt %d", byteCnt, copyByteCnt)

			t.Logf("testCnt %d:  BufList.Length %d  copyByteCnt %d  testSize %d: buf len[]:",
				testCnt, bufList.Length(), copyByteCnt, testSize)
			for i := 0; i < len(bufList.Bufs); i += 1 {
				t.Logf("bufList.Bufs[%d] len %5d", i, len(bufList.Bufs[i]))
			}
		}

		_, byteCnt, _ = bufList.BufListToSlices(0, copyByteCnt)
		if copyByteCnt != byteCnt {
			t.Errorf("testCopyOut(): bufList.BufListToSlices().cnt %d != copyByteCnt %d", byteCnt, copyByteCnt)
		}

		// compare the contents of bufList with the original refSlice
		verifySlice = verifySlice[0:copyByteCnt]
		cnt := bufList.CopyOut(verifySlice, 0)
		if cnt != copyByteCnt {
			t.Errorf("testCopyOut(): bufList.CopyOut().cnt %d != copyByteCnt %d", cnt, copyByteCnt)
		}
		if bytes.Compare(verifySlice, refSlice[testOff:testOff+copyByteCnt]) != 0 {
			t.Errorf("testCopyOut(): verifySlice and refSlice do not match!")
		}

		// now compare some random sub-ranges
		for i := 0; copyByteCnt > 0 && i < 16; i += 1 {
			off := rand.Intn(copyByteCnt)
			byteCnt := rand.Intn(copyByteCnt - off + 1)

			verifySlice = verifySlice[0:byteCnt]
			cnt := bufList.CopyOut(verifySlice, off)
			if cnt != byteCnt {
				t.Errorf("testCopyOut(): bufList.CopyOut(off %d).cnt %d != copyByteCnt %d",
					off, cnt, copyByteCnt)
			}
			if bytes.Compare(verifySlice, refSlice[testOff+off:testOff+off+byteCnt]) != 0 {
				t.Errorf("testCopyOut(): verifySlice and refSlice[%d:%d] do not match!",
					off, byteCnt)
			}
		}

		bufList.Release()
	}
}

// Given a region of memory, memBuf, add bufCnt reference counted memory buffers
// to bufList and copy its contents to them.  The size of the buffers is random
// and they have a maximum size of bufBytesMax, so if bufCnt is small they will
// not copy the entire region (but its random)
//
// Return the number of bytes memory that were copied (mapped).
//
func copySliceToBuflist(t *testing.T, bufList *RefCntBufList, bufPoolSet *RefCntBufPoolSet,
	bufCnt int, bufSizeMax int, memBuf []byte) (byteCnt int) {

	var (
		bufp    *RefCntBuf
		bufSize int
	)

	byteCnt = 0
	for i := 0; i < bufCnt; i += 1 {

		// final bufSize: 0 .. MAX(bufSizeMax, space remaining (which may be 0))
		// average is 1/2 space remaining
		bufSize = len(memBuf) - byteCnt
		if bufSize > bufSizeMax {
			bufSize = bufSizeMax
		}
		bufSize := rand.Intn(bufSize + 1)

		bufp = bufPoolSet.GetRefCntBuf(bufSize)
		bufp.Buf = bufp.Buf[0:bufSize]
		copy(bufp.Buf, memBuf[byteCnt:])
		// t.Logf("copySliceToBuflist: i %d  bufSize %4d  len(bufp.Buf) %4d  byteCnt %4d  len(memBuf) %4d",
		// 	i, bufSize, len(bufp.Buf), byteCnt, len(memBuf))

		bufList.AppendRefCntBuf(bufp)
		byteCnt += bufSize
		bufp.Release()
	}

	return
}
