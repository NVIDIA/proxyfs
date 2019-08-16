# sortedmap

Go package providing sorted maps implemented as either a Left-Leaning Red-Black Tree (in memory) or a B+Tree (pageable)

## API Reference

```
type Key interface{}
type Value interface{}

type Compare func(key1 Key, key2 Key) (result int, err error)

func CompareInt(key1 Key, key2 Key) (result int, err error)
func CompareUint32(key1 Key, key2 Key) (result int, err error)
func CompareUint64(key1 Key, key2 Key) (result int, err error)
func CompareString(key1 Key, key2 Key) (result int, err error)
func CompareByteSlice(key1 Key, key2 Key) (result int, err error)
func CompareTime(key1 Key, key2 Key) (result int, err error)

type SortedMap interface {
	BisectLeft(key Key) (index int, found bool, err error)  // Returns index of matching key:value pair or, if no match, index is to key:value just before where this key would go
	BisectRight(key Key) (index int, found bool, err error) // Returns index of matching key:value pair or, if no match, index is to key:value just after where this key would go
	DeleteByIndex(index int) (ok bool, err error)
	DeleteByKey(key Key) (ok bool, err error)
	Dump() (err error)
	GetByIndex(index int) (key Key, value Value, ok bool, err error)
	GetByKey(key Key) (value Value, ok bool, err error)
	Len() (numberOfItems int, err error)
	PatchByIndex(index int, value Value) (ok bool, err error)
	PatchByKey(key Key, value Value) (ok bool, err error)
	Put(key Key, value Value) (ok bool, err error)
	Validate() (err error)
}

type DumpCallbacks interface {
	DumpKey(key Key) (keyAsString string, err error)
	DumpValue(value Value) (valueAsString string, err error)
}

type LLRBTree interface {
	SortedMap
	Reset()
}

type LLRBTreeCallbacks interface {
	DumpCallbacks
}

func NewLLRBTree(compare Compare, callbacks LLRBTreeCallbacks) (tree LLRBTree)

var OnDiskByteOrder = cstruct.LittleEndian

type LayoutReport map[uint64]uint64

type BPlusTree interface {
	SortedMap
	FetchLocation() (rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64)
	FetchLayoutReport() (layoutReport LayoutReport, err error)
	Flush(andPurge bool) (rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, err error)
	Purge(full bool) (err error)
	Touch() (err error)
	TouchItem(thisItemIndexToTouch uint64) (nextItemIndexToTouch uint64, err error)
	Prune() (err error)
	Discard() (err error)
}

type BPlusTreeCallbacks interface {
	DumpCallbacks
	GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error)
	PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error)
	DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error)
	PackKey(key Key) (packedKey []byte, err error)
	UnpackKey(payloadData []byte) (key Key, bytesConsumed uint64, err error)
	PackValue(value Value) (packedValue []byte, err error)
	UnpackValue(payloadData []byte) (value Value, bytesConsumed uint64, err error)
}

type BPlusTreeCacheStats struct {
	EvictLowLimit  uint64
	EvictHighLimit uint64
	CleanLRUItems  uint64
	DirtyLRUItems  uint64
	CacheHits      uint64
	CacheMisses    uint64
}

type BPlusTreeCache interface {
	Stats() (bPlusTreeCacheStats *BPlusTreeCacheStats)
	UpdateLimits(evictLowLimit uint64, evictHighLimit uint64)
}

func NewBPlusTreeCache(evictLowLimit uint64, evictHighLimit uint64) (bPlusTreeCache BPlusTreeCache)

func NewBPlusTree(maxKeysPerNode uint64, compare Compare, callbacks BPlusTreeCallbacks, bPlusTreeCache BPlusTreeCache) (tree BPlusTree)

func OldBPlusTree(rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, compare Compare, callbacks BPlusTreeCallbacks, bPlusTreeCache BPlusTreeCache) (tree BPlusTree, err error)
```

## Contributors

 * ed@swiftstack.com

## License

See the included LICENSE file
