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

type LLRBTree interface {
	SortedMap
}

func NewLLRBTree(compare sorted_api.Compare) (tree LLRBTree)

type BPlusTree interface {
	SortedMap
	Flush(andPurge bool) (rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, err error)
	Purge() (err error)
}

type BPlusTreeCallbacks interface {
	GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error)
	PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error)
	PackKey(key sorted_api.Key) (packedKey []byte, err error)
	UnpackKey(payloadData []byte) (key sorted_api.Key, bytesConsumed uint64, err error)
	PackValue(value sorted_api.Value) (packedValue []byte, err error)
	UnpackValue(payloadData []byte) (value sorted_api.Value, bytesConsumed uint64, err error)
}

func NewBPlusTree(maxKeysPerNode uint64, compare Compare, callbacks BPlusTreeCallbacks) (tree BPlusTree)
func OldBPlusTree(rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, compare Compare, callbacks BPlusTreeCallbacks) (tree BPlusTree)
```

## Contributors

 * ed@swiftstack.com

## License

TBD
