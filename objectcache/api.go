// The itemcache package provides a thread-safe cache to hold items (values)
// associated with keys. An item can only be associated with one key.  Items can
// be clean or dirty.  Dirty items are "written" before they are discarded.
//
// The cache can be limited to a bounded amount of memory and it flushes dirty
// items and/or discards clean items when it reaches the limit.

package itemcache

// Just ignore this for the time being, other then noting that a pointer to one
// of these gets passed through requests into the keyed cache and down into the
// callbacks.
//
type ReqContext interface {
}

// Items in the cache are identified by a key, which is unique for each
// item.  The key must support the following operations:
//
//   o Equal() -- returns true if the two keys refer to the same item
//   o Hash() -- return a hash value based on the key (two keys that are equal
//     must have the same hash value)
//   o SizeOf() -- return memory used by the key (to track memory consumption);
//     all keys are assumed to be the same size.
//
type Key interface {
	IsEqual(key2 *Key) bool
	GetHash() uint64
	SizeOf() uint64
}

// Items in the cache are "allocated" by calling a NewItem function supplied
// by the client when it created the cache.  New items do not have an
// indentity until they are assigned one by a lookup in the cache.
//
// Items must support the following operations.  These methods are used
// exclusively by the itemcache; clients who get an item from the cache
// should not call them, instead they should call methods provided by the cache
// to flush or invalidate items:
//
//   o Read() -- called to initialize an item when its assigned an identity;
//     it can block arbitrarity but cannot call into this instance of the
//     cache.
//   o Write() -- called to flush the contents of a dirty item; it can
//     block arbitrarity but cannot call into this instance of the cache.
//   o Inval() -- called to destroy the identity of an item before
//     assigning it a new identity; it can block indefinitely but cannot
//     call back into this instance of the cache (key is the old identity
//     of the item)
//   o Free() -- called when the cache is no longer referencing the item;
//     it cannot call back into this instance of the cache (I'm not sure that
//     this method makes sense in Go).
//   o SizeOf() -- return memory used by the item (to track memory consumption);
//     all items are assumed to be the same size.
//
// Note: perhaps the Read() and Write() operations should be asynchronous.
//
type Item interface {
	Read(cntxt *ReqContext, key *Key) (err error)
	Write(cntxt *ReqContext, key *Key) (err error)
	Inval(cntxt *ReqContext, key *Key)
	Free(cntxt *ReqContext)
	SizeOf() uint64
}

// CachedItem holds the information that itemcache uses to manage an item
// stored in the cache.  An item managed by the cache must include an embedded
// pointer to this interface, which it must initialize with a pointer passed to
// the NewItem function supplied by the client of the cache (so when an item
// is created by NewItem() this interface is initialized).
//
// CachedItem must support the following methods. These may be invoked by a
// client that has a hold on the item.  Successful lookup of an item in the
// cache returns a pointer to a held item.
//
//   o Hold() -- get an additional hold on the item
//   o Release() -- release a hold on the item; once all holds on an item
//     are released the item should not be referenced again
//   o MarkDirty() -- mark a held item dirty; dirty items are cleaned
//     (written) by the cache before invalidation unless a forced invalidation
//     is done
//   o IsDirty() -- returns true if the item is dirty
//   o Flush() -- cause a dirty item to be flushed via a call to Write();
//     panics if the item is not dirty
//
// If Flush() fails because the corresponding Write() fails then the item is
// marked dirty.
//
type CachedItem interface {
	Hold(cntxt *ReqContext)
	Release(cntxt *ReqContext)
	MarkDirty(cntxt *ReqContext)
	IsDirty(cntxt *ReqContext) bool
	Flush(cntxt *ReqContext) (err error)
}

// Cache caches items identified by keys, upto a specified maximum amount of
// memory.  Items in the cache are accessed by calling Lookup() with the
// desired key and Lookup() returns a pointer to an item with a hold on it.
// When the caller is done with the item it must release the hold.
//
// If the item is not currently cached, Cache will create a new item
// corresponding to the key or reuse an existing one.  It is initialized by a
// call to Item.Read() before return.  Cache locks the item duing the call
// to Item.Read() so only one item with a given key is present in the cache
// and no thread can access it until it is initialized.
//
// There is no provision for otherwise locking the item.  In particular, an
// item can be marked dirty or flushed while multiple threads have a hold on
// the item.
//
// A Cache provides the following methods:
//
//   o Lookup() -- lookup an item by key, creating it if necessary; may return
//     an error if Read() fails; blocks if the cache is full
//   o Flush() -- flush all items matched by the function predicate(); return
//     an error if any of the Item.Write() fails
//   o Inval() -- invalidate all  items matched by the function predicate();
//     InvalFlush determines whether dirty items are flushed before they are
//     invalidated; Inval() will block until a held item is released; Inval()
//     will return an error if flush failed
//
type Cache interface {
	Lookup(cntxt *ReqContext, key Key, lookupType LookupType) (err error)
	Flush(cntxt *ReqContext, predicate func(cntxt *ReqContext, key *Key) bool) (err error)
	Inval(cntxt *ReqContext, predicate func(cntxt *ReqContext, key *Key) bool,
		flushIfDirty InvalFlush) (err error)
	Free(cntxt *ReqContext)
}

type LookupType int

const (
	InCacheOnly     LookupType = iota
	ReadIfNotCached LookupType = iota
)

type InvalFlush int

const (
	InvalOnly     InvalFlush = iota
	FlushAndInval InvalFlush = iota
)

// Create a new keyed cache whiche consumes a maximum of cacheSz bytes of memory
// (not including hash chains and some other data).  The caller supplies the
// size of keys and items so we can compute the maximum number of items to
// cache.
//
// The caller also supplies newItem(), which is called by the cache when it
// wants to create a new item.  Items must embed a pointer to a CachedItem
// and initialize that pointer with the value passed in.
//
func NewCache(newItemFunc func(cntxt *ReqContext, cachedObj *CachedItem) (cache *Item),
	cacheSz uint64) (cache *Cache) {

	return newCache(newItemFunc, cacheSz)
}
