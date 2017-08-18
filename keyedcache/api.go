// Package keyedcache provides a thread-safe cache to hold clean or dirty
// objects identified by opaque keys in a bounded amount of memory.

package keyedcache

import (
	"sync"
)

// Just ignore this for the time being, other then noting that a pointer to one
// of these gets passed through requests into the keyed cache and down into the
// callbacks.
//
type ReqContext interface {
}

// Objects in the cache are identified by a key, which is unique for each
// object.  The key must support the following operations:
//
//   o Equal() -- returns true if the two keys refer to the same object
//   o Hash() -- return a hash value based on the key (two keys that are equal
//     must have the same hash value)
//   o SizeOf() -- return memory used by the key (to track memory consumption);
//     all keys are assumed to be the same size.
//
type Key interface {
	Equal(key *Key) bool
	Hash() uint64
	SizeOf() uint64
}

// Objects in the cache are "allocated" by calling a NewObject function supplied
// by the client when it created the cache.  New objects do not have an
// indentity until they are assigned one by a lookup in the cache.
//
// Objects must support the following operations.  These methods are used
// exclusively by the keyedcache; clients who get an object from the cache
// should not call them, instead they should call methods provided by the cache
// to flush or invalidate objects:
//
//   o Read() -- called to initialize an object when its assigned an identity;
//     it can block arbitrarity but cannot call into this instance of the
//     cache.
//   o Write() -- called to flush the contents of a dirty object; it can
//     block arbitrarity but cannot call into this instance of the cache.
//   o Inval() -- called to destroy the identity of an object before
//     assigning it a new identity; it can block indefinitely but cannot
//     call back into this instance of the cache (key is the old identity
//     of the object)
//   o Free() -- called when the cache is no longer referencing the object;
//     it cannot call back into this instance of the cache (I'm not sure that
//     this method makes sense in Go).
//   o SizeOf() -- return memory used by the object (to track memory consumption);
//     all objects are assumed to be the same size.
//
// Note: perhaps the Read() and Write() operations should be asynchronous.
//
type Object interface {
	Read(cntxt *ReqContext, key *Key) (err error)
	Write(cntxt *ReqContext, key *Key) (err error)
	Inval(cntxt *ReqContext, key *Key)
	Free(cntxt *ReqContext)
	SizeOf() uint64
}

// CachedObject holds the information that a keyedcache uses to manage an object
// stored in the cache.  An object managed by the cache must include an embedded
// pointer to this interface, which it must initializ with a pointer passed to
// the NewObject function supplied by the client of the cache (so when an object
// is created by NewObject() this interface is initialized).
//
// CachedObject supports the following methods. These may be invoked by a client
// that has a hold on the object.  Successful lookup of an object in the cache
// returns a pointer to a held object.
//
//   o Hold() -- get an additional hold on the object
//   o Release() -- release a hold on the object; once all holds on an object
//     are released the object may be invalidated
//   o MarkDirty() -- mark a held object dirty; dirty objects are cleaned
//     (written) by the cache before invalidation unless a forced invalidation
//     is done
//   o IsDirty() -- returns true if the object is dirty
//   o Flush() -- cause a dirty object to be flushed via a call to Write();
//     panics if the object is not dirty
//
// If Flush() fails because the corresponding Write() fails then the object is
// marked dirty.
//
type CachedObject interface {
	Hold(cntxt *ReqContext)
	Release(cntxt *ReqContext)
	MarkDirty(cntxt *ReqContext)
	IsDirty(cntxt *ReqContext) bool
	Flush(cntxt *ReqContext) (err error)
}

// Cache caches objects identified by keys, upto a specified maximum amount of
// memory.  Objects in the cache are accessed by calling Lookup() with the
// desired key and Lookup() returns a pointer to an object with a hold on it.
// When the caller is done with the object it must release the hold.
//
// If the object is not currently cached, Cache will create a new object
// corresponding to the key or reuse an existing one.  It is initialized by a
// call to Object.Read() before return.  Cache locks the object duing the call
// to Object.Read() so only one object with a given key is present in the cache
// and no thread can access it until it is initialized.
//
// There is no provision for otherwise locking the object.  In particular, an
// object can be marked dirty or flushed while multiple threads have a hold on
// the object.
//
// A Cache provides the following methods:
//
//   o Lookup() -- lookup an object by key, creating it if necessary; may return
//     an error if Read() fails; blocks if the cache is full
//   o Flush() -- flush all objects matched by the function predicate(); return
//     an error if any of the Object.Write() fails
//   o Inval() -- invalidate all  objects matched by the function predicate();
//     InvalFlush determines whether dirty objects are flushed before they are
//     invalidated; Inval() will block until a held object is released; Inval()
//     will return an error if flush failed
//
type Cache interface {
	Lookup(cntxt *ReqContext, key Key) (err error)
	Flush(cntxt *ReqContext, predicate func(cntxt *ReqContext, key *Key) bool) (err error)
	Inval(cntxt *ReqContext, predicate func(cntxt *ReqContext, key *Key) bool,
		flushIfDirty InvalFlush) (err error)
	Free(cntxt *ReqContext)
}

type InvalFlush int

const (
	INVAL_DIRTY InvalFlush = iota
	FLUSH_DIRTY InvalFlush = iota
)

// Create a new keyed cache whiche consumes a maximum of cacheSz bytes of memory
// (not including hash chains and some other data).  The caller supplies the
// size of keys and objects so we can compute the maximum number of objects to
// cache.
//
// The caller also supplies newObject(), which is called by the cache when it
// wants to create a new object.  Objects must embed a pointer to a CachedObject
// and initialize that pointer with the value passed in.
//
// The *ReqContext is passed through without using it, however the cntxt passed
// to newObject() will be the cntxt passed to the call to Lookup() that
// triggered object creation.
//
func NewCache(cntxt *ReqContext,
	newObject func(cntxt *ReqContext, cachedObj *CachedObject) (object Object),
	cacheSz uint64, objectSz uint64, keySz uint64) (cache Cache)
