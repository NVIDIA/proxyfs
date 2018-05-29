// Cache implementation for itemcache package

package itemcache

import (
	"container/list"
	"sync"
)

type ObjCache struct {
	entries map[Key]*CachedItem
}

type CacheItem struct {
	key      Key
	value    interface{}
	refCnt   int
	waiters  int
	LRUentry list.Element
}

func newCache(newItemFunc func(cntxt *ReqContext, cachedObj *CachedItem) (cache *Item),
	cacheSz uint64) (cache *Cache) {

	return nil
}
