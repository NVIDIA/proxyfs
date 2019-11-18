package inode

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/sortedmap"
)

func (vS *volumeStruct) GetFSID() (fsid uint64) {
	fsid = vS.fsid
	return
}

func (vS *volumeStruct) SnapShotCreate(name string) (id uint64, err error) {
	if ("." == name) || (".." == name) {
		err = fmt.Errorf("SnapShot cannot be named either '.' or '..'")
		return
	}

	vS.Lock()
	id, err = vS.headhunterVolumeHandle.SnapShotCreateByInodeLayer(name)
	vS.Unlock()
	return
}

func (vS *volumeStruct) SnapShotDelete(id uint64) (err error) {
	var (
		found                               bool
		keyAsInodeNumber                    InodeNumber
		keyAsKey                            sortedmap.Key
		indexWherePurgesAreToHappen         int
		maxInodeNumberToPurgeFromInodeCache InodeNumber
		minInodeNumberToPurgeFromInodeCache InodeNumber
		ok                                  bool
		valueAsValue                        sortedmap.Value
		valueAsInodeStructPtr               *inMemoryInodeStruct
	)

	vS.Lock()
	err = vS.headhunterVolumeHandle.SnapShotDeleteByInodeLayer(id)
	if nil == err {
		// Purge elements in inodeCache to avoid aliasing if/when SnapShotID is reused

		minInodeNumberToPurgeFromInodeCache = InodeNumber(vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(id, uint64(0)))
		maxInodeNumberToPurgeFromInodeCache = InodeNumber(vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(id+1, uint64(0)))

		indexWherePurgesAreToHappen, found, err = vS.inodeCache.BisectRight(minInodeNumberToPurgeFromInodeCache)
		if nil != err {
			vS.Unlock()
			err = fmt.Errorf("Volume %v InodeCache BisectRight() failed: %v", vS.volumeName, err)
			logger.Error(err)
			return
		}

		// If there is some InodeNumber at or beyond minInodeNumberToPurgeFromInodeCache, found == TRUE

		for found {
			keyAsKey, valueAsValue, ok, err = vS.inodeCache.GetByIndex(indexWherePurgesAreToHappen)
			if nil != err {
				vS.Unlock()
				err = fmt.Errorf("Volume %v InodeCache GetByIndex() failed: %v", vS.volumeName, err)
				logger.Error(err)
				return
			}

			if !ok {
				vS.Unlock()
				return
			}

			keyAsInodeNumber, ok = keyAsKey.(InodeNumber)
			if !ok {
				vS.Unlock()
				err = fmt.Errorf("Volume %v InodeCache GetByIndex() returned non-InodeNumber", vS.volumeName)
				return
			}

			// Redefine found to indicate we've found an InodeCache entry to evict (used next iteration)

			found = (keyAsInodeNumber < maxInodeNumberToPurgeFromInodeCache)

			// func (vS *volumeStruct) inodeCacheDropWhileLocked(inode *inMemoryInodeStruct) (ok bool, err error)
			if found {
				valueAsInodeStructPtr, ok = valueAsValue.(*inMemoryInodeStruct)
				if !ok {
					vS.Unlock()
					err = fmt.Errorf("Volume %v InodeCache GetByIndex() returned non-inMemoryInodeStructPtr", vS.volumeName)
					return
				}
				ok, err = vS.inodeCacheDropWhileLocked(valueAsInodeStructPtr)
				if nil != err {
					vS.Unlock()
					err = fmt.Errorf("Volume %v inodeCacheDropWhileLocked() failed: %v", vS.volumeName, err)
					return
				}
				if !ok {
					vS.Unlock()
					err = fmt.Errorf("Volume %v inodeCacheDropWhileLocked() returned !ok", vS.volumeName)
					return
				}
			}
		}
	}
	vS.Unlock()
	return
}

func (vS *volumeStruct) CheckpointCompleted() {
	var (
		dirEntryCacheHitsDelta        uint64
		dirEntryCacheMissesDelta      uint64
		dirEntryCacheStats            *sortedmap.BPlusTreeCacheStats
		fileExtentMapCacheHitsDelta   uint64
		fileExtentMapCacheMissesDelta uint64
		fileExtentMapCacheStats       *sortedmap.BPlusTreeCacheStats
	)

	dirEntryCacheStats = globals.dirEntryCache.Stats()
	fileExtentMapCacheStats = globals.fileExtentMapCache.Stats()

	globals.Lock()

	dirEntryCacheHitsDelta = dirEntryCacheStats.CacheHits - globals.dirEntryCachePriorCacheHits
	dirEntryCacheMissesDelta = dirEntryCacheStats.CacheMisses - globals.dirEntryCachePriorCacheMisses

	fileExtentMapCacheHitsDelta = fileExtentMapCacheStats.CacheHits - globals.fileExtentMapCachePriorCacheHits
	fileExtentMapCacheMissesDelta = fileExtentMapCacheStats.CacheMisses - globals.fileExtentMapCachePriorCacheMisses

	if 0 != dirEntryCacheHitsDelta {
		stats.IncrementOperationsBy(&stats.DirEntryCacheHits, dirEntryCacheHitsDelta)
		globals.dirEntryCachePriorCacheHits = dirEntryCacheStats.CacheHits
	}
	if 0 != dirEntryCacheMissesDelta {
		stats.IncrementOperationsBy(&stats.DirEntryCacheMisses, dirEntryCacheMissesDelta)
		globals.dirEntryCachePriorCacheMisses = dirEntryCacheStats.CacheMisses
	}

	if 0 != fileExtentMapCacheHitsDelta {
		stats.IncrementOperationsBy(&stats.FileExtentMapCacheHits, fileExtentMapCacheHitsDelta)
		globals.fileExtentMapCachePriorCacheHits = fileExtentMapCacheStats.CacheHits
	}
	if 0 != fileExtentMapCacheMissesDelta {
		stats.IncrementOperationsBy(&stats.FileExtentMapCacheMisses, fileExtentMapCacheMissesDelta)
		globals.fileExtentMapCachePriorCacheMisses = fileExtentMapCacheStats.CacheMisses
	}

	globals.Unlock()
}
