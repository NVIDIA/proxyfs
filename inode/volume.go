package inode

import (
	"github.com/swiftstack/ProxyFS/stats"
)

func (vS *volumeStruct) GetFSID() (fsid uint64) {
	fsid = vS.fsid
	return
}

func (vS *volumeStruct) SnapShotCreateByFSLayer(name string) (id uint64, err error) {
	// TODO: Does Inode Layer need to do anything here?
	id, err = vS.headhunterVolumeHandle.SnapShotCreateByInodeLayer(name)
	return
}

func (vS *volumeStruct) SnapShotDeleteByFSLayer(id uint64) (err error) {
	// TODO: Does Inode Layer need to do anything here?
	err = vS.headhunterVolumeHandle.SnapShotDeleteByInodeLayer(id)
	return
}

func (vS *volumeStruct) CheckpointCompleted() {
	var (
		dirEntryCacheHits             uint64
		dirEntryCacheHitsDelta        uint64
		dirEntryCacheMisses           uint64
		dirEntryCacheMissesDelta      uint64
		fileExtentMapCacheHits        uint64
		fileExtentMapCacheHitsDelta   uint64
		fileExtentMapCacheMisses      uint64
		fileExtentMapCacheMissesDelta uint64
	)

	dirEntryCacheHits, dirEntryCacheMisses, _, _ = globals.dirEntryCache.Stats()
	fileExtentMapCacheHits, fileExtentMapCacheMisses, _, _ = globals.fileExtentMapCache.Stats()

	dirEntryCacheHitsDelta = dirEntryCacheHits - globals.dirEntryCachePriorCacheHits
	dirEntryCacheMissesDelta = dirEntryCacheMisses - globals.dirEntryCachePriorCacheMisses

	fileExtentMapCacheHitsDelta = fileExtentMapCacheHits - globals.fileExtentMapCachePriorCacheHits
	fileExtentMapCacheMissesDelta = fileExtentMapCacheMisses - globals.fileExtentMapCachePriorCacheMisses

	globals.Lock()

	if 0 != dirEntryCacheHitsDelta {
		stats.IncrementOperationsBy(&stats.DirEntryCacheHits, dirEntryCacheHitsDelta)
		globals.dirEntryCachePriorCacheHits = dirEntryCacheHits
	}
	if 0 != dirEntryCacheMissesDelta {
		stats.IncrementOperationsBy(&stats.DirEntryCacheMisses, dirEntryCacheMissesDelta)
		globals.dirEntryCachePriorCacheMisses = dirEntryCacheMisses
	}

	if 0 != fileExtentMapCacheHitsDelta {
		stats.IncrementOperationsBy(&stats.FileExtentMapCacheHits, fileExtentMapCacheHitsDelta)
		globals.fileExtentMapCachePriorCacheHits = fileExtentMapCacheHits
	}
	if 0 != fileExtentMapCacheMissesDelta {
		stats.IncrementOperationsBy(&stats.FileExtentMapCacheMisses, fileExtentMapCacheMissesDelta)
		globals.fileExtentMapCachePriorCacheMisses = fileExtentMapCacheMisses
	}

	globals.Unlock()
}
