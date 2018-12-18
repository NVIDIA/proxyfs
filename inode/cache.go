package inode

import (
	"fmt"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/platform"
)

func adoptVolumeGroupReadCacheParameters(confMap conf.ConfMap) (err error) {
	var (
		readCacheLineCount     uint64
		readCacheMemSize       uint64
		readCacheQuotaFraction float64
		readCacheTotalSize     uint64
		readCacheWeightSum     uint64
		totalMemSize           uint64
		volumeGroup            *volumeGroupStruct
	)

	readCacheWeightSum = 0

	for _, volumeGroup = range globals.volumeGroupMap {
		if 0 < volumeGroup.numServed {
			readCacheWeightSum += volumeGroup.readCacheWeight
		}
	}

	readCacheQuotaFraction, err = confMap.FetchOptionValueFloat64("Peer:"+globals.whoAmI, "ReadCacheQuotaFraction")
	if nil != err {
		return
	}
	if (0 > readCacheQuotaFraction) || (1 < readCacheQuotaFraction) {
		err = fmt.Errorf("%s.ReadCacheQuotaFraction (%v) must be between 0 and 1", globals.whoAmI, readCacheQuotaFraction)
		return
	}

	totalMemSize = platform.MemSize()

	readCacheMemSize = uint64(float64(totalMemSize) * readCacheQuotaFraction / platform.GoHeapAllocationMultiplier)

	logger.Infof("Adopting ReadCache Parameters...")
	logger.Infof("...ReadCacheQuotaFraction(%v) of memSize(0x%016X) totals 0x%016X",
		readCacheQuotaFraction,
		totalMemSize,
		readCacheMemSize)

	for _, volumeGroup = range globals.volumeGroupMap {
		if 0 < volumeGroup.numServed {
			readCacheTotalSize = readCacheMemSize * volumeGroup.readCacheWeight / readCacheWeightSum

			readCacheLineCount = readCacheTotalSize / volumeGroup.readCacheLineSize
			if 0 == readCacheLineCount {
				err = fmt.Errorf("[VolumeGroup:%s]ReadCacheWeight must result in at least one ReadCacheLineSize (%v) of memory", volumeGroup.name, volumeGroup.readCacheLineSize)
				return
			}

			volumeGroup.Lock()
			volumeGroup.readCacheLineCount = readCacheLineCount
			volumeGroup.capReadCacheWhileLocked()
			volumeGroup.Unlock()

			logger.Infof("...0x%08X cache lines (each of size 0x%08X) totalling 0x%016X for Volume Group %v",
				volumeGroup.readCacheLineCount,
				volumeGroup.readCacheLineSize,
				volumeGroup.readCacheLineCount*volumeGroup.readCacheLineSize,
				volumeGroup.name)
		}
	}

	err = nil
	return
}

func startInodeCacheDiscard(confMap conf.ConfMap, volume *volumeStruct, volumeSectionName string) (err error) {
	var (
		LRUCacheMaxBytes       uint64
		LRUDiscardTimeInterval time.Duration
	)

	LRUCacheMaxBytes, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxBytesInodeCache")
	if nil != err {
		LRUCacheMaxBytes = 10485760 // TODO - Remove setting a default value
		err = nil
	}
	volume.inodeCacheLRUMaxBytes = LRUCacheMaxBytes

	LRUDiscardTimeInterval, err = confMap.FetchOptionValueDuration(volumeSectionName, "InodeCacheEvictInterval")
	if nil != err {
		LRUDiscardTimeInterval = 1 * time.Second // TODO - Remove setting a default value
		err = nil
	}

	if LRUDiscardTimeInterval != 0 {
		volume.inodeCacheLRUTickerInterval = LRUDiscardTimeInterval
		volume.inodeCacheLRUTicker = time.NewTicker(volume.inodeCacheLRUTickerInterval)

		logger.Infof("Inode cache discard ticker for 'volume: %v' is: %v MaxBytesInodeCache: %v",
			volume.volumeName, volume.inodeCacheLRUTickerInterval, volume.inodeCacheLRUMaxBytes)

		// Start ticker for inode cache discard thread
		go func() {
			for range volume.inodeCacheLRUTicker.C {
				_, _, _, _ = volume.inodeCacheDiscard()
			}
		}()
	} else {
		logger.Infof("Inode cache discard ticker for 'volume: %v' is disabled.",
			volume.volumeName)
		return
	}

	return
}

func stopInodeCacheDiscard(volume *volumeStruct) {
	if volume.inodeCacheLRUTicker != nil {
		volume.inodeCacheLRUTicker.Stop()
		logger.Infof("Inode cache discard ticker for 'volume: %v' stopped.",
			volume.volumeName)
	}
}
