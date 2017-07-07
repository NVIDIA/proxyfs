// Package stats provides a simple statsd client API.
package stats

import (
	"sync"
)

type MultipleStat int

const (
	DirRead                 MultipleStat = iota // uses operations, entries and bytes stats
	FileRead                                    // uses operations, op bucketed bytes, and bytes stats
	FileReadplan                                // uses operations, op bucketed bytes, and bytes stats
	FileWrite                                   // uses operations, op bucketed bytes, bytes, appended and overwritten stats
	FileWrote                                   // uses operations, op bucketed bytes, and bytes stats
	JrpcfsIoWrite                               // uses operations, op bucketed bytes, and bytes stats
	JrpcfsIoRead                                // uses operations, op bucketed bytes, and bytes stats
	SwiftObjGet                                 // uses operations, op bucketed bytes, and bytes stats
	SwiftObjLoad                                // uses operations, op bucketed bytes, and bytes stats
	SwiftObjTail                                // uses operations and bytes stats
	SwiftObjPutCtxRead                          // uses operations, op bucketed bytes, and bytes stats
	SwiftObjPutCtxSendChunk                     // uses operations, op bucketed bytes, and bytes stats
)

func (ms MultipleStat) findStatStrings(numBytes uint64) (ops *string, bytes *string, entries *string, bbytes *string, app *string, overw *string) {
	switch ms {
	case DirRead:
		// directory read uses operations, entries and bytes stats
		ops = &DirReadOps
		bytes = &DirReadBytes
		entries = &DirReadEntries
	case FileRead:
		// file read uses operations, op bucketed bytes, and bytes stats
		ops = &FileReadOps
		bytes = &FileReadBytes
		// XXX TODO: Factor out the bucket-to-stat-string determination into a
		//           helper function to make this code a little neater
		if numBytes <= 4096 {
			bbytes = &FileReadOps4K
		} else if numBytes <= 8192 {
			bbytes = &FileReadOps8K
		} else if numBytes <= 16384 {
			bbytes = &FileReadOps16K
		} else if numBytes <= 32768 {
			bbytes = &FileReadOps32K
		} else if numBytes <= 65536 {
			bbytes = &FileReadOps64K
		} else {
			bbytes = &FileReadOpsOver64K
		}
	case FileReadplan:
		// file readplan uses operations, op bucketed bytes, and bytes stats
		ops = &FileReadplanOps
		bytes = &FileReadplanBytes
		if numBytes <= 4096 {
			bbytes = &FileReadplanOps4K
		} else if numBytes <= 8192 {
			bbytes = &FileReadplanOps8K
		} else if numBytes <= 16384 {
			bbytes = &FileReadplanOps16K
		} else if numBytes <= 32768 {
			bbytes = &FileReadplanOps32K
		} else if numBytes <= 65536 {
			bbytes = &FileReadplanOps64K
		} else {
			bbytes = &FileReadplanOpsOver64K
		}
	case FileWrite:
		// file write uses operations, op bucketed bytes, bytes, appended and overwritten stats
		ops = &FileWriteOps
		bytes = &FileWriteBytes
		if numBytes <= 4096 {
			bbytes = &FileWriteOps4K
		} else if numBytes <= 8192 {
			bbytes = &FileWriteOps8K
		} else if numBytes <= 16384 {
			bbytes = &FileWriteOps16K
		} else if numBytes <= 32768 {
			bbytes = &FileWriteOps32K
		} else if numBytes <= 65536 {
			bbytes = &FileWriteOps64K
		} else {
			bbytes = &FileWriteOpsOver64K
		}
		app = &FileWriteAppended
		overw = &FileWriteOverwritten
	case FileWrote:
		// file wrote uses operations, op bucketed bytes, and bytes stats
		ops = &FileWroteOps
		bytes = &FileWroteBytes
		if numBytes <= 4096 {
			bbytes = &FileWroteOps4K
		} else if numBytes <= 8192 {
			bbytes = &FileWroteOps8K
		} else if numBytes <= 16384 {
			bbytes = &FileWroteOps16K
		} else if numBytes <= 32768 {
			bbytes = &FileWroteOps32K
		} else if numBytes <= 65536 {
			bbytes = &FileWroteOps64K
		} else {
			bbytes = &FileWroteOpsOver64K
		}
	case JrpcfsIoWrite:
		// jrpcfs write uses operations, op bucketed bytes, and bytes stats
		ops = &JrpcfsIoWriteOps
		bytes = &JrpcfsIoWriteBytes
		if numBytes <= 4096 {
			bbytes = &JrpcfsIoWriteOps4K
		} else if numBytes <= 8192 {
			bbytes = &JrpcfsIoWriteOps8K
		} else if numBytes <= 16384 {
			bbytes = &JrpcfsIoWriteOps16K
		} else if numBytes <= 32768 {
			bbytes = &JrpcfsIoWriteOps32K
		} else if numBytes <= 65536 {
			bbytes = &JrpcfsIoWriteOps64K
		} else {
			bbytes = &JrpcfsIoWriteOpsOver64K
		}
	case JrpcfsIoRead:
		// jrpcfs read uses operations, op bucketed bytes, and bytes stats
		ops = &JrpcfsIoReadOps
		bytes = &JrpcfsIoReadBytes
		if numBytes <= 4096 {
			bbytes = &JrpcfsIoReadOps4K
		} else if numBytes <= 8192 {
			bbytes = &JrpcfsIoReadOps8K
		} else if numBytes <= 16384 {
			bbytes = &JrpcfsIoReadOps16K
		} else if numBytes <= 32768 {
			bbytes = &JrpcfsIoReadOps32K
		} else if numBytes <= 65536 {
			bbytes = &JrpcfsIoReadOps64K
		} else {
			bbytes = &JrpcfsIoReadOpsOver64K
		}
	case SwiftObjGet:
		// swiftclient object-get uses operations, op bucketed bytes, and bytes stats
		ops = &SwiftObjGetOps
		bytes = &SwiftObjGetBytes
		if numBytes <= 4096 {
			bbytes = &SwiftObjGetOps4K
		} else if numBytes <= 8192 {
			bbytes = &SwiftObjGetOps8K
		} else if numBytes <= 16384 {
			bbytes = &SwiftObjGetOps16K
		} else if numBytes <= 32768 {
			bbytes = &SwiftObjGetOps32K
		} else if numBytes <= 65536 {
			bbytes = &SwiftObjGetOps64K
		} else {
			bbytes = &SwiftObjGetOpsOver64K
		}
	case SwiftObjLoad:
		// swiftclient object-load uses operations, op bucketed bytes, and bytes stats
		ops = &SwiftObjLoadOps
		bytes = &SwiftObjLoadBytes
		if numBytes <= 4096 {
			bbytes = &SwiftObjLoadOps4K
		} else if numBytes <= 8192 {
			bbytes = &SwiftObjLoadOps8K
		} else if numBytes <= 16384 {
			bbytes = &SwiftObjLoadOps16K
		} else if numBytes <= 32768 {
			bbytes = &SwiftObjLoadOps32K
		} else if numBytes <= 65536 {
			bbytes = &SwiftObjLoadOps64K
		} else {
			bbytes = &SwiftObjLoadOpsOver64K
		}
	case SwiftObjTail:
		// swiftclient object-tail uses operations and bytes stats
		ops = &SwiftObjTailOps
		bytes = &SwiftObjTailBytes
	case SwiftObjPutCtxRead:
		// swiftclient object-put-context.read uses operations, op bucketed bytes, and bytes stats
		ops = &SwiftObjPutCtxReadOps
		bytes = &SwiftObjPutCtxReadBytes
		if numBytes <= 4096 {
			bbytes = &SwiftObjPutCtxReadOps4K
		} else if numBytes <= 8192 {
			bbytes = &SwiftObjPutCtxReadOps8K
		} else if numBytes <= 16384 {
			bbytes = &SwiftObjPutCtxReadOps16K
		} else if numBytes <= 32768 {
			bbytes = &SwiftObjPutCtxReadOps32K
		} else if numBytes <= 65536 {
			bbytes = &SwiftObjPutCtxReadOps64K
		} else {
			bbytes = &SwiftObjPutCtxReadOpsOver64K
		}
	case SwiftObjPutCtxSendChunk:
		// swiftclient object-put-context.send-chunk uses operations, op bucketed bytes, and bytes stats
		ops = &SwiftObjPutCtxSendChunkOps
		bytes = &SwiftObjPutCtxSendChunkBytes
		if numBytes <= 4096 {
			bbytes = &SwiftObjPutCtxSendChunkOps4K
		} else if numBytes <= 8192 {
			bbytes = &SwiftObjPutCtxSendChunkOps8K
		} else if numBytes <= 16384 {
			bbytes = &SwiftObjPutCtxSendChunkOps16K
		} else if numBytes <= 32768 {
			bbytes = &SwiftObjPutCtxSendChunkOps32K
		} else if numBytes <= 65536 {
			bbytes = &SwiftObjPutCtxSendChunkOps64K
		} else {
			bbytes = &SwiftObjPutCtxSendChunkOpsOver64K
		}
	}
	return
}

// computeBucketSuffix is used to compute the affected bucket stat to increment.
func computeBucketSuffix(bytes uint64) (suffix string) {
	if bytes <= 4096 {
		suffix = ".size-up-to-4KB"
	} else if bytes <= 8192 {
		suffix = ".size-4KB-to-8KB"
	} else if bytes <= 16384 {
		suffix = ".size-8KB-to-16KB"
	} else if bytes <= 32768 {
		suffix = ".size-16KB-to-32KB"
	} else if bytes <= 65536 {
		suffix = ".size-32KB-to-64KB"
	} else {
		suffix = ".size-over-64KB"
	}

	return
}

// Dump returns a map of all accumulated stats since process start.
//
//   Key   is a string containing the name of the stat
//   Value is the accumulation of all increments for the stat since process start
func Dump() (statMap map[string]uint64) {
	globals.Lock()
	numStats := len(globals.statFullMap)
	statMap = make(map[string]uint64, numStats)
	for statKey, statValue := range globals.statFullMap {
		statMap[statKey] = statValue
	}
	globals.Unlock()
	return
}

var statStructPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &statStruct{}
	},
}

func incrementOperations(statName *string) {
	incrementSomething(statName, 1)
}

func incrementSomething(statName *string, incBy uint64) {
	if incBy == 0 {
		// No point in incrementing by zero
		return
	}

	stat := statStructPool.Get().(*statStruct)
	stat.name = statName
	stat.increment = incBy
	globals.statChan <- stat
}

func incrementOperationsAndBytes(stat MultipleStat, bytes uint64) {
	opsStat, bytesStat, _, _, _, _ := stat.findStatStrings(bytes)
	incrementSomething(opsStat, 1)
	incrementSomething(bytesStat, bytes)
}

func incrementOperationsEntriesAndBytes(stat MultipleStat, entries uint64, bytes uint64) {
	opsStat, bytesStat, entriesStat, _, _, _ := stat.findStatStrings(bytes)
	incrementSomething(opsStat, 1)
	incrementSomething(bytesStat, bytes)
	incrementSomething(entriesStat, entries)
}

func incrementOperationsAndBucketedBytes(stat MultipleStat, bytes uint64) {
	opsStat, bytesStat, _, bbytesStat, _, _ := stat.findStatStrings(bytes)
	incrementSomething(opsStat, 1)
	incrementSomething(bytesStat, bytes)
	incrementSomething(bbytesStat, 1)
}

func incrementOperationsBucketedBytesAndAppendedOverwritten(stat MultipleStat, bytes uint64, appended uint64, overwritten uint64) {
	opsStat, bytesStat, _, bbytesStat, appStat, overwStat := stat.findStatStrings(bytes)
	incrementSomething(opsStat, 1)
	incrementSomething(bytesStat, bytes)
	incrementSomething(bbytesStat, 1)
	incrementSomething(appStat, appended)
	incrementSomething(overwStat, overwritten)
}

// IncrementOperations sends an increment of .operations to statsd.
func IncrementOperations(statName *string) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperations(statName)
}

// IncrementOperationsAndBytes sends an increment of .operations and .bytes to statsd.
func IncrementOperationsAndBytes(stat MultipleStat, bytes uint64) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperationsAndBytes(stat, bytes)
}

// IncrementOperationsEntriesAndBytes sends an increment of .operations, .entries, and .bytes to statsd.
func IncrementOperationsEntriesAndBytes(stat MultipleStat, entries uint64, bytes uint64) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperationsEntriesAndBytes(stat, entries, bytes)
}

// IncrementOperationsAndBucketedBytes sends an increment of .operations, .bytes, and the appropriate .operations.size-* to statsd.
func IncrementOperationsAndBucketedBytes(stat MultipleStat, bytes uint64) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperationsAndBucketedBytes(stat, bytes)
}

// IncrementOperationsBucketedBytesAndAppendedOverwritten sends an increment of .operations, .bytes, .appended, .overwritten, and the appropriate .operations.size-* to statsd.
func IncrementOperationsBucketedBytesAndAppendedOverwritten(stat MultipleStat, bytes uint64, appended uint64, overwritten uint64) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperationsBucketedBytesAndAppendedOverwritten(stat, bytes, appended, overwritten)
}
