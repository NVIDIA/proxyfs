// Package stats provides a simple statsd client API.
package stats

type MultipleStat int

const (
	DirRead                 MultipleStat = iota // uses operations, entries and bytes stats
	FileRead                                    // uses operations, op bucketed bytes, and bytes stats
	FileReadplan                                // uses operations, op bucketed bytes, bytes, and bucketed steps stats
	FileWrite                                   // uses operations, op bucketed bytes, bytes, appended and overwritten stats
	FileWrote                                   // uses operations, op bucketed bytes, and bytes stats
	JrpcfsIoWrite                               // uses operations, op bucketed bytes, and bytes stats
	JrpcfsIoRead                                // uses operations, op bucketed bytes, and bytes stats
	SwiftObjGet                                 // uses operations, op bucketed bytes, and bytes stats
	SwiftObjLoad                                // uses operations, op bucketed bytes, and bytes stats
	SwiftObjRead                                // uses operations, op bucketed bytes, and bytes stats
	SwiftObjTail                                // uses operations and bytes stats
	SwiftObjPutCtxRead                          // uses operations, op bucketed bytes, and bytes stats
	SwiftObjPutCtxSendChunk                     // uses operations, op bucketed bytes, and bytes stats
)

// Dump returns a map of all accumulated stats since process start.
//
//   Key   is a string containing the name of the stat
//   Value is the accumulation of all increments for the stat since process start
func Dump() (statMap map[string]uint64) {
	statMap = dump()
	return
}

// IncrementOperations sends an increment of .operations to statsd.
func IncrementOperations(statName *string) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperations(statName)
}

// IncrementOperationsBy sends an increment by <incBy> of .operations to statsd.
func IncrementOperationsBy(statName *string, incBy uint64) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperationsBy(statName, incBy)
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

// IncrementOperationsBuckedtedBytesAndBucketedSteps sends an increment of .operations, .bytes, and the appropriate .operations.size-* to statsd.
func IncrementOperationsBucketedEntriesAndBucketedBytes(stat MultipleStat, entries uint64, bytes uint64) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperationsBucketedEntriesAndBucketedBytes(stat, entries, bytes)
}

// IncrementOperationsBucketedBytesAndAppendedOverwritten sends an increment of .operations, .bytes, .appended, .overwritten, and the appropriate .operations.size-* to statsd.
func IncrementOperationsBucketedBytesAndAppendedOverwritten(stat MultipleStat, bytes uint64, appended uint64, overwritten uint64) {
	// Do this in a goroutine since channel operations are suprisingly expensive due to locking underneath
	go incrementOperationsBucketedBytesAndAppendedOverwritten(stat, bytes, appended, overwritten)
}
