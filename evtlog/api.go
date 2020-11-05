package evtlog

// FormatType is an efficiently passed indicator of the event type being logged.
type FormatType uint32 // Used as index to event slice (so keep this sequence in sync with event slice)

const (
	FormatTestPatternFixed FormatType = iota
	FormatTestPatternS
	FormatTestPatternS03D
	FormatTestPatternS08X
	FormatTestPatternS016X
	FormatTestPatternS016X016X
	FormatTestPatternS016X016X016X
	FormatTestPatternS016Xslice
	FormatTestPatternS016XS
	FormatTestPatternSS
	FormatTestPatternSS03D
	FormatTestPatternSS016X1X
	FormatTestPatternSS016X1X1X
	FormatTestPatternSSS
	FormatTestPatternSSS03D
	FormatTestPatternSSS016X03D
	FormatTestPatternSSS016X016X03D
	FormatHalterArm
	FormatHalterDisarm
	FormatAccountDelete
	FormatAccountGet
	FormatAccountHead
	FormatAccountPost
	FormatAccountPut
	FormatContainerDelete
	FormatContainerGet
	FormatContainerHead
	FormatContainerPost
	FormatContainerPut
	FormatObjectDelete
	FormatObjectGet
	FormatObjectHead
	FormatObjectLoad
	FormatObjectRead
	FormatObjectTail
	FormatObjectPutChunkedStart
	FormatObjectPutChunkedEnd
	FormatHeadhunterCheckpointStart
	FormatHeadhunterCheckpointEndSuccess
	FormatHeadhunterCheckpointEndFailure
	FormatHeadhunterRecordTransactionNonceRangeReserve
	FormatHeadhunterRecordTransactionPutInodeRec
	FormatHeadhunterRecordTransactionPutInodeRecs
	FormatHeadhunterRecordTransactionDeleteInodeRec
	FormatHeadhunterRecordTransactionPutLogSegmentRec
	FormatHeadhunterRecordTransactionDeleteLogSegmentRec
	FormatHeadhunterRecordTransactionPutBPlusTreeObject
	FormatHeadhunterRecordTransactionDeleteBPlusTreeObject
	FormatHeadhunterMissingInodeRec
	FormatHeadhunterMissingLogSegmentRec
	FormatHeadhunterMissingBPlusTreeObject
	FormatHeadhunterBPlusTreeNodeFault
	FormatDirFileBPlusTreeNodeFault
	FormatFlushInodesEntry
	FormatFlushInodesDirOrFilePayloadObjectNumberUpdated
	FormatFlushInodesErrorOnInode
	FormatFlushInodesErrorOnHeadhunterPut
	FormatFlushInodesExit
	FormatLeaseRequest
	FormatLeaseReply
	FormatLeaseInterrupt
	//
	formatTypeCount // Used to quickly check upper limit of FormatType values
)

type patternType uint32

const (
	patternFixed          patternType = iota // <timestamp> + "..."
	patternS                                 // <timestamp> + "...%s..."
	patternS03D                              // <timestamp> + "...%s...%03d..."
	patternS08X                              // <timestamp> + "...%s...%08X..."
	patternS016X                             // <timestamp> + "...%s...%016X..."
	patternS016X016X                         // <timestamp> + "...%s...%016X...%016X..."
	patternS016X016X016X                     // <timestamp> + "...%s...%016X...%016X...%016X..."
	patternS016Xslice                        // <timestamp> + "...%s...[...%016X...]..." where '[' & ']' delineate slice
	patternS016XS                            // <timestamp> + "...%s...%016X...%s..."
	patternSS                                // <timestamp> + "...%s...%s..."
	patternSS03D                             // <timestamp> + "...%s...%s...%03d..."
	patternSS016X1X                          // <timestamp> + "...%s...%s...%016X...%1X..."
	patternSS016X1X1X                        // <timestamp> + "...%s...%s...%016X...%1X...%1X..."
	patternSSS                               // <timestamp> + "...%s...%s...%s..."
	patternSSS03D                            // <timestamp> + "...%s...%s...%s...%03d..."
	patternSSS016X03D                        // <timestamp> + "...%s...%s...%s...%016X...%03d..."
	patternSSS016X016X03D                    // <timestamp> + "...%s...%s...%s...%016X...%016X...%03d..."
)

const (
	patternTimestampFormat = "2006-01-02 15:04:05.0000"
)

type eventType struct {
	patternType
	formatString string
}

var (
	event = []eventType{ // Indexed by FormatType (so keep that const iota seqence in sync here)
		eventType{ // FormatTestPatternFixed
			patternType:  patternFixed,
			formatString: "%s Test for patternFixed",
		},
		eventType{ // FormatTestPatternS
			patternType:  patternS,
			formatString: "%s Test for patternS arg0:%s",
		},
		eventType{ // FormatTestPatternS03D
			patternType:  patternS03D,
			formatString: "%s Test for patternS03D arg0:%s arg1:%03d",
		},
		eventType{ // FormatTestPatternS08X
			patternType:  patternS08X,
			formatString: "%s Test for patternS08X arg0:%s arg1:%08X",
		},
		eventType{ // FormatTestPatternS016X
			patternType:  patternS016X,
			formatString: "%s Test for patternS016X arg0:%s arg1:%016X",
		},
		eventType{ // FormatTestPatternS016X016X
			patternType:  patternS016X016X,
			formatString: "%s Test for patternS016X016X arg0:%s arg1:%016X arg2:%016X",
		},
		eventType{ // FormatTestPatternS016X016X016X
			patternType:  patternS016X016X016X,
			formatString: "%s Test for patternS016X016X016X arg0:%s arg1:%016X arg2:%016X arg3:%016X",
		},
		eventType{ // FormatTestPatternS016Xslice
			patternType:  patternS016Xslice,
			formatString: "%s Test for patternS016Xslice arg0:%s arg1:[0x%016X]",
		},
		eventType{ // FormatTestPatternS016XS
			patternType:  patternS016XS,
			formatString: "%s Test for patternS016XS arg0:%s arg1:%016X arg2:%s",
		},
		eventType{ // FormatTestPatternSS
			patternType:  patternSS,
			formatString: "%s Test for patternSS arg0:%s arg1:%s",
		},
		eventType{ // FormatTestPatternSS03D
			patternType:  patternSS03D,
			formatString: "%s Test for patternSS03D arg0:%s arg1:%s arg2:%03d",
		},
		eventType{ // FormatTestPatternSS016X1X
			patternType:  patternSS016X1X,
			formatString: "%s Test for patternSS016X1X arg0:%s arg1:%s arg2:%016X arg3:%1X",
		},
		eventType{ // FormatTestPatternSS016X1X1X
			patternType:  patternSS016X1X1X,
			formatString: "%s Test for patternSS016X1X1X arg0:%s arg1:%s arg2:%016X arg3:%1X arg4:%1X",
		},
		eventType{ // FormatTestPatternSSS
			patternType:  patternSSS,
			formatString: "%s Test for patternSSS arg0:%s arg1:%s arg2:%s",
		},
		eventType{ // FormatTestPatternSSS03D
			patternType:  patternSSS03D,
			formatString: "%s Test for patternSSS03D arg0:%s arg1:%s arg2:%s arg3:%03d",
		},
		eventType{ // FormatTestPatternSSS016X03D
			patternType:  patternSSS016X03D,
			formatString: "%s Test for patternSSS016X03D arg0:%s arg1:%s arg2:%s arg3:%016X arg4:%03d",
		},
		eventType{ // FormatTestPatternSSS016X016X03D
			patternType:  patternSSS016X016X03D,
			formatString: "%s Test for patternSSS016X016X03D arg0:%s arg1:%s arg2:%s arg3:%016X arg4:%016X arg5:%03d",
		},
		eventType{ // FormatHalterArm
			patternType:  patternS08X,
			formatString: "%s halter.Arm(%s, 0x%08X) called",
		},
		eventType{ // FormatHalterDisarm
			patternType:  patternS,
			formatString: "%s halter.Disarm(%s) called",
		},
		eventType{ // FormatAccountDelete
			patternType:  patternS03D,
			formatString: "%s Account DELETE %s had status %03d",
		},
		eventType{ // FormatAccountGet
			patternType:  patternS03D,
			formatString: "%s Account GET %s had status %03d",
		},
		eventType{ // FormatAccountHead
			patternType:  patternS03D,
			formatString: "%s Account HEAD %s had status %03d",
		},
		eventType{ // FormatAccountPost
			patternType:  patternS03D,
			formatString: "%s Account POST %s had status %03d",
		},
		eventType{ // FormatAccountPut
			patternType:  patternS03D,
			formatString: "%s Account PUT %s had status %03d",
		},
		eventType{ // FormatContainerDelete
			patternType:  patternSS03D,
			formatString: "%s Container DELETE %s/%s had status %03d",
		},
		eventType{ // FormatContainerGet
			patternType:  patternSS03D,
			formatString: "%s Container GET %s/%s had status %03d",
		},
		eventType{ // FormatContainerHead
			patternType:  patternSS03D,
			formatString: "%s Container HEAD %s/%s had status %03d",
		},
		eventType{ // FormatContainerPost
			patternType:  patternSS03D,
			formatString: "%s Container POST %s/%s had status %03d",
		},
		eventType{ // FormatContainerPut
			patternType:  patternSS03D,
			formatString: "%s Container PUT %s/%s had status %03d",
		},
		eventType{ // FormatObjectDelete
			patternType:  patternSSS03D,
			formatString: "%s Object DELETE %s/%s/%s had status %03d",
		},
		eventType{ // FormatObjectGet
			patternType:  patternSSS016X016X03D,
			formatString: "%s Object GET %s/%s/%s (offset 0x%016X length 0x%016X) had status %03d",
		},
		eventType{ // FormatObjectHead
			patternType:  patternSSS03D,
			formatString: "%s Object HEAD %s/%s/%s had status %03d",
		},
		eventType{ // FormatObjectLoad
			patternType:  patternSSS03D,
			formatString: "%s Object LOAD %s/%s/%s had status %03d",
		},
		eventType{ // FormatObjectRead
			patternType:  patternSSS016X016X03D,
			formatString: "%s Object READ %s/%s/%s (offset 0x%016X length 0x%016X) had status %03d",
		},
		eventType{ // FormatObjectTail
			patternType:  patternSSS016X03D,
			formatString: "%s Object TAIL %s/%s/%s (length 0x%016X) had status %03d",
		},
		eventType{ // FormatObjectPutChunkedStart
			patternType:  patternSSS,
			formatString: "%s Object (chunked) PUT %s/%s/%s initiated",
		},
		eventType{ // FormatObjectPutChunkedEnd
			patternType:  patternSSS016X03D,
			formatString: "%s Object (chunked) PUT %s/%s/%s (length 0x%016X) had status %03d",
		},
		eventType{ // FormatHeadhunterCheckpointStart
			patternType:  patternS,
			formatString: "%s headhunter.checkpointDaemon calling putCheckpoint() for Volume '%s'",
		},
		eventType{ // FormatHeadhunterCheckpointEndSuccess
			patternType:  patternS,
			formatString: "%s headhunter.checkpointDaemon completed putCheckpoint() for Volume '%s' successfully",
		},
		eventType{ // FormatHeadhunterCheckpointEndFailure
			patternType:  patternSS,
			formatString: "%s headhunter.checkpointDaemon completed putCheckpoint() for Volume '%s' with error: %s",
		},
		eventType{ // FormatHeadhunterRecordTransactionNonceRangeReserve
			patternType:  patternS016X016X,
			formatString: "%s Headhunter recording reservation of Volume '%s' Nonces 0x%016X thru 0x%016X (inclusive)",
		},
		eventType{ // FormatHeadhunterRecordTransactionPutInodeRec
			patternType:  patternS016X,
			formatString: "%s Headhunter recording PutInodeRec for Volume '%s' Inode# 0x%016X",
		},
		eventType{ // FormatHeadhunterRecordTransactionPutInodeRecs
			patternType:  patternS016Xslice,
			formatString: "%s Headhunter recording PutInodeRecs for Volume '%s' Inode#'s [0x%016X]",
		},
		eventType{ // FormatHeadhunterRecordTransactionDeleteInodeRec
			patternType:  patternS016X,
			formatString: "%s Headhunter recording DeleteInodeRec for Volume '%s' Inode# 0x%016X",
		},
		eventType{ // FormatHeadhunterRecordTransactionPutLogSegmentRec
			patternType:  patternS016XS,
			formatString: "%s Headhunter recording PutLogSegmentRec for Volume '%s' LogSegment# 0x%016X => Container: '%s'",
		},
		eventType{ // FormatHeadhunterRecordTransactionDeleteLogSegmentRec
			patternType:  patternS016X,
			formatString: "%s Headhunter recording DeleteLogSegmentRec for Volume '%s' LogSegment# 0x%016X",
		},
		eventType{ // FormatHeadhunterRecordTransactionPutBPlusTreeObject
			patternType:  patternS016X,
			formatString: "%s Headhunter recording PutBPlusTreeObject for Volume '%s' Virtual Object# 0x%016X",
		},
		eventType{ // FormatHeadhunterRecordTransactionDeleteBPlusTreeObject
			patternType:  patternS016X,
			formatString: "%s Headhunter recording DeleteBPlusTreeObject for Volume '%s' Virtual Object# 0x%016X",
		},
		eventType{ // FormatHeadhunterMissingInodeRec
			patternType:  patternS016X,
			formatString: "%s Headhunter recording DeleteBPlusTreeObject for Volume '%s' Inode# 0x%016X",
		},
		eventType{ // FormatHeadhunterMissingLogSegmentRec
			patternType:  patternS016X,
			formatString: "%s Headhunter recording DeleteBPlusTreeObject for Volume '%s' LogSegment# 0x%016X",
		},
		eventType{ // FormatHeadhunterMissingBPlusTreeObject
			patternType:  patternS016X,
			formatString: "%s Headhunter recording DeleteBPlusTreeObject for Volume '%s' Virtual Object# 0x%016X",
		},
		eventType{ // FormatHeadhunterBPlusTreeNodeFault
			patternType:  patternS016X016X016X,
			formatString: "%s Headhunter B+Tree NodeFault for Volume '%s' from Object# 0x%016X Offset 0x%016X Length 0x%016X",
		},
		eventType{ // FormatDirFileBPlusTreeNodeFault
			patternType:  patternS016X016X,
			formatString: "%s Inode B+Tree NodeFault for Volume '%s' for Inode# 0x%016X from Virtual Object# 0x%016X",
		},
		eventType{ // FormatFlushInodesEntry
			patternType:  patternS016Xslice,
			formatString: "%s inode.flushInodes() entered for Volume '%s' Inode#'s [0x%016X]",
		},
		eventType{ // FormatFlushInodesDirOrFilePayloadObjectNumberUpdated
			patternType:  patternS016X016X,
			formatString: "%s inode.flushInodes() updated payload for Volume '%s' Inode# 0x%016X with ObjectNumber 0x%016X",
		},
		eventType{ // FormatFlushInodesErrorOnInode
			patternType:  patternS016XS,
			formatString: "%s inode.flushInodes() exiting for Volume '%s' Inode# 0x%016X with error: %s",
		},
		eventType{ // FormatFlushInodesErrorOnHeadhunterPut
			patternType:  patternSS,
			formatString: "%s inode.flushInodes() exiting for Volume '%s' with headhunter.PutInodeRecs() error: %s",
		},
		eventType{ // FormatFlushInodesExit
			patternType:  patternS016Xslice,
			formatString: "%s inode.flushInodes() exited for Volume '%s' Inode#'s [0x%016X]",
		},
		eventType{ // FormatLeaseRequest
			patternType:  patternSS016X1X,
			formatString: "%s jrpcfs.RpcLease() entered for Volume '%s' MountID '%s' Inode# 0x%016X RequestType %1X",
		},
		eventType{ // FormatLeaseReply
			patternType:  patternSS016X1X1X,
			formatString: "%s jrpcfs.RpcLease() exited for Volume '%s' MountID '%s' Inode# 0x%016X RequestType %1X ReplyType %1X",
		},
		eventType{ // FormatLeaseInterrupt
			patternType:  patternSS016X1X,
			formatString: "%s jrpcfs.RpcLease() entered for Volume '%s' MountID '%s' Inode# 0x%016X InterruptType %1X",
		},
	}
)

// Record is used to log an event to the shared memory object.
func Record(formatType FormatType, args ...interface{}) {
	if globals.eventLogEnabled {
		record(formatType, args...)
	}
}

// Retrieve is used to fetch an event from the shared memory object.
// If the event log is enabled, the call will block until an event is available.
// If the event log is not enabled, the call will return an empty string ("").
func Retrieve() (formattedRecord string, numDroppedRecords uint64) {
	if globals.eventLogEnabled {
		formattedRecord, numDroppedRecords = retrieve()
	} else {
		formattedRecord = ""
		numDroppedRecords = 0
	}
	return
}

// Mark for deletion causes the the shared memory object to be deleted upon the last Down() call referencing it.
func MarkForDeletion() {
	if globals.eventLogEnabled {
		markForDeletion()
	}
}
