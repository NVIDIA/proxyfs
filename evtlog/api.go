package evtlog

// FormatType is an efficiently passed indicator of the event type being logged.
type FormatType uint32 // Used as index to event slice (so keep this sequence in sync with event slice)

const (
	FormatTestPatternFixed FormatType = iota
	FormatTestPatternS03D
	FormatTestPatternSS03D
	FormatTestPatternSSS
	FormatTestPatternSSS03D
	FormatTestPatternSSS016X03D
	FormatUpSequenceStart
	FormatUpSequenceEnd
	FormatPauseAndContractSequenceStart
	FormatPauseAndContractSequenceEnd
	FormatExpandAndResumeSequenceStart
	FormatExpandAndResumeSequenceEnd
	FormatDownSequenceStart
	FormatDownSequenceEnd
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
	FormatObjectDeleteAsync
	FormatObjectDeleteSync
	FormatObjectGet
	FormatObjectHead
	FormatObjectPost
	FormatObjectPutChunkedStart
	FormatObjectPutChunkedEnd
	//
	formatTypeCount // Used to quickly check upper limit of FormatType values
)

type patternType uint32

const (
	patternFixed      patternType = iota // <timestamp> + "..."
	patternS03D                          // <timestamp> + "...%s...%03d..."
	patternSS03D                         // <timestamp> + "...%s...%s...%03d..."
	patternSSS                           // <timestamp> + "...%s...%s...%s..."
	patternSSS03D                        // <timestamp> + "...%s...%s...%s...%03d..."
	patternSSS016X03D                    // <timestamp> + "...%s...%s...%s...%016X...%03d..."
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
		eventType{ // FormatTestPatternS03D
			patternType:  patternS03D,
			formatString: "%s Test for patternS03D arg0:%s arg1:%03d",
		},
		eventType{ // FormatTestPatternSS03D
			patternType:  patternSS03D,
			formatString: "%s Test for patternSS03D arg0:%s arg1:%s arg2:%03d",
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
		eventType{ // FormatUpSequenceStart
			patternType:  patternFixed,
			formatString: "%s Up() Sequence Start",
		},
		eventType{ // FormatUpSequenceEnd
			patternType:  patternFixed,
			formatString: "%s Up() Sequence End",
		},
		eventType{ // FormatPauseAndContractSequenceStart
			patternType:  patternFixed,
			formatString: "%s PauseAndContract() Sequence Start",
		},
		eventType{ // FormatPauseAndContractSequenceEnd
			patternType:  patternFixed,
			formatString: "%s PauseAndContract() Sequence End",
		},
		eventType{ // FormatExpandAndResumeSequenceStart
			patternType:  patternFixed,
			formatString: "%s ExpandAndResume() Sequence Start",
		},
		eventType{ // FormatExpandAndResumeSequenceEnd
			patternType:  patternFixed,
			formatString: "%s ExpandAndResume() Sequence End",
		},
		eventType{ // FormatDownSequenceStart
			patternType:  patternFixed,
			formatString: "%s Down() Sequence Start",
		},
		eventType{ // FormatDownSequenceEnd
			patternType:  patternFixed,
			formatString: "%s Down() Sequence End",
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
		eventType{ // FormatObjectDeleteAsync
			patternType:  patternSSS,
			formatString: "%s Object DELETE (Async) %s/%s/%s issued",
		},
		eventType{ // FormatObjectDelete
			patternType:  patternSSS03D,
			formatString: "%s Object DELETE (Sync) %s/%s/%s had status %03d",
		},
		eventType{ // FormatObjectGet
			patternType:  patternSSS03D,
			formatString: "%s Object GET %s/%s/%s had status %03d",
		},
		eventType{ // FormatObjectHead
			patternType:  patternSSS03D,
			formatString: "%s Object HEAD %s/%s/%s had status %03d",
		},
		eventType{ // FormatObjectPost
			patternType:  patternSSS03D,
			formatString: "%s Object POST %s/%s/%s had status %03d",
		},
		eventType{ // FormatObjectPutChunkedStart
			patternType:  patternSSS,
			formatString: "%s Object (chunked) PUT %s/%s/%s initiated",
		},
		eventType{ // FormatObjecFormatObjectPutChunkedEndtPut
			patternType:  patternSSS016X03D,
			formatString: "%s Object (chunked) PUT %s/%s/%s (for 0x%016X bytes) had status %03d",
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
func Retrieve() (formattedRecord string) {
	if globals.eventLogEnabled {
		formattedRecord = retrieve()
	} else {
		formattedRecord = ""
	}
	return
}

// Mark for deletion causes the the shared memory object to be deleted upon the last Down() call referencing it.
func MarkForDeletion() {
	if globals.eventLogEnabled {
		markForDeletion()
	}
}
