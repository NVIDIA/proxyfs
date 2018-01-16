package evtlog

import (
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
)

func TestAPI(t *testing.T) {
	var (
		benchmarkConfMap        conf.ConfMap
		benchmarkConfMapStrings []string
		err                     error
		//
		retrievedRecordForFormatTestPatternFixed      string
		retrievedRecordForFormatTestPatternS03D       string
		retrievedRecordForFormatTestPatternSS03D      string
		retrievedRecordForFormatTestPatternSSS        string
		retrievedRecordForFormatTestPatternSSS03D     string
		retrievedRecordForFormatTestPatternSSS016X03D string
	)

	benchmarkConfMapStrings = []string{
		"EventLog.Enabled=true",
		"EventLog.BufferKey=9876",     // Don't conflict with a running instance
		"EventLog.BufferLength=65536", // 64KiB
		"EventLog.MinBackoff=1us",
		"EventLog.MaxBackoff=2us",
	}

	benchmarkConfMap, err = conf.MakeConfMapFromStrings(benchmarkConfMapStrings)
	if nil != err {
		t.Fatal(err)
	}

	err = Up(benchmarkConfMap)
	if nil != err {
		t.Fatal(err)
	}

	MarkForDeletion()

	Record(FormatTestPatternFixed)
	Record(FormatTestPatternS03D, "arg0", uint32(1))
	Record(FormatTestPatternSS03D, "arg0", "arg.1", uint32(2))
	Record(FormatTestPatternSSS, "arg0", "arg.1", "arg..2")
	Record(FormatTestPatternSSS03D, "arg0", "arg.1", "arg..2", uint32(3))
	Record(FormatTestPatternSSS016X03D, "arg0", "arg.1", "arg..2", uint64(3), uint32(4))

	retrievedRecordForFormatTestPatternFixed = Retrieve()
	retrievedRecordForFormatTestPatternS03D = Retrieve()
	retrievedRecordForFormatTestPatternSS03D = Retrieve()
	retrievedRecordForFormatTestPatternSSS = Retrieve()
	retrievedRecordForFormatTestPatternSSS03D = Retrieve()
	retrievedRecordForFormatTestPatternSSS016X03D = Retrieve()

	if "Test for patternFixed" != retrievedRecordForFormatTestPatternFixed[25:] {
		t.Fatalf("Retrieval of FormatTestPatternFixed failed")
	}
	if "Test for patternS03D arg0:arg0 arg1:001" != retrievedRecordForFormatTestPatternS03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS03D failed")
	}
	if "Test for patternSS03D arg0:arg0 arg1:arg.1 arg2:002" != retrievedRecordForFormatTestPatternSS03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSS03D failed")
	}
	if "Test for patternSSS arg0:arg0 arg1:arg.1 arg2:arg..2" != retrievedRecordForFormatTestPatternSSS[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSSS failed")
	}
	if "Test for patternSSS03D arg0:arg0 arg1:arg.1 arg2:arg..2 arg3:003" != retrievedRecordForFormatTestPatternSSS03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSSS03D failed")
	}
	if "Test for patternSSS016X03D arg0:arg0 arg1:arg.1 arg2:arg..2 arg3:0000000000000003 arg4:004" != retrievedRecordForFormatTestPatternSSS016X03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSSS016X03D failed")
	}

	// TODO: Eventually, it would be nice to test the overrun & wrap cases...

	err = Down()
	if nil != err {
		t.Fatal(err)
	}
}
