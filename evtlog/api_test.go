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
		retrievedRecordForFormatTestPatternFixed     string
		retrievedRecordForFormatTestPatternS03D      string
		retrievedRecordForFormatTestPatternSS03D     string
		retrievedRecordForFormatTestPatternSSS03D    string
		retrievedRecordForFormatTestPatternSS016X03D string
	)

	benchmarkConfMapStrings = []string{
		"EventLog.Enabled=true",
		"EventLog.BufferKey=1234",
		"EventLog.BufferLength=65536", //64KiB
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

	Record(FormatTestPatternFixed)
	Record(FormatTestPatternS03D, "arg0", uint32(1))
	Record(FormatTestPatternSS03D, "arg0", "arg1", uint32(2))
	Record(FormatTestPatternSSS03D, "arg0", "arg1", "arg2", uint32(3))
	Record(FormatTestPatternSS016X03D, "arg0", "arg1", uint64(0x2), uint32(3))

	retrievedRecordForFormatTestPatternFixed = Retrieve()
	retrievedRecordForFormatTestPatternS03D = Retrieve()
	retrievedRecordForFormatTestPatternSS03D = Retrieve()
	retrievedRecordForFormatTestPatternSSS03D = Retrieve()
	retrievedRecordForFormatTestPatternSS016X03D = Retrieve()

	if "Test for patternFixed" != retrievedRecordForFormatTestPatternFixed[25:] {
		t.Fatalf("Retrieval of FormatTestPatternFixed failed")
	}
	if "Test for patternS03D arg0:arg0 arg1:001" != retrievedRecordForFormatTestPatternS03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS03D failed")
	}
	if "Test for patternSS03D arg0:arg0 arg1:arg1 arg2:002" != retrievedRecordForFormatTestPatternSS03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSS03D failed")
	}
	if "Test for patternSSS03D arg0:arg0 arg1:arg1 arg2:arg2 arg3:003" != retrievedRecordForFormatTestPatternSSS03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSSS03D failed")
	}
	if "Test for patternSS016X03D arg0:arg0 arg1:arg1 arg2:0000000000000002 arg3:003" != retrievedRecordForFormatTestPatternSS016X03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSS016X03D failed")
	}

	// TODO: Eventually, it would be nice to test the overrun & wrap cases...

	err = Down()
	if nil != err {
		t.Fatal(err)
	}
}
