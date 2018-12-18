package evtlog

import (
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/transitions"
)

func TestAPI(t *testing.T) {
	var (
		testConfMap        conf.ConfMap
		testConfMapStrings []string
		err                error
		//
		retrievedRecordForFormatTestPatternFixed          string
		retrievedRecordForFormatTestPatternS              string
		retrievedRecordForFormatTestPatternS03D           string
		retrievedRecordForFormatTestPatternS08X           string
		retrievedRecordForFormatTestPatternS016X          string
		retrievedRecordForFormatTestPatternS016X016X      string
		retrievedRecordForFormatTestPatternS016X016X016X  string
		retrievedRecordForFormatTestPatternS016XsliceLen0 string
		retrievedRecordForFormatTestPatternS016XsliceLen1 string
		retrievedRecordForFormatTestPatternS016XsliceLen3 string
		retrievedRecordForFormatTestPatternS016XS         string
		retrievedRecordForFormatTestPatternSS             string
		retrievedRecordForFormatTestPatternSS03D          string
		retrievedRecordForFormatTestPatternSSS            string
		retrievedRecordForFormatTestPatternSSS03D         string
		retrievedRecordForFormatTestPatternSSS016X03D     string
		retrievedRecordForFormatTestPatternSSS016X016X03D string
	)

	testConfMapStrings = []string{
		"Logging.LogFilePath=/dev/null",
		"Logging.TraceLevelLogging=none",
		"Logging.DebugLevelLogging=none",
		"Logging.LogToConsole=false",
		"Cluster.WhoAmI=nobody",
		"FSGlobals.VolumeGroupList=",
		"EventLog.Enabled=true",
		"EventLog.BufferKey=9876",     // Don't conflict with a running instance
		"EventLog.BufferLength=65536", // 64KiB
		"EventLog.MinBackoff=1us",
		"EventLog.MaxBackoff=2us",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatal(err)
	}

	err = transitions.Up(testConfMap)
	if nil != err {
		t.Fatal(err)
	}

	MarkForDeletion()

	Record(FormatTestPatternFixed)
	Record(FormatTestPatternS, "arg0")
	Record(FormatTestPatternS03D, "arg0", uint32(1))
	Record(FormatTestPatternS08X, "arg0", uint32(1))
	Record(FormatTestPatternS016X, "arg0", uint64(1))
	Record(FormatTestPatternS016X016X, "arg0", uint64(1), uint64(2))
	Record(FormatTestPatternS016X016X016X, "arg0", uint64(1), uint64(2), uint64(3))
	Record(FormatTestPatternS016Xslice, "arg0", []uint64{})
	Record(FormatTestPatternS016Xslice, "arg0", []uint64{uint64(0x101)})
	Record(FormatTestPatternS016Xslice, "arg0", []uint64{uint64(0x101), uint64(0x102), uint64(0x103)})
	Record(FormatTestPatternS016XS, "arg0", uint64(1), "arg..2")
	Record(FormatTestPatternSS, "arg0", "arg.1")
	Record(FormatTestPatternSS03D, "arg0", "arg.1", uint32(2))
	Record(FormatTestPatternSSS, "arg0", "arg.1", "arg..2")
	Record(FormatTestPatternSSS03D, "arg0", "arg.1", "arg..2", uint32(3))
	Record(FormatTestPatternSSS016X03D, "arg0", "arg.1", "arg..2", uint64(3), uint32(4))
	Record(FormatTestPatternSSS016X016X03D, "arg0", "arg.1", "arg..2", uint64(3), uint64(4), uint32(5))

	retrievedRecordForFormatTestPatternFixed, _ = Retrieve()
	retrievedRecordForFormatTestPatternS, _ = Retrieve()
	retrievedRecordForFormatTestPatternS03D, _ = Retrieve()
	retrievedRecordForFormatTestPatternS08X, _ = Retrieve()
	retrievedRecordForFormatTestPatternS016X, _ = Retrieve()
	retrievedRecordForFormatTestPatternS016X016X, _ = Retrieve()
	retrievedRecordForFormatTestPatternS016X016X016X, _ = Retrieve()
	retrievedRecordForFormatTestPatternS016XsliceLen0, _ = Retrieve()
	retrievedRecordForFormatTestPatternS016XsliceLen1, _ = Retrieve()
	retrievedRecordForFormatTestPatternS016XsliceLen3, _ = Retrieve()
	retrievedRecordForFormatTestPatternS016XS, _ = Retrieve()
	retrievedRecordForFormatTestPatternSS, _ = Retrieve()
	retrievedRecordForFormatTestPatternSS03D, _ = Retrieve()
	retrievedRecordForFormatTestPatternSSS, _ = Retrieve()
	retrievedRecordForFormatTestPatternSSS03D, _ = Retrieve()
	retrievedRecordForFormatTestPatternSSS016X03D, _ = Retrieve()
	retrievedRecordForFormatTestPatternSSS016X016X03D, _ = Retrieve()

	if "Test for patternFixed" != retrievedRecordForFormatTestPatternFixed[25:] {
		t.Fatalf("Retrieval of FormatTestPatternFixed failed")
	}
	if "Test for patternS arg0:arg0" != retrievedRecordForFormatTestPatternS[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS failed")
	}
	if "Test for patternS03D arg0:arg0 arg1:001" != retrievedRecordForFormatTestPatternS03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS03D failed")
	}
	if "Test for patternS08X arg0:arg0 arg1:00000001" != retrievedRecordForFormatTestPatternS08X[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS08X failed")
	}
	if "Test for patternS016X arg0:arg0 arg1:0000000000000001" != retrievedRecordForFormatTestPatternS016X[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS016X failed")
	}
	if "Test for patternS016X016X arg0:arg0 arg1:0000000000000001 arg2:0000000000000002" != retrievedRecordForFormatTestPatternS016X016X[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS016X016X failed")
	}
	if "Test for patternS016X016X016X arg0:arg0 arg1:0000000000000001 arg2:0000000000000002 arg3:0000000000000003" != retrievedRecordForFormatTestPatternS016X016X016X[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS016X016X failed")
	}
	if "Test for patternS016Xslice arg0:arg0 arg1:[]" != retrievedRecordForFormatTestPatternS016XsliceLen0[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS016Xslice with len([]uint64) == 0 failed")
	}
	if "Test for patternS016Xslice arg0:arg0 arg1:[0x0000000000000101]" != retrievedRecordForFormatTestPatternS016XsliceLen1[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS016Xslice with len([]uint64) == 1 failed")
	}
	if "Test for patternS016Xslice arg0:arg0 arg1:[0x0000000000000101 0x0000000000000102 0x0000000000000103]" != retrievedRecordForFormatTestPatternS016XsliceLen3[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS016Xslice with len([]uint64) == 3 failed")
	}
	if "Test for patternS016XS arg0:arg0 arg1:0000000000000001 arg2:arg..2" != retrievedRecordForFormatTestPatternS016XS[25:] {
		t.Fatalf("Retrieval of FormatTestPatternS016XS failed")
	}
	if "Test for patternSS arg0:arg0 arg1:arg.1" != retrievedRecordForFormatTestPatternSS[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSS failed")
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
	if "Test for patternSSS016X016X03D arg0:arg0 arg1:arg.1 arg2:arg..2 arg3:0000000000000003 arg4:0000000000000004 arg5:005" != retrievedRecordForFormatTestPatternSSS016X016X03D[25:] {
		t.Fatalf("Retrieval of FormatTestPatternSSS016X03D failed")
	}

	// TODO: Eventually, it would be nice to test the overrun & wrap cases...

	err = transitions.Down(testConfMap)
	if nil != err {
		t.Fatal(err)
	}
}
