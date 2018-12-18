package evtlog

import (
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/transitions"
)

var (
	benchmarkConfMap conf.ConfMap
)

func benchmarkSetup(b *testing.B, enable bool) {
	var (
		benchmarkConfMapStrings []string
		err                     error
	)

	if enable {
		benchmarkConfMapStrings = []string{
			"Logging.LogFilePath=/dev/null",
			"Logging.TraceLevelLogging=none",
			"Logging.DebugLevelLogging=none",
			"Logging.LogToConsole=false",
			"Cluster.WhoAmI=nobody",
			"FSGlobals.VolumeGroupList=",
			"EventLog.Enabled=true",
			"EventLog.BufferKey=1234",
			"EventLog.BufferLength=65536", //64KiB
			"EventLog.MinBackoff=1us",
			"EventLog.MaxBackoff=2us",
		}
	} else {
		benchmarkConfMapStrings = []string{
			"Logging.LogFilePath=/dev/null",
			"Logging.TraceLevelLogging=none",
			"Logging.DebugLevelLogging=none",
			"Logging.LogToConsole=false",
			"Cluster.WhoAmI=nobody",
			"FSGlobals.VolumeGroupList=",
			"EventLog.Enabled=false",
		}
	}

	benchmarkConfMap, err = conf.MakeConfMapFromStrings(benchmarkConfMapStrings)
	if nil != err {
		b.Fatal(err)
	}

	err = transitions.Up(benchmarkConfMap)
	if nil != err {
		b.Fatal(err)
	}

	MarkForDeletion()
}

func benchmarkTeardown(b *testing.B) {
	var (
		err error
	)

	err = transitions.Down(benchmarkConfMap)
	if nil != err {
		b.Fatal(err)
	}
}

func Benchmark1KRecordTestPatternFixedWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternFixed)
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS, "arg0")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS03DWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS03D, "arg0", uint32(1))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS08XWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS03D, "arg0", uint32(1))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016XWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016X, "arg0", uint64(1))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016X016XWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016X016X, "arg0", uint64(1), uint64(2))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016XsliceWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016Xslice, "arg0", []uint64{uint64(0x101), uint64(0x102), uint64(0x103)})
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016XSWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016XS, "arg0", uint64(1), "arg..2")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSS, "arg0", "arg.1")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSS03DWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSS03D, "arg0", "arg.1", uint32(2))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSSWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSSS, "arg0", "arg.1", "arg..2")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSS03DWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSSS03D, "arg0", "arg.1", "arg..2", uint32(3))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSS016X03DWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSSS016X03D, "arg0", "arg.1", "arg..2", uint64(3), uint32(4))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternFixedWhileEnabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternFixed)
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS, "arg0")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS03DWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS03D, "arg0", uint32(1))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS08XWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS03D, "arg0", uint32(1))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016XWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016X, "arg0", uint64(1))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016X016XWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016X016X, "arg0", uint64(1), uint64(2))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016XsliceWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016Xslice, "arg0", []uint64{uint64(0x101), uint64(0x102), uint64(0x103)})
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternS016XSWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS016XS, "arg0", uint64(1), "arg..2")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSS, "arg0", "arg.1")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSS03DWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSS03D, "arg0", "arg.1", uint32(2))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSSWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSSS, "arg0", "arg.1", "arg..2")
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSS03DWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSSS03D, "arg0", "arg.1", "arg..2", uint32(3))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSSS016X03DWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSSS016X03D, "arg0", "arg.1", "arg..2", uint64(3), uint32(4))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}
