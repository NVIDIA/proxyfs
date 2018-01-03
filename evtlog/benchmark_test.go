package evtlog

import (
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
)

/*
	FormatTestPatternFixed FormatType = iota
	FormatTestPatternS03D
	FormatTestPatternSS03D
	FormatTestPatternSSS03D
	FormatTestPatternSS016X03D
*/

var (
	benchmarkConfMap conf.ConfMap
)

func benchmarkSetup(b *testing.B, enable bool) {
	var (
		benchmarkConfMap        conf.ConfMap
		benchmarkConfMapStrings []string
		err                     error
	)

	if enable {
		benchmarkConfMapStrings = []string{
			"EventLog.Enabled=true",
			"EventLog.BufferKey=1234",
			"EventLog.BufferLength=65536", //64KiB
			"EventLog.MinBackoff=1us",
			"EventLog.MaxBackoff=2us",
		}
	} else {
		benchmarkConfMapStrings = []string{
			"EventLog.Enabled=false",
		}
	}

	benchmarkConfMap, err = conf.MakeConfMapFromStrings(benchmarkConfMapStrings)
	if nil != err {
		b.Fatal(err)
	}

	err = Up(benchmarkConfMap)
	if nil != err {
		b.Fatal(err)
	}
}

func benchmarkTeardown(b *testing.B) {
	var (
		err error
	)

	err = Down()
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

func Benchmark1KRecordTestPatternS03DWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternS03D, "arg0", 1)
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
			Record(FormatTestPatternSS03D, "arg0", "arg1", 2)
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
			Record(FormatTestPatternSSS03D, "arg0", "arg1", "arg2", 3)
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSS016X03DWhileDisabled(b *testing.B) {
	benchmarkSetup(b, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSS016X03D, "arg0", "arg1", 0x2, 3)
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

func Benchmark1KRecordTestPatternSS03DWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSS03D, "arg0", "arg1", uint32(2))
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
			Record(FormatTestPatternSSS03D, "arg0", "arg1", "arg2", uint32(3))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}

func Benchmark1KRecordTestPatternSS016X03DWhileEnabled(b *testing.B) {
	benchmarkSetup(b, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			Record(FormatTestPatternSS016X03D, "arg0", "arg1", uint64(0x2), uint32(3))
		}
	}
	b.StopTimer()
	benchmarkTeardown(b)
}
