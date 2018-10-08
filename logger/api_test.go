package logger

import (
	"fmt"
	"strings"
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/utils"
)

func testNestedFunc() {
	myint := 3
	myctx := TraceEnter("my_args_prefix", 1, myint)
	defer myctx.TraceExit("my_no_args_prefix")

	test2ndNestedFunc()
}

func test2ndNestedFunc() {
	myint := 77
	myctx := TraceEnter("favorite_ints", myint, 23, 64, "hike")
	defer myctx.TraceExit("no_favorite_ints")
}

func TestAPI(t *testing.T) {
	confStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"Logging.TraceLevelLogging=logger",
	}

	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("logger.Up(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	Tracef("hello there!")
	Tracef("hello again, %s!", "you")
	Tracef("%v: %v", utils.GetFnName(), err)
	Warnf("%v: %v", "IAmTheCaller", "this is the error")
	err = fmt.Errorf("this is the error")
	ErrorfWithError(err, "we had an error!")

	testNestedFunc()

	testLogTargets(t)

	err = Down(confMap)
	if nil != err {
		tErr := fmt.Sprintf("logger.Down(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}
}

func testLogTargets(t *testing.T) {
	var targ LogTarget
	targ.Init(10)

	// targ will get a copy of each log entry (and will live on after this
	// function returns)
	AddLogTarget(targ)

	Warnf("Beware the Ides of March")
	if targ.LogBuf.LogEntries[0] == "" {
		t.Error("log target didn't receive the logged warning")
	}
	if !strings.Contains(targ.LogBuf.LogEntries[0], "the Ides of March") {
		t.Error("log entry didn't include the 'Ides of March'")
	}
	if targ.LogBuf.TotalEntries != 1 {
		t.Error("log target doesn''t count correctly")
	}

	Tracef("%s %s", "Hello,", "World")
	if !strings.Contains(targ.LogBuf.LogEntries[0], "Hello, World") {
		t.Errorf("log entry '%s' didn't include the 'Hello, World'", targ.LogBuf.LogEntries[0])
	}
	if targ.LogBuf.TotalEntries != 2 {
		t.Error("log target doesn't count correctly")
	}

}
