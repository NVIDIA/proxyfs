package logger

import (
	"fmt"
	"testing"

	"github.com/swiftstack/conf"

	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/utils"
)

func testNestedFunc() {
	myint := 3
	TraceEnter("the prefix", 1, myint)
}

func TestAPI(t *testing.T) {
	confStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
	}

	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = stats.Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("stats.Up(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	Tracef("hello there!")
	Tracef("hello again, %s!", "you")
	Tracef("%v: %v", utils.GetFnName(), err)
	Warnf("%v: %v", "IAmTheCaller", "this is the error")
	err = fmt.Errorf("this is the error")
	ErrorfWithError(err, "we had an error!")

	testNestedFunc()

	err = stats.Down()
	if nil != err {
		tErr := fmt.Sprintf("stats.Down() failed: %v", err)
		t.Fatalf(tErr)
	}
}
