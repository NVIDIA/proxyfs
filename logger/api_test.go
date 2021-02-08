// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/utils"
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
		"Logging.LogFilePath=/dev/null",
		"Logging.LogToConsole=false",
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

	testParseLogEntry(t)

	err = Down(confMap)
	if nil != err {
		tErr := fmt.Sprintf("logger.Down(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}
}

func testLogTargets(t *testing.T) {
	assert := assert.New(t)

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

	fields, err := ParseLogEntry(targ.LogBuf.LogEntries[0])
	assert.Nil(err, "ParseLogEntry of '%s' should not fail", targ.LogBuf.LogEntries[0])

	names := []string{
		"time", "level", "msg", "goroutine", "package",
	}
	for _, nm := range names {
		if fields[nm] == "" {
			t.Errorf("unable to parse field '%s' in log entry '%s'",
				nm, targ.LogBuf.LogEntries[0])
		}
	}
}

func testParseLogEntry(t *testing.T) {
	assert := assert.New(t)

	var targ LogTarget
	targ.Init(10)

	// targ will get a copy of each log entry (and will live on after this
	// function returns)
	AddLogTarget(targ)

	var (
		fields map[string]string
		names  []string
		err    error
	)

	// test a trace log entry (logs as level=info
	Tracef("%s %s", "Hello,", "World")

	fields, err = ParseLogEntry(targ.LogBuf.LogEntries[0])
	assert.Nil(err, "ParseLogEntry of '%s' should not fail", targ.LogBuf.LogEntries[0])

	names = []string{
		"time", "level", "msg", "goroutine", "package",
	}
	for _, nm := range names {
		if fields[nm] == "" {
			t.Errorf("unable to parse field '%s' in log entry '%s'",
				nm, targ.LogBuf.LogEntries[0])
		}
	}
	if fields["error"] != "" {
		t.Errorf("found 'error' field containing '%s' in log entry '%s'",
			fields["error"], targ.LogBuf.LogEntries[0])
	}
	if fields["level"] != "info" {
		t.Errorf("'level' field contains '%s' should be 'trace' in entry '%s'",
			fields["level"], targ.LogBuf.LogEntries[0])
	}
	if fields["msg"] != "Hello, World" {
		t.Errorf("'msg' field contains '%s' should be 'Hello, World' in entry '%s'",
			fields["trace"], targ.LogBuf.LogEntries[0])
	}

	// test an error entry
	err = fmt.Errorf("this is the error")
	ErrorfWithError(err, "we had an error!")

	fields, err = ParseLogEntry(targ.LogBuf.LogEntries[0])
	assert.Nil(err, "ParseLogEntry of '%s' should not fail", targ.LogBuf.LogEntries[0])

	names = []string{
		"time", "level", "msg", "goroutine", "package", "error",
	}
	for _, nm := range names {
		if fields[nm] == "" {
			t.Errorf("unable to parse field '%s' in log entry '%s'",
				nm, targ.LogBuf.LogEntries[0])
		}
	}
	if fields["error"] != "this is the error" {
		t.Errorf("field 'error' contains '%s' should be 'this is the error' in log entry '%s'",
			fields["error"], targ.LogBuf.LogEntries[0])
	}
	if fields["level"] != "error" {
		t.Errorf("'level' field contains '%s' should be 'error' in entry '%s'",
			fields["level"], targ.LogBuf.LogEntries[0])
	}
	if fields["msg"] != "we had an error!" {
		t.Errorf("'msg' field contains '%s' should be 'we had an error!' in entry '%s'",
			fields["msg"], targ.LogBuf.LogEntries[0])
	}

	msg_in := `When you put "something" in double quotes it means "something else"`
	msg_out := `When you put \"something\" in double quotes it means \"something else\"`
	Tracef(msg_in)
	fields, err = ParseLogEntry(targ.LogBuf.LogEntries[0])
	assert.Nil(err, "ParseLogEntry of '%s' should not fail", targ.LogBuf.LogEntries[0])

	if fields["msg"] != msg_out {
		t.Errorf("'msg' field contains '%s' should be '%s' in entry '%s'",
			fields["msg"], msg_out, targ.LogBuf.LogEntries[0])
	}

	// Double-plus ungood (both msg and error have nested '"')
	errmsg_in := `Finding " when you don't expect " can drive you """`
	errmsg_out := `Finding \" when you don't expect \" can drive you \"\"\"`
	err = fmt.Errorf(errmsg_in)
	ErrorfWithError(err, msg_in)

	fields, err = ParseLogEntry(targ.LogBuf.LogEntries[0])
	assert.Nil(err, "ParseLogEntry of '%s' should not fail", targ.LogBuf.LogEntries[0])

	if fields["msg"] != msg_out {
		t.Errorf("'msg' field contains '%s' should be '%s' in entry '%s'",
			fields["msg"], msg_out, targ.LogBuf.LogEntries[0])
	}
	if fields["error"] != errmsg_out {
		t.Errorf("'error' field contains '%s' should be '%s' in entry '%s'",
			fields["error"], errmsg_out, targ.LogBuf.LogEntries[0])
	}
}
