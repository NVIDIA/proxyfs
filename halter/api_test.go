package halter

import (
	"fmt"
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/transitions"
)

var (
	testHaltErr error
)

func TestAPI(t *testing.T) {
	testConfMapStrings := []string{
		"Logging.LogFilePath=/dev/null",
		"Cluster.WhoAmI=nobody",
		"FSGlobals.VolumeGroupList=",
	}

	testConfMap, err := conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatal(err)
	}

	err = transitions.Up(testConfMap)
	if nil != err {
		t.Fatal(err)
	}

	configureTestModeHaltCB(testHalt)

	availableTriggers := List()
	apiTestHaltLabel1Found := false
	apiTestHaltLabel2Found := false
	for _, s := range availableTriggers {
		if "halter.testHaltLabel1" == s {
			apiTestHaltLabel1Found = true
		}
		if "halter.testHaltLabel2" == s {
			apiTestHaltLabel2Found = true
		}
	}
	if !apiTestHaltLabel1Found {
		t.Fatalf("List() unexpectedly missing 'halter.testHaltLabel1'")
	}
	if !apiTestHaltLabel2Found {
		t.Fatalf("List() unexpectedly missing 'halter.testHaltLabel2'")
	}

	m1 := Dump()
	if 0 != len(m1) {
		t.Fatalf("Dump() unexpectedly returned length %v map at start-up", len(m1))
	}

	testHaltErr = nil
	Arm("halter.testHaltLabel0", 1)
	if nil == testHaltErr {
		t.Fatalf("Arm(apiTestHaltLabel0,) unexpectedly left testHaltErr as nil")
	}
	if "halter.Arm(haltLabelString='halter.testHaltLabel0',) - label unknown" != testHaltErr.Error() {
		t.Fatalf("Arm(apiTestHaltLabel0,) unexpectedly set testHaltErr to %v", testHaltErr)
	}
	_, err = Stat("halter.testHaltLabel0")
	if nil == err {
		t.Fatalf("Stat(\"halter.testHaltLabel0\") unexpectedly succeeded")
	}

	testHaltErr = nil
	Arm("halter.testHaltLabel1", 0)
	if nil == testHaltErr {
		t.Fatalf("Arm(apiTestHaltLabel1,0) unexpectedly left testHaltErr as nil")
	}
	if "halter.Arm(haltLabel==halter.testHaltLabel1,) called with haltAfterCount==0" != testHaltErr.Error() {
		fmt.Println(testHaltErr.Error())
		t.Fatalf("Arm(apiTestHaltLabel0,) unexpectedly set testHaltErr to %v", testHaltErr)
	}
	v0, err := Stat("halter.testHaltLabel1")
	if nil != err {
		t.Fatalf("Stat(\"halter.testHaltLabel1\") unexpectedly failed: %v", err)
	}
	if 0 != v0 {
		t.Fatalf("Stat(\"halter.testHaltLabel1\") unexpectedly returned haltAfterCount == %v (should have been 0)", v0)
	}

	Arm("halter.testHaltLabel1", 1)
	m2 := Dump()
	if 1 != len(m2) {
		t.Fatalf("Dump() unexpectedly returned length %v map after Arm(apiTestHaltLabel1,)", len(m2))
	}
	m2v1, ok := m2["halter.testHaltLabel1"]
	if !ok {
		t.Fatalf("Dump() unexpectedly missing m2[apiTestHaltLabel1]")
	}
	if 1 != m2v1 {
		t.Fatalf("Dump() unexpectedly returned %v for m2[apiTestHaltLabel1]", m2v1)
	}
	v1, err := Stat("halter.testHaltLabel1")
	if nil != err {
		t.Fatalf("Stat(\"halter.testHaltLabel1\") unexpectedly failed: %v", err)
	}
	if 1 != v1 {
		t.Fatalf("Stat(\"halter.testHaltLabel1\") unexpectedly returned haltAfterCount == %v (should have been 1)", v1)
	}

	Arm("halter.testHaltLabel2", 2)
	m3 := Dump()
	if 2 != len(m3) {
		t.Fatalf("Dump() unexpectedly returned length %v map after Arm(apiTestHaltLabel2,)", len(m3))
	}
	m3v1, ok := m3["halter.testHaltLabel1"]
	if !ok {
		t.Fatalf("Dump() unexpectedly missing m3[apiTestHaltLabel1]")
	}
	if 1 != m3v1 {
		t.Fatalf("Dump() unexpectedly returned %v for m3[apiTestHaltLabel1]", m3v1)
	}
	m3v2, ok := m3["halter.testHaltLabel2"]
	if !ok {
		t.Fatalf("Dump() unexpectedly missing m3[apiTestHaltLabel2]")
	}
	if 2 != m3v2 {
		t.Fatalf("Dump() unexpectedly returned %v for m3[apiTestHaltLabel1]", m3v2)
	}

	testHaltErr = nil
	Disarm("halter.testHaltLabel0")
	if nil == testHaltErr {
		t.Fatalf("Disarm(apiTestHaltLabel) unexpectedly left testHaltErr as nil")
	}
	if "halter.Disarm(haltLabelString='halter.testHaltLabel0') - label unknown" != testHaltErr.Error() {
		t.Fatalf("Disarm(apiTestHaltLabel0) unexpectedly set testHaltErr to %v", testHaltErr)
	}

	Disarm("halter.testHaltLabel1")
	m4 := Dump()
	if 1 != len(m4) {
		t.Fatalf("Dump() unexpectedly returned length %v map after Disarm(apiTestHaltLabel1)", len(m4))
	}
	m4v2, ok := m4["halter.testHaltLabel2"]
	if !ok {
		t.Fatalf("Dump() unexpectedly missing m4[apiTestHaltLabel2]")
	}
	if 2 != m4v2 {
		t.Fatalf("Dump() unexpectedly returned %v for m4[apiTestHaltLabel2]", m4v2)
	}

	testHaltErr = nil
	Trigger(apiTestHaltLabel2)
	if nil != testHaltErr {
		t.Fatalf("Trigger(apiTestHaltLabel2) [case 1] unexpectedly set testHaltErr to %v", testHaltErr)
	}
	m5 := Dump()
	if 1 != len(m5) {
		t.Fatalf("Dump() unexpectedly returned length %v map after Trigger(apiTestHaltLabel2)", len(m5))
	}
	m5v2, ok := m5["halter.testHaltLabel2"]
	if !ok {
		t.Fatalf("Dump() unexpectedly missing m5[apiTestHaltLabel2]")
	}
	if 2 != m4v2 {
		t.Fatalf("Dump() unexpectedly returned %v for m5[apiTestHaltLabel2]", m5v2)
	}

	Trigger(apiTestHaltLabel2)
	if nil == testHaltErr {
		t.Fatalf("Trigger(apiTestHaltLabel2) [case 2] unexpectedly left testHaltErr as nil")
	}
	if "halter.TriggerArm(haltLabelString==halter.testHaltLabel2) triggered HALT" != testHaltErr.Error() {
		t.Fatalf("Trigger(apiTestHaltLabel2) [case 2] unexpectedly set testHaltErr to %v", testHaltErr)
	}

	err = transitions.Down(testConfMap)
	if nil != err {
		t.Fatal(err)
	}
}

func testHalt(err error) {
	testHaltErr = err
}
