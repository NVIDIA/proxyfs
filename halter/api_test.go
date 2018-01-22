package halter

import (
	"fmt"
	"testing"
)

var (
	testPanicErr error
)

func TestAPI(t *testing.T) {
	Up(nil)

	configurTestModePanicCB(testPanic)

	m1 := Dump()
	if 0 != len(m1) {
		t.Fatalf("Dump() unexpectedly returned length %v map at start-up", len(m1))
	}

	testPanicErr = nil
	Arm("halter.testHaltLabel0", 1)
	if nil == testPanicErr {
		t.Fatalf("Arm(apiTestHaltLabel0,) unexpectedly left testPanicErr as nil")
	}
	if "halter.Arm(haltLabelString='halter.testHaltLabel0',) - label unknown" != testPanicErr.Error() {
		t.Fatalf("Arm(apiTestHaltLabel0,) unexpectedly set testPanicErr to %v", testPanicErr)
	}

	testPanicErr = nil
	Arm("halter.testHaltLabel1", 0)
	if nil == testPanicErr {
		t.Fatalf("Arm(apiTestHaltLabel1,0) unexpectedly left testPanicErr as nil")
	}
	if "halter.Arm(haltLabel==halter.testHaltLabel1,) called with haltAfterCount==0" != testPanicErr.Error() {
		fmt.Println(testPanicErr.Error())
		t.Fatalf("Arm(apiTestHaltLabel0,) unexpectedly set testPanicErr to %v", testPanicErr)
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

	testPanicErr = nil
	Disarm("halter.testHaltLabel0")
	if nil == testPanicErr {
		t.Fatalf("Disarm(apiTestHaltLabel) unexpectedly left testPanicErr as nil")
	}
	if "halter.Disarm(haltLabelString='halter.testHaltLabel0') - label unknown" != testPanicErr.Error() {
		t.Fatalf("Disarm(apiTestHaltLabel0) unexpectedly set testPanicErr to %v", testPanicErr)
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

	testPanicErr = nil
	Trigger(apiTestHaltLabel2)
	if nil != testPanicErr {
		t.Fatalf("Trigger(apiTestHaltLabel2) [case 1] unexpectedly set testPanicErr to %v", testPanicErr)
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
	if nil == testPanicErr {
		t.Fatalf("Trigger(apiTestHaltLabel2) [case 2] unexpectedly left testPanicErr as nil")
	}
	if "halter.TriggerArm(haltLabelString==halter.testHaltLabel2) triggered panic()" != testPanicErr.Error() {
		t.Fatalf("Trigger(apiTestHaltLabel2) [case 2] unexpectedly set testPanicErr to %v", testPanicErr)
	}
}

func testPanic(err error) {
	testPanicErr = err
}
