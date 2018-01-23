package halter

import (
	"fmt"
	"os"
	"syscall"
)

// Note 1: Following const block and HaltLabelStrings should be kept in sync
// Note 2: HaltLabelStrings should be easily parseable as URL components

const (
	apiTestHaltLabel1 = iota
	apiTestHaltLabel2
	InodeFlushInodesEntry
	InodeFlushInodesExit
)

var (
	HaltLabelStrings = []string{
		"halter.testHaltLabel1",
		"halter.testHaltLabel2",
		"inode.flushInodes_Entry",
		"inode.flushInodes_Exit",
	}
)

// Arm sets up a HALT on the haltAfterCount'd call to Trigger()
func Arm(haltLabelString string, haltAfterCount uint32) {
	globals.Lock()
	haltLabel, ok := globals.triggerNamesToNumbers[haltLabelString]
	if !ok {
		err := fmt.Errorf("halter.Arm(haltLabelString='%v',) - label unknown", haltLabelString)
		haltWithErr(err)
	}
	if 0 == haltAfterCount {
		err := fmt.Errorf("halter.Arm(haltLabel==%v,) called with haltAfterCount==0", haltLabelString)
		haltWithErr(err)
	}
	globals.armedTriggers[haltLabel] = haltAfterCount
	globals.Unlock()
}

// Disarm removes a previously armed trigger via a call to Arm()
func Disarm(haltLabelString string) {
	globals.Lock()
	haltLabel, ok := globals.triggerNamesToNumbers[haltLabelString]
	if !ok {
		err := fmt.Errorf("halter.Disarm(haltLabelString='%v') - label unknown", haltLabelString)
		haltWithErr(err)
	}
	delete(globals.armedTriggers, haltLabel)
	globals.Unlock()
}

// Trigger decrements the haltAfterCount if armed and, should it reach 0, HALTs
func Trigger(haltLabel uint32) {
	globals.Lock()
	numTriggersRemaining, armed := globals.armedTriggers[haltLabel]
	if !armed {
		globals.Unlock()
		return
	}
	numTriggersRemaining--
	if 0 == numTriggersRemaining {
		err := fmt.Errorf("halter.TriggerArm(haltLabelString==%v) triggered HALT", globals.triggerNumbersToNames[haltLabel])
		haltWithErr(err)
	}
	globals.armedTriggers[haltLabel] = numTriggersRemaining
	globals.Unlock()
}

// Dump returns a map of currently armed triggers and their remaining trigger count
func Dump() (armedTriggers map[string]uint32) {
	armedTriggers = make(map[string]uint32)
	for k, v := range globals.armedTriggers {
		armedTriggers[globals.triggerNumbersToNames[k]] = v
	}
	return
}

// List returns a slice of available triggers
func List() (availableTriggers []string) {
	availableTriggers = make([]string, 0, len(globals.triggerNumbersToNames))
	for k := range globals.triggerNamesToNumbers {
		availableTriggers = append(availableTriggers, k)
	}
	return
}

func haltWithErr(err error) {
	if nil == globals.testModeHaltCB {
		fmt.Println(err)
		os.Exit(int(syscall.SIGKILL))
	} else {
		globals.testModeHaltCB(err)
	}
}
