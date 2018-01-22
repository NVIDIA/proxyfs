package halter

import "fmt"

// Note: Following const block and HaltLabelStrings should be kept in sync

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
		"inode.flushInodes() entry",
		"inode.flushInodes() exit",
	}
)

// Arm sets up a call to panic() on the haltAfterCount'd call to Trigger()
func Arm(haltLabelString string, haltAfterCount uint32) {
	globals.Lock()
	haltLabel, ok := globals.triggerNamesToNumbers[haltLabelString]
	if !ok {
		err := fmt.Errorf("halter.Arm(haltLabelString='%v',) - label unknown", haltLabelString)
		if nil == globals.testModePanicCB {
			panic(err)
		} else {
			globals.testModePanicCB(err)
		}
	}
	if 0 == haltAfterCount {
		err := fmt.Errorf("halter.Arm(haltLabel==%v,) called with haltAfterCount==0", haltLabelString)
		if nil == globals.testModePanicCB {
			panic(err)
		} else {
			globals.testModePanicCB(err)
		}
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
		if nil == globals.testModePanicCB {
			panic(err)
		} else {
			globals.testModePanicCB(err)
		}
	}
	delete(globals.armedTriggers, haltLabel)
	globals.Unlock()
}

// Trigger decrements the haltAfterCount if armed and, should it reach 0, calls panic()
func Trigger(haltLabel uint32) {
	globals.Lock()
	numTriggersRemaining, armed := globals.armedTriggers[haltLabel]
	if !armed {
		globals.Unlock()
		return
	}
	numTriggersRemaining--
	if 0 == numTriggersRemaining {
		err := fmt.Errorf("halter.TriggerArm(haltLabelString==%v) triggered panic()", globals.triggerNumbersToNames[haltLabel])
		if nil == globals.testModePanicCB {
			panic(err)
		} else {
			globals.testModePanicCB(err)
		}
	}
	globals.armedTriggers[haltLabel] = numTriggersRemaining
	globals.Unlock()
}

// Dump returns a map of currently armed triggers and their remaining trigger cound
func Dump() (armedTriggers map[string]uint32) {
	armedTriggers = make(map[string]uint32)
	for k, v := range globals.armedTriggers {
		armedTriggers[globals.triggerNumbersToNames[k]] = v
	}
	return
}
