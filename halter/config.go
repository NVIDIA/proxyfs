package halter

import (
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
)

type globalsStruct struct {
	sync.Mutex
	armedTriggers         map[uint32]uint32 // key: haltLabel; value: haltAfterCount (remaining)
	triggerNamesToNumbers map[string]uint32
	triggerNumbersToNames map[uint32]string
	testModePanicCB       func(err error)
}

var globals globalsStruct

// Up initializes the package and must successfully return before any API functions are invoked
func Up(confMap conf.ConfMap) (err error) {
	globals.armedTriggers = make(map[uint32]uint32)
	globals.triggerNamesToNumbers = make(map[string]uint32)
	globals.triggerNumbersToNames = make(map[uint32]string)
	for i, s := range HaltLabelStrings {
		globals.triggerNamesToNumbers[s] = uint32(i)
		globals.triggerNumbersToNames[uint32(i)] = s
	}
	globals.testModePanicCB = nil
	err = nil
	return
}

// PauseAndContract pauses the evtlog package and applies any removals from the supplied confMap
func PauseAndContract(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

// ExpandAndResume applies any additions from the supplied confMap and resumes the evtlog package
func ExpandAndResume(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

// Down terminates the halter package
func Down() (err error) {
	// Nothing to do here
	err = nil
	return
}

func configurTestModePanicCB(testPanic func(err error)) {
	globals.Lock()
	globals.testModePanicCB = testPanic
	globals.Unlock()
}
