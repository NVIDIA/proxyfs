package halter

import (
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/transitions"
)

type globalsStruct struct {
	sync.Mutex
	armedTriggers         map[uint32]uint32 // key: haltLabel; value: haltAfterCount (remaining)
	triggerNamesToNumbers map[string]uint32
	triggerNumbersToNames map[uint32]string
	testModeHaltCB        func(err error)
}

var globals globalsStruct

func init() {
	transitions.Register("halter", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	globals.armedTriggers = make(map[uint32]uint32)
	globals.triggerNamesToNumbers = make(map[string]uint32)
	globals.triggerNumbersToNames = make(map[uint32]string)
	for i, s := range HaltLabelStrings {
		globals.triggerNamesToNumbers[s] = uint32(i)
		globals.triggerNumbersToNames[uint32(i)] = s
	}
	globals.testModeHaltCB = nil
	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) Signaled(confMap conf.ConfMap) (err error) {
	return nil
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	err = nil
	return
}

func configureTestModeHaltCB(testHalt func(err error)) {
	globals.Lock()
	globals.testModeHaltCB = testHalt
	globals.Unlock()
}
