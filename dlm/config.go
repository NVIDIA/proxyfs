package dlm

// Configuration variables for DLM

import (
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/transitions"
)

type globalsStruct struct {
	sync.Mutex

	// Map used to store locks owned locally
	// NOTE: This map is protected by the Mutex
	localLockMap map[string]*localLockTrack

	// TODO - channels for STOP and from DLM lock master?
	// is the channel lock one per lock or a global one from DLM?
	// how could it be... probably just one receive thread the locks
	// map, checks bit and releases lock if no one using, otherwise
	// blocks until it is free...
}

var globals globalsStruct

func init() {
	transitions.Register("dlm", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	// Create map used to store locks
	globals.localLockMap = make(map[string]*localLockTrack)
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
	return nil
}
