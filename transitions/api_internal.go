package transitions

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
)

type loggerCallbacksInterfaceStruct struct {
}

var loggerCallbacksInterface loggerCallbacksInterfaceStruct

type registrationItemStruct struct {
	packageName string
	callbacks   Callbacks
}

type volumeStruct struct {
	name        string
	served      bool
	volumeGroup *volumeGroupStruct
}

type volumeGroupStruct struct {
	name          string
	served        bool
	activePeer    string
	virtualIPAddr string
	volumeList    map[string]*volumeStruct // Key: volumeStruct.name
}

type confMapDeltaStruct struct {
	volumeGroupList       map[string]*volumeGroupStruct // Key: volumeGroupStruct.name
	servedVolumeGroupList map[string]*volumeGroupStruct // Key: volumeGroupStruct.name
	remoteVolumeGroupList map[string]*volumeGroupStruct // Key: volumeGroupStruct.name

	createdVolumeGroupList   map[string]*volumeGroupStruct // Key: volumeGroupStruct.name
	movedVolumeGroupList     map[string]*volumeGroupStruct // Key: volumeGroupStruct.name
	destroyedVolumeGroupList map[string]*volumeGroupStruct // Key: volumeGroupStruct.name

	volumeList       map[string]*volumeStruct // Key: volumeStruct.name
	servedVolumeList map[string]*volumeStruct // Key: volumeStruct.name
	remoteVolumeList map[string]*volumeStruct // Key: volumeStruct.name

	createdVolumeList   map[string]*volumeStruct // Key: volumeStruct.name
	movedVolumeList     map[string]*volumeStruct // Key: volumeStruct.name
	destroyedVolumeList map[string]*volumeStruct // Key: volumeStruct.name

	toStopServingVolumeList  map[string]*volumeStruct // Key: volumeStruct.name
	toStartServingVolumeList map[string]*volumeStruct // Key: volumeStruct.name
}

type globalsStruct struct {
	sync.Mutex          //                                    Used only for protecting insertions into registration{List|Set} during init() phase
	registrationList    *list.List
	registrationSet     map[string]*registrationItemStruct // Key: registrationItemStruct.packageName
	currentConfMapDelta *confMapDeltaStruct
}

var globals globalsStruct

func init() {
	globals.Lock()
	globals.registrationList = list.New()
	globals.registrationSet = make(map[string]*registrationItemStruct)
	globals.Unlock()

	Register("logger", &loggerCallbacksInterface)
}

func register(packageName string, callbacks Callbacks) {
	var (
		alreadyRegisted  bool
		registrationItem *registrationItemStruct
	)

	globals.Lock()
	_, alreadyRegisted = globals.registrationSet[packageName]
	if alreadyRegisted {
		logger.Fatalf("transitions.Register(%s,) called twice", packageName)
	}
	registrationItem = &registrationItemStruct{packageName, callbacks}
	_ = globals.registrationList.PushBack(registrationItem)
	globals.registrationSet[packageName] = registrationItem
	globals.Unlock()
}

func up(confMap conf.ConfMap) (err error) {
	var (
		newConfMapDelta                        *confMapDeltaStruct
		registrationItem                       *registrationItemStruct
		registrationListElement                *list.Element
		registrationListPackageNameStringSlice []string
		volume                                 *volumeStruct
		volumeGroup                            *volumeGroupStruct
		volumeGroupName                        string
		volumeName                             string
	)

	defer func() {
		if nil == err {
			logger.Infof("transitions.Up() returning successfully")
		} else {
			// On the relatively good likelihood that at least logger.Up() worked...
			logger.Errorf("transitions.Up() returning with failure: %v", err)
		}
	}()

	globals.currentConfMapDelta = &confMapDeltaStruct{
		volumeGroupList:       make(map[string]*volumeGroupStruct),
		servedVolumeGroupList: make(map[string]*volumeGroupStruct),
		remoteVolumeGroupList: make(map[string]*volumeGroupStruct),

		createdVolumeGroupList:   make(map[string]*volumeGroupStruct),
		movedVolumeGroupList:     make(map[string]*volumeGroupStruct),
		destroyedVolumeGroupList: make(map[string]*volumeGroupStruct),

		volumeList:       make(map[string]*volumeStruct),
		servedVolumeList: make(map[string]*volumeStruct),
		remoteVolumeList: make(map[string]*volumeStruct),

		createdVolumeList:   make(map[string]*volumeStruct),
		movedVolumeList:     make(map[string]*volumeStruct),
		destroyedVolumeList: make(map[string]*volumeStruct),

		toStopServingVolumeList:  make(map[string]*volumeStruct),
		toStartServingVolumeList: make(map[string]*volumeStruct),
	}

	newConfMapDelta, err = computeConfMapDelta(confMap)
	if nil != err {
		return
	}

	if 0 != len(newConfMapDelta.movedVolumeGroupList) {
		err = fmt.Errorf("transitions.Up() did not expect movedVolumeGroupList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.destroyedVolumeGroupList) {
		err = fmt.Errorf("transitions.Up() did not expect destroyedVolumeGroupList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.movedVolumeList) {
		err = fmt.Errorf("transitions.Up() did not expect movedVolumeList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.destroyedVolumeList) {
		err = fmt.Errorf("transitions.Up() did not expect destroyedVolumeList to be non-empty")
		return
	}

	globals.currentConfMapDelta = newConfMapDelta

	// Issue Callbacks.Up() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		logger.Tracef("transitions.Up() calling %s.Up()", registrationItem.packageName)
		err = registrationItem.callbacks.Up(confMap)
		if nil != err {
			logger.Errorf("transitions.Up() call to %s.Up() failed: %v", registrationItem.packageName, err)
			err = fmt.Errorf("%s.Up() failed: %v", registrationItem.packageName, err)
			return
		}
		registrationListElement = registrationListElement.Next()
	}

	// Log transitions registrationList from Front() to Back()

	registrationListPackageNameStringSlice = make([]string, 0, globals.registrationList.Len())

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		registrationListPackageNameStringSlice = append(registrationListPackageNameStringSlice, registrationItem.packageName)
		registrationListElement = registrationListElement.Next()
	}

	logger.Infof("Transitions Package Registration List: %v", registrationListPackageNameStringSlice)

	// Issue Callbacks.VolumeGroupCreated() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeGroupName, volumeGroup = range globals.currentConfMapDelta.createdVolumeGroupList {
			logger.Tracef("transitions.Up() calling %s.VolumeGroupCreated(,%s,%s,%s)", registrationItem.packageName, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr)
			err = registrationItem.callbacks.VolumeGroupCreated(confMap, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr)
			if nil != err {
				logger.Errorf("transitions.Up() call to %s.VolumeGroupCreated(,%s,%s,%s) failed: %v", registrationItem.packageName, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr, err)
				err = fmt.Errorf("%s.VolumeGroupCreated(,%s,,) failed: %v", registrationItem.packageName, volumeGroupName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.VolumeCreated() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName, volume = range globals.currentConfMapDelta.createdVolumeList {
			logger.Tracef("transitions.Up() calling %s.VolumeCreated(,%s,%s)", registrationItem.packageName, volumeName, volume.volumeGroup.name)
			err = registrationItem.callbacks.VolumeCreated(confMap, volumeName, volume.volumeGroup.name)
			if nil != err {
				logger.Errorf("transitions.Up() call to %s.VolumeCreated(,%s,%s) failed: %v", registrationItem.packageName, volumeName, volume.volumeGroup.name, err)
				err = fmt.Errorf("%s.VolumeCreated(,%s,) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.ServeVolume() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName, volume = range globals.currentConfMapDelta.servedVolumeList {
			logger.Tracef("transitions.Up() calling %s.ServeVolume(,%s)", registrationItem.packageName, volumeName)
			err = registrationItem.callbacks.ServeVolume(confMap, volumeName)
			if nil != err {
				logger.Errorf("transitions.Up() call to %s.ServeVolume(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				err = fmt.Errorf("%s.ServeVolume(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.SignaledFinish() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		logger.Tracef("transitions.SignaledFinish() calling %s.SignaledFinish()", registrationItem.packageName)
		err = registrationItem.callbacks.SignaledFinish(confMap)
		if nil != err {
			logger.Errorf("transitions.SignaledFinish() call to %s.SignaledFinish() failed: %v", registrationItem.packageName, err)
			err = fmt.Errorf("%s.SignaledFinish() failed: %v", registrationItem.packageName, err)
			return
		}
		registrationListElement = registrationListElement.Next()
	}

	return
}

func signaled(confMap conf.ConfMap) (err error) {
	var (
		newConfMapDelta         *confMapDeltaStruct
		registrationItem        *registrationItemStruct
		registrationListElement *list.Element
		volume                  *volumeStruct
		volumeGroup             *volumeGroupStruct
		volumeGroupName         string
		volumeName              string
	)

	logger.Infof("transitions.Signaled() called")
	defer func() {
		if nil == err {
			logger.Infof("transitions.Signaled() returning successfully")
		} else {
			logger.Errorf("transitions.Signaled() returning with failure: %v", err)
		}
	}()

	newConfMapDelta, err = computeConfMapDelta(confMap)
	if nil != err {
		return
	}

	globals.currentConfMapDelta = newConfMapDelta

	// Issue Callbacks.VolumeToBeUnserved() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName = range globals.currentConfMapDelta.toStopServingVolumeList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeToBeUnserved(,%s)", registrationItem.packageName, volumeName)
			err = registrationItem.callbacks.VolumeToBeUnserved(confMap, volumeName)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeToBeUnserved(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				err = fmt.Errorf("%s.VolumeToBeUnserved(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.SignaledStart() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		logger.Tracef("transitions.Signaled() calling %s.SignaledStart()", registrationItem.packageName)
		err = registrationItem.callbacks.SignaledStart(confMap)
		if nil != err {
			logger.Errorf("transitions.Signaled() call to %s.SignaledStart() failed: %v", registrationItem.packageName, err)
			err = fmt.Errorf("%s.SignaledStart() failed: %v", registrationItem.packageName, err)
			return
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.UnserveVolume() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName = range globals.currentConfMapDelta.toStopServingVolumeList {
			logger.Tracef("transitions.Signaled() calling %s.UnserveVolume(,%s)", registrationItem.packageName, volumeName)
			err = registrationItem.callbacks.UnserveVolume(confMap, volumeName)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.UnserveVolume(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				err = fmt.Errorf("%s.UnserveVolume(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.VolumeGroupCreated() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeGroupName, volumeGroup = range globals.currentConfMapDelta.createdVolumeGroupList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeGroupCreated(,%s,%s,%s)", registrationItem.packageName, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr)
			err = registrationItem.callbacks.VolumeGroupCreated(confMap, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeGroupCreated(,%s,%s,%s) failed: %v", registrationItem.packageName, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr, err)
				err = fmt.Errorf("%s.VolumeGroupCreated(,%s,,) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.VolumeCreated() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName, volume = range globals.currentConfMapDelta.createdVolumeList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeCreated(,%s,%s)", registrationItem.packageName, volumeName, volume.volumeGroup.name)
			err = registrationItem.callbacks.VolumeCreated(confMap, volumeName, volume.volumeGroup.name)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeCreated(,%s,%s) failed: %v", registrationItem.packageName, volumeName, volume.volumeGroup.name, err)
				err = fmt.Errorf("%s.VolumeCreated(,%s,) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.VolumeGroupMoved() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeGroupName, volumeGroup = range globals.currentConfMapDelta.movedVolumeGroupList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeGroupMoved(,%s,%s,%s)", registrationItem.packageName, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr)
			err = registrationItem.callbacks.VolumeGroupMoved(confMap, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeGroupMoved(,%s,%s,%s) failed: %v", registrationItem.packageName, volumeGroupName, volumeGroup.activePeer, volumeGroup.virtualIPAddr, err)
				err = fmt.Errorf("%s.VolumeGroupMoved(,%s,,) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.VolumeMoved() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName, volume = range globals.currentConfMapDelta.movedVolumeList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeMoved(,%s,%s)", registrationItem.packageName, volumeName, volume.volumeGroup.name)
			err = registrationItem.callbacks.VolumeMoved(confMap, volumeName, volume.volumeGroup.name)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeMoved(,%s,%s) failed: %v", registrationItem.packageName, volumeName, volume.volumeGroup.name, err)
				err = fmt.Errorf("%s.VolumeMoved(,%s,) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.VolumeDestroyed() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName, volume = range globals.currentConfMapDelta.destroyedVolumeList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeDestroyed(,%s)", registrationItem.packageName, volumeName)
			err = registrationItem.callbacks.VolumeDestroyed(confMap, volumeName)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeDestroyed(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				err = fmt.Errorf("%s.VolumeDestroyed(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.VolumeGroupDestroyed() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeGroupName = range globals.currentConfMapDelta.destroyedVolumeGroupList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeGroupDestroyed(,%s)", registrationItem.packageName, volumeGroupName)
			err = registrationItem.callbacks.VolumeGroupDestroyed(confMap, volumeGroupName)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeGroupDestroyed(,%s) failed: %v", registrationItem.packageName, volumeGroupName, err)
				err = fmt.Errorf("%s.VolumeGroupDestroyed(,%s) failed: %v", registrationItem.packageName, volumeGroupName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.ServeVolume() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName, volume = range globals.currentConfMapDelta.toStartServingVolumeList {
			if volume.served {
				logger.Tracef("transitions.Signaled() calling %s.ServeVolume(,%s)", registrationItem.packageName, volumeGroupName)
				err = registrationItem.callbacks.ServeVolume(confMap, volumeName)
				if nil != err {
					logger.Errorf("transitions.Signaled() call to %s.ServeVolume(,%s) failed: %v", registrationItem.packageName, volumeGroupName, err)
					err = fmt.Errorf("%s.ServeVolume(,%s) failed: %v", registrationItem.packageName, volumeName, err)
					return
				}
			}
		}
		registrationListElement = registrationListElement.Next()
	}

	// Issue Callbacks.SignaledFinish() calls from Front() to Back() of globals.registrationList

	registrationListElement = globals.registrationList.Front()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		logger.Tracef("transitions.Signaled() calling %s.SignaledFinish()", registrationItem.packageName)
		err = registrationItem.callbacks.SignaledFinish(confMap)
		if nil != err {
			logger.Errorf("transitions.Signaled() call to %s.SignaledFinish() failed: %v", registrationItem.packageName, err)
			err = fmt.Errorf("%s.SignaledFinish() failed: %v", registrationItem.packageName, err)
			return
		}
		registrationListElement = registrationListElement.Next()
	}

	return
}

func down(confMap conf.ConfMap) (err error) {
	var (
		newConfMapDelta         *confMapDeltaStruct
		registrationItem        *registrationItemStruct
		registrationListElement *list.Element
		volumeGroupName         string
		volumeName              string
	)

	logger.Infof("transitions.Down() called")
	defer func() {
		if nil != err {
			// On the relatively good likelihood that the failure occurred before calling logger.Down()...
			logger.Errorf("transitions.Down() returning with failure: %v", err)
		}
	}()

	newConfMapDelta, err = computeConfMapDelta(confMap)
	if nil != err {
		return
	}

	if 0 != len(newConfMapDelta.createdVolumeGroupList) {
		err = fmt.Errorf("transitions.Down() did not expect createdVolumeGroupList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.movedVolumeGroupList) {
		err = fmt.Errorf("transitions.Down() did not expect movedVolumeGroupList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.destroyedVolumeGroupList) {
		err = fmt.Errorf("transitions.Down() did not expect destroyedVolumeGroupList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.createdVolumeList) {
		err = fmt.Errorf("transitions.Down() did not expect createdVolumeList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.movedVolumeList) {
		err = fmt.Errorf("transitions.Down() did not expect movedVolumeList to be non-empty")
		return
	}
	if 0 != len(newConfMapDelta.destroyedVolumeList) {
		err = fmt.Errorf("transitions.Down() did not expect destroyedVolumeList to be non-empty")
		return
	}

	globals.currentConfMapDelta = newConfMapDelta

	// Issue Callbacks.VolumeToBeUnserved() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName = range globals.currentConfMapDelta.servedVolumeList {
			logger.Tracef("transitions.Signaled() calling %s.VolumeToBeUnserved(,%s)", registrationItem.packageName, volumeName)
			err = registrationItem.callbacks.VolumeToBeUnserved(confMap, volumeName)
			if nil != err {
				logger.Errorf("transitions.Signaled() call to %s.VolumeToBeUnserved(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				err = fmt.Errorf("%s.VolumeToBeUnserved(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.SignaledStart() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		logger.Tracef("transitions.Down() calling %s.SignaledStart()", registrationItem.packageName)
		err = registrationItem.callbacks.SignaledStart(confMap)
		if nil != err {
			logger.Errorf("transitions.Down() call to %s.SignaledStart() failed: %v", registrationItem.packageName, err)
			err = fmt.Errorf("%s.SignaledStart() failed: %v", registrationItem.packageName, err)
			return
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.UnserveVolume() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName = range globals.currentConfMapDelta.servedVolumeList {
			logger.Tracef("transitions.Down() calling %s.UnserveVolume(,%s)", registrationItem.packageName, volumeName)
			err = registrationItem.callbacks.UnserveVolume(confMap, volumeName)
			if nil != err {
				logger.Errorf("transitions.Down() call to %s.UnserveVolume(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				err = fmt.Errorf("%s.UnserveVolume(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.VolumeDestroyed() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeName = range globals.currentConfMapDelta.volumeList {
			logger.Tracef("transitions.Down() calling %s.VolumeDestroyed(,%s)", registrationItem.packageName, volumeName)
			err = registrationItem.callbacks.VolumeDestroyed(confMap, volumeName)
			if nil != err {
				logger.Errorf("transitions.Down() call to %s.VolumeDestroyed(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				err = fmt.Errorf("%s.VolumeDestroyed(,%s) failed: %v", registrationItem.packageName, volumeName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.VolumeGroupDestroyed() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		for volumeGroupName = range globals.currentConfMapDelta.volumeGroupList {
			logger.Tracef("transitions.Down() calling %s.VolumeGroupDestroyed(,%s)", registrationItem.packageName, volumeGroupName)
			err = registrationItem.callbacks.VolumeGroupDestroyed(confMap, volumeGroupName)
			if nil != err {
				logger.Errorf("transitions.Down() call to %s.VolumeGroupDestroyed(,%s) failed: %v", registrationItem.packageName, volumeGroupName, err)
				err = fmt.Errorf("%s.VolumeGroupDestroyed(,%s) failed: %v", registrationItem.packageName, volumeGroupName, err)
				return
			}
		}
		registrationListElement = registrationListElement.Prev()
	}

	// Issue Callbacks.Down() calls from Back() to Front() of globals.registrationList

	registrationListElement = globals.registrationList.Back()

	for nil != registrationListElement {
		registrationItem = registrationListElement.Value.(*registrationItemStruct)
		logger.Tracef("transitions.Down() calling %s.Down()", registrationItem.packageName)
		err = registrationItem.callbacks.Down(confMap)
		if nil != err {
			logger.Errorf("transitions.Down() call to %s.Down() failed: %v", registrationItem.packageName, err)
			err = fmt.Errorf("%s.Down() failed: %v", registrationItem.packageName, err)
			return
		}
		registrationListElement = registrationListElement.Prev()
	}

	return
}

func computeConfMapDelta(confMap conf.ConfMap) (newConfMapDelta *confMapDeltaStruct, err error) {
	var (
		fsGlobalsVolumeGroupList []string
		ok                       bool
		volume                   *volumeStruct
		volumeGroup              *volumeGroupStruct
		volumeGroupName          string
		volumeGroupVolumeList    []string
		volumeGroupPreviously    *volumeGroupStruct
		volumeName               string
		volumePreviously         *volumeStruct
		whoAmI                   string
	)

	// TODO: Remove call to upgradeConfMapIfNeeded() once backwards compatibility is no longer required

	err = upgradeConfMapIfNeeded(confMap)
	if nil != err {
		return
	}

	// Initialize lists used in computation

	newConfMapDelta = &confMapDeltaStruct{
		volumeGroupList:       make(map[string]*volumeGroupStruct),
		servedVolumeGroupList: make(map[string]*volumeGroupStruct),
		remoteVolumeGroupList: make(map[string]*volumeGroupStruct),

		createdVolumeGroupList:   make(map[string]*volumeGroupStruct),
		movedVolumeGroupList:     make(map[string]*volumeGroupStruct),
		destroyedVolumeGroupList: make(map[string]*volumeGroupStruct),

		volumeList:       make(map[string]*volumeStruct),
		servedVolumeList: make(map[string]*volumeStruct),
		remoteVolumeList: make(map[string]*volumeStruct),

		createdVolumeList:   make(map[string]*volumeStruct),
		movedVolumeList:     make(map[string]*volumeStruct),
		destroyedVolumeList: make(map[string]*volumeStruct),

		toStopServingVolumeList:  make(map[string]*volumeStruct),
		toStartServingVolumeList: make(map[string]*volumeStruct),
	}

	// Injest confMap

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	fsGlobalsVolumeGroupList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != err {
		return
	}

	for _, volumeGroupName = range fsGlobalsVolumeGroupList {
		volumeGroup = &volumeGroupStruct{name: volumeGroupName, volumeList: make(map[string]*volumeStruct)}

		newConfMapDelta.volumeGroupList[volumeGroupName] = volumeGroup

		volumeGroup.activePeer, err = confMap.FetchOptionValueString("VolumeGroup:"+volumeGroupName, "PrimaryPeer")
		if nil != err {
			if nil == confMap.VerifyOptionValueIsEmpty("VolumeGroup:"+volumeGroupName, "PrimaryPeer") {
				volumeGroup.activePeer = ""
			} else {
				return
			}
		}

		volumeGroup.served = (whoAmI == volumeGroup.activePeer)

		if volumeGroup.served {
			newConfMapDelta.servedVolumeGroupList[volumeGroupName] = volumeGroup
		} else {
			newConfMapDelta.remoteVolumeGroupList[volumeGroupName] = volumeGroup
		}

		volumeGroup.virtualIPAddr, err = confMap.FetchOptionValueString("VolumeGroup:"+volumeGroupName, "VirtualIPAddr")
		if nil != err {
			if nil == confMap.VerifyOptionValueIsEmpty("VolumeGroup:"+volumeGroupName, "VirtualIPAddr") {
				volumeGroup.virtualIPAddr = ""
			} else {
				return
			}
		}

		volumeGroupVolumeList, err = confMap.FetchOptionValueStringSlice("VolumeGroup:"+volumeGroupName, "VolumeList")
		if nil != err {
			return
		}

		for _, volumeName = range volumeGroupVolumeList {
			volume = &volumeStruct{name: volumeName, served: volumeGroup.served, volumeGroup: volumeGroup}

			newConfMapDelta.volumeList[volumeName] = volume

			if volume.served {
				newConfMapDelta.servedVolumeList[volumeName] = volume
			} else {
				newConfMapDelta.remoteVolumeList[volumeName] = volume
			}

			volumeGroup.volumeList[volumeName] = volume
		}
	}

	// Compute changes to VolumeGroupList

	for volumeGroupName, volumeGroup = range newConfMapDelta.volumeGroupList {
		volumeGroupPreviously, ok = globals.currentConfMapDelta.volumeGroupList[volumeGroupName]
		if ok {
			if volumeGroupPreviously.activePeer != volumeGroup.activePeer {
				newConfMapDelta.movedVolumeGroupList[volumeGroupName] = volumeGroup
			}
		} else {
			newConfMapDelta.createdVolumeGroupList[volumeGroupName] = volumeGroup
		}
	}

	for volumeGroupName, volumeGroup = range globals.currentConfMapDelta.volumeGroupList {
		_, ok = newConfMapDelta.volumeGroupList[volumeGroupName]
		if !ok {
			newConfMapDelta.destroyedVolumeGroupList[volumeGroupName] = volumeGroup
		}
	}

	// Compute changes to VolumeList

	for volumeName, volume = range newConfMapDelta.volumeList {
		volumePreviously, ok = globals.currentConfMapDelta.volumeList[volumeName]
		if ok {
			if volumePreviously.volumeGroup.name != volume.volumeGroup.name {
				newConfMapDelta.movedVolumeList[volumeName] = volume
			}
		} else {
			newConfMapDelta.createdVolumeList[volumeName] = volume
		}
	}

	for volumeName, volume = range globals.currentConfMapDelta.volumeList {
		_, ok = newConfMapDelta.volumeList[volumeName]
		if !ok {
			newConfMapDelta.destroyedVolumeList[volumeName] = volume
		}
	}

	// Compute to{Stop|Start}ServingVolumeList

	for volumeName, volume = range newConfMapDelta.destroyedVolumeList {
		_, ok = globals.currentConfMapDelta.servedVolumeList[volumeName]
		if ok {
			newConfMapDelta.toStopServingVolumeList[volumeName] = volume
		}
	}
	for volumeName, volume = range newConfMapDelta.movedVolumeList {
		_, ok = globals.currentConfMapDelta.servedVolumeList[volumeName]
		if ok {
			newConfMapDelta.toStopServingVolumeList[volumeName] = volume
		}
	}
	for _, volumeGroup = range newConfMapDelta.movedVolumeGroupList {
		for volumeName, volume = range volumeGroup.volumeList {
			_, ok = globals.currentConfMapDelta.servedVolumeList[volumeName]
			if ok {
				newConfMapDelta.toStopServingVolumeList[volumeName] = volume
			}
		}
	}

	for _, volumeGroup = range newConfMapDelta.movedVolumeGroupList {
		for volumeName, volume = range volumeGroup.volumeList {
			_, ok = newConfMapDelta.servedVolumeList[volumeName]
			if ok {
				newConfMapDelta.toStartServingVolumeList[volumeName] = volume
			}
		}
	}
	for volumeName, volume = range newConfMapDelta.movedVolumeList {
		_, ok = newConfMapDelta.servedVolumeList[volumeName]
		if ok {
			newConfMapDelta.toStartServingVolumeList[volumeName] = volume
		}
	}
	for volumeName, volume = range newConfMapDelta.createdVolumeList {
		_, ok = newConfMapDelta.servedVolumeList[volumeName]
		if ok {
			newConfMapDelta.toStartServingVolumeList[volumeName] = volume
		}
	}

	// All done

	err = nil
	return
}

// upgradeConfMapIfNeeded should be removed once backwards compatibility is no longer required...
//
// In the meantime, the changes are:
//
//   MaxFlushSize                          moves from FlowControl: section to the Volume: section (for each Volume referencing it)
//   MaxFlushTime                          moves from FlowControl: section to the Volume: section (for each Volume referencing it)
//   FileDefragmentChunkSize  (if present) moves from FlowControl: section to the Volume: section (for each Volume referencing it)
//   FileDefragmentChunkDelay (if present) moves from FlowControl: section to the Volume: section (for each Volume referencing it)
//
//   VolumeList in FSGlobals is renamed VolumeGroupList
//
//   VolumeGroup: section is created for every Volume: section
//   VolumeList in VolumeGroup: section references the identically named Volume
//   VirtualIPAddr in VolumeGroup: section is empty
//   PrimaryPeer moves from Volume: section to VolumeGroup: section
//   ReadCacheLineSize moves from FlowControl: section to VolumeGroup: section (for each Volume referencing it)
//   ReadCacheWeight   moves from FlowControl: section to VolumeGroup: section (for each Volume referencing it)
//
//   PrimaryPeer is removed from Volume: section
//   FlowControl is removed from Volume: section
//
//   FlowControl: section is removed
//
//   Generation of the name of the created VolumeGroup for each Volume
//   is computed by prepending an optional prefix and appending an
//   optional suffix as specified in the conf.ConfMap
//
// The upgrade will commence if FSGlobals section contains a VolumeList
// The upgrade is unnecessary if FSGlobals section already container a VolumeGroupList
//
func upgradeConfMapIfNeeded(confMap conf.ConfMap) (err error) {
	var (
		autoVolumeGroupNamePrefix      string
		autoVolumeGroupNamePrefixOK    bool
		autoVolumeGroupNamePrefixSlice []string
		autoVolumeGroupNameSuffix      string
		autoVolumeGroupNameSuffixOK    bool
		autoVolumeGroupNameSuffixSlice []string
		fileDefragmentChunkDelay       conf.ConfMapOption
		fileDefragmentChunkDelayOK     bool
		fileDefragmentChunkSize        conf.ConfMapOption
		fileDefragmentChunkSizeOK      bool
		flowControl                    conf.ConfMapSection
		flowControlName                conf.ConfMapOption
		flowControlNameOK              bool
		flowControlOK                  bool
		flowControlSet                 map[string]struct{}
		flowControlSetElement          string
		fsGlobals                      conf.ConfMapSection
		fsGlobalsOK                    bool
		maxFlushSize                   conf.ConfMapOption
		maxFlushSizeOK                 bool
		maxFlushTime                   conf.ConfMapOption
		maxFlushTimeOK                 bool
		primaryPeer                    conf.ConfMapOption
		primaryPeerOK                  bool
		readCacheLineSize              conf.ConfMapOption
		readCacheLineSizeOK            bool
		readCacheWeight                conf.ConfMapOption
		readCacheWeightOK              bool
		transitions                    conf.ConfMapSection
		transitionsOK                  bool
		volume                         conf.ConfMapSection
		volumeGroup                    conf.ConfMapSection
		volumeGroupList                conf.ConfMapOption
		volumeGroupListOK              bool
		volumeOK                       bool
		volumeList                     conf.ConfMapOption
		volumeListOK                   bool
		volumeName                     string
	)

	fsGlobals, fsGlobalsOK = confMap["FSGlobals"]

	if !fsGlobalsOK {
		err = fmt.Errorf("confMap must contain an FSGlobals section")
		return
	}

	volumeList, volumeListOK = fsGlobals["VolumeList"]
	_, volumeGroupListOK = fsGlobals["VolumeGroupList"]

	if (!volumeListOK && !volumeGroupListOK) || (volumeListOK && volumeGroupListOK) {
		err = fmt.Errorf("confMap must contain precisely one of FSGlobals.VolumeList or FSGlobals.VolumeGroupList")
		return
	}

	if volumeGroupListOK {
		// No need to upgrade confMap
		err = nil
		return
	}

	transitions, transitionsOK = confMap["Transitions"]
	if transitionsOK {
		autoVolumeGroupNamePrefixSlice, autoVolumeGroupNamePrefixOK = transitions["AutoVolumeGroupPrefix"]
		if autoVolumeGroupNamePrefixOK {
			switch len(autoVolumeGroupNamePrefixSlice) {
			case 0:
				autoVolumeGroupNamePrefix = ""
			case 1:
				autoVolumeGroupNamePrefix = autoVolumeGroupNamePrefixSlice[0]
			default:
				err = fmt.Errorf("confMap must not contain a multi-valued Transitions:AutoVolumeGroupPrefix key")
				return
			}
		}
		autoVolumeGroupNameSuffixSlice, autoVolumeGroupNameSuffixOK = transitions["AutoVolumeGroupSuffix"]
		if autoVolumeGroupNameSuffixOK {
			switch len(autoVolumeGroupNameSuffixSlice) {
			case 0:
				autoVolumeGroupNameSuffix = ""
			case 1:
				autoVolumeGroupNameSuffix = autoVolumeGroupNameSuffixSlice[0]
			default:
				err = fmt.Errorf("confMap must not contain a multi-valued Transitions:AutoVolumeGroupSuffix key")
				return
			}
		}
	} else {
		autoVolumeGroupNamePrefix = ""
		autoVolumeGroupNameSuffix = ""
	}

	volumeGroupList = make(conf.ConfMapOption, 0, len(volumeList))
	for _, volumeName = range volumeList {
		volumeGroupList = append(volumeGroupList, autoVolumeGroupNamePrefix+volumeName+autoVolumeGroupNameSuffix)
	}
	fsGlobals["VolumeGroupList"] = volumeGroupList

	delete(fsGlobals, "VolumeList")

	flowControlSet = make(map[string]struct{})

	for _, volumeName = range volumeList {
		volume, volumeOK = confMap["Volume:"+volumeName]
		if !volumeOK {
			err = fmt.Errorf("confMap must contain a Volume:%s section", volumeName)
			return
		}

		primaryPeer, primaryPeerOK = volume["PrimaryPeer"]
		if !primaryPeerOK || (1 < len(primaryPeer)) {
			err = fmt.Errorf("confMap must contain an empty or single-valued Volume:%s.PrimaryPeer key", volumeName)
			return
		}

		flowControlName, flowControlNameOK = volume["FlowControl"]
		if !flowControlNameOK || (1 != len(flowControlName)) {
			err = fmt.Errorf("confMap must contain a single-valued Volume:%s.FlowControl key", volumeName)
			return
		}

		flowControlSet[flowControlName[0]] = struct{}{}

		flowControl, flowControlOK = confMap["FlowControl:"+flowControlName[0]]
		if !flowControlOK {
			err = fmt.Errorf("confMap must contain a FlowControl:%s section", flowControlName)
			return
		}

		maxFlushSize, maxFlushSizeOK = flowControl["MaxFlushSize"]
		if !maxFlushSizeOK {
			err = fmt.Errorf("confMap must contain a FlowControl:%s.MaxFlushSize key", flowControlName[0])
			return
		}

		maxFlushTime, maxFlushTimeOK = flowControl["MaxFlushTime"]
		if !maxFlushTimeOK {
			err = fmt.Errorf("confMap must contain a FlowControl:%s.MaxFlushTime key", flowControlName[0])
			return
		}

		fileDefragmentChunkSize, fileDefragmentChunkSizeOK = flowControl["FileDefragmentChunkSize"]
		fileDefragmentChunkDelay, fileDefragmentChunkDelayOK = flowControl["FileDefragmentChunkDelay"]

		readCacheLineSize, readCacheLineSizeOK = flowControl["ReadCacheLineSize"]
		if !readCacheLineSizeOK {
			err = fmt.Errorf("confMap must contain a FlowControl:%s.ReadCacheLineSize key", flowControlName[0])
			return
		}

		readCacheWeight, readCacheWeightOK = flowControl["ReadCacheWeight"]
		if !readCacheWeightOK {
			err = fmt.Errorf("confMap must contain a FlowControl:%s.ReadCacheWeight key", flowControlName[0])
			return
		}

		volumeGroup = make(conf.ConfMapSection)

		volumeGroup["VolumeList"] = []string{volumeName}
		volumeGroup["VirtualIPAddr"] = []string{}
		volumeGroup["PrimaryPeer"] = primaryPeer
		volumeGroup["ReadCacheLineSize"] = readCacheLineSize
		volumeGroup["ReadCacheWeight"] = readCacheWeight

		confMap["VolumeGroup:"+autoVolumeGroupNamePrefix+volumeName+autoVolumeGroupNameSuffix] = volumeGroup

		volume["MaxFlushSize"] = maxFlushSize
		volume["MaxFlushTime"] = maxFlushTime

		if fileDefragmentChunkSizeOK {
			volume["FileDefragmentChunkSize"] = fileDefragmentChunkSize
		}
		if fileDefragmentChunkDelayOK {
			volume["FileDefragmentChunkDelay"] = fileDefragmentChunkDelay
		}

		delete(volume, "PrimaryPeer")
		delete(volume, "FlowControl")
	}

	for flowControlSetElement = range flowControlSet {
		delete(confMap, "FlowControl:"+flowControlSetElement)
	}

	err = nil
	return
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) Up(confMap conf.ConfMap) (err error) {
	return logger.Up(confMap)
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) VolumeToBeUnserved(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	return logger.SignaledStart(confMap)
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	return logger.SignaledFinish(confMap)
}

func (loggerCallbacksInterface *loggerCallbacksInterfaceStruct) Down(confMap conf.ConfMap) (err error) {
	return logger.Down(confMap)
}

func dumpGlobals(indent string) {
	var (
		registrationItem        *registrationItemStruct
		registrationListElement *list.Element
		volume                  *volumeStruct
		volumeGroup             *volumeGroupStruct
		volumeGroupName         string
		volumeName              string
	)

	registrationListElement = globals.registrationList.Front()

	if nil == registrationListElement {
		fmt.Printf("%sregistrationList: <empty>\n", indent)
	} else {
		fmt.Printf("%sregistrationList:", indent)
		for nil != registrationListElement {
			registrationItem = registrationListElement.Value.(*registrationItemStruct)
			fmt.Printf(" %s", registrationItem.packageName)
			registrationListElement = registrationListElement.Next()
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.volumeGroupList) {
		fmt.Printf("%svolumeGroupList: <empty>\n", indent)
	} else {
		fmt.Printf("%svolumeGroupList:\n", indent)
		for volumeGroupName, volumeGroup = range globals.currentConfMapDelta.volumeGroupList {
			fmt.Printf("%s  %+v [volumeList:", indent, volumeGroup)
			for volumeName = range volumeGroup.volumeList {
				fmt.Printf(" %s", volumeName)
			}
			fmt.Printf("]\n")
		}
	}

	if 0 == len(globals.currentConfMapDelta.servedVolumeGroupList) {
		fmt.Printf("%sservedVolumeGroupList: <empty>\n", indent)
	} else {
		fmt.Printf("%sservedVolumeGroupList:", indent)
		for volumeGroupName = range globals.currentConfMapDelta.servedVolumeGroupList {
			fmt.Printf(" %s", volumeGroupName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.remoteVolumeGroupList) {
		fmt.Printf("%sremoteVolumeGroupList: <empty>\n", indent)
	} else {
		fmt.Printf("%sremoteVolumeGroupList:", indent)
		for volumeGroupName = range globals.currentConfMapDelta.remoteVolumeGroupList {
			fmt.Printf(" %s", volumeGroupName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.createdVolumeGroupList) {
		fmt.Printf("%screatedVolumeGroupList: <empty>\n", indent)
	} else {
		fmt.Printf("%screatedVolumeGroupList:", indent)
		for volumeGroupName = range globals.currentConfMapDelta.createdVolumeGroupList {
			fmt.Printf(" %s", volumeGroupName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.movedVolumeGroupList) {
		fmt.Printf("%smovedVolumeGroupList: <empty>\n", indent)
	} else {
		fmt.Printf("%smovedVolumeGroupList:", indent)
		for volumeGroupName = range globals.currentConfMapDelta.movedVolumeGroupList {
			fmt.Printf(" %s", volumeGroupName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.destroyedVolumeGroupList) {
		fmt.Printf("%sdestroyedVolumeGroupList: <empty>\n", indent)
	} else {
		fmt.Printf("%sdestroyedVolumeGroupList:", indent)
		for volumeGroupName = range globals.currentConfMapDelta.destroyedVolumeGroupList {
			fmt.Printf(" %s", volumeGroupName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.volumeList) {
		fmt.Printf("%svolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%svolumeList:\n", indent)
		for volumeName, volume = range globals.currentConfMapDelta.volumeList {
			fmt.Printf("%s  %+v [volumeGroup: %s]\n", indent, volume, volume.volumeGroup.name)
		}
	}

	if 0 == len(globals.currentConfMapDelta.servedVolumeList) {
		fmt.Printf("%sservedVolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%sservedVolumeList:", indent)
		for volumeName = range globals.currentConfMapDelta.servedVolumeList {
			fmt.Printf(" %s", volumeName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.remoteVolumeList) {
		fmt.Printf("%sremoteVolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%sremoteVolumeList:", indent)
		for volumeName = range globals.currentConfMapDelta.remoteVolumeList {
			fmt.Printf(" %s", volumeName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.createdVolumeList) {
		fmt.Printf("%screatedVolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%screatedVolumeList:", indent)
		for volumeName = range globals.currentConfMapDelta.createdVolumeList {
			fmt.Printf(" %s", volumeName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.movedVolumeList) {
		fmt.Printf("%smovedVolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%smovedVolumeList:", indent)
		for volumeName = range globals.currentConfMapDelta.movedVolumeList {
			fmt.Printf(" %s", volumeName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.destroyedVolumeList) {
		fmt.Printf("%sdestroyedVolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%sdestroyedVolumeList:", indent)
		for volumeName = range globals.currentConfMapDelta.destroyedVolumeList {
			fmt.Printf(" %s", volumeName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.toStopServingVolumeList) {
		fmt.Printf("%stoStopServingVolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%stoStopServingVolumeList:", indent)
		for volumeName = range globals.currentConfMapDelta.toStopServingVolumeList {
			fmt.Printf(" %s", volumeName)
		}
		fmt.Println()
	}

	if 0 == len(globals.currentConfMapDelta.toStartServingVolumeList) {
		fmt.Printf("%stoStartServingVolumeList: <empty>\n", indent)
	} else {
		fmt.Printf("%stoStartServingVolumeList:", indent)
		for volumeName = range globals.currentConfMapDelta.toStartServingVolumeList {
			fmt.Printf(" %s", volumeName)
		}
		fmt.Println()
	}
}
