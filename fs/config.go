package fs

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/swiftstack/conf"

	"github.com/swiftstack/ProxyFS/inode"
)

// Stores the volume related information across all mount points. E.g file lock information.
type volumeStruct struct {
	sync.Mutex
	FLockMap  map[inode.InodeNumber]*list.List
	mountList []MountID
}

type mountStruct struct {
	id         MountID
	volumeName string
	options    MountOptions
	volStruct  *volumeStruct
	inode.VolumeHandle
}

type globalsStruct struct {
	sync.Mutex
	whoAmI      string
	lastMountID MountID
	mountMap    map[MountID]*mountStruct
	volumeMap   map[string]*volumeStruct
}

var globals globalsStruct

func Up(confMap conf.ConfMap) (err error) {
	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v", err)
		return
	}

	globals.lastMountID = MountID(0)
	globals.mountMap = make(map[MountID]*mountStruct)
	globals.volumeMap = make(map[string]*volumeStruct)

	return
}

func PauseAndContract(confMap conf.ConfMap) (err error) {
	var (
		id                MountID
		ok                bool
		primaryPeerList   []string
		removedVolumeList []string
		updatedVolumeMap  map[string]bool // key == volumeName; value ignored
		volStruct         *volumeStruct
		volumeList        []string
		volumeName        string
		whoAmI            string
	)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v", err)
		return
	}
	if whoAmI != globals.whoAmI {
		err = fmt.Errorf("confMap change not allowed to alter [Cluster]WhoAmI")
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	updatedVolumeMap = make(map[string]bool)

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice(volumeName, "PrimaryPeer")
		if nil != err {
			err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"%s\", \"PrimaryPeer\") failed: %v", volumeName, err)
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if globals.whoAmI == primaryPeerList[0] {
				updatedVolumeMap[volumeName] = true
			}
		} else {
			err = fmt.Errorf("%v.PrimaryPeer cannot be multi-valued", volumeName)
			return
		}
	}

	removedVolumeList = make([]string, 0, len(globals.volumeMap))

	for volumeName = range globals.volumeMap {
		_, ok = updatedVolumeMap[volumeName]
		if !ok {
			removedVolumeList = append(removedVolumeList, volumeName)
		}
	}

	for _, volumeName = range removedVolumeList {
		volStruct = globals.volumeMap[volumeName]
		for _, id = range volStruct.mountList {
			delete(globals.mountMap, id)
		}
		delete(globals.volumeMap, volumeName)
	}

	err = nil
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

func Down() (err error) {
	err = nil

	return
}
