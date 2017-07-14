package fs

import (
	"container/list"
	"sync"

	"github.com/swiftstack/conf"

	"github.com/swiftstack/ProxyFS/inode"
)

type mountStruct struct {
	id         MountID
	userID     inode.InodeUserID  // TODO: Remove this
	groupID    inode.InodeGroupID // TODO: Remove this
	volumeName string
	options    MountOptions
	inode.VolumeHandle
}

// Stores the volume related information across all mount points. E.g file lock information.
type volumeStruct struct {
	sync.Mutex
	FLockMap map[inode.InodeNumber]*list.List
}

type globalsStruct struct {
	sync.Mutex
	lastMountID MountID
	mountMap    map[MountID]*mountStruct
	volumeMap   map[string]*volumeStruct
}

var globals globalsStruct

func Up(confMap conf.ConfMap) (err error) {
	globals.lastMountID = MountID(0)
	globals.mountMap = make(map[MountID]*mountStruct)
	globals.volumeMap = make(map[string]*volumeStruct)

	return
}

func PauseAndContract(confMap conf.ConfMap) (err error) {
	err = nil // TODO
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	err = nil // TODO
	return
}

func Down() (err error) {
	err = nil

	return
}
