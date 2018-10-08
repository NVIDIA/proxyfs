package jrpcfs

import (
	"container/list"
	"fmt"
	"net"
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"
)

type globalsStruct struct {
	mapsLock sync.Mutex   // protects volumeMap/mountIDMap/lastMountID/bimodalMountMap
	gate     sync.RWMutex // API Requests RLock()/RUnlock()
	//                       confMap changes Lock()/Unlock()

	whoAmI          string
	ipAddr          string
	portString      string
	fastPortString  string
	dataPathLogging bool

	// Map used to enumerate volumes served by this peer
	volumeMap map[string]bool // key == volumeName; value is ignored

	// Map used to store volumes by ID already mounted for bimodal support
	// TODO: These never get purged !!!
	mountIDMap  map[uint64]fs.MountHandle // key == mountID
	lastMountID uint64

	// Map used to store volumes by name already mounted for bimodal support
	bimodalMountMap map[string]fs.MountHandle // key == volumeName

	// Connection list and listener list to close during shutdown:
	halting     bool
	connLock    sync.Mutex
	connections *list.List
	connWG      sync.WaitGroup
	listeners   []net.Listener
	listenersWG sync.WaitGroup
}

var globals globalsStruct

func init() {
	transitions.Register("jrpcfs", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	globals.volumeMap = make(map[string]bool)
	globals.mountIDMap = make(map[uint64]fs.MountHandle)
	globals.lastMountID = uint64(0) // The only invalid MountID
	globals.bimodalMountMap = make(map[string]fs.MountHandle)

	// Fetch IPAddr from config file
	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get Cluster.WhoAmI from config file")
		return
	}
	globals.ipAddr, err = confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PrivateIPAddr")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get %s.PrivateIPAddr from config file", globals.whoAmI)
		return
	}

	// Fetch port number from config file
	globals.portString, err = confMap.FetchOptionValueString("JSONRPCServer", "TCPPort")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.TCPPort from config file")
		return
	}

	// Fetch fastPort number from config file
	globals.fastPortString, err = confMap.FetchOptionValueString("JSONRPCServer", "FastTCPPort")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.TCPFastPort from config file")
		return
	}

	// Set data path logging level to true, so that all trace logging is controlled by settings
	// in the logger package. To enable jrpcfs trace logging, set Logging.TraceLevelLogging to jrpcfs.
	// This will enable all jrpcfs trace logs, including those formerly controled by globals.dataPathLogging.
	// To disable read/write/flush trace logs separately, change this setting here to false.
	globals.dataPathLogging, err = confMap.FetchOptionValueBool("JSONRPCServer", "DataPathLogging")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.DataPathLogging from config file")
		return
	}

	// Init listeners
	globals.listeners = make([]net.Listener, 0, 2)
	globals.connections = list.New()

	// Init JSON RPC server stuff
	jsonRpcServerUp(globals.ipAddr, globals.portString)

	// Now kick off our other, faster RPC server
	ioServerUp(globals.ipAddr, globals.fastPortString)

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
	globals.gate.Lock()
	globals.volumeMap[volumeName] = true
	globals.gate.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		mountHandle        fs.MountHandle
		mountID            uint64
		removedMountID     uint64
		removedMountIDList []uint64
	)

	globals.gate.Lock()

	removedMountIDList = make([]uint64, 0, len(globals.mountIDMap))

	for mountID, mountHandle = range globals.mountIDMap {
		if mountHandle.VolumeName() == volumeName {
			removedMountIDList = append(removedMountIDList, mountID)
		}
	}

	for _, removedMountID = range removedMountIDList {
		delete(globals.mountIDMap, removedMountID)
	}

	delete(globals.volumeMap, volumeName)
	delete(globals.bimodalMountMap, volumeName)

	globals.gate.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) Signaled(confMap conf.ConfMap) (err error) {
	return nil
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	if 0 != len(globals.volumeMap) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.volumeMap")
		return
	}
	if 0 != len(globals.mountIDMap) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.mountIDMap")
		return
	}
	if 0 != len(globals.bimodalMountMap) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.bimodalMountMap")
		return
	}

	err = nil
	return
}
