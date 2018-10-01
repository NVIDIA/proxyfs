package jrpcfs

import (
	"container/list"
	"fmt"
	"net"
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/logger"
)

type globalsStruct struct {
	sync.Mutex

	gate sync.RWMutex // SIGHUP triggered confMap change control
	//                   API Requests RLock()/RUnlock
	//                   SIGHUP confMap changes Lock()/Unlock()

	whoAmI          string
	ipAddr          string
	portString      string
	fastPortString  string
	dataPathLogging bool

	// Map used to enumerate volumes served by this peer
	volumeMap map[string]bool // key == volumeName; value is ignored

	// Map used to store volumes already mounted for bimodal support
	// TODO: These never get purged !!!
	mountIDMap  map[uint64]fs.MountHandle
	lastMountID uint64

	// Map used to store volumes already mounted for bimodal support
	bimodalMountMap map[string]fs.MountHandle

	// Connection list and listener list to close during shutdown:
	halting     bool
	connLock    sync.Mutex
	connections *list.List
	connWG      sync.WaitGroup
	listeners   []net.Listener
	listenersWG sync.WaitGroup
}

var globals globalsStruct

// NOTE: Don't use logger.Fatal* to error out from this function; it prevents us
//       from handling returned errors and gracefully unwinding.
func Up(confMap conf.ConfMap) (err error) {
	var (
		primaryPeerList []string
		volumeList      []string
		volumeName      string
	)

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

	// Compute volumeMap
	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	globals.volumeMap = make(map[string]bool)

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice("Volume:"+volumeName, "PrimaryPeer")
		if nil != err {
			err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"%s\", \"PrimaryPeer\") failed: %v", volumeName, err)
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if globals.whoAmI == primaryPeerList[0] {
				globals.volumeMap[volumeName] = true
			}
		} else {
			err = fmt.Errorf("%v.PrimaryPeer cannot be multi-valued", volumeName)
			return
		}
	}

	globals.listeners = make([]net.Listener, 0, 2)
	globals.connections = list.New()

	// Init JSON RPC server stuff
	jsonRpcServerUp(globals.ipAddr, globals.portString)

	// Now kick off our other, faster RPC server
	ioServerUp(globals.ipAddr, globals.fastPortString)

	return
}

func PauseAndContract(confMap conf.ConfMap) (err error) {
	var (
		dataPathLogging    bool
		fastPortString     string
		ipAddr             string
		mountHandle        fs.MountHandle
		mountID            uint64
		ok                 bool
		portString         string
		primaryPeerList    []string
		removedMountIDList []uint64
		removedVolumeList  []string
		updatedVolumeMap   map[string]bool
		volumeList         []string
		volumeName         string
		whoAmI             string
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

	ipAddr, err = confMap.FetchOptionValueString("Peer:"+whoAmI, "PrivateIPAddr")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"<whoAmI>\", \"PrivateIPAddr\") failed: %v", err)
		return
	}
	if ipAddr != globals.ipAddr {
		err = fmt.Errorf("confMap change not allowed to alter [<whoAmI>]PrivateIPAddr")
		return
	}

	portString, err = confMap.FetchOptionValueString("JSONRPCServer", "TCPPort")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"JSONRPCServer\", \"TCPPort\") failed: %v", err)
		return
	}
	if portString != globals.portString {
		err = fmt.Errorf("confMap change not allowed to alter [JSONRPCServer]TCPPort")
		return
	}

	fastPortString, err = confMap.FetchOptionValueString("JSONRPCServer", "FastTCPPort")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"JSONRPCServer\", \"FastTCPPort\") failed: %v", err)
		return
	}
	if fastPortString != globals.fastPortString {
		err = fmt.Errorf("confMap change not allowed to alter [JSONRPCServer]FastTCPPort")
		return
	}

	dataPathLogging, err = confMap.FetchOptionValueBool("JSONRPCServer", "DataPathLogging")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"JSONRPCServer\", \"DataPathLogging\") failed: %v", err)
		return
	}
	if dataPathLogging != globals.dataPathLogging {
		err = fmt.Errorf("confMap change not allowed to alter [JSONRPCServer]DataPathLogging")
		return
	}

	globals.gate.Lock()

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	updatedVolumeMap = make(map[string]bool)

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice("Volume:"+volumeName, "PrimaryPeer")
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
	removedMountIDList = make([]uint64, 0, len(globals.mountIDMap))

	for volumeName = range globals.volumeMap {
		_, ok = updatedVolumeMap[volumeName]
		if !ok {
			removedVolumeList = append(removedVolumeList, volumeName)
			for mountID, mountHandle = range globals.mountIDMap {
				if mountHandle.VolumeName() == volumeName {
					removedMountIDList = append(removedMountIDList, mountID)
				}
			}
		}
	}

	for _, volumeName = range removedVolumeList {
		delete(globals.volumeMap, volumeName)
		_, ok = globals.bimodalMountMap[volumeName]
		if ok {
			delete(globals.bimodalMountMap, volumeName)
		}
	}

	for _, mountID = range removedMountIDList {
		delete(globals.mountIDMap, mountID)
	}

	err = nil
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	var (
		primaryPeerList  []string
		updatedVolumeMap map[string]bool
		volumeList       []string
		volumeName       string
	)

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	updatedVolumeMap = make(map[string]bool)

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice("Volume:"+volumeName, "PrimaryPeer")
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

	globals.volumeMap = updatedVolumeMap

	globals.gate.Unlock()

	err = nil
	return
}

func Down() (err error) {
	err = nil
	globals.halting = true

	jsonRpcServerDown()
	ioServerDown()

	// Close the listeners first, so that there are no new connections.
	globals.connLock.Lock()
	for _, listener := range globals.listeners {
		if listener != nil {
			listener.Close()
		}
	}

	globals.connLock.Unlock()

	globals.listenersWG.Wait()

	globals.connLock.Lock()
	for elm := globals.connections.Front(); elm != nil; elm = elm.Next() {
		conn := elm.Value.(net.Conn)
		conn.Close()
	}
	globals.connLock.Unlock()

	globals.connWG.Wait()

	return
}
