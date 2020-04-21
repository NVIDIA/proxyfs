package jrpcfs

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/retryrpc"
	"github.com/swiftstack/ProxyFS/transitions"
)

type globalsStruct struct {
	mapsLock sync.Mutex   // protects volumeMap/mountIDMap/bimodalMountMap
	gate     sync.RWMutex // API Requests RLock()/RUnlock()
	//                       confMap changes Lock()/Unlock()

	whoAmI                   string
	publicIPAddr             string
	privateIPAddr            string
	portString               string
	fastPortString           string
	retryRPCPort             uint16
	retryRPCTTLCompleted     time.Duration
	retryRPCAckTrim          time.Duration
	retryRPCDeadlineIO       time.Duration
	retryRPCKEEPALIVEPeriod  time.Duration
	rootCAx509CertificatePEM []byte
	dataPathLogging          bool

	// Map used to enumerate volumes served by this peer
	volumeMap map[string]bool // key == volumeName; value is ignored

	// Map used to store volumes by ID already mounted for bimodal support
	// TODO: These never get purged !!!
	mountIDAsByteArrayMap map[MountIDAsByteArray]fs.MountHandle
	mountIDAsStringMap    map[MountIDAsString]fs.MountHandle

	// Map used to store volumes by name already mounted for bimodal support
	bimodalMountMap map[string]fs.MountHandle // key == volumeName

	// RetryRPC server
	retryrpcSvr *retryrpc.Server

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
	globals.mountIDAsByteArrayMap = make(map[MountIDAsByteArray]fs.MountHandle)
	globals.mountIDAsStringMap = make(map[MountIDAsString]fs.MountHandle)
	globals.bimodalMountMap = make(map[string]fs.MountHandle)

	// Fetch IPAddrs from config file
	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get Cluster.WhoAmI from config file")
		return
	}
	globals.publicIPAddr, err = confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PublicIPAddr")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get %s.PublicIPAddr from config file", globals.whoAmI)
		return
	}
	globals.privateIPAddr, err = confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PrivateIPAddr")
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
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.FastTCPPort from config file")
		return
	}

	globals.retryRPCPort, err = confMap.FetchOptionValueUint16("JSONRPCServer", "RetryRPCPort")
	if nil == err {
		globals.retryRPCTTLCompleted, err = confMap.FetchOptionValueDuration("JSONRPCServer", "RetryRPCTTLCompleted")
		if nil != err {
			logger.Infof("failed to get JSONRPCServer.RetryRPCTTLCompleted from config file - defaulting to 10m")
			globals.retryRPCTTLCompleted = 10 * time.Minute
		}
		globals.retryRPCAckTrim, err = confMap.FetchOptionValueDuration("JSONRPCServer", "RetryRPCAckTrim")
		if nil != err {
			logger.Infof("failed to get JSONRPCServer.RetryRPCAckTrim from config file - defaulting to 100ms")
			globals.retryRPCAckTrim = 100 * time.Millisecond
		}
		globals.retryRPCDeadlineIO, err = confMap.FetchOptionValueDuration("JSONRPCServer", "RetryRPCDeadlineIO")
		if nil != err {
			logger.Infof("failed to get JSONRPCServer.RetryRPCDeadlineIO from config file - defaulting to 60s")
			globals.retryRPCDeadlineIO = 60 * time.Second
		}
		globals.retryRPCKEEPALIVEPeriod, err = confMap.FetchOptionValueDuration("JSONRPCServer", "RetryRPCKEEPALIVEPeriod")
		if nil != err {
			logger.Infof("failed to get JSONRPCServer.RetryRPCKEEPALIVEPeriod from config file - defaulting to 60s")
			globals.retryRPCKEEPALIVEPeriod = 60 * time.Second
		}
	} else {
		logger.Infof("failed to get JSONRPCServer.RetryRPCPort from config file - skipping......")
		globals.retryRPCPort = 0
		globals.retryRPCTTLCompleted = time.Duration(0)
		globals.retryRPCAckTrim = time.Duration(0)
		globals.retryRPCDeadlineIO = time.Duration(0)
		globals.retryRPCKEEPALIVEPeriod = time.Duration(0)
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

	// Ensure gate starts out in the Exclusively Locked state
	closeGate()

	// Init listeners
	globals.listeners = make([]net.Listener, 0, 2)
	globals.connections = list.New()
	globals.halting = false

	// Init JSON RPC server stuff

	// jsonRpcServerUp(globals.privateIPAddr, globals.portString)
	jsonRpcServerUp("0.0.0.0", globals.portString)

	// Now kick off our other, faster RPC server
	ioServerUp(globals.privateIPAddr, globals.fastPortString)

	// Init Retry RPC server
	retryRPCServerUp(jserver, globals.publicIPAddr, globals.retryRPCPort, globals.retryRPCTTLCompleted, globals.retryRPCAckTrim,
		globals.retryRPCDeadlineIO, globals.retryRPCKEEPALIVEPeriod)

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
	globals.volumeMap[volumeName] = true

	err = nil
	return
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		mountHandle                    fs.MountHandle
		mountIDAsByteArray             MountIDAsByteArray
		mountIDAsString                MountIDAsString
		toRemoveMountIDAsByteArrayList []MountIDAsByteArray
		toRemoveMountIDAsStringList    []MountIDAsString
	)

	toRemoveMountIDAsByteArrayList = make([]MountIDAsByteArray, 0, len(globals.mountIDAsByteArrayMap))

	for mountIDAsByteArray, mountHandle = range globals.mountIDAsByteArrayMap {
		if mountHandle.VolumeName() == volumeName {
			toRemoveMountIDAsByteArrayList = append(toRemoveMountIDAsByteArrayList, mountIDAsByteArray)
		}
	}

	for _, mountIDAsByteArray = range toRemoveMountIDAsByteArrayList {
		delete(globals.mountIDAsByteArrayMap, mountIDAsByteArray)
	}

	toRemoveMountIDAsStringList = make([]MountIDAsString, 0, len(globals.mountIDAsStringMap))

	for mountIDAsString, mountHandle = range globals.mountIDAsStringMap {
		if mountHandle.VolumeName() == volumeName {
			toRemoveMountIDAsStringList = append(toRemoveMountIDAsStringList, mountIDAsString)
		}
	}

	for _, mountIDAsString = range toRemoveMountIDAsStringList {
		delete(globals.mountIDAsStringMap, mountIDAsString)
	}

	delete(globals.volumeMap, volumeName)
	delete(globals.bimodalMountMap, volumeName)

	err = nil
	return
}

func (dummy *globalsStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	closeGate()

	err = nil
	return
}

func (dummy *globalsStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	openGate()

	err = nil
	return
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	if 0 != len(globals.volumeMap) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.volumeMap)")
		return
	}
	if 0 != len(globals.mountIDAsByteArrayMap) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.mountIDAsByteArrayMap)")
		return
	}
	if 0 != len(globals.mountIDAsStringMap) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.mountIDAsStringMap)")
		return
	}
	if 0 != len(globals.bimodalMountMap) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.bimodalMountMap)")
		return
	}

	globals.halting = true

	jsonRpcServerDown()
	ioServerDown()
	retryRPCServerDown()

	globals.listenersWG.Wait()

	openGate() // In case we are restarted... Up() expects Gate to initially be open

	err = nil
	return
}

func openGate() {
	globals.gate.Unlock()
}

func closeGate() {
	globals.gate.Lock()
}

func enterGate() {
	globals.gate.RLock()
}

func leaveGate() {
	globals.gate.RUnlock()
}
