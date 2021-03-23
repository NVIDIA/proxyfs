// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/fs"
	"github.com/NVIDIA/proxyfs/inode"
	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/proxyfs/retryrpc"
	"github.com/NVIDIA/proxyfs/transitions"
)

type leaseRequestOperationStruct struct {
	mount      *mountStruct
	inodeLease *inodeLeaseStruct
	LeaseRequestType
	replyChan chan *LeaseReply
}

type leaseRequestStateType uint32

const (
	leaseRequestStateNone leaseRequestStateType = iota
	leaseRequestStateSharedRequested
	leaseRequestStateSharedGranted
	leaseRequestStateSharedPromoting
	leaseRequestStateSharedReleasing
	leaseRequestStateExclusiveRequested
	leaseRequestStateExclusiveGranted
	leaseRequestStateExclusiveDemoting
	leaseRequestStateExclusiveReleasing
)

type leaseRequestStruct struct {
	mount        *mountStruct
	inodeLease   *inodeLeaseStruct
	requestState leaseRequestStateType
	replyChan    chan *LeaseReply // copied from leaseRequestOperationStruct.replyChan for LeaseRequestType == LeaseRequestType{Shared|Promote|Exclusive}
	listElement  *list.Element    // used when on one of inodeList.*List's
}

type inodeLeaseStateType uint32

const (
	inodeLeaseStateNone inodeLeaseStateType = iota
	inodeLeaseStateSharedGrantedRecently
	inodeLeaseStateSharedGrantedLongAgo
	inodeLeaseStateSharedPromoting
	inodeLeaseStateSharedReleasing
	inodeLeaseStateSharedExpired
	inodeLeaseStateExclusiveGrantedRecently
	inodeLeaseStateExclusiveGrantedLongAgo
	inodeLeaseStateExclusiveDemoting
	inodeLeaseStateExclusiveReleasing
	inodeLeaseStateExclusiveExpired
)

type inodeLeaseStruct struct {
	volume *volumeStruct
	inode.InodeNumber
	lruElement   *list.Element // link into volumeStruct.inodeLeaseLRU
	beingEvicted bool
	leaseState   inodeLeaseStateType

	requestChan chan *leaseRequestOperationStruct
	stopChan    chan struct{} // closing this chan will trigger *inodeLeaseStruct.handler() to:
	//                             revoke/reject all leaseRequestStruct's in *Holder* & requestedList
	//                             issue volume.leaseHandlerWG.Done()
	//                             and exit

	sharedHoldersList    *list.List          // each list.Element.Value.(*leaseRequestStruct).requestState == leaseRequestStateSharedGranted
	promotingHolder      *leaseRequestStruct // leaseRequest.requestState == leaseRequestStateSharedPromoting
	exclusiveHolder      *leaseRequestStruct // leaseRequest.requestState == leaseRequestStateExclusiveGranted
	demotingHolder       *leaseRequestStruct // leaseRequest.requestState == leaseRequestStateExclusiveDemoting
	releasingHoldersList *list.List          // each list.Element.Value.(*leaseRequestStruct).requestState == leaseRequestState{Shared|Exclusive}Releasing
	requestedList        *list.List          // each list.Element.Value.(*leaseRequestStruct).requestState == leaseRequestState{Shared|Exclusive}Requested

	lastGrantTime     time.Time // records the time at which the last exclusive or shared holder was set/added-to exclusiveHolder/sharedHoldersList
	lastInterruptTime time.Time // records the time at which the last Interrupt was sent
	interruptsSent    uint32

	longAgoTimer   *time.Timer // if .C != nil, timing when to state transition from {Shared|Exclusive}LeaseGrantedRecently to {Shared|Exclusive}LeaseGrantedLogAgo
	interruptTimer *time.Timer // if .C != nil, timing when to issue next Interrupt... or expire a Lease
}

type mountStruct struct {
	volume                 *volumeStruct
	mountIDAsByteArray     MountIDAsByteArray
	mountIDAsString        MountIDAsString
	retryRpcUniqueID       uint64
	acceptingLeaseRequests bool                                      // also an indicator (when false) that mount is being unmounted
	leaseRequestMap        map[inode.InodeNumber]*leaseRequestStruct // if     present, there is an ongoing Lease Request for this inode.InodeNumber
	//                                                                  if not present, there is no ongoing Lease Request for this inode.InodeNumber
}

type volumeStruct struct {
	volumeName                      string
	volumeHandle                    fs.VolumeHandle
	acceptingMountsAndLeaseRequests bool
	delayedUnmountList              *list.List
	mountMapByMountIDAsByteArray    map[MountIDAsByteArray]*mountStruct     // key == mountStruct.mountIDAsByteArray
	mountMapByMountIDAsString       map[MountIDAsString]*mountStruct        // key == mountStruct.mountIDAsString
	inodeLeaseMap                   map[inode.InodeNumber]*inodeLeaseStruct // key == inodeLeaseStruct.InodeNumber
	inodeLeaseLRU                   *list.List                              // .Front() is the LRU inodeLeaseStruct.listElement
	ongoingLeaseEvictions           uint64                                  // tracks the number of inodeLeaseStruct evictions currently ongoing
	activeLeaseEvictLowLimit        uint64                                  // number if inodeLeaseStruct's desired after evictions
	activeLeaseEvictHighLimit       uint64                                  // trigger on inodeLease{Map|LRU}.Len() for evicting inodeLeaseStructs
	leaseHandlerWG                  sync.WaitGroup                          // .Add(1) each inodeLease insertion into inodeLeaseMap
	//                                                                         .Done() each inodeLease after it is removed from inodeLeaseMap
}

type globalsStruct struct {
	gate sync.RWMutex //   API Requests RLock()/RUnlock()
	//                     confMap changes Lock()/Unlock()

	volumesLock sync.Mutex // protects mountMapByMountIDAsByteArray & mountMapByMountIDAsString
	//                        as well as each volumeStruct/mountStruct map

	whoAmI                  string
	publicIPAddr            string
	privateIPAddr           string
	portString              string
	fastPortString          string
	retryRPCPort            uint16
	retryRPCTTLCompleted    time.Duration
	retryRPCAckTrim         time.Duration
	retryRPCDeadlineIO      time.Duration
	retryRPCKeepAlivePeriod time.Duration
	minLeaseDuration        time.Duration
	leaseInterruptInterval  time.Duration
	leaseInterruptLimit     uint32
	dataPathLogging         bool

	volumeMap                    map[string]*volumeStruct            // key == volumeStruct.volumeName
	mountMapByMountIDAsByteArray map[MountIDAsByteArray]*mountStruct // key == mountStruct.mountIDAsByteArray
	mountMapByMountIDAsString    map[MountIDAsString]*mountStruct    // key == mountStruct.mountIDAsString

	// RetryRPC server
	retryrpcSvr              *retryrpc.Server
	rootCAx509CertificatePEM []byte

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
	globals.volumeMap = make(map[string]*volumeStruct)
	globals.mountMapByMountIDAsByteArray = make(map[MountIDAsByteArray]*mountStruct)
	globals.mountMapByMountIDAsString = make(map[MountIDAsString]*mountStruct)

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
		globals.retryRPCKeepAlivePeriod, err = confMap.FetchOptionValueDuration("JSONRPCServer", "RetryRPCKeepAlivePeriod")
		if nil != err {
			logger.Infof("failed to get JSONRPCServer.RetryRPCKeepAlivePeriod from config file - defaulting to 60s")
			globals.retryRPCKeepAlivePeriod = 60 * time.Second
		}
	} else {
		logger.Infof("failed to get JSONRPCServer.RetryRPCPort from config file - skipping......")
		globals.retryRPCPort = 0
		globals.retryRPCTTLCompleted = time.Duration(0)
		globals.retryRPCAckTrim = time.Duration(0)
		globals.retryRPCDeadlineIO = time.Duration(0)
		globals.retryRPCKeepAlivePeriod = time.Duration(0)
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

	globals.minLeaseDuration, err = confMap.FetchOptionValueDuration("JSONRPCServer", "MinLeaseDuration")
	if nil != err {
		logger.Infof("failed to get JSONRPCServer.MinLeaseDuration from config file - defaulting to 250ms")
		globals.minLeaseDuration = 250 * time.Millisecond
	}
	globals.leaseInterruptInterval, err = confMap.FetchOptionValueDuration("JSONRPCServer", "LeaseInterruptInterval")
	if nil != err {
		logger.Infof("failed to get JSONRPCServer.LeaseInterruptInterval from config file - defaulting to 250ms")
		globals.leaseInterruptInterval = 250 * time.Millisecond
	}
	globals.leaseInterruptLimit, err = confMap.FetchOptionValueUint32("JSONRPCServer", "LeaseInterruptLimit")
	if nil != err {
		logger.Infof("failed to get JSONRPCServer.LeaseInterruptLimit from config file - defaulting to 20")
		globals.leaseInterruptLimit = 20
	}

	// Ensure gate starts out in the Exclusively Locked state
	closeGate()

	// Init listeners
	globals.listeners = make([]net.Listener, 0, 2)
	globals.connections = list.New()
	globals.halting = false

	// Init JSON RPC server stuff
	jsonRpcServerUp(globals.privateIPAddr, globals.portString)

	// Now kick off our other, faster RPC server
	ioServerUp(globals.privateIPAddr, globals.fastPortString)

	// Init Retry RPC server
	retryRPCServerUp(jserver, globals.publicIPAddr, globals.retryRPCPort, globals.retryRPCTTLCompleted, globals.retryRPCAckTrim,
		globals.retryRPCDeadlineIO, globals.retryRPCKeepAlivePeriod)

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
	var (
		currentlyInVolumeMap bool
		volume               *volumeStruct
		volumeHandle         fs.VolumeHandle
	)

	globals.volumesLock.Lock()

	_, currentlyInVolumeMap = globals.volumeMap[volumeName]
	if currentlyInVolumeMap {
		globals.volumesLock.Unlock()
		err = fmt.Errorf("Cannot be told to ServeVolume(,\"%s\") twice", volumeName)
		return
	}

	volumeHandle, err = fs.FetchVolumeHandleByVolumeName(volumeName)
	if nil != err {
		globals.volumesLock.Unlock()
		return
	}

	volume = &volumeStruct{
		volumeName:                      volumeName,
		volumeHandle:                    volumeHandle,
		acceptingMountsAndLeaseRequests: true,
		delayedUnmountList:              list.New(),
		mountMapByMountIDAsByteArray:    make(map[MountIDAsByteArray]*mountStruct),
		mountMapByMountIDAsString:       make(map[MountIDAsString]*mountStruct),
		inodeLeaseMap:                   make(map[inode.InodeNumber]*inodeLeaseStruct),
		inodeLeaseLRU:                   list.New(),
		ongoingLeaseEvictions:           0,
	}

	volume.activeLeaseEvictLowLimit, err = confMap.FetchOptionValueUint64("Volume:"+volumeName, "ActiveLeaseEvictLowLimit")
	if nil != err {
		logger.Infof("failed to get Volume:" + volumeName + ".ActiveLeaseEvictLowLimit from config file - defaulting to 5000")
		volume.activeLeaseEvictLowLimit = 500000
	}
	volume.activeLeaseEvictHighLimit, err = confMap.FetchOptionValueUint64("Volume:"+volumeName, "ActiveLeaseEvictHighLimit")
	if nil != err {
		logger.Infof("failed to get Volume:" + volumeName + ".ActiveLeaseEvictHighLimit from config file - defaulting to 5010")
		volume.activeLeaseEvictHighLimit = 500010
	}

	globals.volumeMap[volumeName] = volume

	globals.volumesLock.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		currentlyInVolumeMap bool
		mountIDAsByteArray   MountIDAsByteArray
		mountIDAsString      MountIDAsString
		volume               *volumeStruct
	)

	globals.volumesLock.Lock()

	volume, currentlyInVolumeMap = globals.volumeMap[volumeName]
	if !currentlyInVolumeMap {
		globals.volumesLock.Unlock()
		err = fmt.Errorf("Cannot be told to UnserveVolume(,\"%s\") a non-served volume", volumeName)
		return
	}

	volume.acceptingMountsAndLeaseRequests = false

	// TODO: Lease Management changes - somehow while *not* holding volumesLock.Lock():
	//         Prevent new lease requests
	//         Fail outstanding lease requests
	//         Revoke granted leases

	delete(globals.volumeMap, volumeName)

	for mountIDAsByteArray = range volume.mountMapByMountIDAsByteArray {
		delete(globals.mountMapByMountIDAsByteArray, mountIDAsByteArray)
	}

	for mountIDAsString = range volume.mountMapByMountIDAsString {
		delete(globals.mountMapByMountIDAsString, mountIDAsString)
	}

	globals.volumesLock.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) VolumeToBeUnserved(confMap conf.ConfMap, volumeName string) (err error) {
	return nil // TODO: this is where we get a chance to tell our clients to unmount !!!
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
	if 0 != len(globals.mountMapByMountIDAsByteArray) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.mountMapByMountIDAsByteArray)")
		return
	}
	if 0 != len(globals.mountMapByMountIDAsString) {
		err = fmt.Errorf("jrpcfs.Down() called with 0 != len(globals.mountMapByMountIDAsString)")
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
