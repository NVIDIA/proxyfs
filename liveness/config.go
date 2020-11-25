package liveness

import (
	"container/list"
	"crypto/rand"
	"fmt"
	"hash/crc64"
	"math"
	"net"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/trackedlock"
	"github.com/swiftstack/ProxyFS/transitions"
)

const (
	PrivateClusterUDPPortDefault = uint16(8123)

	UDPPacketSizeMin = uint64(1000) // Presumably >> udpPacketHeaderSize
	UDPPacketSizeMax = uint64(8000) // Presumably >> udpPacketHeaderSize

	UDPPacketSendSizeDefault = uint64(1400)
	UDPPacketRecvSizeDefault = uint64(1500)

	UDPPacketCapPerMessageDefault = uint8(math.MaxUint8)

	HeartBeatDurationDefault = "1s"

	HeartBeatMissLimitMin     = uint64(2)
	HeartBeatMissLimitDefault = uint64(3)

	MessageQueueDepthPerPeerMin     = uint64(1)
	MessageQueueDepthPerPeerDefault = uint64(4)

	MaxRequestDurationDefault = "1s"

	LivenessCheckRedundancyMin     = uint64(1)
	LivenessCheckRedundancyDefault = uint64(2)

	LogLevelNone           = uint64(0)
	LogLevelStateChanges   = uint64(1)
	LogLevelMessages       = uint64(2)
	LogLevelMessageDetails = uint64(3)
	LogLevelMax            = uint64(4)

	LogLevelDefault = LogLevelNone

	DefaultSwiftReconNoWriteThreshold   = 80
	DefaultSwiftReconReadOnlyThreshold  = 90
	DefaultSwiftConfDir                 = "/etc/swift"
	DefaultSwiftReconChecksPerConfCheck = 10
)

type volumeStruct struct {
	volumeGroup        *volumeGroupStruct
	name               string
	fuseMountPointName string
	nfsExported        bool
	smbShareName       string
	accountName        string
}

type volumeGroupStruct struct {
	peer          *peerStruct //              == nil if in globals.myVolumeGroupMap
	name          string
	virtualIPAddr net.IP                   // Adopts the value of peer.publicIPAddr if not provided
	volumeMap     map[string]*volumeStruct // Key == volumeStruct.name
}

type peerStruct struct {
	name                    string
	publicIPAddr            net.IP
	privateIPAddr           net.IP
	udpAddr                 *net.UDPAddr
	curRecvMsgNonce         uint64
	curRecvPacketCount      uint8
	curRecvPacketSumSize    uint64
	curRecvPacketMap        map[uint8][]byte // Key is PacketIndex
	prevRecvMsgQueueElement *recvMsgQueueElementStruct
	incompleteRecvMsgMap    map[uint64]*recvMsgQueueElementStruct // Key == recvMsgQueueElementStruct.msgNonce
	incompleteRecvMsgQueue  *list.List                            // LRU ordered
	completeRecvMsgQueue    *list.List                            // FIFO ordered
	volumeGroupMap          map[string]*volumeGroupStruct         // Key == volumeGroupStruct.name
}

type internalVolumeReportStruct struct {
	volumeGroup   *internalVolumeGroupReportStruct
	name          string
	state         string // One of const State{Alive|Dead|Unknown}
	lastCheckTime time.Time
}

type internalVolumeGroupReportStruct struct {
	servingPeer   *internalServingPeerReportStruct
	name          string
	state         string // One of const State{Alive|Dead|Unknown}
	lastCheckTime time.Time
	volume        map[string]*internalVolumeReportStruct // Key = internalVolumeReportStruct.name
}

type internalServingPeerReportStruct struct {
	observingPeer *internalObservingPeerReportStruct
	name          string
	state         string // One of const State{Alive|Dead|Unknown}
	lastCheckTime time.Time
	volumeGroup   map[string]*internalVolumeGroupReportStruct // Key = internalVolumeGroupReportStruct.name
}

type internalReconEndpointReportStruct struct {
	observingPeer          *internalObservingPeerReportStruct
	ipAddrPort             string
	maxDiskUsagePercentage uint8
}

type internalObservingPeerReportStruct struct {
	name          string
	servingPeer   map[string]*internalServingPeerReportStruct   // Key = internalServingPeerReportStruct.name
	reconEndpoint map[string]*internalReconEndpointReportStruct // Key = internalReconEndpointReportStruct.ipAddrPort
}

type internalLivenessReportStruct struct {
	observingPeer map[string]*internalObservingPeerReportStruct // Key = internalObservingPeerReportStruct.name
}

type globalsStruct struct {
	trackedlock.Mutex
	active                         bool
	enabled                        bool
	whoAmI                         string
	myPublicIPAddr                 net.IP
	myPrivateIPAddr                net.IP
	myUDPAddr                      *net.UDPAddr
	myUDPConn                      *net.UDPConn
	myVolumeGroupMap               map[string]*volumeGroupStruct // Key == volumeGroupStruct.name
	peersByName                    map[string]*peerStruct        // Key == peerStruct.name
	peersByTuple                   map[string]*peerStruct        // Key == peerStruct.udpAddr.String() (~= peerStruct.tuple)
	udpPacketSendSize              uint64
	udpPacketSendPayloadSize       uint64
	udpPacketRecvSize              uint64
	udpPacketRecvPayloadSize       uint64
	udpPacketCapPerMessage         uint8
	sendMsgMessageSizeMax          uint64
	heartbeatDuration              time.Duration
	heartbeatMissLimit             uint64
	heartbeatMissDuration          time.Duration
	messageQueueDepthPerPeer       uint64
	maxRequestDuration             time.Duration
	livenessCheckRedundancy        uint64
	logLevel                       uint64
	jsonRPCServerPort              uint16
	swiftReconNoWriteThreshold     uint8
	swiftReconReadOnlyThreshold    uint8
	swiftConfDir                   string
	swiftReconChecksPerConfCheck   uint64
	swiftReconChecksUntilConfCheck uint64
	swiftConfFileMap               map[string]time.Time //          Key == os.FileInfo.Name(); Value == os.FileInfo.ModTime()
	swiftReconEndpointSet          map[string]struct{}  //          Key == IPAddrPort of ReconEndpoint
	crc64ECMATable                 *crc64.Table
	nextNonce                      uint64 //                        Randomly initialized... skips 0
	recvMsgsDoneChan               chan struct{}
	recvMsgQueue                   *list.List //                    FIFO ordered
	recvMsgChan                    chan struct{}
	requestsByExpirationTime       *list.List                //     FIFO ordered
	requestsByMsgTag               map[uint64]*requestStruct //     Key == requestStruct.msgTag
	requestExpirerStartChan        chan struct{}             //     Signaled when inserting the first element of requestsByExpirationTime
	requestExpirerStopChan         chan struct{}             //     Signaled when asking requestExpirer() to halt
	requestExpirerDone             sync.WaitGroup            //     Signaled when requestExpirer() has exited
	currentLeader                  *peerStruct
	currentVote                    *peerStruct
	currentTerm                    uint64
	nextState                      func()
	stateMachineStopChan           chan struct{}
	stateMachineDone               sync.WaitGroup
	livenessCheckerControlChan     chan bool //                     Send true  to trigger livenessChecker() to recompute polling schedule
	//                                                              Send false to trigger livenessChecker() to exit
	livenessCheckerWG          sync.WaitGroup
	volumeToCheckList          []*volumeStruct
	emptyVolumeGroupToCheckSet map[string]string   //               List (in "set" form) of VolumeGroups (by name) with no Volumes (Value == ServingPeer)
	emptyServingPeerToCheckSet map[string]struct{} //               List (in "set" form) of ServingPeers (by name) with no VolumeGroups
	myObservingPeerReport      *internalObservingPeerReportStruct
	livenessReport             *internalLivenessReportStruct
	curRWMode                  inode.RWModeType
}

var globals globalsStruct

func init() {
	transitions.Register("liveness", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	var (
		u64RandBuf []byte
	)

	// Ensure API behavior is disabled at startup

	globals.active = false

	// Do one-time initialization

	globals.crc64ECMATable = crc64.MakeTable(crc64.ECMA)

	u64RandBuf = make([]byte, 8)
	_, err = rand.Read(u64RandBuf)
	if nil != err {
		err = fmt.Errorf("read.Rand() failed: %v", err)
		return
	}
	globals.nextNonce = deserializeU64LittleEndian(u64RandBuf)
	if 0 == globals.nextNonce {
		globals.nextNonce = 1
	}

	globals.requestsByExpirationTime = list.New()
	globals.requestsByMsgTag = make(map[uint64]*requestStruct)
	globals.requestExpirerStartChan = make(chan struct{}, 1)
	globals.requestExpirerStopChan = make(chan struct{}, 1)

	globals.curRWMode = inode.RWModeNormal
	inode.SetRWMode(globals.curRWMode)

	globals.requestExpirerDone.Add(1)
	go requestExpirer()

	globals.livenessCheckerControlChan = make(chan bool, 1)

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
func (dummy *globalsStruct) VolumeToBeUnserved(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

// SignaledStart will be used to halt the cluster leadership process. This is to support
// SIGHUP handling incorporates all confMap changes are incorporated... not just during a restart.
func (dummy *globalsStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	var (
		stillDeactivating bool
	)

	// If the liveness checker is not enabled, stopping it will hang

	if !globals.enabled {
		return
	}

	// Disable API behavior as we enter the SIGHUP-handling state

	globals.active = false

	// Stop livenessChecker()

	globals.livenessCheckerControlChan <- false
	globals.livenessCheckerWG.Wait()

	// Stop state machine

	globals.stateMachineStopChan <- struct{}{}
	globals.stateMachineDone.Wait()

	// Shut off recvMsgs()

	err = globals.myUDPConn.Close()
	if nil != err {
		logger.Errorf("liveness.globals.myUDPConn.Close() failed: %v", err)
	}

	stillDeactivating = true

	for stillDeactivating {
		select {
		case <-globals.recvMsgChan:
			// Just discard it
		case <-globals.recvMsgsDoneChan:
			// Since recvMsgs() exited, we are done deactivating
			stillDeactivating = false
		}
	}

	// Clear out Swift recon settings and computed details

	globals.swiftReconNoWriteThreshold = 101  // Never enforce NoWrite Mode
	globals.swiftReconReadOnlyThreshold = 101 // Never enforce ReadOnly Mode
	globals.swiftConfDir = ""
	globals.swiftReconChecksPerConfCheck = 0 // Disabled

	// Free up remaining allocated resources

	globals.myVolumeGroupMap = nil

	globals.peersByName = nil
	globals.peersByTuple = nil

	globals.recvMsgQueue = list.New()

	globals.myObservingPeerReport = nil
	globals.livenessReport = nil

	globals.volumeToCheckList = nil
	globals.emptyVolumeGroupToCheckSet = nil
	globals.emptyServingPeerToCheckSet = nil

	globals.myObservingPeerReport = nil
	globals.livenessReport = nil

	err = nil
	return
}

// SignaledFinish will be used to kick off the cluster leadership process. This is to support
// SIGHUP handling incorporates all confMap changes are incorporated... not just during a restart.
func (dummy *globalsStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	var (
		myTuple                       string
		ok                            bool
		enabled                       bool
		peer                          *peerStruct
		peerName                      string
		peerList                      []string
		peerTuple                     string
		privateClusterUDPPortAsString string
		privateClusterUDPPortAsUint16 uint16
		privateIPAddr                 string
		publicIPAddr                  string
		virtualIPAddr                 string
		volume                        *volumeStruct
		volumeGroup                   *volumeGroupStruct
		volumeGroupList               []string
		volumeGroupName               string
		volumeList                    []string
		volumeName                    string
	)

	// see if liveness checker is enabled
	globals.enabled = false
	enabled, err = confMap.FetchOptionValueBool("Cluster", "LivenessCheckerEnabled")
	if nil != err {
		logger.InfoWithError(err, "Unable to find and/or parse [Cluster]LivenessCheckerEnabled;"+
			" defaulting to disabled")
		err = nil
		return
	}
	if !enabled {
		logger.Infof("Liveness checker disabled")
		return
	}

	// don't set globals.enabled = true until it's actually started
	logger.Infof("Liveness checker will be enabled")

	// Fetch cluster parameters

	privateClusterUDPPortAsUint16, err = confMap.FetchOptionValueUint16("Cluster", "PrivateClusterUDPPort")
	if nil != err {
		privateClusterUDPPortAsUint16 = PrivateClusterUDPPortDefault // TODO: Eventually just return
	}
	privateClusterUDPPortAsString = fmt.Sprintf("%d", privateClusterUDPPortAsUint16)

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	publicIPAddr, err = confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PublicIPAddr")
	if nil != err {
		return
	}

	globals.myPublicIPAddr = net.ParseIP(publicIPAddr)
	if nil == globals.myPublicIPAddr {
		err = fmt.Errorf("Unable to parse myPublicIPAddr")
		return
	}

	privateIPAddr, err = confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PrivateIPAddr")
	if nil != err {
		return
	}

	globals.myPrivateIPAddr = net.ParseIP(privateIPAddr)
	if nil == globals.myPrivateIPAddr {
		err = fmt.Errorf("Unable to parse myPrivateIPAddr")
		return
	}

	myTuple = net.JoinHostPort(privateIPAddr, privateClusterUDPPortAsString)

	globals.myUDPAddr, err = net.ResolveUDPAddr("udp", myTuple)
	if nil != err {
		err = fmt.Errorf("Cannot parse myTuple (%s): %v", myTuple, err)
		return
	}

	globals.myUDPConn, err = net.ListenUDP("udp", globals.myUDPAddr)
	if nil != err {
		err = fmt.Errorf("Cannot bind to myTuple (%v): %v", globals.myUDPAddr, err)
		return
	}

	globals.myVolumeGroupMap = make(map[string]*volumeGroupStruct)

	peerList, err = confMap.FetchOptionValueStringSlice("Cluster", "Peers")
	if nil != err {
		return
	}

	globals.peersByName = make(map[string]*peerStruct)
	globals.peersByTuple = make(map[string]*peerStruct)

	// Initialize emptyServingPeerToCheckSet with all ServingPeers (including self)
	// This set will be pruned later as VolumeGroups are assigned to a ServingPeer

	globals.emptyServingPeerToCheckSet = make(map[string]struct{})

	for _, peerName = range peerList {
		globals.emptyServingPeerToCheckSet[peerName] = struct{}{}

		if peerName != globals.whoAmI {
			peer = &peerStruct{
				name:                    peerName,
				curRecvMsgNonce:         0,
				curRecvPacketCount:      0,
				curRecvPacketSumSize:    0,
				curRecvPacketMap:        nil,
				prevRecvMsgQueueElement: nil,
				incompleteRecvMsgMap:    make(map[uint64]*recvMsgQueueElementStruct),
				incompleteRecvMsgQueue:  list.New(),
				completeRecvMsgQueue:    list.New(),
				volumeGroupMap:          make(map[string]*volumeGroupStruct),
			}

			publicIPAddr, err = confMap.FetchOptionValueString("Peer:"+peerName, "PublicIPAddr")
			if nil != err {
				return
			}

			peer.publicIPAddr = net.ParseIP(publicIPAddr)
			if nil == peer.publicIPAddr {
				err = fmt.Errorf("Cannot parse [Peer:%v]PublicIPAddr", peerName)
				return
			}

			privateIPAddr, err = confMap.FetchOptionValueString("Peer:"+peerName, "PrivateIPAddr")
			if nil != err {
				return
			}

			peer.privateIPAddr = net.ParseIP(privateIPAddr)
			if nil == peer.privateIPAddr {
				err = fmt.Errorf("Cannot parse [Peer:%v]PrivateIPAddr", peerName)
				return
			}

			peerTuple = net.JoinHostPort(privateIPAddr, privateClusterUDPPortAsString)

			peer.udpAddr, err = net.ResolveUDPAddr("udp", peerTuple)
			if nil != err {
				err = fmt.Errorf("Cannot parse peerTuple (%s): %v", peerTuple, err)
				return
			}

			if globals.myUDPAddr.String() == peer.udpAddr.String() {
				err = fmt.Errorf("peerTuple cannot match myTuple (%v)", globals.myUDPAddr)
				return
			}
			_, ok = globals.peersByName[peer.name]
			if ok {
				err = fmt.Errorf("peerName must not match multiple peers (%v)", peer.name)
				return
			}
			_, ok = globals.peersByTuple[peer.udpAddr.String()]
			if ok {
				err = fmt.Errorf("peerTuple must not match multiple peers (%v)", peer.udpAddr)
				return
			}

			globals.peersByName[peer.name] = peer
			globals.peersByTuple[peer.udpAddr.String()] = peer
		}
	}

	globals.udpPacketSendSize, err = confMap.FetchOptionValueUint64("Cluster", "UDPPacketSendSize")
	if nil != err {
		globals.udpPacketSendSize = UDPPacketSendSizeDefault // TODO: Eventually just return
	}
	if (globals.udpPacketSendSize < UDPPacketSizeMin) || (globals.udpPacketSendSize > UDPPacketSizeMax) {
		err = fmt.Errorf("udpPacketSendSize (%v) must be between %v and %v (inclusive)", globals.udpPacketSendSize, UDPPacketSizeMin, UDPPacketSizeMax)
		return
	}

	globals.udpPacketSendPayloadSize = globals.udpPacketSendSize - udpPacketHeaderSize

	globals.udpPacketRecvSize, err = confMap.FetchOptionValueUint64("Cluster", "UDPPacketRecvSize")
	if nil != err {
		globals.udpPacketRecvSize = UDPPacketRecvSizeDefault // TODO: Eventually just return
	}
	if (globals.udpPacketRecvSize < UDPPacketSizeMin) || (globals.udpPacketRecvSize > UDPPacketSizeMax) {
		err = fmt.Errorf("udpPacketRecvSize (%v) must be between %v and %v (inclusive)", globals.udpPacketRecvSize, UDPPacketSizeMin, UDPPacketSizeMax)
		return
	}

	globals.udpPacketRecvPayloadSize = globals.udpPacketRecvSize - udpPacketHeaderSize

	globals.udpPacketCapPerMessage, err = confMap.FetchOptionValueUint8("Cluster", "UDPPacketCapPerMessage")
	if nil != err {
		globals.udpPacketCapPerMessage = UDPPacketCapPerMessageDefault // TODO: Eventually just return
	}
	if 0 == globals.udpPacketCapPerMessage {
		err = fmt.Errorf("udpPacketCapPerMessage must be non-zero")
		return
	}

	globals.sendMsgMessageSizeMax = uint64(globals.udpPacketCapPerMessage) * globals.udpPacketSendPayloadSize

	globals.heartbeatDuration, err = confMap.FetchOptionValueDuration("Cluster", "HeartBeatDuration")
	if nil != err {
		// TODO: Eventually just return
		globals.heartbeatDuration, err = time.ParseDuration(HeartBeatDurationDefault)
		if nil != err {
			return
		}
	}
	if time.Duration(0) == globals.heartbeatDuration {
		err = fmt.Errorf("heartbeatDuration must be non-zero")
		return
	}

	globals.heartbeatMissLimit, err = confMap.FetchOptionValueUint64("Cluster", "HeartBeatMissLimit")
	if nil != err {
		globals.heartbeatMissLimit = HeartBeatMissLimitDefault // TODO: Eventually just return
	}
	if globals.heartbeatMissLimit < HeartBeatMissLimitMin {
		err = fmt.Errorf("heartbeatMissLimit (%v) must be at least %v", globals.heartbeatMissLimit, HeartBeatMissLimitMin)
		return
	}

	globals.heartbeatMissDuration = time.Duration(globals.heartbeatMissLimit) * globals.heartbeatDuration

	globals.messageQueueDepthPerPeer, err = confMap.FetchOptionValueUint64("Cluster", "MessageQueueDepthPerPeer")
	if nil != err {
		globals.messageQueueDepthPerPeer = MessageQueueDepthPerPeerDefault // TODO: Eventually just return
	}
	if globals.messageQueueDepthPerPeer < MessageQueueDepthPerPeerMin {
		err = fmt.Errorf("messageQueueDepthPerPeer (%v) must be at least %v", globals.messageQueueDepthPerPeer, MessageQueueDepthPerPeerMin)
		return
	}

	globals.maxRequestDuration, err = confMap.FetchOptionValueDuration("Cluster", "MaxRequestDuration")
	if nil != err {
		// TODO: Eventually just return
		globals.maxRequestDuration, err = time.ParseDuration(MaxRequestDurationDefault)
		if nil != err {
			return
		}
	}
	if time.Duration(0) == globals.maxRequestDuration {
		err = fmt.Errorf("maxRequestDuration must be non-zero")
		return
	}

	globals.livenessCheckRedundancy, err = confMap.FetchOptionValueUint64("Cluster", "LivenessCheckRedundancy")
	if nil != err {
		globals.livenessCheckRedundancy = LivenessCheckRedundancyDefault // TODO: Eventually just return
	}
	if globals.livenessCheckRedundancy < LivenessCheckRedundancyMin {
		err = fmt.Errorf("livenessCheckRedundancy (%v) must be at least %v", globals.livenessCheckRedundancy, LivenessCheckRedundancyMin)
		return
	}

	// Set LogLevel as specified or use default

	globals.logLevel, err = confMap.FetchOptionValueUint64("Cluster", "LogLevel")
	if nil != err {
		globals.logLevel = LogLevelDefault
	}
	if globals.logLevel > LogLevelMax {
		err = fmt.Errorf("logLevel (%v) must be between 0 and %v (inclusive)", globals.logLevel, LogLevelMax)
		return
	}

	// Record current Peer->VolumeGroup->Volume mapping

	globals.volumeToCheckList = make([]*volumeStruct, 0)

	volumeGroupList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != err {
		return
	}

	globals.emptyVolumeGroupToCheckSet = make(map[string]string)

	for _, volumeGroupName = range volumeGroupList {
		virtualIPAddr, err = confMap.FetchOptionValueString("VolumeGroup:"+volumeGroupName, "VirtualIPAddr")
		if nil != err {
			virtualIPAddr = ""
		}

		peerList, err = confMap.FetchOptionValueStringSlice("VolumeGroup:"+volumeGroupName, "PrimaryPeer")
		if nil != err {
			return
		}

		switch len(peerList) {
		case 0:
			// Since VolumeGroup has no PrimaryPeer, just skip it
		case 1:
			// Include this VolumeGroup

			peerName = peerList[0]

			delete(globals.emptyServingPeerToCheckSet, peerName)

			if peerName == globals.whoAmI {
				_, ok = globals.myVolumeGroupMap[volumeGroupName]
				if ok {
					err = fmt.Errorf("Duplicate VolumeGroup (%v) not allowed", volumeGroupName)
					return
				}

				volumeGroup = &volumeGroupStruct{
					peer:      nil,
					name:      volumeGroupName,
					volumeMap: make(map[string]*volumeStruct),
				}

				if "" == virtualIPAddr {
					volumeGroup.virtualIPAddr = globals.myPublicIPAddr
				} else {

					// virtualIPAddr must be a valid IP address or valid
					// IP address in CIDR notation
					volumeGroup.virtualIPAddr = net.ParseIP(virtualIPAddr)
					if nil == volumeGroup.virtualIPAddr {

						volumeGroup.virtualIPAddr, _, err = net.ParseCIDR(virtualIPAddr)
						if err != nil {
							err = fmt.Errorf("Cannot parse [VolumeGroup:%v]VirtualIPAddr: '%s' "+
								" as IP address or CIDR IP address: %v",
								volumeGroupName, virtualIPAddr, err)
							return
						}
					}
				}

				globals.myVolumeGroupMap[volumeGroupName] = volumeGroup
			} else {
				peer, ok = globals.peersByName[peerName]
				if !ok {
					err = fmt.Errorf("[VolumeGroup:%v]PrimaryPeer (%v) not found in [Cluster]Peers", volumeGroupName, peerName)
					return
				}

				_, ok = peer.volumeGroupMap[volumeGroupName]
				if ok {
					err = fmt.Errorf("Duplicate VolumeGroup (%v) not allowed", volumeGroupName)
					return
				}

				volumeGroup = &volumeGroupStruct{
					peer:      peer,
					name:      volumeGroupName,
					volumeMap: make(map[string]*volumeStruct),
				}

				if "" == virtualIPAddr {
					volumeGroup.virtualIPAddr = peer.publicIPAddr
				} else {

					// virtualIPAddr must be a valid IP address or valid
					// IP address in CIDR notation
					volumeGroup.virtualIPAddr = net.ParseIP(virtualIPAddr)
					if nil == volumeGroup.virtualIPAddr {

						volumeGroup.virtualIPAddr, _, err = net.ParseCIDR(virtualIPAddr)
						if err != nil {
							err = fmt.Errorf("Cannot parse [VolumeGroup:%v]VirtualIPAddr: '%s' "+
								" as IP address or CIDR IP address: %v",
								volumeGroupName, virtualIPAddr, err)
							return
						}
					}
				}

				peer.volumeGroupMap[volumeGroupName] = volumeGroup
			}

			volumeList, err = confMap.FetchOptionValueStringSlice("VolumeGroup:"+volumeGroupName, "VolumeList")
			if nil != err {
				return
			}

			if 0 == len(volumeList) {
				globals.emptyVolumeGroupToCheckSet[volumeGroupName] = peerName
			} else {
				for _, volumeName = range volumeList {
					_, ok = volumeGroup.volumeMap[volumeName]
					if ok {
						err = fmt.Errorf("[VolumeGroup:%v]VolumeList contains Volume %v more than once", volumeGroupName, volumeName)
						return
					}

					volume = &volumeStruct{
						volumeGroup: volumeGroup,
						name:        volumeName,
					}

					volume.fuseMountPointName, err = confMap.FetchOptionValueString("Volume:"+volumeName, "FUSEMountPointName")
					if nil != err {
						return
					}

					volume.nfsExported, err = confMap.FetchOptionValueBool("Volume:"+volumeName, "NFSExported")
					if nil != err {
						// Default to no NFS Export
						volume.nfsExported = false
					}

					volume.smbShareName, err = confMap.FetchOptionValueString("Volume:"+volumeName, "SMBShareName")
					if nil != err {
						// Default to no SMB Share
						volume.smbShareName = ""
					}

					volume.accountName, err = confMap.FetchOptionValueString("Volume:"+volumeName, "AccountName")
					if nil != err {
						return
					}

					volumeGroup.volumeMap[volumeName] = volume

					globals.volumeToCheckList = append(globals.volumeToCheckList, volume)
				}
			}
		default:
			err = fmt.Errorf("[VolumeGroup:%s]PrimaryPeer must be empty or single-valued", volumeGroupName)
			return
		}
	}

	// Fetch JSON RPC Port to be used when polling Peers

	globals.jsonRPCServerPort, err = confMap.FetchOptionValueUint16("JSONRPCServer", "TCPPort")
	if nil != err {
		return
	}

	// Fetch Swift recon settings

	err = confMap.VerifyOptionIsMissing("SwiftClient", "SwiftReconChecksPerConfCheck")
	if nil == err {
		logger.WarnfWithError(err, "Unable to fetch [SwiftClient]SwiftReconChecksPerConfCheck... defaulting to %d", DefaultSwiftReconChecksPerConfCheck)
		globals.swiftReconChecksPerConfCheck = DefaultSwiftReconChecksPerConfCheck
	} else {
		globals.swiftReconChecksPerConfCheck, err = confMap.FetchOptionValueUint64("SwiftClient", "SwiftReconChecksPerConfCheck")
		if nil != err {
			logger.ErrorfWithError(err, "Unable to parse [SwiftClient]SwiftReconChecksPerConfCheck")
			return
		}
	}

	if 0 == globals.swiftReconChecksPerConfCheck {
		logger.Warnf("[SwiftClient]SwiftReconChecksPerConfCheck == 0... disabling recon checks")
	} else {
		globals.swiftReconNoWriteThreshold, err = confMap.FetchOptionValueUint8("SwiftClient", "SwiftReconNoWriteThreshold")
		if nil == err {
			if 100 < globals.swiftReconNoWriteThreshold {
				err = fmt.Errorf("[SwiftClient]SwiftReconNoWriteThreshold cannot be greater than 100")
				return
			}
		} else {
			logger.WarnfWithError(err, "Unable to fetch [SwiftClient]SwiftReconNoWriteThreshold... defaulting to %d", DefaultSwiftReconNoWriteThreshold)
			globals.swiftReconNoWriteThreshold = DefaultSwiftReconNoWriteThreshold
		}

		globals.swiftReconReadOnlyThreshold, err = confMap.FetchOptionValueUint8("SwiftClient", "SwiftReconReadOnlyThreshold")
		if nil == err {
			if 100 < globals.swiftReconReadOnlyThreshold {
				err = fmt.Errorf("[SwiftClient]SwiftReconReadOnlyThreshold cannot be greater than 100")
				return
			}
			if globals.swiftReconReadOnlyThreshold < globals.swiftReconNoWriteThreshold {
				err = fmt.Errorf("[SwiftClient]SwiftReconReadOnlyThreshold cannot be less than [SwiftClient]SwiftReconNoWriteThreshold")
				return
			}
		} else {
			if globals.swiftReconNoWriteThreshold > DefaultSwiftReconReadOnlyThreshold {
				logger.WarnfWithError(err, "Unable to fetch [SwiftClient]SwiftReconReadOnlyThreadhold... defaulting to %d", globals.swiftReconNoWriteThreshold)
				globals.swiftReconReadOnlyThreshold = globals.swiftReconNoWriteThreshold
			} else {
				logger.WarnfWithError(err, "Unable to fetch [SwiftClient]SwiftReconReadOnlyThreadhold... defaulting to %d", DefaultSwiftReconReadOnlyThreshold)
				globals.swiftReconReadOnlyThreshold = DefaultSwiftReconReadOnlyThreshold
			}
		}

		globals.swiftConfDir, err = confMap.FetchOptionValueString("SwiftClient", "SwiftConfDir")
		if nil != err {
			logger.WarnfWithError(err, "Unable to fetch [SwiftClient]SwiftConfDir... defaulting to %s", DefaultSwiftConfDir)
			globals.swiftConfDir = DefaultSwiftConfDir
		}
	}

	// the liveness checker will be enabled (no more error out cases)

	globals.enabled = true

	// Initialize remaining globals

	globals.swiftReconChecksUntilConfCheck = 0 // First ReconCheck will trigger a ConfCheck
	globals.swiftConfFileMap = make(map[string]time.Time)

	globals.recvMsgQueue = list.New()

	globals.recvMsgChan = make(chan struct{}, 1)

	globals.recvMsgsDoneChan = make(chan struct{}, 1)
	go recvMsgs()

	globals.currentLeader = nil
	globals.currentVote = nil
	globals.currentTerm = 0

	globals.nextState = doFollower

	globals.stateMachineStopChan = make(chan struct{}, 1)

	// Initialize internal Liveness Report data as being empty

	globals.myObservingPeerReport = nil
	globals.livenessReport = nil

	// Start up livenessChecker()

	globals.livenessCheckerWG.Add(1)
	go livenessChecker()

	// Become an active participant in the cluster

	globals.stateMachineDone.Add(1)
	go stateMachine()

	// Enable API behavior as we leave the SIGHUP-handling state

	globals.active = true

	err = nil
	return
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	globals.requestExpirerStopChan <- struct{}{}
	globals.requestExpirerDone.Wait()

	return nil
}
