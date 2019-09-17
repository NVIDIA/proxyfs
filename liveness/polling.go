package liveness

import (
	"container/list"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

type pingReqStruct struct {
	JSONrpc string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	Params  [1]jrpcfs.PingReq `json:"params"`
	ID      uint64            `json:"id"`
}

type pingReplyStruct struct {
	ID     uint64           `json:"id"`
	Result jrpcfs.PingReply `json:"result"`
	Error  string           `json:"error"`
}

const maxRPCReplySize = 4096

func livenessChecker() {
	var (
		checkEntityList                 *list.List
		durationPerCheck                time.Duration
		entitiesToCheck                 uint64
		entityToCheck                   *list.Element
		err                             error
		livenessCheckerControlChanValue bool
		myObservingPeerReport           *internalObservingPeerReportStruct
		servingPeer                     *internalServingPeerReportStruct
		volume                          *internalVolumeReportStruct
		volumeGroup                     *internalVolumeGroupReportStruct
	)

	for {
		globals.Lock()
		myObservingPeerReport = globals.myObservingPeerReport
		globals.Unlock()

		if nil == myObservingPeerReport {
			// Just wait to be signaled to check again... or exit
			livenessCheckerControlChanValue = <-globals.livenessCheckerControlChan
			if livenessCheckerControlChanValue {
				// Just loop back and re-check globals.myObservingPeerReport
			} else {
				// Exit
				globals.livenessCheckerWG.Done()
				return
			}
		} else { // nil != myObservingPeerReport
			// Check to see if we are supposed to exit

			select {
			case livenessCheckerControlChanValue = <-globals.livenessCheckerControlChan:
				if livenessCheckerControlChanValue {
					// Just loop back and re-check globals.myObservingPeerReport
				} else {
					// Exit
					globals.livenessCheckerWG.Done()
					return
				}
			default:
				// Just fall into checkEntityList processing
			}

			// Compute randomly-ordered list of entities to check

			checkEntityList = list.New()

			for _, servingPeer = range myObservingPeerReport.servingPeer {
				if utils.FetchRandomBool() {
					_ = checkEntityList.PushFront(servingPeer)
				} else {
					_ = checkEntityList.PushBack(servingPeer)
				}

				for _, volumeGroup = range servingPeer.volumeGroup {
					if utils.FetchRandomBool() {
						_ = checkEntityList.PushFront(volumeGroup)
					} else {
						_ = checkEntityList.PushBack(volumeGroup)
					}

					for _, volume = range volumeGroup.volume {
						if utils.FetchRandomBool() {
							_ = checkEntityList.PushFront(volume)
						} else {
							_ = checkEntityList.PushBack(volume)
						}
					}
				}
			}

			// Compute number of entities to check & time between each check
			// Allow for one extra time slice to hopefully get all entities checked

			entitiesToCheck = uint64(checkEntityList.Len())

			durationPerCheck = globals.heartbeatDuration / time.Duration(entitiesToCheck+1)

			// Perform each check spaced out by durationPerCheck

			entityToCheck = checkEntityList.Front()

			for nil != entityToCheck {
				switch reflect.TypeOf(entityToCheck.Value) {
				case reflect.TypeOf(servingPeer):
					livenessCheckServingPeer(entityToCheck.Value.(*internalServingPeerReportStruct))
				case reflect.TypeOf(volumeGroup):
					livenessCheckVolumeGroup(entityToCheck.Value.(*internalVolumeGroupReportStruct))
				case reflect.TypeOf(volume):
					livenessCheckVolume(entityToCheck.Value.(*internalVolumeReportStruct))
				default:
					err = fmt.Errorf("Unrecognized reflect.TypeOf(entityToCheck.Value): %v", reflect.TypeOf(entityToCheck.Value))
					panic(err)
				}

				// Delay before next entityToCheck

				select {
				case livenessCheckerControlChanValue = <-globals.livenessCheckerControlChan:
					if livenessCheckerControlChanValue {
						// Just finish current checks before re-checking globals.myObservingPeerReport
					} else {
						// Exit
						globals.livenessCheckerWG.Done()
						return
					}
				case <-time.After(durationPerCheck):
					// Proceed to next check
				}

				// Loop back for next entityToCheck

				entityToCheck = entityToCheck.Next()
			}
		}
	}
}

func livenessCheckServingPeer(servingPeer *internalServingPeerReportStruct) {
	var (
		err              error
		nextID           uint64
		pingReply        pingReplyStruct
		pingReplyBuf     []byte
		pingReplyLen     int
		pingReq          pingReqStruct
		pingReqBuf       []byte
		servingPeerState string
		tcpAddr          *net.TCPAddr
		tcpAddrToResolve string
		tcpConn          *net.TCPConn
		timeNow          time.Time
	)

	// Setup exit path to atomically update servingPeer (initially servingPeer.state == StateUnknown)

	timeNow = time.Now()
	servingPeerState = StateUnknown

	defer func() {
		globals.Lock()
		servingPeer.state = servingPeerState
		servingPeer.lastCheckTime = timeNow
		globals.Unlock()
	}()

	// Form RpcPing message to poll servingPeer's JSONRPCServer.TCPPort with

	pingReq.JSONrpc = "2.0"
	pingReq.Method = "Server.RpcPing"
	pingReq.Params[0].Message = "Ping at " + timeNow.Format(time.RFC3339)
	pingReq.ID = nextID

	pingReqBuf, err = json.Marshal(pingReq)
	if nil != err {
		err = fmt.Errorf("json.Marshal(pingReq) failed: %v", err)
		logger.Error(err)
		return
	}

	// Compute tcpAddr of servingPeer's JSONRPCServer.TCPPort

	if servingPeer.name == globals.whoAmI {
		tcpAddrToResolve = net.JoinHostPort(globals.myPrivateIPAddr.String(), fmt.Sprintf("%d", globals.jsonRPCServerPort))
	} else {
		tcpAddrToResolve = net.JoinHostPort(globals.peersByName[servingPeer.name].privateIPAddr.String(), fmt.Sprintf("%d", globals.jsonRPCServerPort))
	}

	tcpAddr, err = net.ResolveTCPAddr("tcp", tcpAddrToResolve)

	if nil != err {
		err = fmt.Errorf("net.ResolveTCPAddr(\"tcp\", \"%v\") failed: %v", tcpAddrToResolve, err)
		logger.Error(err)
		return
	}

	// Perform RpcPing... from here on, default servingPeer.state == StateDead

	servingPeerState = StateDead

	tcpConn, err = net.DialTCP("tcp", nil, tcpAddr)
	if nil != err {
		return
	}

	_, err = tcpConn.Write(pingReqBuf)
	if nil != err {
		return
	}

	pingReplyBuf = make([]byte, maxRPCReplySize)

	pingReplyLen, err = tcpConn.Read(pingReplyBuf)
	if nil != err {
		return
	}

	err = tcpConn.Close()
	if nil != err {
		return
	}

	pingReplyBuf = pingReplyBuf[:pingReplyLen]

	err = json.Unmarshal(pingReplyBuf, &pingReply)
	if nil != err {
		return
	}

	// RpcPing worked... so ensure servingPeer.state == StateAlive

	servingPeerState = StateAlive
}

func livenessCheckVolumeGroup(volumeGroup *internalVolumeGroupReportStruct) {
	var (
		volumeGroupState string
		timeNow          time.Time
	)

	// Setup exit path to atomically update servingPeer (initially servingPeer.state == StateUnknown)

	timeNow = time.Now()
	volumeGroupState = StateUnknown

	defer func() {
		globals.Lock()
		volumeGroup.state = volumeGroupState
		volumeGroup.lastCheckTime = timeNow
		globals.Unlock()
	}()

	// TODO: Implement livenessCheckVolumeGroup()
}

func livenessCheckVolume(volume *internalVolumeReportStruct) {
	var (
		volumeState string
		timeNow     time.Time
	)

	// Setup exit path to atomically update servingPeer (initially servingPeer.state == StateUnknown)

	timeNow = time.Now()
	volumeState = StateUnknown

	defer func() {
		globals.Lock()
		volume.state = volumeState
		volume.lastCheckTime = timeNow
		globals.Unlock()
	}()

	// TODO: Implement livenessCheckVolume()
}

// computeLivenessCheckAssignments takes a list of ObservingPeer and produces a
// template internalLivenessReport that is to be filled in by this collection of peers.
// While the elements of the resultant internalLivenessReport have State and LastCheckTime
// fields, these are ignored as they will ultimately be filled in by each ObservingPeer.
// The livenessCheckRedundancy is used to ensure that each ServingPeer, VolumeGroup,
// and Volume is adequately covered. As every Volume is part of a VolumeGroup and every
// VolumeGroup is assigned to a single ServingPeer, this amounts to just dolling out
// the Volumes to ObervingPeers with the required livenessCheckRedundancy. That said,
// it is a bit misleading for an ObservingPeer to report that a VolumeGroup is "alive"
// when not all of that VolumeGroup's Volumes have been checked. Similarly, it is a
// bit misleading for an ObservingPeer to report that a ServingPeer is "alive" when
// not all of that ServingPeer's VolumeGroups have been checked. Therefore, to get an
// accurate picture of that state of a VolumeGroup or ServingPeer, all results from
// all ObservingPeers should be consulted as a set when making any availability
// decision. As there is no way to check an empty VolumeGroup, there state will not
// be in the resultant internalLivenessReport. However, ServingPeers that have no
// VolumeGroups assigned will still be in the resultant internalLivenessReport.
func computeLivenessCheckAssignments(observingPeerNameList []string) (internalLivenessReport *internalLivenessReportStruct) {
	var (
		effectiveLivenessCheckRedundancy      uint64
		effectiveLivenessCheckRedundancyIndex uint64
		err                                   error
		internalObservingPeerReport           *internalObservingPeerReportStruct
		internalServingPeerReport             *internalServingPeerReportStruct
		internalVolumeGroupReport             *internalVolumeGroupReportStruct
		internalVolumeReport                  *internalVolumeReportStruct
		notYetAdded                           bool
		observingPeerIndex                    uint64
		observingPeerName                     string
		ok                                    bool
		servingPeer                           *peerStruct
		servingPeerName                       string
		volumeGroup                           *volumeGroupStruct
		volumeGroupName                       string
		volumeName                            string
		volumeToCheck                         *volumeStruct
	)

	if 0 == len(observingPeerNameList) {
		err = fmt.Errorf("computeLivenessCheckAssignments(): len(observingPeerNameList) cannot be zero")
		panic(err)
	}

	internalLivenessReport = &internalLivenessReportStruct{
		observingPeer: make(map[string]*internalObservingPeerReportStruct),
	}

	// Adjust effectiveLivenessCheckRedundancy to be no more than len(observingPeerNameList)

	if uint64(len(observingPeerNameList)) < globals.livenessCheckRedundancy {
		effectiveLivenessCheckRedundancy = uint64(len(observingPeerNameList))
	} else {
		effectiveLivenessCheckRedundancy = globals.livenessCheckRedundancy
	}

	// Iterate through observingPeerNameList effectiveLivenessCheckRedundancy times scheduling Volumes

	observingPeerIndex = 0

	for effectiveLivenessCheckRedundancyIndex = 0; effectiveLivenessCheckRedundancyIndex < effectiveLivenessCheckRedundancy; effectiveLivenessCheckRedundancyIndex++ {
		for _, volumeToCheck = range globals.volumeToCheckList {
			// Add volumeToCheck to currently indexed ObservingPeer

			volumeName = volumeToCheck.name
			volumeGroup = volumeToCheck.volumeGroup
			volumeGroupName = volumeGroup.name
			servingPeer = volumeGroup.peer
			if nil == servingPeer {
				servingPeerName = globals.whoAmI
			} else {
				servingPeerName = servingPeer.name
			}

			notYetAdded = true // Avoid duplicate assignments

			for notYetAdded {
				observingPeerName = observingPeerNameList[observingPeerIndex]

				internalObservingPeerReport, ok = internalLivenessReport.observingPeer[observingPeerName]
				if !ok {
					internalObservingPeerReport = &internalObservingPeerReportStruct{
						name:        observingPeerName,
						servingPeer: make(map[string]*internalServingPeerReportStruct),
					}
					internalLivenessReport.observingPeer[observingPeerName] = internalObservingPeerReport
				}

				internalServingPeerReport, ok = internalObservingPeerReport.servingPeer[servingPeerName]
				if !ok {
					internalServingPeerReport = &internalServingPeerReportStruct{
						observingPeer: internalObservingPeerReport,
						name:          servingPeerName,
						state:         StateUnknown,
						lastCheckTime: time.Time{},
						volumeGroup:   make(map[string]*internalVolumeGroupReportStruct),
					}
					internalObservingPeerReport.servingPeer[servingPeerName] = internalServingPeerReport
				}

				internalVolumeGroupReport, ok = internalServingPeerReport.volumeGroup[volumeGroupName]
				if !ok {
					internalVolumeGroupReport = &internalVolumeGroupReportStruct{
						servingPeer:   internalServingPeerReport,
						name:          volumeGroupName,
						state:         StateUnknown,
						lastCheckTime: time.Time{},
						volume:        make(map[string]*internalVolumeReportStruct),
					}
					internalServingPeerReport.volumeGroup[volumeGroupName] = internalVolumeGroupReport
				}

				_, ok = internalVolumeGroupReport.volume[volumeName]

				if ok {
					// Need to step to the next ObservingPeer because this one is already watching this Volume
				} else {
					// New Volume for this ObservingPeer... so add it

					internalVolumeReport = &internalVolumeReportStruct{
						volumeGroup:   internalVolumeGroupReport,
						name:          volumeName,
						state:         StateUnknown,
						lastCheckTime: time.Time{},
					}

					internalVolumeGroupReport.volume[volumeName] = internalVolumeReport

					notYetAdded = false
				}

				// Cycle to next ObservingPeer

				observingPeerIndex++
				if observingPeerIndex == uint64(len(observingPeerNameList)) {
					observingPeerIndex = 0
				}
			}
		}
	}

	// Iterate through observingPeerNameList effectiveLivenessCheckRedundancy times scheduling "empty" VolumeGroups

	for effectiveLivenessCheckRedundancyIndex = 0; effectiveLivenessCheckRedundancyIndex < effectiveLivenessCheckRedundancy; effectiveLivenessCheckRedundancyIndex++ {
		for volumeGroupName, servingPeerName = range globals.emptyVolumeGroupToCheckSet {
			// Add "empty" VolumeGroup to currently indexed ObservingPeer

			notYetAdded = true // Avoid duplicate assignments

			for notYetAdded {
				observingPeerName = observingPeerNameList[observingPeerIndex]

				internalObservingPeerReport, ok = internalLivenessReport.observingPeer[observingPeerName]
				if !ok {
					internalObservingPeerReport = &internalObservingPeerReportStruct{
						name:        observingPeerName,
						servingPeer: make(map[string]*internalServingPeerReportStruct),
					}
					internalLivenessReport.observingPeer[observingPeerName] = internalObservingPeerReport
				}

				internalServingPeerReport, ok = internalObservingPeerReport.servingPeer[servingPeerName]
				if !ok {
					internalServingPeerReport = &internalServingPeerReportStruct{
						observingPeer: internalObservingPeerReport,
						name:          servingPeerName,
						state:         StateUnknown,
						lastCheckTime: time.Time{},
						volumeGroup:   make(map[string]*internalVolumeGroupReportStruct),
					}
				}
				internalObservingPeerReport.servingPeer[servingPeerName] = internalServingPeerReport

				_, ok = internalServingPeerReport.volumeGroup[volumeGroupName]

				if ok {
					// Need to step to the next ObservingPeer because this one is already watching this VolumeGroup
				} else {
					// New VolumeGroup for this ObservingPeer->ServingPeer... so add it

					internalVolumeGroupReport = &internalVolumeGroupReportStruct{
						servingPeer:   internalServingPeerReport,
						name:          volumeGroupName,
						state:         StateUnknown,
						lastCheckTime: time.Time{},
						volume:        make(map[string]*internalVolumeReportStruct),
					}

					internalServingPeerReport.volumeGroup[volumeGroupName] = internalVolumeGroupReport

					notYetAdded = false
				}

				// Cycle to next ObservingPeer

				observingPeerIndex++
				if observingPeerIndex == uint64(len(observingPeerNameList)) {
					observingPeerIndex = 0
				}
			}
		}
	}

	// Iterate through observingPeerNameList effectiveLivenessCheckRedundancy times scheduling "empty" ServingPeers

	for effectiveLivenessCheckRedundancyIndex = 0; effectiveLivenessCheckRedundancyIndex < effectiveLivenessCheckRedundancy; effectiveLivenessCheckRedundancyIndex++ {
		for servingPeerName = range globals.emptyServingPeerToCheckSet {
			// Add "empty" ServingPeer to currently indexed ObservingPeer

			notYetAdded = true // Avoid duplicate assignments

			for notYetAdded {
				observingPeerName = observingPeerNameList[observingPeerIndex]

				internalObservingPeerReport, ok = internalLivenessReport.observingPeer[observingPeerName]
				if !ok {
					internalObservingPeerReport = &internalObservingPeerReportStruct{
						name:        observingPeerName,
						servingPeer: make(map[string]*internalServingPeerReportStruct),
					}
					internalLivenessReport.observingPeer[observingPeerName] = internalObservingPeerReport
				}

				_, ok = internalObservingPeerReport.servingPeer[servingPeerName]

				if ok {
					// Need to step to the next ObservingPeer because this one is already watching this ServingPeer
				} else {
					// New ServingPeer for this ObservingPeer... so add it

					internalServingPeerReport = &internalServingPeerReportStruct{
						observingPeer: internalObservingPeerReport,
						name:          servingPeerName,
						state:         StateUnknown,
						lastCheckTime: time.Time{},
						volumeGroup:   make(map[string]*internalVolumeGroupReportStruct),
					}

					internalObservingPeerReport.servingPeer[servingPeerName] = internalServingPeerReport

					notYetAdded = false
				}

				// Cycle to next ObservingPeer

				observingPeerIndex++
				if observingPeerIndex == uint64(len(observingPeerNameList)) {
					observingPeerIndex = 0
				}
			}
		}
	}

	return
}

func mergeObservingPeerReportIntoLivenessReport(internalObservingPeerReport *internalObservingPeerReportStruct, internalLivenessReport *internalLivenessReportStruct) {
	var (
		ok bool
	)

	_, ok = internalLivenessReport.observingPeer[internalObservingPeerReport.name]
	if ok {
		delete(internalLivenessReport.observingPeer, internalObservingPeerReport.name)
	}

	internalLivenessReport.observingPeer[internalObservingPeerReport.name] = internalObservingPeerReport
}

func updateMyObservingPeerReportWhileLocked(internalObservingPeerReport *internalObservingPeerReportStruct) {
	var (
		ok                 bool
		servingPeerName    string
		servingPeerNameSet map[string]struct{}
		servingPeerNew     *internalServingPeerReportStruct
		servingPeerOld     *internalServingPeerReportStruct
		volumeGroupName    string
		volumeGroupNameSet map[string]struct{}
		volumeGroupNew     *internalVolumeGroupReportStruct
		volumeGroupOld     *internalVolumeGroupReportStruct
		volumeName         string
		volumeNameSet      map[string]struct{}
	)

	if (nil == globals.myObservingPeerReport) || (nil == internalObservingPeerReport) {
		globals.myObservingPeerReport = internalObservingPeerReport
		return
	}

	// Remove any ServingPeers from globals.myObservingPeerReport missing from internalObservingPeerReport

	servingPeerNameSet = make(map[string]struct{})

	for servingPeerName = range globals.myObservingPeerReport.servingPeer {
		_, ok = internalObservingPeerReport.servingPeer[servingPeerName]
		if !ok {
			servingPeerNameSet[servingPeerName] = struct{}{}
		}
	}

	for servingPeerName = range servingPeerNameSet {
		delete(globals.myObservingPeerReport.servingPeer, servingPeerName)
	}

	// Add any ServingPeers from internalObservingPeerReport missing from globals.myObservingPeerReport

	for servingPeerName = range internalObservingPeerReport.servingPeer {
		_, ok = globals.myObservingPeerReport.servingPeer[servingPeerName]
		if !ok {
			globals.myObservingPeerReport.servingPeer[servingPeerName] = &internalServingPeerReportStruct{
				observingPeer: globals.myObservingPeerReport,
				name:          servingPeerName,
				state:         StateUnknown,
				lastCheckTime: time.Time{},
				volumeGroup:   make(map[string]*internalVolumeGroupReportStruct),
			}
		}
	}

	// Now loop inside each ServingPeer (must now exist in both globals.myObservingPeerReport & internalObservingPeerReport)

	for servingPeerName, servingPeerOld = range globals.myObservingPeerReport.servingPeer {
		servingPeerNew = internalObservingPeerReport.servingPeer[servingPeerName]

		// Remove any VolumeGroups from servingPeerOld missing from servingPeerNew

		volumeGroupNameSet = make(map[string]struct{})

		for volumeGroupName = range servingPeerOld.volumeGroup {
			_, ok = servingPeerNew.volumeGroup[volumeGroupName]
			if !ok {
				volumeGroupNameSet[volumeGroupName] = struct{}{}
			}
		}

		for volumeGroupName = range volumeGroupNameSet {
			delete(servingPeerOld.volumeGroup, volumeGroupName)
		}

		// Add any VolumeGroups from servingPeerNew missing from servingPeerOld

		for volumeGroupName = range servingPeerNew.volumeGroup {
			_, ok = servingPeerOld.volumeGroup[volumeGroupName]
			if !ok {
				servingPeerOld.volumeGroup[volumeGroupName] = &internalVolumeGroupReportStruct{
					servingPeer:   servingPeerOld,
					name:          volumeGroupName,
					state:         StateUnknown,
					lastCheckTime: time.Time{},
					volume:        make(map[string]*internalVolumeReportStruct),
				}
			}
		}

		// Now loop inside each VolumeGroup (must now exist in both servingPeerOld & servingPeerNew)

		for volumeGroupName, volumeGroupOld = range servingPeerOld.volumeGroup {
			volumeGroupNew = servingPeerNew.volumeGroup[volumeGroupName]

			// Remove any Volumes from volumeGroupOld missing from volumeGroupNew

			volumeNameSet = make(map[string]struct{})

			for volumeName = range volumeGroupOld.volume {
				_, ok = volumeGroupNew.volume[volumeName]
				if !ok {
					volumeNameSet[volumeName] = struct{}{}
				}
			}

			for volumeName = range volumeNameSet {
				delete(volumeGroupOld.volume, volumeName)
			}

			// Add any Volumes from volumeGroupNew missing from VolumeGroupOld

			for volumeName = range volumeGroupNew.volume {
				_, ok = volumeGroupOld.volume[volumeName]
				if !ok {
					volumeGroupOld.volume[volumeName] = &internalVolumeReportStruct{
						volumeGroup:   volumeGroupOld,
						name:          volumeName,
						state:         StateUnknown,
						lastCheckTime: time.Time{},
					}
				}
			}
		}
	}
}
