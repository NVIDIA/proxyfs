package liveness

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"reflect"
	"regexp"
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

type ringFilePayloadJSONDevStruct struct {
	IP   string `json:"ip"`
	Port uint16 `json:"port"`
}

type ringFilePayloadJSONStruct struct {
	Devs []*ringFilePayloadJSONDevStruct `json:"devs"`
}

type reconDevReportStruct struct {
	Size int64 `json:"size"`
	Used int64 `json:"used"`
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
		reconEndpoint                   *internalReconEndpointReportStruct
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
				_ = checkEntityList.PushBack(servingPeer)

				for _, volumeGroup = range servingPeer.volumeGroup {
					_ = checkEntityList.PushBack(volumeGroup)

					for _, volume = range volumeGroup.volume {
						_ = checkEntityList.PushBack(volume)
					}
				}
			}

			for _, reconEndpoint = range myObservingPeerReport.reconEndpoint {
				_ = checkEntityList.PushBack(reconEndpoint)
			}

			utils.RandomizeList(checkEntityList)

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
				case reflect.TypeOf(reconEndpoint):
					livenessCheckReconEndpoint(entityToCheck.Value.(*internalReconEndpointReportStruct))
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

	err = tcpConn.SetDeadline(time.Now().Add(globals.maxRequestDuration))
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

func livenessCheckReconEndpoint(reconEndpoint *internalReconEndpointReportStruct) {
	var (
		bigDividend         *big.Int
		bigDivisor          *big.Int
		bigQuotient         *big.Int
		bigRemainder        *big.Int
		devUtilization      uint8
		err                 error
		quotient            int64
		reconDevReport      *reconDevReportStruct
		reconDevReportSlice []*reconDevReportStruct
		reconResp           *http.Response
		reconRespBody       []byte
		remainder           int64
		url                 string
	)

	reconEndpoint.maxDiskUsagePercentage = 0

	url = fmt.Sprintf("http://%s/recon/diskusage", reconEndpoint.ipAddrPort)

	reconResp, err = http.Get(url)
	if nil == err {
		reconRespBody, err = ioutil.ReadAll(reconResp.Body)
		if nil == err {
			if http.StatusOK == reconResp.StatusCode {
				reconDevReportSlice = make([]*reconDevReportStruct, 0)
				err = json.Unmarshal(reconRespBody, &reconDevReportSlice)
				if nil == err {
					for _, reconDevReport = range reconDevReportSlice {
						if (reconDevReport.Used > 0) && (reconDevReport.Size > 0) && (reconDevReport.Used <= reconDevReport.Size) {
							bigDividend = new(big.Int).Mul(big.NewInt(100), big.NewInt(reconDevReport.Used))
							bigDivisor = big.NewInt(reconDevReport.Size)
							bigQuotient = new(big.Int).Quo(bigDividend, bigDivisor)
							bigRemainder = new(big.Int).Rem(bigDividend, bigDivisor)
							quotient = bigQuotient.Int64()
							remainder = bigRemainder.Int64()
							if 0 == remainder {
								devUtilization = uint8(quotient)
							} else {
								devUtilization = uint8(quotient) + 1
							}
							if devUtilization > reconEndpoint.maxDiskUsagePercentage {
								reconEndpoint.maxDiskUsagePercentage = devUtilization
							}
						} else {
							logger.Warnf("livenessCheckReconEndpoint() GET to %s got responseBody with unreasonable used and size values", url)
						}
					}
				} else {
					logger.WarnfWithError(err, "livenessCheckReconEndpoint() GET to %s got response.Body with invalid JSON", url)
				}
			} else {
				logger.WarnfWithError(err, "livenessCheckReconEndpoint() GET to %s got bad status: %s", url, reconResp.Status)
			}
		} else {
			logger.WarnfWithError(err, "livenessCheckReconEndpoint() GET to %s response.Body() read failed", url)
		}
		err = reconResp.Body.Close()
		if nil != err {
			logger.WarnfWithError(err, "livenessCheckReconEndpoint() GET to %s response.Body.Close() failed", url)
		}
	} else {
		logger.WarnfWithError(err, "livenessCheckReconEndpoint() failed to issue GET to %s", url)
	}
}

// computeLivenessCheckAssignments takes a list of ObservingPeer and produces a
// template internalLivenessReport that is to be filled in by this collection of peers.
// While the elements of the resultant internalLivenessReport have State, LastCheckTime,
// and MaxDiskUsagePercentage fields, these are ignored as they will ultimately be filled
// in by each ObservingPeer. The livenessCheckRedundancy is used to ensure that each
// ServingPeer, VolumeGroup, Volume, and ReconEndpoint is adequately covered. As every
// Volume is part of a VolumeGroup and every VolumeGroup is assigned to a single ServingPeer,
// this amounts to just dolling out the Volumes to ObervingPeers with the required
// livenessCheckRedundancy. Similarly, the ReconEndpoints are dolled out with this
// same livenessCheckRedundancy.
//
// It is a bit misleading for an ObservingPeer to report that a VolumeGroup is "alive"
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
		alreadyInSwiftReconEndpointIAddrSet   bool
		curSwiftConfFileMap                   map[string]time.Time
		effectiveLivenessCheckRedundancy      uint64
		effectiveLivenessCheckRedundancyIndex uint64
		err                                   error
		fileInfo                              os.FileInfo
		fileInfoSlice                         []os.FileInfo
		fileInfoModTime                       time.Time
		fileInfoName                          string
		inSwiftConfFileMap                    bool
		internalObservingPeerReport           *internalObservingPeerReportStruct
		internalReconEndpointReport           *internalReconEndpointReportStruct
		internalServingPeerReport             *internalServingPeerReportStruct
		internalVolumeGroupReport             *internalVolumeGroupReportStruct
		internalVolumeReport                  *internalVolumeReportStruct
		matchedRingFilename                   bool
		needToUpdateSwiftConfFileMap          bool
		notYetAdded                           bool
		observingPeerIndex                    uint64
		observingPeerName                     string
		ok                                    bool
		prevFileInfoModTime                   time.Time
		ringFileData                          []byte
		ringFileName                          string
		ringFileMagic                         []byte
		ringFilePayload                       []byte
		ringFilePayloadJSON                   *ringFilePayloadJSONStruct
		ringFilePayloadJSONDev                *ringFilePayloadJSONDevStruct
		ringFilePayloadLen                    int32
		ringFileReader                        *gzip.Reader
		ringFileReadLen                       int
		ringFileVersion                       uint16
		servingPeer                           *peerStruct
		servingPeerName                       string
		swiftReconEndpoint                    string
		swiftReconEndpointIPAddrSet           map[string]struct{}
		volumeGroup                           *volumeGroupStruct
		volumeGroupName                       string
		volumeName                            string
		volumeToCheck                         *volumeStruct
	)

	if 0 == len(observingPeerNameList) {
		err = fmt.Errorf("computeLivenessCheckAssignments(): len(observingPeerNameList) cannot be zero")
		panic(err)
	}

	// Determine reconEndpoints

	if 0 == globals.swiftReconChecksPerConfCheck {
		globals.swiftReconEndpointSet = make(map[string]struct{})
	} else {
		if 0 == globals.swiftReconChecksUntilConfCheck {
			// Time to potentially refresh globals.swiftConfFileMap & globals.swiftReconEndpointSet

			globals.swiftReconChecksUntilConfCheck = globals.swiftReconChecksPerConfCheck

			fileInfoSlice, err = ioutil.ReadDir(globals.swiftConfDir)
			if nil != err {
				logger.FatalfWithError(err, "Unable to read [SwiftClient]SwiftConfDir (%s)", globals.swiftConfDir)
			}

			curSwiftConfFileMap = make(map[string]time.Time)

			for _, fileInfo = range fileInfoSlice {
				fileInfoName = fileInfo.Name()
				switch fileInfoName {
				case "account.ring.gz":
					matchedRingFilename = true
				case "container.ring.gz":
					matchedRingFilename = true
				default:
					matchedRingFilename, err = regexp.MatchString("^object.*\\.ring\\.gz$", fileInfoName)
					if nil != err {
						logger.FatalfWithError(err, "Unexpected failure calling regexp.MatchString()")
					}
				}

				if matchedRingFilename {
					curSwiftConfFileMap[fileInfoName] = fileInfo.ModTime()
				}
			}

			if len(globals.swiftConfFileMap) != len(curSwiftConfFileMap) {
				needToUpdateSwiftConfFileMap = true
			} else {
				needToUpdateSwiftConfFileMap = false
				for fileInfoName, fileInfoModTime = range curSwiftConfFileMap {
					prevFileInfoModTime, inSwiftConfFileMap = globals.swiftConfFileMap[fileInfoName]
					if !inSwiftConfFileMap || (fileInfoModTime != prevFileInfoModTime) {
						needToUpdateSwiftConfFileMap = true
					}
				}
			}

			if needToUpdateSwiftConfFileMap {
				// We must refresh globals.swiftConfFileMap & globals.swiftReconEndpointSet

				globals.swiftConfFileMap = curSwiftConfFileMap

				swiftReconEndpointIPAddrSet = make(map[string]struct{})
				globals.swiftReconEndpointSet = make(map[string]struct{})

				for ringFileName = range globals.swiftConfFileMap {
					ringFileData, err = ioutil.ReadFile(globals.swiftConfDir + "/" + ringFileName)
					if nil == err {
						ringFileReader, err = gzip.NewReader(bytes.NewReader(ringFileData))
						if nil == err {
							ringFileMagic = make([]byte, 4)
							ringFileReadLen, err = ringFileReader.Read(ringFileMagic)
							if nil == err {
								if ringFileReadLen == len(ringFileMagic) {
									if bytes.Equal([]byte("R1NG"), ringFileMagic) {
										err = binary.Read(ringFileReader, binary.BigEndian, &ringFileVersion)
										if nil == err {
											if 1 == ringFileVersion {
												err = binary.Read(ringFileReader, binary.BigEndian, &ringFilePayloadLen)
												if nil == err {
													ringFilePayload = make([]byte, ringFilePayloadLen)
													ringFileReadLen, err = ringFileReader.Read(ringFilePayload)
													if nil == err {
														if ringFileReadLen == len(ringFilePayload) {
															ringFilePayloadJSON = &ringFilePayloadJSONStruct{}
															err = json.Unmarshal(ringFilePayload, ringFilePayloadJSON)
															if nil == err {
																for _, ringFilePayloadJSONDev = range ringFilePayloadJSON.Devs {
																	_, alreadyInSwiftReconEndpointIAddrSet = swiftReconEndpointIPAddrSet[ringFilePayloadJSONDev.IP]
																	if !alreadyInSwiftReconEndpointIAddrSet {
																		swiftReconEndpointIPAddrSet[ringFilePayloadJSONDev.IP] = struct{}{}
																		swiftReconEndpoint = fmt.Sprintf("%s:%d", ringFilePayloadJSONDev.IP, ringFilePayloadJSONDev.Port)
																		globals.swiftReconEndpointSet[swiftReconEndpoint] = struct{}{}
																	}
																}
															} else {
																logger.WarnfWithError(err, "Unable to json.Unmarshal ringFilePayload from ring file %s", fileInfoName)
															}
														} else {
															logger.Warnf("Misread of ringFilePayload from ring file %s", fileInfoName)
														}
													} else {
														logger.WarnfWithError(err, "Unable to read ringFilePayload from ring file %s", fileInfoName)
													}
												} else {
													logger.WarnfWithError(err, "Unable to read ringFilePayloadLen from ring file %s", fileInfoName)
												}
											} else {
												logger.Warnf("Value of ringFileVersion unexpected from ring file %s", fileInfoName)
											}
										} else {
											logger.WarnfWithError(err, "Unable to read ringFileVersion from ring file %s", fileInfoName)
										}
									} else {
										logger.Warnf("Value of ringFileMagic unexpected from ring file %s", fileInfoName)
									}
								} else {
									logger.Warnf("Misread of ringFileMagic from ring file %s", fileInfoName)
								}
							} else {
								logger.WarnfWithError(err, "Unable to read ringFileMagic from ring file %s", fileInfoName)
							}
							err = ringFileReader.Close()
							if nil != err {
								logger.WarnfWithError(err, "Unable to close gzip.Reader from ring file %s", fileInfoName)
							}
						} else {
							logger.WarnfWithError(err, "Unable to create gzip.Reader from ring file %s", fileInfoName)
						}
					} else {
						logger.WarnfWithError(err, "Unable to read ring file %s", fileInfoName)
					}
				}
			}
		} else {
			globals.swiftReconChecksUntilConfCheck--
		}
	}

	// Prepare fresh internalLivenessReport

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
						name:          observingPeerName,
						servingPeer:   make(map[string]*internalServingPeerReportStruct),
						reconEndpoint: make(map[string]*internalReconEndpointReportStruct),
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
						name:          observingPeerName,
						servingPeer:   make(map[string]*internalServingPeerReportStruct),
						reconEndpoint: make(map[string]*internalReconEndpointReportStruct),
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
						name:          observingPeerName,
						servingPeer:   make(map[string]*internalServingPeerReportStruct),
						reconEndpoint: make(map[string]*internalReconEndpointReportStruct),
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

	// Iterate through observingPeerNameList effectiveLivenessCheckRedundancy times scheduling ReconEndpoints

	for effectiveLivenessCheckRedundancyIndex = 0; effectiveLivenessCheckRedundancyIndex < effectiveLivenessCheckRedundancy; effectiveLivenessCheckRedundancyIndex++ {
		for swiftReconEndpoint = range globals.swiftReconEndpointSet {
			// Add volumeToCheck to currently indexed ObservingPeer

			notYetAdded = true // Avoid duplicate assignments

			for notYetAdded {
				observingPeerName = observingPeerNameList[observingPeerIndex]

				internalObservingPeerReport, ok = internalLivenessReport.observingPeer[observingPeerName]
				if !ok {
					internalObservingPeerReport = &internalObservingPeerReportStruct{
						name:          observingPeerName,
						servingPeer:   make(map[string]*internalServingPeerReportStruct),
						reconEndpoint: make(map[string]*internalReconEndpointReportStruct),
					}
					internalLivenessReport.observingPeer[observingPeerName] = internalObservingPeerReport
				}

				_, ok = internalObservingPeerReport.reconEndpoint[swiftReconEndpoint]

				if ok {
					// Need to step to the next ObservingPeer because this one is already watching this ReconEndpoint
				} else {
					// New ReconEndpoint for this ObservingPeer... so add it

					internalReconEndpointReport = &internalReconEndpointReportStruct{
						observingPeer:          internalObservingPeerReport,
						ipAddrPort:             swiftReconEndpoint,
						maxDiskUsagePercentage: 0,
					}

					internalObservingPeerReport.reconEndpoint[swiftReconEndpoint] = internalReconEndpointReport

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
		ok                         bool
		reconEndpointIPAddrPort    string
		reconEndpointIPAddrPortSet map[string]struct{}
		servingPeerName            string
		servingPeerNameSet         map[string]struct{}
		servingPeerNew             *internalServingPeerReportStruct
		servingPeerOld             *internalServingPeerReportStruct
		volumeGroupName            string
		volumeGroupNameSet         map[string]struct{}
		volumeGroupNew             *internalVolumeGroupReportStruct
		volumeGroupOld             *internalVolumeGroupReportStruct
		volumeName                 string
		volumeNameSet              map[string]struct{}
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

	// Remove any ReconEndpoints from globals.myObservingPeerReport missing from internalObservingPeerReport

	reconEndpointIPAddrPortSet = make(map[string]struct{})

	for reconEndpointIPAddrPort = range globals.myObservingPeerReport.reconEndpoint {
		_, ok = internalObservingPeerReport.reconEndpoint[reconEndpointIPAddrPort]
		if !ok {
			reconEndpointIPAddrPortSet[reconEndpointIPAddrPort] = struct{}{}
		}
	}

	for reconEndpointIPAddrPort = range reconEndpointIPAddrPortSet {
		delete(globals.myObservingPeerReport.reconEndpoint, reconEndpointIPAddrPort)
	}

	// Add any ReconEndpoints from internalObservingPeerReport missing from globals.myObservingPeerReport

	for reconEndpointIPAddrPort = range internalObservingPeerReport.reconEndpoint {
		_, ok = globals.myObservingPeerReport.reconEndpoint[reconEndpointIPAddrPort]
		if !ok {
			globals.myObservingPeerReport.reconEndpoint[reconEndpointIPAddrPort] = &internalReconEndpointReportStruct{
				observingPeer:          globals.myObservingPeerReport,
				ipAddrPort:             reconEndpointIPAddrPort,
				maxDiskUsagePercentage: 0,
			}
		}
	}
}
