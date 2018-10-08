package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/version"
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

type httpRequestHandler struct{}

const (
	stateAlive      = "alive"
	stateFragmented = "fragmented"
	stateDead       = "dead"
	stateUnknown    = "unknown"
)

type volumeStruct struct {
	next          *volumeStruct
	volumeGroup   *volumeGroupStruct
	Name          string
	State         string
	LastCheckTime time.Time
}

type volumeGroupStruct struct {
	next          *volumeGroupStruct
	peer          *peerStruct
	Name          string
	headVolume    *volumeStruct // links to first volumeStruct; nil-terminated
	virtualIPAddr string
}

type peerStruct struct {
	Name          string
	publicIPAddr  string
	privateIPAddr string
	ipAddrTCPPort string
}

type globalsStruct struct {
	confMap             conf.ConfMap
	whoAmI              string
	volumeGroupsToWatch []string
	watchInterval       time.Duration
	daemonTCPPort       uint16
	jrpcTCPPort         uint16
	headVolumeGroup     *volumeGroupStruct     // links to first volumeGroupStruct; nil-terminated
	peerMap             map[string]*peerStruct // key == peerStruct.Name
	nextVolumeToWatch   *volumeStruct
	numVolumesToWatch   uint64
	volumesToWatch      []*volumeStruct
	pollingInterval     time.Duration // watchInterval / numVolumesToWatch
	netListener         net.Listener
	childrenWG          sync.WaitGroup
}

var globals globalsStruct

func main() {
	var (
		args                   []string
		err                    error
		ipAddr                 string
		ipAddrTCPPort          string
		ok                     bool
		peer                   *peerStruct
		peerSectionName        string
		primaryPeer            string
		volume                 *volumeStruct
		volumeGroup            *volumeGroupStruct
		volumeGroupName        string
		volumeGroupSectionName string
		volumeList             []string
		volumeName             string
	)

	// Parse arguments

	args = os.Args[1:]

	// Check that os.Args[1] was supplied... it might be a .conf or an option list (followed by a .conf)
	if 0 == len(args) {
		log.Fatalf("No .conf file specified")
	}

	globals.confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Update confMap with any extra os.Args supplied
	err = globals.confMap.UpdateFromStrings(args[1:])
	if nil != err {
		log.Fatalf("Failed to load config overrides: %v", err)
	}

	// Process resultant confMap

	globals.whoAmI, err = globals.confMap.FetchOptionValueString("AliveDaemon", "WhoAmI")
	if nil != err {
		log.Fatal(err)
	}

	globals.volumeGroupsToWatch, err = globals.confMap.FetchOptionValueStringSlice("AliveDaemon", "VolumeGroupsToWatch")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == len(globals.volumeGroupsToWatch) {
		log.Fatalf("No VolumeGroups to watch")
	}

	globals.watchInterval, err = globals.confMap.FetchOptionValueDuration("AliveDaemon", "WatchInterval")
	if nil != err {
		log.Fatal(err)
	}

	globals.daemonTCPPort, err = globals.confMap.FetchOptionValueUint16("AliveDaemon", "TCPPort")
	if nil != err {
		log.Fatal(err)
	}

	globals.jrpcTCPPort, err = globals.confMap.FetchOptionValueUint16("JSONRPCServer", "TCPPort")
	if nil != err {
		log.Fatal(err)
	}

	globals.headVolumeGroup = nil
	globals.peerMap = make(map[string]*peerStruct)
	globals.nextVolumeToWatch = nil
	globals.numVolumesToWatch = uint64(0)

	for _, volumeGroupName = range globals.volumeGroupsToWatch {
		volumeGroup = &volumeGroupStruct{
			next:       globals.headVolumeGroup,
			Name:       volumeGroupName,
			headVolume: nil,
		}

		volumeGroupSectionName = "VolumeGroup:" + volumeGroup.Name

		volumeList, err = globals.confMap.FetchOptionValueStringSlice(volumeGroupSectionName, "VolumeList")
		if nil != err {
			log.Fatal(err)
		}
		if 0 == len(volumeList) {
			log.Fatalf("No Volumes in VolumeGroup \"%s\" to watch", volumeGroup.Name)
		}

		for _, volumeName = range volumeList {
			volume = &volumeStruct{
				next:          volumeGroup.headVolume,
				volumeGroup:   volumeGroup,
				Name:          volumeName,
				State:         stateUnknown,
				LastCheckTime: time.Now(),
			}

			volumeGroup.headVolume = volume
			globals.nextVolumeToWatch = volume
			globals.numVolumesToWatch++
		}

		volumeGroup.virtualIPAddr, err = globals.confMap.FetchOptionValueString(volumeGroupSectionName, "VirtualIPAddr")
		if nil != err {
			if nil == globals.confMap.VerifyOptionValueIsEmpty(volumeGroupSectionName, "VirtualIPAddr") {
				volumeGroup.virtualIPAddr = ""
			} else {
				log.Fatal(err)
			}
		}

		primaryPeer, err = globals.confMap.FetchOptionValueString(volumeGroupSectionName, "PrimaryPeer")
		if nil != err {
			log.Fatal(err)
		}

		volumeGroup.peer, ok = globals.peerMap[primaryPeer]

		if !ok {
			peer = &peerStruct{
				Name: primaryPeer,
			}

			peerSectionName = "Peer:" + peer.Name

			peer.publicIPAddr, err = globals.confMap.FetchOptionValueString(peerSectionName, "PublicIPAddr")
			if nil != err {
				log.Fatal(err)
			}
			peer.privateIPAddr, err = globals.confMap.FetchOptionValueString(peerSectionName, "PrivateIPAddr")
			if nil != err {
				log.Fatal(err)
			}

			peer.ipAddrTCPPort = net.JoinHostPort(peer.privateIPAddr, strconv.Itoa(int(globals.jrpcTCPPort)))

			globals.peerMap[peer.Name] = peer

			volumeGroup.peer = peer
		}

		globals.headVolumeGroup = volumeGroup
	}

	globals.volumesToWatch = make([]*volumeStruct, 0, globals.numVolumesToWatch)

	volumeGroup = globals.headVolumeGroup

	for nil != volumeGroup {
		volume = volumeGroup.headVolume

		for nil != volume {
			globals.volumesToWatch = append(globals.volumesToWatch, volume)

			volume = volume.next
		}

		volumeGroup = volumeGroup.next
	}

	globals.pollingInterval = globals.watchInterval / time.Duration(globals.numVolumesToWatch)

	ipAddr, err = globals.confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PrivateIPAddr")
	if nil != err {
		log.Fatal(err)
	}

	ipAddrTCPPort = net.JoinHostPort(ipAddr, strconv.Itoa(int(globals.daemonTCPPort)))

	globals.netListener, err = net.Listen("tcp", ipAddrTCPPort)
	if nil != err {
		log.Fatal(err)
	}

	globals.childrenWG.Add(2)

	go doPolling()
	go serveHTTP()

	globals.childrenWG.Wait()
}

func doPolling() {
	var (
		err          error
		nextID       uint64
		pingReply    pingReplyStruct
		pingReplyBuf []byte
		pingReplyLen int
		pingReq      pingReqStruct
		pingReqBuf   []byte
		tcpAddr      *net.TCPAddr
		tcpConn      *net.TCPConn
		timeNow      time.Time
		volume       *volumeStruct
	)

	defer globals.childrenWG.Done()

	nextID = 0

	for {
		time.Sleep(globals.pollingInterval)
		volume = globals.nextVolumeToWatch
		timeNow = time.Now()
		pingReq.JSONrpc = "2.0"
		pingReq.Method = "Server.RpcPing"
		pingReq.Params[0].Message = "Ping at " + timeNow.Format(time.RFC3339)
		pingReq.ID = nextID
		pingReqBuf, err = json.Marshal(pingReq)
		if nil == err {
			tcpAddr, err = net.ResolveTCPAddr("tcp", volume.volumeGroup.peer.ipAddrTCPPort)
			if nil == err {
				tcpConn, err = net.DialTCP("tcp", nil, tcpAddr)
				if nil == err {
					_, err = tcpConn.Write(pingReqBuf)
					if nil == err {
						pingReplyBuf = make([]byte, maxRPCReplySize)
						pingReplyLen, err = tcpConn.Read(pingReplyBuf)
						if nil == err {
							err = tcpConn.Close()
							if nil == err {
								pingReplyBuf = pingReplyBuf[:pingReplyLen]
								err = json.Unmarshal(pingReplyBuf, &pingReply)
								if nil == err {
									volume.State = stateAlive
								} else {
									volume.State = stateDead
								}
							} else {
								volume.State = stateDead
							}
						} else {
							_ = tcpConn.Close()
							volume.State = stateDead
						}
					} else {
						_ = tcpConn.Close()
						volume.State = stateDead
					}
				} else {
					volume.State = stateDead
				}
			} else {
				volume.State = stateDead
			}
		} else {
			volume.State = stateDead
		}
		volume.LastCheckTime = timeNow
		if nil == volume.next {
			if nil == volume.volumeGroup.next {
				globals.nextVolumeToWatch = globals.headVolumeGroup.headVolume
			} else {
				globals.nextVolumeToWatch = volume.volumeGroup.next.headVolume
			}
		} else {
			globals.nextVolumeToWatch = volume.next
		}
		nextID++
	}
}

func serveHTTP() {
	var (
		err error
	)

	defer globals.childrenWG.Done()

	err = http.Serve(globals.netListener, httpRequestHandler{})
	if nil != err {
		log.Fatal(err)
	}
}

func (h httpRequestHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		doGet(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func doGet(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		path string
	)

	path = strings.TrimRight(request.URL.Path, "/")

	switch {
	case "" == path:
		doGetOfIndexDotHTML(responseWriter, request)
	case "/config" == path:
		doGetOfConfig(responseWriter, request)
	case "/index.html" == path:
		doGetOfIndexDotHTML(responseWriter, request)
	case "/status" == path:
		doGetOfStatus(responseWriter, request)
	case "/version" == path:
		doGetOfVersion(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doGetOfIndexDotHTML(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/html")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write([]byte(indexDotHTML))
}

func doGetOfConfig(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		confMapJSON       bytes.Buffer
		confMapJSONPacked []byte
		ok                bool
		paramList         []string
		sendPackedConfig  bool
	)

	paramList, ok = request.URL.Query()["compact"]
	if ok {
		if 0 == len(paramList) {
			sendPackedConfig = false
		} else {
			sendPackedConfig = !((paramList[0] == "") || (paramList[0] == "0") || (paramList[0] == "false"))
		}
	} else {
		sendPackedConfig = false
	}

	confMapJSONPacked, _ = json.Marshal(globals.confMap)

	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)

	if sendPackedConfig {
		_, _ = responseWriter.Write(confMapJSONPacked)
	} else {
		json.Indent(&confMapJSON, confMapJSONPacked, "", "\t")
		_, _ = responseWriter.Write(confMapJSON.Bytes())
		_, _ = responseWriter.Write([]byte("\n"))
	}
}

func doGetOfStatus(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		ok               bool
		paramList        []string
		sendPackedStatus bool
		statusJSON       bytes.Buffer
		statusJSONPacked []byte
	)

	paramList, ok = request.URL.Query()["compact"]
	if ok {
		if 0 == len(paramList) {
			sendPackedStatus = false
		} else {
			sendPackedStatus = !((paramList[0] == "") || (paramList[0] == "0") || (paramList[0] == "false"))
		}
	} else {
		sendPackedStatus = false
	}

	statusJSONPacked, _ = json.Marshal(globals.volumesToWatch)

	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)

	if sendPackedStatus {
		_, _ = responseWriter.Write(statusJSONPacked)
	} else {
		json.Indent(&statusJSON, statusJSONPacked, "", "\t")
		_, _ = responseWriter.Write(statusJSON.Bytes())
		_, _ = responseWriter.Write([]byte("\n"))
	}
}

func doGetOfVersion(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write([]byte(version.ProxyFSVersion))
}
