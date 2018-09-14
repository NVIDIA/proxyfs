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
	volumeStateAlive   = "alive"
	volumeStateDead    = "dead"
	volumeStateUnknown = "unknown"
)

type volumeStruct struct {
	peer          *peerStruct
	next          *volumeStruct // circular linked list currently pointed to by globalsStruct.volumesToWatch
	Name          string
	State         string
	LastCheckTime time.Time
}

type peerStruct struct {
	Name           string
	PublicIPAddr   string
	PrivateIPAddr  string
	ipAddrTCPPort  string
	VolumesToWatch map[string]*volumeStruct // key == volumeStruct.name
}

type globalsStruct struct {
	confMap           conf.ConfMap
	whoAmI            string
	peersToWatch      map[string]*peerStruct // key == peerStruct.name
	volumesToWatch    *volumeStruct          // links to next volumeStruct to examine
	volumesToWatchLen time.Duration          // using this type makes pollingInterval computation avoid casting
	pollingInterval   time.Duration          // volumesToWatchLen / AliveDaemon.WatchInterval
	netListener       net.Listener
	childrenWG        sync.WaitGroup
}

var globals globalsStruct

func main() {
	var (
		args          []string
		err           error
		ipAddr        string
		ipAddrTCPPort string
		ok            bool
		peer          *peerStruct
		peerName      string
		peersToWatch  []string
		tcpPort       uint16
		volume        *volumeStruct
		volumeList    []string
		volumeName    string
		watchInterval time.Duration
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

	peersToWatch, err = globals.confMap.FetchOptionValueStringSlice("AliveDaemon", "PeersToWatch")
	if nil != err {
		log.Fatal(err)
	}

	tcpPort, err = globals.confMap.FetchOptionValueUint16("JSONRPCServer", "TCPPort")
	if nil != err {
		log.Fatal(err)
	}

	globals.peersToWatch = make(map[string]*peerStruct)

	for _, peerName = range peersToWatch {
		peer = &peerStruct{
			Name:           peerName,
			VolumesToWatch: make(map[string]*volumeStruct),
		}
		peer.PublicIPAddr, err = globals.confMap.FetchOptionValueString("Peer:"+peerName, "PublicIPAddr")
		if nil != err {
			log.Fatal(err)
		}
		peer.PrivateIPAddr, err = globals.confMap.FetchOptionValueString("Peer:"+peerName, "PrivateIPAddr")
		if nil != err {
			log.Fatal(err)
		}
		peer.ipAddrTCPPort = net.JoinHostPort(peer.PrivateIPAddr, strconv.Itoa(int(tcpPort)))
		globals.peersToWatch[peer.Name] = peer
	}

	volumeList, err = globals.confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		log.Fatal(err)
	}

	globals.volumesToWatch = nil
	globals.volumesToWatchLen = time.Duration(0)

	for _, volumeName = range volumeList {
		peerName, err = globals.confMap.FetchOptionValueString("Volume:"+volumeName, "PrimaryPeer")
		if nil != err {
			log.Fatal(err)
		}
		peer, ok = globals.peersToWatch[peerName]
		if ok {
			volume = &volumeStruct{
				peer:          peer,
				next:          globals.volumesToWatch,
				Name:          volumeName,
				State:         volumeStateUnknown,
				LastCheckTime: time.Now(),
			}
			if 0 == globals.volumesToWatchLen {
				volume.next = volume
			} else {
				globals.volumesToWatch.next = volume
			}
			globals.volumesToWatch = volume
			globals.volumesToWatchLen++
			peer.VolumesToWatch[volume.Name] = volume
		}
	}

	if 0 == globals.volumesToWatchLen {
		log.Fatalf("No volumes to watch")
	}

	watchInterval, err = globals.confMap.FetchOptionValueDuration("AliveDaemon", "WatchInterval")
	if nil != err {
		log.Fatal(err)
	}

	globals.pollingInterval = watchInterval / globals.volumesToWatchLen

	tcpPort, err = globals.confMap.FetchOptionValueUint16("AliveDaemon", "TCPPort")
	if nil != err {
		log.Fatal(err)
	}

	ipAddr, err = globals.confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PrivateIPAddr")
	if nil != err {
		log.Fatal(err)
	}

	ipAddrTCPPort = net.JoinHostPort(ipAddr, strconv.Itoa(int(tcpPort)))

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
		volume = globals.volumesToWatch
		timeNow = time.Now()
		pingReq.JSONrpc = "2.0"
		pingReq.Method = "Server.RpcPing"
		pingReq.Params[0].Message = "Ping at " + timeNow.Format(time.RFC3339)
		pingReq.ID = nextID
		pingReqBuf, err = json.Marshal(pingReq)
		if nil == err {
			tcpAddr, err = net.ResolveTCPAddr("tcp", volume.peer.ipAddrTCPPort)
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
									volume.State = volumeStateAlive
								} else {
									volume.State = volumeStateDead
								}
							} else {
								volume.State = volumeStateDead
							}
						} else {
							_ = tcpConn.Close()
							volume.State = volumeStateDead
						}
					} else {
						_ = tcpConn.Close()
						volume.State = volumeStateDead
					}
				} else {
					volume.State = volumeStateDead
				}
			} else {
				volume.State = volumeStateDead
			}
		} else {
			volume.State = volumeStateDead
		}
		volume.LastCheckTime = timeNow
		globals.volumesToWatch = volume.next
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

	statusJSONPacked, _ = json.Marshal(globals.peersToWatch)

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
