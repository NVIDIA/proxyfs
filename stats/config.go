package stats

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
)

const (
	expectedNumberOfDistinctStatNames = 100
)

type statStruct struct {
	name      *string
	increment uint64
}

type statNameLinkStruct struct {
	name string
	next *statNameLinkStruct
}

type globalsStruct struct {
	sync.Mutex       //                     Used only for snapshotting statFullMap
	ipAddr           string
	udpPort          uint16
	tcpPort          uint16
	useUDP           bool   //              Logically useTCP == !useUDP
	connectionType   string //              Either "udp" or "tcp"
	udpLAddr         *net.UDPAddr
	udpRAddr         *net.UDPAddr
	tcpLAddr         *net.TCPAddr
	tcpRAddr         *net.TCPAddr
	bufferLength     uint16
	maxLatency       time.Duration //       Timer should pop in maxLatency/statTree.Len()
	statChan         chan *statStruct
	tickChan         <-chan time.Time
	stopChan         chan bool
	doneChan         chan bool
	statDeltaMap     map[string]uint64 //   Key is stat.name, Value is the sum of all un-sent/accumulated stat.increment's
	statFullMap      map[string]uint64 //   Key is stat.name, Value is the sum of all accumulated stat.increment's
	headStatNameLink *statNameLinkStruct
	tailStatNameLink *statNameLinkStruct
}

var globals globalsStruct

// Up initializes the package and must successfully return before any API functions are invoked
func Up(confMap conf.ConfMap) (err error) {
	var (
		errFetchingTCPPort error
		errFetchingUDPPort error
	)

	globals.ipAddr = "localhost" // Hard-coded since we only want to talk to the local StatsD

	globals.udpPort, errFetchingUDPPort = confMap.FetchOptionValueUint16("Stats", "UDPPort")
	globals.tcpPort, errFetchingTCPPort = confMap.FetchOptionValueUint16("Stats", "TCPPort")

	if (nil != errFetchingUDPPort) && (nil != errFetchingTCPPort) {
		err = fmt.Errorf("confMap.FetchOptionValueUint16(\"Stats\", \"UDPPort\") failed: %v AND confMap.FetchOptionValueUint16(\"Stats\", \"TCPPort\") failed: %v", errFetchingTCPPort, errFetchingUDPPort)
		return
	}

	if (nil == errFetchingUDPPort) && (nil == errFetchingTCPPort) {
		err = fmt.Errorf("Only one of [Stats]UDPPort and [Stats]TCPPort may be specified")
		return
	}

	globals.useUDP = (nil == errFetchingUDPPort)

	if globals.useUDP {
		globals.udpLAddr, err = net.ResolveUDPAddr("udp", globals.ipAddr+":0")
		if nil != err {
			return
		}
		globals.udpRAddr, err = net.ResolveUDPAddr("udp", globals.ipAddr+":"+strconv.FormatUint(uint64(globals.udpPort), 10))
		if nil != err {
			return
		}
	} else { // globals.useTCP
		globals.tcpLAddr, err = net.ResolveTCPAddr("tcp", globals.ipAddr+":0")
		if nil != err {
			return
		}
		globals.tcpRAddr, err = net.ResolveTCPAddr("tcp", globals.ipAddr+":"+strconv.FormatUint(uint64(globals.tcpPort), 10))
		if nil != err {
			return
		}
	}

	globals.bufferLength, err = confMap.FetchOptionValueUint16("Stats", "BufferLength")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueUint16(\"Stats\", \"BufferLength\") failed: %v", err)
		return
	}

	globals.maxLatency, err = confMap.FetchOptionValueDuration("Stats", "MaxLatency")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueUint16(\"Stats\", \"MaxLatency\") failed: %v", err)
		return
	}

	globals.statChan = make(chan *statStruct, globals.bufferLength)
	globals.stopChan = make(chan bool, 1)
	globals.doneChan = make(chan bool, 1)

	globals.statDeltaMap = make(map[string]uint64, expectedNumberOfDistinctStatNames)
	globals.headStatNameLink = nil
	globals.tailStatNameLink = nil

	globals.statFullMap = make(map[string]uint64, expectedNumberOfDistinctStatNames)

	// Start the ticker
	var timeoutDuration time.Duration
	if expectedNumberOfDistinctStatNames > 0 {
		timeoutDuration = globals.maxLatency
	}
	// else our ticker is disabled
	globals.tickChan = time.Tick(timeoutDuration)

	go sender()

	err = nil
	return
}

// PauseAndContract pauses the stats package and applies any removals from the supplied confMap
func PauseAndContract(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

// ExpandAndResume applies any additions from the supplied confMap and resumes the stats package
func ExpandAndResume(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

// Down terminates statsd logging and should only be called once no API functions are active or subsequently invoked
func Down() (err error) {
	globals.statChan = nil

	globals.stopChan <- true

	_ = <-globals.doneChan

	err = nil

	return
}
