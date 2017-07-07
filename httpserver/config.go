package httpserver

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/conf"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/logger"
)

type volumeStruct struct {
	sync.Mutex
	name           string
	lastStartTime  *time.Time
	lastStopTime   *time.Time
	lastFinishTime *time.Time
	lastRunErr     error
	fsckRunning    bool
	stopChan       chan bool
	errChan        chan error
}

type globalsStruct struct {
	sync.Mutex
	ipAddr      string
	tcpPort     uint16
	netListener net.Listener
	wg          sync.WaitGroup
	confMap     conf.ConfMap
	volumeLLRB  sortedmap.LLRBTree // Key == volumeStruct.name, Value == *volumeStruct
}

var globals globalsStruct

func Up(confMap conf.ConfMap) (err error) {
	globals.confMap = confMap

	volumeList, err := confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	globals.volumeLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

	for _, volumeName := range volumeList {
		volume := &volumeStruct{
			name:           volumeName,
			lastStartTime:  nil,
			lastStopTime:   nil,
			lastFinishTime: nil,
			lastRunErr:     nil,
			fsckRunning:    false,
			stopChan:       make(chan bool, 1),
			errChan:        make(chan error, 1),
		}
		ok, nonShadowingErr := globals.volumeLLRB.Put(volumeName, volume)
		if nil != nonShadowingErr {
			panic(fmt.Errorf("statsLLRB.Put(%v,) failed: %v", volumeName, nonShadowingErr))
		}
		if !ok {
			panic(fmt.Errorf("statsLLRB.Put(%v,) returned ok == false", volumeName))
		}
	}

	peerName, err := confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	globals.ipAddr, err = confMap.FetchOptionValueString(peerName, "PrivateIPAddr")
	if nil != err {
		return
	}

	globals.tcpPort, err = confMap.FetchOptionValueUint16("HTTPServer", "TCPPort")
	if nil != err {
		return
	}

	globals.netListener, err = net.Listen("tcp", net.JoinHostPort(globals.ipAddr, strconv.Itoa(int(globals.tcpPort))))
	if nil != err {
		logger.PanicfWithError(err, "net.Listen() failed")
	}

	globals.wg.Add(1)
	go serveHTTP()

	err = nil
	return
}

func Down() (err error) {
	globals.netListener.Close()

	globals.wg.Wait()

	err = nil
	return
}
