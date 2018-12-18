package httpserver

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/transitions"

	// Force importing of the following next "top-most" packages
	_ "github.com/swiftstack/ProxyFS/fuse"
	_ "github.com/swiftstack/ProxyFS/jrpcfs"
	_ "github.com/swiftstack/ProxyFS/statslogger"
)

type ExtentMapElementStruct struct {
	FileOffset    uint64 `json:"file_offset"`
	ContainerName string `json:"container_name"`
	ObjectName    string `json:"object_name"`
	ObjectOffset  uint64 `json:"object_offset"`
	Length        uint64 `json:"length"`
}

type jobState uint8

const (
	jobRunning jobState = iota
	jobHalted
	jobCompleted
)

type jobTypeType uint8

const (
	fsckJobType jobTypeType = iota
	scrubJobType
	limitJobType
)

type jobStruct struct {
	id        uint64
	volume    *volumeStruct
	jobHandle fs.JobHandle
	state     jobState
	startTime time.Time
	endTime   time.Time
}

// JobStatusJSONPackedStruct describes all the possible fields returned in JSON-encoded job GET body
type JobStatusJSONPackedStruct struct {
	StartTime string   `json:"start time"`
	HaltTime  string   `json:"halt time"`
	DoneTime  string   `json:"done time"`
	ErrorList []string `json:"error list"`
	InfoList  []string `json:"info list"`
}

type volumeStruct struct {
	sync.Mutex
	name                   string
	fsMountHandle          fs.MountHandle
	inodeVolumeHandle      inode.VolumeHandle
	headhunterVolumeHandle headhunter.VolumeHandle
	fsckActiveJob          *jobStruct
	fsckJobs               sortedmap.LLRBTree // Key == jobStruct.id, Value == *jobStruct
	scrubActiveJob         *jobStruct
	scrubJobs              sortedmap.LLRBTree // Key == jobStruct.id, Value == *jobStruct
}

type globalsStruct struct {
	sync.Mutex
	active            bool
	jobHistoryMaxSize uint32
	whoAmI            string
	ipAddr            string
	tcpPort           uint16
	ipAddrTCPPort     string
	netListener       net.Listener
	wg                sync.WaitGroup
	confMap           conf.ConfMap
	volumeLLRB        sortedmap.LLRBTree // Key == volumeStruct.name, Value == *volumeStruct
}

var globals globalsStruct

func init() {
	transitions.Register("httpserver", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	globals.volumeLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

	globals.jobHistoryMaxSize, err = confMap.FetchOptionValueUint32("HTTPServer", "JobHistoryMaxSize")
	if nil != err {
		/*
			TODO: Eventually change this to:
				err = fmt.Errorf("confMap.FetchOptionValueString(\"HTTPServer\", \"JobHistoryMaxSize\") failed: %v", err)
				return
		*/
		globals.jobHistoryMaxSize = 5
	}

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v", err)
		return
	}

	globals.ipAddr, err = confMap.FetchOptionValueString("Peer:"+globals.whoAmI, "PrivateIPAddr")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"<whoAmI>\", \"PrivateIPAddr\") failed: %v", err)
		return
	}

	globals.tcpPort, err = confMap.FetchOptionValueUint16("HTTPServer", "TCPPort")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueUint16(\"HTTPServer\", \"TCPPort\") failed: %v", err)
		return
	}

	globals.ipAddrTCPPort = net.JoinHostPort(globals.ipAddr, strconv.Itoa(int(globals.tcpPort)))

	globals.netListener, err = net.Listen("tcp", globals.ipAddrTCPPort)
	if nil != err {
		err = fmt.Errorf("net.Listen(\"tcp\", \"%s\") failed: %v", globals.ipAddrTCPPort, err)
		return
	}

	globals.active = false

	globals.wg.Add(1)
	go serveHTTP()

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
		ok     bool
		volume *volumeStruct
	)

	volume = &volumeStruct{
		name:           volumeName,
		fsckActiveJob:  nil,
		fsckJobs:       sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil),
		scrubActiveJob: nil,
		scrubJobs:      sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil),
	}

	volume.fsMountHandle, err = fs.Mount(volume.name, 0)
	if nil != err {
		return
	}

	volume.inodeVolumeHandle, err = inode.FetchVolumeHandle(volume.name)
	if nil != err {
		return
	}

	volume.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(volume.name)
	if nil != err {
		return
	}

	globals.Lock()

	ok, err = globals.volumeLLRB.Put(volumeName, volume)
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("globals.volumeLLRB.Put(%s,) failed: %v", volumeName, err)
		return
	}
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("globals.volumeLLRB.Put(%s,) returned ok == false", volumeName)
		return
	}

	globals.Unlock()

	return // return err from globals.volumeLLRB.Put() sufficient
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		ok            bool
		volume        *volumeStruct
		volumeAsValue sortedmap.Value
	)

	globals.Lock()

	volumeAsValue, ok, err = globals.volumeLLRB.GetByKey(volumeName)
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("globals.volumeLLRB.Get(%v) failed: %v", volumeName, err)
		return
	}

	volume, ok = volumeAsValue.(*volumeStruct)
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("volumeAsValue.(*volumeStruct) returned ok == false for volume %s", volumeName)
		return
	}

	if !ok {
		globals.Unlock()
		return // return err from globals.volumeLLRB.GetByKey() sufficient
	}

	volume.Lock()
	if nil != volume.fsckActiveJob {
		volume.fsckActiveJob.jobHandle.Cancel()
		volume.fsckActiveJob.state = jobHalted
		volume.fsckActiveJob.endTime = time.Now()
		volume.fsckActiveJob = nil
	}
	if nil != volume.scrubActiveJob {
		volume.scrubActiveJob.jobHandle.Cancel()
		volume.scrubActiveJob.state = jobHalted
		volume.scrubActiveJob.endTime = time.Now()
		volume.scrubActiveJob = nil
	}
	volume.Unlock()

	ok, err = globals.volumeLLRB.DeleteByKey(volumeName)
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("globals.volumeLLRB.DeleteByKey(%v) failed: %v", volumeName, err)
		return
	}

	globals.Unlock()

	return // return err from globals.volumeLLRB.DeleteByKey sufficient
}

func (dummy *globalsStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	globals.confMap = confMap
	globals.active = false
	return nil
}

func (dummy *globalsStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	globals.confMap = confMap
	globals.active = true
	return nil
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	var (
		numVolumes int
	)

	globals.Lock()

	numVolumes, err = globals.volumeLLRB.Len()
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("httpserver.Down() couldn't read globals.volumeLLRB: %v", err)
		return
	}
	if 0 != numVolumes {
		globals.Unlock()
		err = fmt.Errorf("httpserver.Down() called with 0 != globals.volumeLLRB.Len()")
		return
	}

	_ = globals.netListener.Close()

	globals.Unlock()

	globals.wg.Wait()

	err = nil
	return
}
