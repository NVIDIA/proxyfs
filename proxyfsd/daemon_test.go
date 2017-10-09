package proxyfsd

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/ramswift"
)

func TestMain(m *testing.M) {
	mRunReturn := m.Run()
	os.Exit(mRunReturn)
}

func TestDaemon(t *testing.T) {
	var (
		bytesWritten                 uint64
		confMapStrings               []string
		createdFileInodeNumber       inode.InodeNumber
		err                          error
		errChan                      chan error
		mountHandle                  fs.MountHandle
		proxyfsdSignalHandlerIsArmed bool
		ramswiftDoneChan             chan bool
		ramswiftSignalHandlerIsArmed bool
		readData                     []byte
		testVersion                  uint64
		testVersionConfFile          *os.File
		testVersionConfFileName      string
		toReadFileInodeNumber        inode.InodeNumber
		wg                           sync.WaitGroup
	)

	// Setup a ramswift instance leveraging test config

	ramswiftSignalHandlerIsArmed = false
	ramswiftDoneChan = make(chan bool, 1)

	confMapStrings = []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",

		"StatsLogger.Period=10m",

		"Logging.LogFilePath=",

		"Peer1.PublicIPAddr=127.0.0.1",
		"Peer1.PrivateIPAddr=127.0.0.1",
		"Peer1.ReadCacheQuotaFraction=0.20",

		"Cluster.WhoAmI=Peer1",
		"Cluster.Peers=Peer1",
		"Cluster.ServerGuid=a66488e9-a051-4ff7-865d-87bfb84cc2ae",
		"Cluster.PrivateClusterUDPPort=5002", // 5002 instead of 5001 so that test can run if proxyfsd is already running
		"Cluster.HeartBeatInterval=100ms",
		"Cluster.HeartBeatVariance=5ms",
		"Cluster.HeartBeatExpiration=400ms",
		"Cluster.MessageExpiration=700ms",
		"Cluster.RequestExpiration=1s",
		"Cluster.UDPReadSize=8000",
		"Cluster.UDPWriteSize=7000",

		"HTTPServer.TCPPort=53461",

		"SwiftClient.NoAuthTCPPort=45262",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=1",
		"SwiftClient.RetryLimitObject=1",
		"SwiftClient.RetryDelay=10ms",
		"SwiftClient.RetryDelayObject=10ms",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"SwiftClient.StarvationCallbackFrequency=100ms",

		"CommonFlowControl.MaxFlushSize=10000000",
		"CommonFlowControl.MaxFlushTime=10s",
		"CommonFlowControl.ReadCacheLineSize=1000000",
		"CommonFlowControl.ReadCacheWeight=100",

		"PhysicalContainerLayoutReplicated3Way.ContainerStoragePolicyIndex=0",
		"PhysicalContainerLayoutReplicated3Way.ContainerNamePrefix=Replicated3Way_",
		"PhysicalContainerLayoutReplicated3Way.ContainersPerPeer=1000",
		"PhysicalContainerLayoutReplicated3Way.MaxObjectsPerContainer=1000000",

		"CommonVolume.FSID=1",
		"CommonVolume.FUSEMountPointName=CommonMountPoint",
		"CommonVolume.NFSExportName=CommonExport",
		"CommonVolume.SMBShareName=CommonShare",
		"CommonVolume.PrimaryPeer=Peer1",
		"CommonVolume.AccountName=AUTH_CommonAccount",
		"CommonVolume.CheckpointContainerName=.__checkpoint__",
		"CommonVolume.CheckpointInterval=10s",
		"CommonVolume.CheckpointIntervalsPerCompaction=100",
		"CommonVolume.DefaultPhysicalContainerLayout=PhysicalContainerLayoutReplicated3Way",
		"CommonVolume.FlowControl=CommonFlowControl",
		"CommonVolume.NonceValuesToReserve=100",

		"FSGlobals.VolumeList=CommonVolume",

		"JSONRPCServer.TCPPort=12346",     // 12346 instead of 12345 so that test can run if proxyfsd is already running
		"JSONRPCServer.FastTCPPort=32346", // ...and similarly here...
		"JSONRPCServer.DataPathLogging=false",
		"JSONRPCServer.DontWriteConf=true",

		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
	}

	testVersionConfFile, err = ioutil.TempFile(os.TempDir(), "proxyfsdTest_")
	if nil != err {
		t.Fatalf("ioutil.TempFile() failed: %v", err)
	}
	testVersionConfFileName = testVersionConfFile.Name()

	_, err = testVersionConfFile.WriteString("[TestVersionSection]\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 1] failed: %v", err)
	}
	_, err = testVersionConfFile.WriteString("Version: 1\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 2] failed: %v", err)
	}

	err = testVersionConfFile.Close()
	if nil != err {
		t.Fatalf("testVersionConfFile.Close() [case 1] failed: %v", err)
	}

	go ramswift.Daemon(testVersionConfFileName, confMapStrings, &ramswiftSignalHandlerIsArmed, ramswiftDoneChan, unix.SIGINT)

	for !ramswiftSignalHandlerIsArmed {
		time.Sleep(100 * time.Millisecond)
	}

	// Launch an instance of proxyfsd using that same config

	proxyfsdSignalHandlerIsArmed = false
	errChan = make(chan error, 1) // Must be buffered to avoid race

	go Daemon("/dev/null", confMapStrings, &proxyfsdSignalHandlerIsArmed, errChan, &wg, unix.SIGTERM, unix.SIGHUP)

	for !proxyfsdSignalHandlerIsArmed {
		select {
		case err = <-errChan:
			if nil == err {
				t.Fatalf("Daemon() exited successfully despite not being told to do so [case 1]")
			} else {
				t.Fatalf("Daemon() exited with error [case 1a]: %v", err)
			}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Write to the volume (with no flush so that only time-based/restart flush is performed)

	mountHandle, err = fs.Mount("CommonVolume", fs.MountOptions(0))
	if nil != err {
		t.Fatalf("fs.Mount() failed [case 1]: %v", err)
	}

	createdFileInodeNumber, err = mountHandle.Create(
		inode.InodeRootUserID,
		inode.InodeRootGroupID,
		nil,
		inode.RootDirInodeNumber,
		"TestFile",
		inode.R_OK|inode.W_OK,
	)
	if nil != err {
		t.Fatalf("fs.Create() failed: %v", err)
	}

	bytesWritten, err = mountHandle.Write(
		inode.InodeRootUserID,
		inode.InodeRootGroupID,
		nil,
		createdFileInodeNumber,
		0,
		[]byte{0x00, 0x01, 0x02, 0x03},
		nil,
	)
	if nil != err {
		t.Fatalf("fs.Write() failed: %v", err)
	}
	if 4 != bytesWritten {
		t.Fatalf("fs.Write() returned unexpected bytesWritten")
	}

	// Verify written data before restart

	toReadFileInodeNumber, err = mountHandle.Lookup(
		inode.InodeRootUserID,
		inode.InodeRootGroupID,
		nil,
		inode.RootDirInodeNumber,
		"TestFile",
	)
	if nil != err {
		t.Fatalf("fs.Lookup() failed [case 1]: %v", err)
	}

	readData, err = mountHandle.Read(
		inode.InodeRootUserID,
		inode.InodeRootGroupID,
		nil,
		toReadFileInodeNumber,
		0,
		4,
		nil,
	)
	if nil != err {
		t.Fatalf("fs.Read() failed [case 1]: %v", err)
	}
	if 0 != bytes.Compare([]byte{0x00, 0x01, 0x02, 0x03}, readData) {
		t.Fatalf("fs.Read() returned unexpected readData [case 1]")
	}

	// Send ourself a SIGTERM to signal normal termination of mainWithArgs()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	err = <-errChan

	wg.Wait() // wait for services to go Down()

	if nil != err {
		t.Fatalf("Daemon() exited with error [case 1b]: == %v", err)
	}

	// Relaunch an instance of proxyfsd

	proxyfsdSignalHandlerIsArmed = false
	errChan = make(chan error, 1) // Must be buffered to avoid race

	go Daemon(testVersionConfFileName, confMapStrings, &proxyfsdSignalHandlerIsArmed, errChan, &wg, unix.SIGTERM, unix.SIGHUP)

	for !proxyfsdSignalHandlerIsArmed {
		select {
		case err = <-errChan:
			if nil == err {
				t.Fatalf("Daemon() exited successfully despite not being told to do so [case 2]")
			} else {
				t.Fatalf("Daemon() exited with error [case 2a]: %v", err)
			}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Verify written data after restart

	mountHandle, err = fs.Mount("CommonVolume", fs.MountOptions(0))
	if nil != err {
		t.Fatalf("fs.Mount() failed [case 2]: %v", err)
	}

	toReadFileInodeNumber, err = mountHandle.Lookup(
		inode.InodeRootUserID,
		inode.InodeRootGroupID,
		nil,
		inode.RootDirInodeNumber,
		"TestFile",
	)
	if nil != err {
		t.Fatalf("fs.Lookup() failed [case 2]: %v", err)
	}

	readData, err = mountHandle.Read(
		inode.InodeRootUserID,
		inode.InodeRootGroupID,
		nil,
		toReadFileInodeNumber,
		0,
		4,
		nil,
	)
	if nil != err {
		t.Fatalf("fs.Read() failed [case 2]: %v", err)
	}
	if 0 != bytes.Compare([]byte{0x00, 0x01, 0x02, 0x03}, readData) {
		t.Fatalf("fs.Read() returned unexpected readData [case 2]")
	}

	// Verify [TestVersionSection]Version == 1

	testVersion = fetchTestVersionSectionDotVersion(t)
	if 1 != testVersion {
		t.Fatalf("Before SIGHUP, fetchTestVersionSectionDotVersion() should have returned 1")
	}

	// Update testVersionConfFileName to bump [TestVersionSection]Version to 2

	testVersionConfFile, err = os.OpenFile(testVersionConfFileName, os.O_TRUNC|os.O_WRONLY, 0)
	if nil != err {
		t.Fatalf("os.OpenFile() failed: %v", err)
	}

	_, err = testVersionConfFile.WriteString("[TestVersionSection]\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 3] failed: %v", err)
	}
	_, err = testVersionConfFile.WriteString("Version: 2\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 4] failed: %v", err)
	}

	err = testVersionConfFile.Close()
	if nil != err {
		t.Fatalf("testVersionConfFile.Close() [case 2] failed: %v", err)
	}

	// Send ourself a SIGHUP to signal reload of testVersionConfFileName
	//   and wait a sufficient amount of time for the reload to complete

	unix.Kill(unix.Getpid(), unix.SIGHUP)
	time.Sleep(time.Second)

	// Verify [TestVersionSection]Version == 2

	testVersion = fetchTestVersionSectionDotVersion(t)
	if 2 != testVersion {
		t.Fatalf("After SIGHUP, fetchTestVersionSectionDotVersion() should have returned 2")
	}

	// Send ourself a SIGTERM to signal normal termination of mainWithArgs()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	err = <-errChan

	wg.Wait() // wait for services to go Down()

	if nil != err {
		t.Fatalf("Daemon() exited with error [case 2b]: %v", err)
	}

	// Send ourself a SIGINT to also terminate ramswift

	unix.Kill(unix.Getpid(), unix.SIGINT)

	_ = <-ramswiftDoneChan

	// Clean up

	err = os.Remove(testVersionConfFileName)
	if nil != err {
		t.Fatalf("os.Remove(testVersionConfFileName) failed: %v", err)
	}
}

func fetchTestVersionSectionDotVersion(t *testing.T) (version uint64) {
	var (
		body                                         []byte
		bodySections                                 map[string]interface{}
		err                                          error
		ok                                           bool
		resp                                         *http.Response
		testVersionSection                           interface{}
		testVersionSectionMap                        map[string]interface{}
		testVersionSectionMapVersion                 interface{}
		testVersionSectionMapVersionSlice            []interface{}
		testVersionSectionMapVersionSliceElementZero interface{}
		versionAsString                              string
	)

	resp, err = http.Get("http://127.0.0.1:53461/config")
	if nil != err {
		t.Fatalf("http.Get() failed: %v", err)
	}
	body, err = ioutil.ReadAll(resp.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() failed: %v", err)
	}
	err = resp.Body.Close()
	if nil != err {
		t.Fatalf("resp.Body.Close() failed: %v", err)
	}
	bodySections = make(map[string]interface{})
	err = json.Unmarshal(body, &bodySections)
	if nil != err {
		t.Fatalf("json.Unmarshal() failed: %v", err)
	}
	testVersionSection, ok = bodySections["TestVersionSection"]
	if !ok {
		t.Fatalf("bodySections[\"TestVersionSection\"] not found")
	}
	testVersionSectionMap, ok = testVersionSection.(map[string]interface{})
	if !ok {
		t.Fatalf("testVersionSection.(map[string]interface{}) failed")
	}
	testVersionSectionMapVersion, ok = testVersionSectionMap["Version"]
	if !ok {
		t.Fatalf("testVersionSectionMap[\"Version\"] not found")
	}
	testVersionSectionMapVersionSlice, ok = testVersionSectionMapVersion.([]interface{})
	if !ok {
		t.Fatalf("testVersionSectionMapVersion.([]interface{}) failed")
	}
	if 1 != len(testVersionSectionMapVersionSlice) {
		t.Fatalf("testVersionSectionMapVersionSlice should have a single element")
	}
	testVersionSectionMapVersionSliceElementZero = testVersionSectionMapVersionSlice[0]
	versionAsString, ok = testVersionSectionMapVersionSliceElementZero.(string)
	if !ok {
		t.Fatalf("testVersionSectionMapVersionSliceElementZero.(string) failed")
	}
	version, err = strconv.ParseUint(versionAsString, 10, 64)
	if nil != err {
		t.Fatalf("strconv(versionAsString, 10, 64) failed: %v", err)
	}

	return
}
