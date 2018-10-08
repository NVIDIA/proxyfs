package headhunter

import (
	"bytes"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/transitions"
)

func inodeRecPutGet(t *testing.T, volume VolumeHandle, key uint64, value []byte) {
	err := volume.PutInodeRec(key, value)
	if nil != err {
		t.Fatalf("Failed to Put %d %s :	 %v", key, value, err)
	}

	value1, ok, err := volume.GetInodeRec(key)
	if nil != err || !ok {
		t.Fatalf("Failed to Get %d %s :	 %v", key, value, err)
	}

	if 0 != bytes.Compare(value, value1) {
		t.Fatalf("Get Value does not match Inital Value: %d %v %v", key, value, err)
	}
}

func logsegmentRecPutGet(t *testing.T, volume VolumeHandle, key uint64, value []byte) {
	err := volume.PutLogSegmentRec(key, value)
	if nil != err {
		t.Fatalf("Failed to Put %d %s :	 %v", key, value, err)
	}

	value1, err := volume.GetLogSegmentRec(key)
	if nil != err {
		t.Fatalf("Failed to Get %d %s :	 %v", key, value, err)
	}

	if 0 != bytes.Compare(value, value1) {
		t.Fatalf("Get Value does not match Inital Value: %d %v %v", key, value, err)
	}
}

func putInodeRecsTest(t *testing.T, volume VolumeHandle) {
	var keys []uint64
	var values [][]byte

	keys = make([]uint64, 10)
	values = make([][]byte, 10)

	for i := 0; i < 10; i++ {
		keys[i] = uint64(i)
	}

	for i := 0; i < 10; i++ {
		values[i] = make([]byte, 10)
		values[i] = []byte("Value 1")
	}

	err := volume.PutInodeRecs(keys, values)
	if nil != err {
		t.Fatalf("Failed to PutInodeRecs: %v", err)
	}

	for i := 0; i < 10; i++ {
		var value []byte
		value, ok, err := volume.GetInodeRec(keys[i])
		if err != nil || !ok {
			t.Fatalf("Unable to get inode %d", keys[i])
		}
		if bytes.Compare(value, values[i]) != 0 {
			t.Fatalf("Get Value does not match Initial Value: %v %v %v", keys[i], values[i], value[i])
		}

		err = volume.DeleteInodeRec(keys[i])
		if nil != err {
			t.Fatalf("Failed to delete InodeRec key: %v %v", keys[i], err)
		}
	}
}

func TestHeadHunterAPI(t *testing.T) {
	var (
		confMap                conf.ConfMap
		confStrings            []string
		doneChan               chan bool
		err                    error
		firstUpNonce           uint64
		key                    uint64
		replayLogFile          *os.File
		replayLogFileName      string
		secondUpNonce          uint64
		signalHandlerIsArmedWG sync.WaitGroup
		value                  []byte
		volume                 VolumeHandle
	)

	confStrings = []string{
		"Logging.LogFilePath=/dev/null",
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"SwiftClient.NoAuthIPAddr=127.0.0.1",
		"SwiftClient.NoAuthTCPPort=9999",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=0",
		"SwiftClient.RetryLimitObject=0",
		"SwiftClient.RetryDelay=1s",
		"SwiftClient.RetryDelayObject=1s",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"Cluster.WhoAmI=Peer0",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"Volume:TestVolume.PrimaryPeer=Peer0",
		"Volume:TestVolume.AccountName=TestAccount",
		"Volume:TestVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:TestVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:TestVolume.CheckpointInterval=10s",
		"Volume:TestVolume.MaxFlushSize=10000000",
		"Volume:TestVolume.NonceValuesToReserve=100",
		"Volume:TestVolume.MaxInodesPerMetadataNode=32",
		"Volume:TestVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:TestVolume.MaxDirFileNodesPerMetadataNode=16",
		"VolumeGroup:TestVolumeGroup.VolumeList=TestVolume",
		"VolumeGroup:TestVolumeGroup.VirtualIPAddr=",
		"VolumeGroup:TestVolumeGroup.PrimaryPeer=Peer0",
		"FSGlobals.VolumeGroupList=TestVolumeGroup",
		"FSGlobals.InodeRecCacheEvictLowLimit=10000",
		"FSGlobals.InodeRecCacheEvictHighLimit=10010",
		"FSGlobals.LogSegmentRecCacheEvictLowLimit=10000",
		"FSGlobals.LogSegmentRecCacheEvictHighLimit=10010",
		"FSGlobals.BPlusTreeObjectCacheEvictLowLimit=10000",
		"FSGlobals.BPlusTreeObjectCacheEvictHighLimit=10010",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
	}

	// Construct replayLogFileName to use as Volume:TestVolume.ReplayLogFileName

	replayLogFile, err = ioutil.TempFile("", "TestVolume_Replay_Log_")
	if nil != err {
		t.Fatalf("ioutil.TempFile() returned error: %v", err)
	}

	replayLogFileName = replayLogFile.Name()

	err = replayLogFile.Close()
	if nil != err {
		t.Fatalf("replayLogFile.Close() returned error: %v", err)
	}

	err = os.Remove(replayLogFileName)
	if nil != err {
		t.Fatalf("os.Remove(replayLogFileName) returned error: %v", err)
	}

	confStrings = append(confStrings, "Volume:TestVolume.ReplayLogFileName="+replayLogFileName)

	// Launch a ramswift instance

	signalHandlerIsArmedWG.Add(1)
	doneChan = make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon("/dev/null", confStrings, &signalHandlerIsArmedWG, doneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned error: %v", err)
	}

	// Schedule a Format of TestVolume on first Up()

	err = confMap.UpdateFromString("Volume:TestVolume.AutoFormat=true")
	if nil != err {
		t.Fatalf("conf.UpdateFromString(\"Volume:TestVolume.AutoFormat=true\") returned error: %v", err)
	}

	// Up packages (TestVolume will be formatted)

	err = transitions.Up(confMap)
	if nil != err {
		t.Fatalf("transitions.Up() [case 1] returned error: %v", err)
	}

	// Unset AutoFormat for all subsequent uses of ConfMap

	err = confMap.UpdateFromString("Volume:TestVolume.AutoFormat=false")
	if nil != err {
		t.Fatalf("conf.UpdateFromString(\"Volume:TestVolume.AutoFormat=false\") returned error: %v", err)
	}

	// Perform test

	volume, err = FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") [case 1] returned error: %v", err)
	}

	volume.RegisterForEvents(nil)
	volume.UnregisterForEvents(nil)

	firstUpNonce, err = volume.FetchNonce()
	if nil != err {
		t.Fatalf("FetchNonce() [case 1] returned error: %v", err)
	}

	err = transitions.Down(confMap)
	if nil != err {
		t.Fatalf("transitions.Up() [case 1] returned error: %v", err)
	}

	err = transitions.Up(confMap)
	if nil != err {
		t.Fatalf("transitions.Up() [case 2] returned error: %v", err)
	}

	volume, err = FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") [case 2] returned error: %v", err)
	}

	secondUpNonce, err = volume.FetchNonce()
	if nil != err {
		t.Fatalf("FetchNonce() [case 2] returned error: %v", err)
	}
	if firstUpNonce >= secondUpNonce {
		t.Fatalf("FetchNonce() [case 2] returned unexpected nonce: %v (should have been > %v)", secondUpNonce, firstUpNonce)
	}

	key = 1234
	value = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	inodeRecPutGet(t, volume, key, value)

	value = []byte{11, 12, 13, 14, 15, 16, 17, 18, 19}
	inodeRecPutGet(t, volume, key, value)

	err = volume.DeleteInodeRec(key)
	if nil != err {
		t.Fatalf("Delete of key %d failed: %v", key, err)
	}

	putInodeRecsTest(t, volume)

	logsegmentRecPutGet(t, volume, key, value)

	value = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	logsegmentRecPutGet(t, volume, key, value)

	err = volume.DeleteLogSegmentRec(key)
	if nil != err {
		t.Fatalf("Delete of key %d failed: %v", key, err)
	}

	// Shutdown packages

	err = transitions.Down(confMap)
	if nil != err {
		t.Fatalf("transitions.Up() [case 2] returned error: %v", err)
	}

	err = os.Remove(replayLogFileName)
	if nil == err {
		t.Fatal("os.Remove(replayLogFileName) should not have succeeded")
	} else {
		if os.IsNotExist(err) {
			// This is what we expect
		} else {
			t.Fatalf("os.Remove(replayLogFileName) returned unexpected error: %v", err)
		}
	}

	// Send ourself a SIGTERM to terminate ramswift.Daemon()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	_ = <-doneChan
}
