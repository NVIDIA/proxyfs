package headhunter

import (
	"bytes"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
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
	confStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
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
		"SwiftClient.StarvationCallbackFrequency=100ms",
		"Cluster.WhoAmI=Peer0",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"FlowControl:TestFlowControl.MaxFlushSize=10000000",
		"Volume:TestVolume.PrimaryPeer=Peer0",
		"Volume:TestVolume.AccountName=TestAccount",
		"Volume:TestVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:TestVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:TestVolume.CheckpointInterval=10s",
		"Volume:TestVolume.CheckpointIntervalsPerCompaction=100",
		"Volume:TestVolume.FlowControl=TestFlowControl",
		"Volume:TestVolume.NonceValuesToReserve=100",
		"Volume:TestVolume.MaxInodesPerMetadataNode=32",
		"Volume:TestVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:TestVolume.MaxDirFileNodesPerMetadataNode=16",
		"FSGlobals.VolumeList=TestVolume",
		"FSGlobals.InodeRecCacheEvictLowLimit=10000",
		"FSGlobals.InodeRecCacheEvictHighLimit=10010",
		"FSGlobals.LogSegmentRecCacheEvictLowLimit=10000",
		"FSGlobals.LogSegmentRecCacheEvictHighLimit=10010",
		"FSGlobals.BPlusTreeObjectCacheEvictLowLimit=10000",
		"FSGlobals.BPlusTreeObjectCacheEvictHighLimit=10010",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
	}

	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned error: %v", err)
	}

	signalHandlerIsArmed := false
	doneChan := make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon("/dev/null", confStrings, &signalHandlerIsArmed, doneChan, unix.SIGTERM)

	for !signalHandlerIsArmed {
		time.Sleep(100 * time.Millisecond)
	}

	err = logger.Up(confMap)
	if nil != err {
		t.Fatalf("logger.Up() [case 1] returned error: %v", err)
	}

	err = stats.Up(confMap)
	if nil != err {
		t.Fatalf("stats.Up() [case 1] returned error: %v", err)
	}

	err = dlm.Up(confMap)
	if nil != err {
		t.Fatalf("dlm.Up() [case 1] returned error: %v", err)
	}

	err = swiftclient.Up(confMap)
	if nil != err {
		t.Fatalf("swiftclient.Up() [case 1] returned error: %v", err)
	}

	err = Format(confMap, "TestVolume")
	if nil != err {
		t.Fatalf("headhunter.Format() returned error: %v", err)
	}

	err = Up(confMap)
	if nil != err {
		t.Fatalf("headhunter.Up() [case 1] returned error: %v", err)
	}

	volume, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") [case 1] returned error: %v", err)
	}

	firstUpNonce, err := volume.FetchNonce()
	if nil != err {
		t.Fatalf("FetchNonce() [case 1] returned error: %v", err)
	}

	err = Down()
	if nil != err {
		t.Fatalf("headhunter.Down() [case 1] returned error: %v", err)
	}

	err = swiftclient.Down()
	if nil != err {
		t.Fatalf("swiftclient.Down() [case 1] returned error: %v", err)
	}

	err = dlm.Down()
	if nil != err {
		t.Fatalf("dlm.Down() [case 1] returned error: %v", err)
	}

	err = stats.Down()
	if nil != err {
		t.Fatalf("stats.Down() [case 1] returned error: %v", err)
	}

	err = logger.Down()
	if nil != err {
		t.Fatalf("logger.Down() [case 1] returned error: %v", err)
	}

	err = logger.Up(confMap)
	if nil != err {
		t.Fatalf("logger.Up() [case 2] returned error: %v", err)
	}

	err = stats.Up(confMap)
	if nil != err {
		t.Fatalf("stats.Up() [case 2] returned error: %v", err)
	}

	err = dlm.Up(confMap)
	if nil != err {
		t.Fatalf("dlm.Up() [case 2] returned error: %v", err)
	}

	err = swiftclient.Up(confMap)
	if nil != err {
		t.Fatalf("swiftclient.Up() [case 2] returned error: %v", err)
	}

	err = Up(confMap)
	if nil != err {
		t.Fatalf("headhunter.Up() [case 2] returned error: %v", err)
	}

	volume, err = FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") [case 2] returned error: %v", err)
	}

	secondUpNonce, err := volume.FetchNonce()
	if nil != err {
		t.Fatalf("FetchNonce() [case 2] returned error: %v", err)
	}
	if firstUpNonce >= secondUpNonce {
		t.Fatalf("FetchNonce() [case 2] returned unexpected nonce: %v (should have been > %v)", secondUpNonce, firstUpNonce)
	}

	var key uint64
	key = 1234
	value := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

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

	err = Down()
	if nil != err {
		t.Fatalf("headhunter.Down() [case 2] returned error: %v", err)
	}

	err = swiftclient.Down()
	if nil != err {
		t.Fatalf("swiftclient.Down() [case 2] returned error: %v", err)
	}

	err = dlm.Down()
	if nil != err {
		t.Fatalf("dlm.Down() [case 2] returned error: %v", err)
	}

	err = stats.Down()
	if nil != err {
		t.Fatalf("stats.Down() [case 2] returned error: %v", err)
	}

	err = logger.Down()
	if nil != err {
		t.Fatalf("logger.Down() [case 2] returned error: %v", err)
	}

	// Send ourself a SIGTERM to terminate ramswift.Daemon()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	_ = <-doneChan
}
