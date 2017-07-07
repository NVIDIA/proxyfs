package proxyfsd

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/conf"

	"github.com/swiftstack/ProxyFS/ramswift"
)

func TestMain(m *testing.M) {
	mRunReturn := m.Run()
	os.Exit(mRunReturn)
}

func TestDaemon(t *testing.T) {
	HeadhunterDirPath, err := ioutil.TempDir("", "headhunter.db")
	if nil != err {
		t.Fatalf("ioutil.TempDir(\"\", \"headhunter.db\") returned error: %v", err)
	}

	signalHandlerIsArmed := false
	errChan := make(chan error, 1) // Must be buffered to avoid race

	ramswiftSignalHandlerIsArmed := false
	ramswiftDoneChan := make(chan bool, 1)

	goodConfs := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"Logging.LogFilePath=proxyfsd.log",
		"Peer1.PublicIPAddr=127.0.0.1",
		"Peer1.PrivateIPAddr=127.0.0.1",
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
		"SwiftClient.RetryDelay=50ms",
		"SwiftClient.RetryLimit=10",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"CommonFlowControl.MaxFlushSize=10000000",
		"CommonFlowControl.MaxFlushTime=10s",
		"CommonFlowControl.ReadCacheLineSize=1000000",
		"CommonFlowControl.ReadCacheTotalSize=100000000",
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
		"CommonVolume.PhysicalContainerLayoutList=PhysicalContainerLayoutReplicated3Way",
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

	goodConfMap, err := conf.MakeConfMapFromStrings(goodConfs)
	if err != nil {
		t.Fatalf("%v", err)
	}

	var wg sync.WaitGroup

	go Daemon(goodConfMap, &signalHandlerIsArmed, errChan, &wg)

	go ramswift.Daemon(goodConfMap, &ramswiftSignalHandlerIsArmed, ramswiftDoneChan)

	for !signalHandlerIsArmed {
		select {
		case err := <-errChan:
			if nil == err {
				t.Fatalf("Daemon() exited successfully despite not being told to do so")
			} else {
				t.Fatalf("Daemon() exited with error == \"%v\"", err)
			}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	for !ramswiftSignalHandlerIsArmed {
		time.Sleep(100 * time.Millisecond)
	}

	// Send ourself a SIGTERM to signal normal termination of mainWithArgs()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	err = <-errChan

	if nil != err {
		t.Fatalf("Daemon() exited with error == \"%v\"", err)
	}

	err = os.RemoveAll(HeadhunterDirPath)
	if nil != err {
		t.Fatalf("os.RemoveAll() returned error == \"%v\"", err)
	}
}
