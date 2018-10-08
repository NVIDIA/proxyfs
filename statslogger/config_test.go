package statslogger

/*
 * Test the statistics logger.
 *
 * Most of this file is copied from swiftclient/api_test.go because we want to
 * perform some swift client operations where we can look at the updated
 * statistics.
 */

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/transitions"
)

func (tOCCS *testObjectCopyCallbackStruct) BytesRemaining(bytesRemaining uint64) (chunkSize uint64) {
	chunkSize = tOCCS.chunkSize
	return
}

type testObjectCopyCallbackStruct struct {
	srcAccountName   string
	srcContainerName string
	srcObjectName    string
	dstAccountName   string
	dstContainerName string
	dstObjectName    string
	chunkSize        uint64
}

func TestAPI(t *testing.T) {
	var (
		confMap                conf.ConfMap
		confStrings            []string
		err                    error
		signalHandlerIsArmedWG sync.WaitGroup
	)

	confStrings = []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",

		"StatsLogger.Period=0s",

		"SwiftClient.NoAuthIPAddr=127.0.0.1",
		"SwiftClient.NoAuthTCPPort=9999",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=5",
		"SwiftClient.RetryLimitObject=5",
		"SwiftClient.RetryDelay=250ms",
		"SwiftClient.RetryDelayObject=250ms",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",

		"Cluster.WhoAmI=Peer0",

		"Peer:Peer0.ReadCacheQuotaFraction=0.20",

		"FSGlobals.VolumeGroupList=",

		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
	}

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	signalHandlerIsArmedWG.Add(1)
	doneChan := make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon("/dev/null", confStrings, &signalHandlerIsArmedWG, doneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	err = transitions.Up(confMap)
	if nil != err {
		t.Fatalf("transitions.Up(confMap) failed: %v", err)
	}

	if globals.statsLogPeriod != 0 {
		t.Fatalf("after Up('StatsLogger.Period=0s') globals.statsLogPeriod != 0")
	}

	err = confMap.UpdateFromString("StatsLogger.Period=1s")
	if nil != err {
		t.Fatalf("UpdateFromString('StatsLogger.Period=1s') failed: %v", err)
	}

	err = transitions.Signaled(confMap)
	if nil != err {
		t.Fatalf("transitions.Signaled(confMap) failed: %v", err)
	}

	if globals.statsLogPeriod != 1*time.Second {
		t.Fatalf("after Signaled('StatsLogger.Period=1s') globals.statsLogPeriod != 1 sec")
	}

	// Run the tests
	//
	// "Real" unit tests would verify the information written into the log
	//
	// t.Run("testRetry", testRetry)
	// t.Run("testOps", testOps)
	testOps(t)
	testChunkedPut(t)
	testReload(t, confMap)

	// Shutdown packages

	err = transitions.Down(confMap)
	if nil != err {
		t.Fatalf("transitions.Down(confMap) failed: %v", err)
	}

	// Send ourself a SIGTERM to terminate ramswift.Daemon()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	_ = <-doneChan
}

// Test normal Swift client operations so we have something in the log
//
func testOps(t *testing.T) {
	// headers for testing

	catDogHeaderMap := make(map[string][]string)
	catDogHeaderMap["Cat"] = []string{"Dog"}

	mouseBirdHeaderMap := make(map[string][]string)
	mouseBirdHeaderMap["Mouse"] = []string{"Bird"}

	mouseDeleteHeaderMap := make(map[string][]string)
	mouseDeleteHeaderMap["Mouse"] = []string{""}

	// Send a PUT for account "TestAccount" and header Cat: Dog

	err := swiftclient.AccountPut("TestAccount", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog

	accountHeaders, err := swiftclient.AccountHead("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountHead(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok := accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}

	// Send a GET for account "TestAccount" expecting header Cat: Dog and containerList []string{}

	accountHeaders, containerList, err := swiftclient.AccountGet("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountGet(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok = accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if 0 != len(containerList) {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return expected containerList")
	}

	// Send a POST for account "TestAccount" adding header Mouse: Bird

	err = swiftclient.AccountPost("TestAccount", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPost(\"TestAccount\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	accountHeaders, err = swiftclient.AccountHead("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountHead(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok = accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	mouseBirdHeader, ok := accountHeaders["Mouse"]
	if !ok {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Mouse\"")
	}
	if (1 != len(mouseBirdHeader)) || ("Bird" != mouseBirdHeader[0]) {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Bird\" having value []string{\"Mouse\"}")
	}

	// Send a POST for account "TestAccount" deleting header Mouse

	err = swiftclient.AccountPost("TestAccount", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPost(\"TestAccount\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	accountHeaders, err = swiftclient.AccountHead("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountHead(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok = accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	_, ok = accountHeaders["Mouse"]
	if ok {
		t.Fatalf("AccountHead(\"TestAccount\") shouldn't have returned Header \"Mouse\"")
	}

	// Send a PUT for account "TestAccount" adding header Mouse: Bird

	err = swiftclient.AccountPut("TestAccount", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	accountHeaders, err = swiftclient.AccountHead("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountHead(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok = accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	mouseBirdHeader, ok = accountHeaders["Mouse"]
	if !ok {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Mouse\"")
	}
	if (1 != len(mouseBirdHeader)) || ("Bird" != mouseBirdHeader[0]) {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Bird\" having value []string{\"Mouse\"}")
	}

	// Send a PUT for account "TestAccount" deleting header Mouse

	err = swiftclient.AccountPut("TestAccount", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	accountHeaders, err = swiftclient.AccountHead("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountHead(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok = accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountHead(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	_, ok = accountHeaders["Mouse"]
	if ok {
		t.Fatalf("AccountHead(\"TestAccount\") shouldn't have returned Header \"Mouse\"")
	}

	// Send a PUT for container "TestContainer" and header Cat: Dog

	err = swiftclient.ContainerPut("TestAccount", "TestContainer", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for account "TestAccount" expecting header Cat: Dog and containerList []string{"TestContainer"}

	accountHeaders, containerList, err = swiftclient.AccountGet("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountGet(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok = accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if (1 != len(containerList)) || ("TestContainer" != containerList[0]) {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return expected containerList")
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog

	containerHeaders, err := swiftclient.ContainerHead("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerHead(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok := containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{}

	containerHeaders, objectList, err := swiftclient.ContainerGet("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerGet(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if 0 != len(objectList) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return expected objectList")
	}

	// Send a POST for container "TestContainer" adding header Mouse: Bird

	err = swiftclient.ContainerPost("TestAccount", "TestContainer", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPost(\"TestAccount\", \"TestContainer\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	containerHeaders, err = swiftclient.ContainerHead("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerHead(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	containerMouseHeader, ok := containerHeaders["Mouse"]
	if !ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Mouse\"")
	}
	if (1 != len(containerMouseHeader)) || ("Bird" != containerMouseHeader[0]) {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Mouse\" having value []string{\"Bird\"}")
	}

	// Send a POST for container "TestContainer" deleting header Mouse

	err = swiftclient.ContainerPost("TestAccount", "TestContainer", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPost(\"TestAccount\", \"TestContainer\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	containerHeaders, err = swiftclient.ContainerHead("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerHead(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	_, ok = containerHeaders["Mouse"]
	if ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") shouldn't have returned Header \"Mouse\"")
	}

	// Send a PUT for container "TestContainer" adding header Mouse: Bird

	err = swiftclient.ContainerPut("TestAccount", "TestContainer", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	containerHeaders, err = swiftclient.ContainerHead("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerHead(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	containerMouseHeader, ok = containerHeaders["Mouse"]
	if !ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Mouse\"")
	}
	if (1 != len(containerMouseHeader)) || ("Bird" != containerMouseHeader[0]) {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Mouse\" having value []string{\"Bird\"}")
	}

	// Send a PUT for container "TestContainer" deleting header Mouse

	err = swiftclient.ContainerPut("TestAccount", "TestContainer", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	containerHeaders, err = swiftclient.ContainerHead("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerHead(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	_, ok = containerHeaders["Mouse"]
	if ok {
		t.Fatalf("ContainerHead(\"TestAccount\", \"TestContainer\") shouldn't have returned Header \"Mouse\"")
	}

	// Start a chunked PUT for object "FooBar"

	chunkedPutContext, err := swiftclient.ObjectFetchChunkedPutContext("TestAccount", "TestContainer", "FooBar", "")
	if nil != err {
		tErr := fmt.Sprintf("ObjectFetchChunkedPutContext(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a chunk for object "FooBar" of []byte{0xAA, 0xBB}

	err = chunkedPutContext.SendChunk([]byte{0xAA, 0xBB})
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.SendChunk([]byte{0xAA, 0xBB}) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a chunk for object "FooBar" of []byte{0xCC, 0xDD, 0xEE}

	err = chunkedPutContext.SendChunk([]byte{0xCC, 0xDD, 0xEE})
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.SendChunk([]byte{0xCC, 0xDD, 0xEE}) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Fetch BytesPut for object "FooBar" expecting 5

	bytesPut, err := chunkedPutContext.BytesPut()
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.BytesPut() failed: %v", err)
		t.Fatalf(tErr)
	}
	if 5 != bytesPut {
		t.Fatalf("chunkedPutContext.BytesPut() didn't return expected bytesPut")
	}

	// Read back chunked PUT data at offset 1 for length 3 expecting []byte{0xBB, 0xCC, 0xDD}

	readBuf, err := chunkedPutContext.Read(uint64(1), uint64(3))
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.Read(uint64(1), uint64(3)) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, readBuf) {
		t.Fatalf("chunkedPutContext.Read(uint64(1), uint64(3)) didn't return expected []byte")
	}

	// Finish the chunked PUT for object "FooBar"

	err = chunkedPutContext.Close()
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.Close() failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{"FooBar"}

	containerHeaders, objectList, err = swiftclient.ContainerGet("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerGet(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if (1 != len(objectList)) || ("FooBar" != objectList[0]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return expected objectList")
	}

	// Send a HEAD for object "FooBar" expecting Content-Length: 5

	objectHeaders, err := swiftclient.ObjectHead("TestAccount", "TestContainer", "FooBar")
	if nil != err {
		tErr := fmt.Sprintf("ObjectHead(\"TestAccount\", \"TestContainer\", \"FooBar\") failed: %v", err)
		t.Fatalf(tErr)
	}
	contentLengthHeader, ok := objectHeaders["Content-Length"]
	if !ok {
		t.Fatalf("ObjectHead(\"TestAccount\", \"TestContainer\", \"FooBar\") didn't return Header \"Content-Length\"")
	}
	if (1 != len(contentLengthHeader)) || ("5" != contentLengthHeader[0]) {
		t.Fatalf("ObjectHead(\"TestAccount\", \"TestContainer\", \"FooBar\") didn't return Header \"Content-Length\" having value []string{\"5\"}")
	}

	// Fetch Content-Length for object "FooBar" expecting 5

	objectLength, err := swiftclient.ObjectContentLength("TestAccount", "TestContainer", "FooBar")
	if nil != err {
		tErr := fmt.Sprintf("ObjectContentLength(\"TestAccount\", \"TestContainer\", \"FooBar\") failed: %v", err)
		t.Fatalf(tErr)
	}
	if uint64(5) != objectLength {
		tErr := fmt.Sprintf("ObjectContentLength(\"TestAccount\", \"TestContainer\", \"FooBar\") didn't return expected Content-Length")
		t.Fatalf(tErr)
	}

	// Send a range GET of bytes at offset 1 for length 3 for object "FooBar" expecting []byte{0xBB, 0xCC, 0xDD}

	getBuf, err := swiftclient.ObjectGet("TestAccount", "TestContainer", "FooBar", uint64(1), uint64(3))
	if nil != err {
		tErr := fmt.Sprintf("ObjectGet(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), uint64(3)) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, getBuf) {
		tErr := fmt.Sprintf("ObjectGet(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), uint64(3)) didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Send a full GET for object "FooBar" expecting []byte{0xAA, 0xBB, 0xCC, 0xDD, OxEE}

	loadBuf, err := swiftclient.ObjectLoad("TestAccount", "TestContainer", "FooBar")
	if nil != err {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBar\") failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}, loadBuf) {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBar\") didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Send a tail GET of the last 2 bytes for object "FooBar" expecting []byte{0xDD, 0xEE}

	tailBuf, err := swiftclient.ObjectTail("TestAccount", "TestContainer", "FooBar", uint64(2))
	if nil != err {
		tErr := fmt.Sprintf("ObjectTail(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(2)) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xDD, 0xEE}, tailBuf) {
		tErr := fmt.Sprintf("ObjectTail(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(2)) didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Copy object "FooBar" to object "FooBarCopy"

	tOCCS := &testObjectCopyCallbackStruct{
		srcAccountName:   "TestAccount",
		srcContainerName: "TestContainer",
		srcObjectName:    "FooBar",
		dstAccountName:   "TestAccount",
		dstContainerName: "TestContainer",
		dstObjectName:    "FooBarCopy",
		chunkSize:        1, // Causes a callback for each of the five bytes of FooBar
	}

	err = swiftclient.ObjectCopy(tOCCS.srcAccountName, tOCCS.srcContainerName, tOCCS.srcObjectName, tOCCS.dstAccountName, tOCCS.dstContainerName, tOCCS.dstObjectName, tOCCS)
	if nil != err {
		tErr := fmt.Sprintf("ObjectCopy(\"TestAccount/TestContainer/FooBar\" to \"TestAccount/TestContainer/FooBarCopy\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a full GET for object "FooBarCopy" expecting []byte{0xAA, 0xBB, 0xCC, 0xDD, OxEE}

	loadBuf, err = swiftclient.ObjectLoad("TestAccount", "TestContainer", "FooBarCopy")
	if nil != err {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBarCopy\") failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}, loadBuf) {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBarCopy\") didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{"FooBar", "FooBarCopy"}

	containerHeaders, objectList, err = swiftclient.ContainerGet("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerGet(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if (2 != len(objectList)) || ("FooBar" != objectList[0]) || ("FooBarCopy" != objectList[1]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return expected objectList")
	}

	// Send a DELETE for object "FooBar"

	err = swiftclient.ObjectDelete("TestAccount", "TestContainer", "FooBar", 0)
	if nil != err {
		tErr := fmt.Sprintf("ObjectDelete(\"TestAccount\", \"TestContainer\". \"FooBar\", 0) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{"FooBarCopy"}

	containerHeaders, objectList, err = swiftclient.ContainerGet("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerGet(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if (1 != len(objectList)) || ("FooBarCopy" != objectList[0]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return expected objectList")
	}

	// Send a DELETE for object "FooBarCopy"

	err = swiftclient.ObjectDelete("TestAccount", "TestContainer", "FooBarCopy", 0)
	if nil != err {
		tErr := fmt.Sprintf("ObjectDelete(\"TestAccount\", \"TestContainer\". \"FooBarCopy\", 0) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{}

	containerHeaders, objectList, err = swiftclient.ContainerGet("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerGet(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}
	containerCatHeader, ok = containerHeaders["Cat"]
	if !ok {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\"")
	}
	if (1 != len(containerCatHeader)) || ("Dog" != containerCatHeader[0]) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if 0 != len(objectList) {
		t.Fatalf("ContainerGet(\"TestAccount\", \"TestContainer\") didn't return expected objectList")
	}

	// Send a DELETE for container "TestContainer"

	err = swiftclient.ContainerDelete("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerDelete(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// create and delete container "TestContainer" again so we're sure the retry code is hit

	err = swiftclient.ContainerPut("TestAccount", "TestContainer", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}
	err = swiftclient.ContainerDelete("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerDelete(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for account "TestAccount" expecting header Cat: Dog and containerList []string{}

	accountHeaders, containerList, err = swiftclient.AccountGet("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountGet(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
	accountCatHeader, ok = accountHeaders["Cat"]
	if !ok {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return Header \"Cat\"")
	}
	if (1 != len(accountCatHeader)) || ("Dog" != accountCatHeader[0]) {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return Header \"Cat\" having value []string{\"Dog\"}")
	}
	if 0 != len(containerList) {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return expected containerList")
	}

	// Send a DELETE for account "TestAccount"

	err = swiftclient.AccountDelete("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountDelete(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Create and delete "TestAccount" again so we're sure the retry code is hit

	err = swiftclient.AccountPut("TestAccount", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}
	err = swiftclient.AccountDelete("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountDelete(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}
}

// Extended testing of chunked put interface to exercise internal retries
//
func testChunkedPut(t *testing.T) {
	var (
		accountName   = "TestAccount"
		containerName = "TestContainer"
		objNameFmt    = "chunkObj%d"
		objName       string
	)

	// (lack of) headers for putting
	catDogHeaderMap := make(map[string][]string)

	// (re)create the test account and continer

	err := swiftclient.AccountPut(accountName, catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("testChunkedPut.AccountPut('%s', catDogHeaderMap) failed: %v", accountName, err)
		t.Fatalf(tErr)
	}

	err = swiftclient.ContainerPut(accountName, containerName, catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("testChunkedPut.ContainerPut('%s', '%s', catDogHeaderMap) failed: %v",
			accountName, containerName, err)
		t.Fatalf(tErr)
	}

	for i := 0; i < 5; i++ {
		objName = fmt.Sprintf(objNameFmt, i)

		err = testObjectWriteVerify(t, accountName, containerName, objName, 4096, 4)
		if nil != err {
			tErr := fmt.Sprintf("testChunkedPut.testObjectWriteVerify('%s/%s/%s', %d, %d ) failed: %v",
				accountName, containerName, objName, 4096, 4, err)
			t.Fatalf(tErr)
		}
	}

	// cleanup the mess we made (objects, container, and account)
	for i := 0; i < 5; i++ {
		objName = fmt.Sprintf(objNameFmt, i)

		err = swiftclient.ObjectDelete(accountName, containerName, objName, 0)
		if nil != err {
			tErr := fmt.Sprintf("ObjectDelete('%s', '%s', '%s') failed: %v",
				accountName, containerName, objName, err)
			t.Fatalf(tErr)
		}
	}

	err = swiftclient.ContainerDelete(accountName, containerName)
	if nil != err {
		tErr := fmt.Sprintf("ContainerDelete('%s', '%s') failed: %v", accountName, containerName, err)
		t.Fatalf(tErr)
	}

	err = swiftclient.AccountDelete(accountName)
	if nil != err {
		tErr := fmt.Sprintf("AccountDelete('%s') failed: %v", accountName, err)
		t.Fatalf(tErr)
	}
}

// write objSize worth of random bytes to the object using nWrite calls to
// SendChunk() and then read it back to verify.
//
func testObjectWriteVerify(t *testing.T, accountName string, containerName string, objName string,
	objSize int, nwrite int) (err error) {

	writeBuf := make([]byte, objSize)
	readBuf := make([]byte, 0)

	for i := 0; i < objSize; i++ {
		writeBuf[i] = byte(rand.Uint32())
	}
	if writeBuf[0] == 0 && writeBuf[1] == 0 && writeBuf[2] == 0 && writeBuf[3] == 0 {
		tErr := "unix.GetRandom() is not very random"
		t.Fatalf(tErr)
	}
	if writeBuf[objSize-1] == 0 && writeBuf[objSize-2] == 0 && writeBuf[objSize-3] == 0 &&
		writeBuf[objSize-4] == 0 {
		tErr := "unix.GetRandom() is not very radnom at end of buffer"
		t.Fatalf(tErr)
	}

	// Start a chunked PUT for the object
	chunkedPutContext, err := swiftclient.ObjectFetchChunkedPutContext(accountName, containerName, objName, "")
	if nil != err {
		tErr := fmt.Sprintf("ObjectFetchChunkedPutContext('%s', '%s', '%s') failed: %v",
			accountName, containerName, objName, err)
		return errors.New(tErr)
	}

	wsz := len(writeBuf) / nwrite
	for off := 0; off < len(writeBuf); off += wsz {
		if off+wsz < objSize {
			err = chunkedPutContext.SendChunk(writeBuf[off : off+wsz])
		} else {
			err = chunkedPutContext.SendChunk(writeBuf[off:])
		}
		if nil != err {
			tErr := fmt.Sprintf("chunkedPutContext.SendChunk(writeBuf[%d:%d]) failed: %v",
				off, off+wsz, err)
			return errors.New(tErr)
		}
	}
	err = chunkedPutContext.Close()
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.Close('%s/%s/%s') failed: %v",
			accountName, containerName, objName, err)
		return errors.New(tErr)
	}

	// read and compare
	readBuf, err = swiftclient.ObjectLoad(accountName, containerName, objName)
	if nil != err {
		tErr := fmt.Sprintf("ObjectLoad('%s/%s/%s') failed: %v", accountName, containerName, objName, err)
		return errors.New(tErr)
	}
	if !bytes.Equal(readBuf, writeBuf) {
		tErr := fmt.Sprintf("Object('%s/%s/%s') read back something different then written",
			accountName, containerName, objName)
		return errors.New(tErr)
	}

	return nil
}

// Make sure we can shutdown and re-enable the statsLogger
//
func testReload(t *testing.T, confMap conf.ConfMap) {
	var err error

	// Reload statslogger with logging disabled
	err = confMap.UpdateFromString("StatsLogger.Period=0s")
	if nil != err {
		tErr := fmt.Sprintf("UpdateFromString('StatsLogger.Period=0s') failed: %v", err)
		t.Fatalf(tErr)
	}

	err = transitions.Signaled(confMap)
	if nil != err {
		t.Fatalf("transitions.Signaled(confMap) failed: %v", err)
	}

	// Enable logging again
	err = confMap.UpdateFromString("StatsLogger.Period=1s")
	if nil != err {
		tErr := fmt.Sprintf("UpdateFromString('StatsLogger.Period=1s') failed: %v", err)
		t.Fatalf(tErr)
	}

	err = transitions.Signaled(confMap)
	if nil != err {
		t.Fatalf("transitions.Signaled(confMap) failed: %v", err)
	}
}
