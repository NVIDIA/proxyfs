package swiftclient

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/transitions"
)

type testObjectCopyCallbackStruct struct {
	srcAccountName   string
	srcContainerName string
	srcObjectName    string
	dstAccountName   string
	dstContainerName string
	dstObjectName    string
	chunkSize        uint64
}

func (tOCCS *testObjectCopyCallbackStruct) BytesRemaining(bytesRemaining uint64) (chunkSize uint64) {
	chunkSize = tOCCS.chunkSize
	return
}

func TestAPI(t *testing.T) {
	var (
		confMap                conf.ConfMap
		confStrings            []string
		doneChan               chan bool
		err                    error
		signalHandlerIsArmedWG sync.WaitGroup
	)

	confStrings = []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",

		"SwiftClient.NoAuthIPAddr=127.0.0.1",
		"SwiftClient.NoAuthTCPPort=9999",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=3",
		"SwiftClient.RetryLimitObject=3",
		"SwiftClient.RetryDelay=25ms",
		"SwiftClient.RetryDelayObject=25ms",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",

		// small pool sizes so test hangs if we leak connections
		"SwiftClient.ChunkedConnectionPoolSize=1",
		"SwiftClient.NonChunkedConnectionPoolSize=1",

		// checksum chunked put buffers
		"SwiftClient.ChecksumChunkedPutChunks=true",

		"Cluster.WhoAmI=Peer0",

		"Peer:Peer0.ReadCacheQuotaFraction=0.20",

		"FSGlobals.VolumeGroupList=",

		"Logging.LogFilePath=/dev/null",
		"Logging.LogToConsole=false",
		//"Logging.LogToConsole=true",

		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",

		"RamSwiftChaos.AccountDeleteFailureRate=2",
		"RamSwiftChaos.AccountGetFailureRate=2",
		"RamSwiftChaos.AccountHeadFailureRate=2",
		"RamSwiftChaos.AccountPostFailureRate=2",
		"RamSwiftChaos.AccountPutFailureRate=2",

		"RamSwiftChaos.ContainerDeleteFailureRate=2",
		"RamSwiftChaos.ContainerGetFailureRate=2",
		"RamSwiftChaos.ContainerHeadFailureRate=2",
		"RamSwiftChaos.ContainerPostFailureRate=2",
		"RamSwiftChaos.ContainerPutFailureRate=2",

		"RamSwiftChaos.ObjectDeleteFailureRate=3",
		"RamSwiftChaos.ObjectGetFailureRate=2",
		"RamSwiftChaos.ObjectHeadFailureRate=2",
		"RamSwiftChaos.ObjectPostFailureRate=2",
		"RamSwiftChaos.ObjectPutFailureRate=3",

		"RamSwiftChaos.FailureHTTPStatus=599",
	}

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	signalHandlerIsArmedWG.Add(1)
	doneChan = make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon("/dev/null", confStrings, &signalHandlerIsArmedWG, doneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	err = transitions.Up(confMap)
	if nil != err {
		t.Fatalf("transitions.Up(confMap) failed: %v", err)
	}

	// additional error injection settings
	globals.chaosSendChunkFailureRate = 7
	globals.chaosFetchChunkedPutFailureRate = 2

	// Run the tests
	// t.Run("testRetry", testRetry)
	// t.Run("testOps", testOps)
	testRetry(t)
	testOps(t)
	testChunkedPut(t)

	// Shutdown packages

	err = transitions.Down(confMap)
	if nil != err {
		t.Fatalf("logger.transitions() failed: %v", err)
	}

	// Send ourself a SIGTERM to terminate ramswift.Daemon()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	_ = <-doneChan
}

// Test the use of the API for normal Swift operations
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

	err := AccountPut("TestAccount", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog

	accountHeaders, err := AccountHead("TestAccount")
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

	accountHeaders, containerList, err := AccountGet("TestAccount")
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

	err = AccountPost("TestAccount", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPost(\"TestAccount\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	accountHeaders, err = AccountHead("TestAccount")
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

	err = AccountPost("TestAccount", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPost(\"TestAccount\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	accountHeaders, err = AccountHead("TestAccount")
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

	err = AccountPut("TestAccount", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	accountHeaders, err = AccountHead("TestAccount")
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

	err = AccountPut("TestAccount", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	accountHeaders, err = AccountHead("TestAccount")
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

	err = ContainerPut("TestAccount", "TestContainer", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a PUT for container "Z"

	err = ContainerPut("TestAccount", "Z", map[string][]string{})
	if nil != err {
		t.Fatalf("ContainerPut(\"TestAccount\", \"Z\", nil) failed: %v", err)
	}

	// Send a GET for account "TestAccount" expecting header Cat: Dog and containerList []string{"TestContainer", "Z"}

	accountHeaders, containerList, err = AccountGet("TestAccount")
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
	if (2 != len(containerList)) || ("TestContainer" != containerList[0]) || ("Z" != containerList[1]) {
		t.Fatalf("AccountGet(\"TestAccount\") didn't return expected containerList")
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog

	containerHeaders, err := ContainerHead("TestAccount", "TestContainer")
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

	containerHeaders, objectList, err := ContainerGet("TestAccount", "TestContainer")
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

	err = ContainerPost("TestAccount", "TestContainer", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPost(\"TestAccount\", \"TestContainer\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	containerHeaders, err = ContainerHead("TestAccount", "TestContainer")
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

	err = ContainerPost("TestAccount", "TestContainer", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPost(\"TestAccount\", \"TestContainer\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	containerHeaders, err = ContainerHead("TestAccount", "TestContainer")
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

	err = ContainerPut("TestAccount", "TestContainer", mouseBirdHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", mouseBirdHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	containerHeaders, err = ContainerHead("TestAccount", "TestContainer")
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

	err = ContainerPut("TestAccount", "TestContainer", mouseDeleteHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", mouseDeleteHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	containerHeaders, err = ContainerHead("TestAccount", "TestContainer")
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

	chunkedPutContext, err := ObjectFetchChunkedPutContext("TestAccount", "TestContainer", "FooBar", "")
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

	containerHeaders, objectList, err = ContainerGet("TestAccount", "TestContainer")
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

	objectHeaders, err := ObjectHead("TestAccount", "TestContainer", "FooBar")
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

	objectLength, err := ObjectContentLength("TestAccount", "TestContainer", "FooBar")
	if nil != err {
		tErr := fmt.Sprintf("ObjectContentLength(\"TestAccount\", \"TestContainer\", \"FooBar\") failed: %v", err)
		t.Fatalf(tErr)
	}
	if uint64(5) != objectLength {
		tErr := fmt.Sprintf("ObjectContentLength(\"TestAccount\", \"TestContainer\", \"FooBar\") didn't return expected Content-Length")
		t.Fatalf(tErr)
	}

	// Send a range GET of bytes at offset 1 for length 3 for object "FooBar" expecting []byte{0xBB, 0xCC, 0xDD}

	getBuf, err := ObjectGet("TestAccount", "TestContainer", "FooBar", uint64(1), uint64(3))
	if nil != err {
		tErr := fmt.Sprintf("ObjectGet(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), uint64(3)) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, getBuf) {
		tErr := fmt.Sprintf("ObjectGet(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), uint64(3)) didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Send a range READ of bytes at offset 1 for length 3 for object "FooBar" expecting []byte{0xBB, 0xCC, 0xDD}

	readLen, err := ObjectRead("TestAccount", "TestContainer", "FooBar", uint64(1), readBuf)
	if nil != err {
		tErr := fmt.Sprintf("ObjectRead(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), readBuf) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 3 != readLen {
		tErr := fmt.Sprintf("ObjectRead(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), readBuf) didn't return expected len")
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, readBuf) {
		tErr := fmt.Sprintf("ObjectRead(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), readBuf) didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Send a full GET for object "FooBar" expecting []byte{0xAA, 0xBB, 0xCC, 0xDD, OxEE}

	loadBuf, err := ObjectLoad("TestAccount", "TestContainer", "FooBar")
	if nil != err {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBar\") failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}, loadBuf) {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBar\") didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Send a tail GET of the last 2 bytes for object "FooBar" expecting []byte{0xDD, 0xEE}

	tailBuf, err := ObjectTail("TestAccount", "TestContainer", "FooBar", uint64(2))
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

	err = ObjectCopy(tOCCS.srcAccountName, tOCCS.srcContainerName, tOCCS.srcObjectName, tOCCS.dstAccountName, tOCCS.dstContainerName, tOCCS.dstObjectName, tOCCS)
	if nil != err {
		tErr := fmt.Sprintf("ObjectCopy(\"TestAccount/TestContainer/FooBar\" to \"TestAccount/TestContainer/FooBarCopy\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a full GET for object "FooBarCopy" expecting []byte{0xAA, 0xBB, 0xCC, 0xDD, OxEE}

	loadBuf, err = ObjectLoad("TestAccount", "TestContainer", "FooBarCopy")
	if nil != err {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBarCopy\") failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}, loadBuf) {
		tErr := fmt.Sprintf("ObjectLoad(\"TestAccount\", \"TestContainer\", \"FooBarCopy\") didn't return expected []byte")
		t.Fatalf(tErr)
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{"FooBar", "FooBarCopy"}

	containerHeaders, objectList, err = ContainerGet("TestAccount", "TestContainer")
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

	err = ObjectDelete("TestAccount", "TestContainer", "FooBar", 0)
	if nil != err {
		tErr := fmt.Sprintf("ObjectDelete(\"TestAccount\", \"TestContainer\". \"FooBar\", 0) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{"FooBarCopy"}

	containerHeaders, objectList, err = ContainerGet("TestAccount", "TestContainer")
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

	err = ObjectDelete("TestAccount", "TestContainer", "FooBarCopy", 0)
	if nil != err {
		tErr := fmt.Sprintf("ObjectDelete(\"TestAccount\", \"TestContainer\". \"FooBarCopy\", 0) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for container "TestContainer" expecting header Cat: Dog and objectList []string{}

	containerHeaders, objectList, err = ContainerGet("TestAccount", "TestContainer")
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

	err = ContainerDelete("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerDelete(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a DELETE for container "Z"

	err = ContainerDelete("TestAccount", "Z")
	if nil != err {
		t.Fatalf("ContainerDelete(\"TestAccount\", \"Z\") failed: %v", err)
	}

	// create and delete container "TestContainer" again so we're sure the retry code is hit

	err = ContainerPut("TestAccount", "TestContainer", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("ContainerPut(\"TestAccount\", \"TestContainer\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}
	err = ContainerDelete("TestAccount", "TestContainer")
	if nil != err {
		tErr := fmt.Sprintf("ContainerDelete(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a GET for account "TestAccount" expecting header Cat: Dog and containerList []string{}

	accountHeaders, containerList, err = AccountGet("TestAccount")
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

	err = AccountDelete("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountDelete(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Create and delete "TestAccount" again so we're sure the retry code is hit

	err = AccountPut("TestAccount", catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("AccountPut(\"TestAccount\", catDogHeaderMap) failed: %v", err)
		t.Fatalf(tErr)
	}
	err = AccountDelete("TestAccount")
	if nil != err {
		tErr := fmt.Sprintf("AccountDelete(\"TestAccount\") failed: %v", err)
		t.Fatalf(tErr)
	}

}

// Extended testing of chunked put interface to exercise internal retries
//
func testChunkedPut(t *testing.T) {

	// preserve the original settings of these globals that we change
	var (
		chaosFetchChunkedPutFailureRate = globals.chaosFetchChunkedPutFailureRate
		chaosSendChunkFailureRate       = globals.chaosSendChunkFailureRate
		chaosCloseChunkFailureRate      = globals.chaosCloseChunkFailureRate
		retryLimitObject                = globals.retryLimitObject
		chunkedConnectionPoolSize       = globals.chunkedConnectionPool.poolCapacity

		cleanup = func() {
			globals.chaosFetchChunkedPutFailureRate = chaosFetchChunkedPutFailureRate
			globals.chaosSendChunkFailureRate = chaosSendChunkFailureRate
			globals.chaosCloseChunkFailureRate = chaosCloseChunkFailureRate
			globals.retryLimitObject = retryLimitObject
			globals.chunkedConnectionPool.poolCapacity = chunkedConnectionPoolSize
		}
	)
	defer cleanup()

	var (
		accountName   = "TestAccount"
		containerName = "TestContainer"
		objNameFmt    = "chunkObj%d"
		objName       string
	)

	// increase the pool sizes to get some concurrency (non-chunked isn't tested here)
	poolSize := 12
	globals.chunkedConnectionPool.poolCapacity = uint16(poolSize)
	globals.chunkedConnectionPool.lifoOfActiveConnections = make([]*connectionStruct, poolSize)
	for i := 0; i < poolSize; i++ {
		globals.chunkedConnectionPool.lifoOfActiveConnections[i] = nil
	}
	globals.chunkedConnectionPool.lifoIndex = 0

	// (lack of) headers for putting
	catDogHeaderMap := make(map[string][]string)

	// (re)create the test account and container

	err := AccountPut(accountName, catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("testChunkedPut.AccountPut('%s', catDogHeaderMap) failed: %v", accountName, err)
		t.Fatalf(tErr)
	}

	err = ContainerPut(accountName, containerName, catDogHeaderMap)
	if nil != err {
		tErr := fmt.Sprintf("testChunkedPut.ContainerPut('%s', '%s', catDogHeaderMap) failed: %v",
			accountName, containerName, err)
		t.Fatalf(tErr)
	}

	// Create an object and perform a sequence of randomly sized writes that
	// total upto 11 Mibyte (maximum).  Inject errors every 23rd
	// SendChunk(), every 3rd FetchContext(), and every 5th Close().
	// Because ramwswift is also injecting an error every 3rd chunked put
	// increase globals.retryLimitObject for this test because a series of
	// failures at different levels pushes us over the base limit.
	//
	// Since the size of the writes is random it could require more than 23
	// writes to reach 11 Mibyte, it which case the test would fail.
	// However, the sequence of "random" numbers from randGen is the same
	// every time so this test always passes (unless the seed is changed).
	globals.chaosFetchChunkedPutFailureRate = 3
	globals.chaosSendChunkFailureRate = 20
	globals.chaosCloseChunkFailureRate = 5
	globals.retryLimitObject = 6

	randGen := rand.New(rand.NewSource(2))
	doneChan := make(chan error, 1)
	for i := 0; i < 6; i++ {
		objName = fmt.Sprintf(objNameFmt, i)
		objSize := ((i % 11) + 1) * 1024 * 1024

		// start an ObjectWriteVerify and wait for it to complete
		testObjectWriteVerifyThreaded(t, accountName, containerName, objName, objSize,
			randGen, doneChan)
		err = <-doneChan
		if err != nil {
			fmt.Printf("testChunkedPut: FATAL ERROR: %v\n", err)
			t.Error(err)
		}
	}

	// Now do the same thing with concurrent threads.  Because the threads
	// are running non-deterministically there is no real bound on the
	// number of simulated errors a thread might see, so crank up the
	// failure rates (less failures) and increase allowed retries
	// significantly.  Even so, its possible that a spurious error might
	// occur (in which case the failure rates could be made even higher).
	globals.chaosSendChunkFailureRate = globals.chaosSendChunkFailureRate * 3
	globals.chaosCloseChunkFailureRate = 11
	globals.retryLimitObject = 10

	maxThread := 6
	doneChan = make(chan error, maxThread)
	threads := 0
	for i := 0; i < 18; i++ {
		objName = fmt.Sprintf(objNameFmt, i)
		randGen = rand.New(rand.NewSource(int64(i) + 3))

		objSize := randGen.Intn(12 * 1024 * 1024)

		// fire off a go routine to do the work and count it
		threads++
		go testObjectWriteVerifyThreaded(t, accountName, containerName, objName, objSize,
			randGen, doneChan)

		// if we've hit the maximum number of concurrent threads
		// (requests) wait for 1 to finish
		for threads >= maxThread {

			// trigger garbage collection
			runtime.GC()

			err = <-doneChan
			threads--
			if err != nil {
				t.Error(err)
			}
		}
	}
	for threads > 0 {
		err = <-doneChan
		threads--
		if err != nil {
			t.Error(err)
		}
	}

	// cleanup the mess we made (objects, container, and account)
	err = ContainerDelete(accountName, containerName)
	if nil != err {
		tErr := fmt.Sprintf("ContainerDelete('%s', '%s') failed: %v", accountName, containerName, err)
		t.Error(tErr)
	}

	err = AccountDelete(accountName)
	if nil != err {
		tErr := fmt.Sprintf("AccountDelete('%s') failed: %v", accountName, err)
		t.Error(tErr)
	}
}

// Only the "top" testing goroutine can call t.Fatal() or t.Error() and be
// heard; others are ignored.  Since this is called by a child, it can't call
// one of those routines and all errors must be returned for handling.
//
func testObjectWriteVerifyThreaded(t *testing.T, accountName string, containerName string, objName string,
	objSize int, randGen *rand.Rand, doneChan chan error) {

	// write the object and delete it (even if the verify failed)
	verifyErr := testObjectWriteVerify(t, accountName, containerName, objName, objSize, randGen)
	deleteErr := ObjectDelete(accountName, containerName, objName, 0)

	// if the write failed report it
	if verifyErr != nil {
		tErr := fmt.Errorf("testChunkedPut.testObjectWriteVerify('%s/%s/%s', %d) failed: %v",
			accountName, containerName, objName, objSize, verifyErr)
		doneChan <- tErr
		return
	}

	// if the write succeeded but the verify failed, report it
	if deleteErr != nil {
		tErr := fmt.Errorf("ObjectDelete('%s/%s/%s', %d) failed: %v",
			accountName, containerName, objName, objSize, deleteErr)
		doneChan <- tErr
		return
	}

	doneChan <- nil
	return
}

// write objSize worth of random bytes to the object using nWrite calls to
// SendChunk() and then read it back to verify.
//
// Only the "top" testing goroutine can call t.Fatal() or t.Error() and be
// heard; others are ignored.  Since this is called by a child, it can't call
// one of those routines and all errors must be returned for handling.
//
func testObjectWriteVerify(t *testing.T, accountName string, containerName string, objName string,
	objSize int, randGen *rand.Rand) (err error) {

	writeBuf := make([]byte, objSize)
	readBuf := make([]byte, 0)

	for i := 0; i < objSize; i++ {
		writeBuf[i] = byte(randGen.Uint32())
	}
	if writeBuf[0] == 0 && writeBuf[1] == 0 && writeBuf[2] == 0 && writeBuf[3] == 0 {
		err = fmt.Errorf("testObjectWriteVerify(): randGen is not very random")
		return
	}
	if writeBuf[objSize-1] == 0 && writeBuf[objSize-2] == 0 && writeBuf[objSize-3] == 0 &&
		writeBuf[objSize-4] == 0 {
		err = fmt.Errorf("testObjectWriteVerify(): randGen is not very random at end of buffer")
		return
	}

	// Start a chunked PUT for the object
	chunkedPutContext, err := ObjectFetchChunkedPutContext(accountName, containerName, objName, "")
	if nil != err {
		err = fmt.Errorf("testObjectWriteVerify(): ObjectFetchChunkedPutContext('%s/%s/%s') failed: %v",
			accountName, containerName, objName, err)
		return
	}

	var (
		off int
		sz  int
		nio int
	)

	for off = 0; off < objSize; off += sz {
		// write a random number of bytes, but at least 32
		sz = randGen.Intn(objSize-off) + 1
		if sz < 32 {
			sz = 32
			if off+sz > objSize {
				sz = objSize - off
			}
		}

		err = chunkedPutContext.SendChunk(writeBuf[off : off+sz])
		if nil != err {
			tErr := fmt.Errorf("testObjectWriteVerify('%s/%s/%s'): "+
				"chunkedPutContext.SendChunk(writeBuf[%d:%d]) failed: %v",
				accountName, containerName, objName, off, off+sz, err)
			return tErr
		}
		nio++

		// every 16 i/o requests, or so, wait upto 30 sec
		// if randGen.Intn(16) == 0 {
		// 	waitSec := randGen.Intn(31)
		// 	fmt.Printf("object %10s sleeping %d sec after send\n", objName, waitSec)
		// 	time.Sleep(time.Duration(waitSec) * time.Second)
		// }
	}

	// every 8 i/o requests, or so, wait upto 60 sec before Close()
	// if randGen.Intn(4) == 0 {
	// 	waitSec := randGen.Intn(61)
	// 	fmt.Printf("object %10s sleeping %d sec before close\n", objName, waitSec)
	// 	time.Sleep(time.Duration(waitSec) * time.Second)
	// }

	// verify bytes using Read() at 16 random ranges
	for nio = 0; nio < 16; nio++ {
		off = randGen.Intn(objSize)
		sz = randGen.Intn(objSize-off) + 1

		readBuf, err = chunkedPutContext.Read(uint64(off), uint64(sz))
		if err != nil {
			tErr := fmt.Errorf("testObjectWriteVerify('%s/%s/%s'): "+
				"chunkedPutContext.Read(%d, %d) failed: %v",
				accountName, containerName, objName, off, sz, err)
			// panic because this should *never* happen
			panic(tErr)
			// return tErr
		}

		if !bytes.Equal(readBuf, writeBuf[off:off+sz]) {
			tErr := fmt.Errorf("testObjectWriteVerify('%s/%s/%s'): "+
				"chunkedPutContext.Read(%d, %d) dat does not match SendChunk()",
				accountName, containerName, objName, off, sz)
			// panic because this should *never* happen
			panic(tErr)
			// return tErr
		}
	}

	err = chunkedPutContext.Close()
	if nil != err {
		tErr := fmt.Errorf("testObjectWriteVerify('%s/%s/%s'): chunkedPutContext.Close() failed: %v",
			accountName, containerName, objName, err)
		return tErr
	}

	// read and compare
	readBuf, err = ObjectLoad(accountName, containerName, objName)
	if nil != err {
		tErr := fmt.Errorf("testObjectWriteVerify('%s/%s/%s'): ObjectLoad() failed: %v",
			accountName, containerName, objName, err)
		return tErr
	}
	if !bytes.Equal(readBuf, writeBuf) {
		tErr := fmt.Errorf("testObjectWriteVerify('%s/%s/%s'): "+
			"Object() read back something different then written",
			accountName, containerName, objName)
		// panic because this should *never* happen
		panic(tErr)
		// return tErr
	}

	return nil
}

// Parse a log entry generated by testRetry and return the important values as
// strings, where the definition of what's important is somewhat arbitrary.
//
// matching log entries look like:
//
// time="2017-07-27T01:30:46Z" level=info msg="retry.RequestWithRetry(): swiftclient.testRetry.request(1) succeeded after 4 attempts in 0.031 sec" function=RequestWithRetry goroutine=6 package=swiftclient
//
// time="2017-07-27T02:18:19Z" level=error msg="retry.RequestWithRetry(): swiftclient.testRetry.request(1) failed after 7 attempts in 0.053 sec with retriable error" error="Simulate a retriable errror" function=RequestWithRetry goroutine=6 package=swiftclient
//
// time="2017-07-27T02:09:32Z" level=error msg="retry.RequestWithRetry(): swiftclient.testRetry.request(1) failed after 6 attempts in 0.054 sec with unretriable error" error="Simulate an unretriable error" function=RequestWithRetry goroutine=20 package=swiftclient
//
func parseRetryLogEntry(entry string) map[string]string {
	var (
		fields  = make(map[string]string)
		matches []string
	)

	var fieldRE = regexp.MustCompile(
		`^time="([-:0-9.A-Z]+)" level=([a-zA-Z]+) msg="([^"]+)" (error="([^"]+)")? ?function=(\w+) (.*)`)

	matches = fieldRE.FindStringSubmatch(entry)
	if matches == nil {
		return nil
	}
	fields["time"] = matches[1]
	fields["level"] = matches[2]
	fields["msg"] = matches[3]
	fields["error"] = matches[5]
	fields["function"] = matches[6]

	var msgRE = regexp.MustCompile(
		`^retry.RequestWithRetry\(\): swiftclient.testRetry.request\((\d+)\) (succeeded|failed) after (\d+) attempts in (\d+\.\d*) sec( with (retriable|unretriable) error)?$`)

	matches = msgRE.FindStringSubmatch(fields["msg"])
	if matches == nil {
		return nil
	}
	fields["reqno"] = matches[1]
	fields["result"] = matches[2]
	fields["attempts"] = matches[3]
	fields["seconds"] = matches[4]
	fields["retriable"] = matches[6]

	return fields
}

// Test the retry handler
func testRetry(t *testing.T) {

	// a new log target to capture messages written to the log
	var logcopy logger.LogTarget
	logcopy.Init(50)

	// logcopy will live on long after this function returns
	logger.AddLogTarget(logcopy)

	// request is a function that, through the miracle of closure, uses and
	// updates these variables so we can use them to affect its behavior
	// when its called by by RequestWithRetry()
	var (
		callCnt       int = 0
		successOn     int = 2
		unretriableOn int = 2
	)
	request := func() (bool, error) {
		callCnt++
		if successOn == callCnt {
			return false, nil
		}
		if unretriableOn == callCnt {
			return false, errors.New("Simulate an unretriable error")
		}
		return true, errors.New("Simulate a retriable errror")
	}

	type requestStatisticsIncarnate struct {
		RetryOps          string
		RetrySuccessCnt   string
		ClientRequestTime bucketstats.BucketLog2Round
		ClientFailureCnt  bucketstats.Total
		SwiftRequestTime  bucketstats.BucketLog2Round
		SwiftRetryOps     bucketstats.Average
	}

	var (
		opname   string
		retryObj *RetryCtrl

		reqStat = requestStatisticsIncarnate{
			RetryOps:        "proxyfs.switclient.test.operations",
			RetrySuccessCnt: "proxyfs.switclient.test.success.operations",
		}
		statNm = requestStatistics{
			retryCnt:          &reqStat.RetryOps,
			retrySuccessCnt:   &reqStat.RetrySuccessCnt,
			clientRequestTime: &reqStat.ClientRequestTime,
			clientFailureCnt:  &reqStat.ClientFailureCnt,
			swiftRequestTime:  &reqStat.SwiftRequestTime,
			swiftRetryOps:     &reqStat.SwiftRetryOps,
		}
	)

	// the statistics should be registered before use
	bucketstats.Register("swiftclient", "api_test", &reqStat)
	defer bucketstats.UnRegister("swiftclient", "api_test")

	// requests succeeds on first try (no log entry, stats not updated)
	//
	opname = "swiftclient.testRetry.request(1)"
	retryObj = NewRetryCtrl(5, 1*time.Second, 2.0)
	callCnt = 0
	successOn = 1
	unretriableOn = 0 // never happens
	testRetrySucceeds(t, &logcopy, opname, retryObj, statNm, request, successOn, 0.000, 1.000)

	// request succeeds after 3 retries (4th attempt) which should take
	// [30 msec, 60 msec) with expBackoff set to 1.0
	//
	opname = "swiftclient.testRetry.request(2)"
	retryObj = NewRetryCtrl(5, 10*time.Millisecond, 1.0)
	callCnt = 0
	successOn = 4
	unretriableOn = 0 // never happens
	testRetrySucceeds(t, &logcopy, opname, retryObj, statNm, request, successOn, 0.030, 0.060)

	// requests fails after 4 retries (5th attempt) with a retriable error
	// which should take [150 msec, 300 msec) with expBackoff set to 2.0
	//
	opname = "swiftclient.testRetry.request(3)"
	retryObj = NewRetryCtrl(4, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0     // no success
	unretriableOn = 0 // no unretriable failure
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 5, 0.150, 0.300, "retriable")

	// requests fails after 2 retries (3rd attempt) with an unretriable
	// error, which should take [30 msec, 60 msec) with expBackoff set to 2.0
	//
	opname = "swiftclient.testRetry.request(4)"
	retryObj = NewRetryCtrl(4, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0 // no success
	unretriableOn = 3
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 3, 0.030, 0.060, "unretriable")

	// retries disabled, no errors
	//
	opname = "swiftclient.testRetry.request(5)"
	retryObj = NewRetryCtrl(0, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 1 // success on first try
	unretriableOn = 0
	testRetrySucceeds(t, &logcopy, opname, retryObj, statNm, request, successOn, 0.000, 0.010)

	// retries disabled and request fails with retriable error
	//
	opname = "swiftclient.testRetry.request(6)"
	retryObj = NewRetryCtrl(0, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0 // no success
	unretriableOn = 0
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 1, 0.000, 0.010, "retriable")

	// retries disabled and request fails withan unretriable error
	//
	opname = "swiftclient.testRetry.request(7)"
	retryObj = NewRetryCtrl(0, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0 // no success
	unretriableOn = 1
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 1, 0.000, 0.010, "unretriable")
}

// Test an operation that succeeds on attempt number successOn after at least
// minSec of delay...but less than maxSec of delay.
//
// successOn may be 1, in which case no log messages should be generated and the
// retry counters are unchanged (because no retries occurred), else we expect
// properly formatted log messages and updated retry counters.
//
func testRetrySucceeds(t *testing.T, logcopy *logger.LogTarget,
	opname string, retryObj *RetryCtrl, reqStat requestStatistics,
	request func() (bool, error), successOn int, minSec float32, maxSec float32) {

	var (
		totalEntriesPre     int = logcopy.LogBuf.TotalEntries
		retryCntPre         uint64
		retryCntPost        uint64
		retrySuccessCntPre  uint64
		retrySuccessCntPost uint64
		statMap             map[string]uint64
		logEntry            string
		logval              map[string]string
		err                 error
	)
	statMap = stats.Dump()
	retryCntPre, _ = statMap[*reqStat.retryCnt]
	retrySuccessCntPre, _ = statMap[*reqStat.retrySuccessCnt]

	err = retryObj.RequestWithRetry(request, &opname, &reqStat)
	if err != nil {
		t.Errorf("%s: should have succeeded, error: %s", opname, err)
	}

	// validate proper log entry found, including number of attempts and
	// elapsed time
	logEntry = logcopy.LogBuf.LogEntries[0]
	logval = parseRetryLogEntry(logEntry)
	switch {
	case successOn == 1:
		if logcopy.LogBuf.TotalEntries != totalEntriesPre {
			t.Errorf("%s: should not have created a log entry", opname)
		}
	case logcopy.LogBuf.TotalEntries != totalEntriesPre+1:
		t.Errorf("%s: should have created exactly one log entry", opname)
	case logval == nil:
		t.Errorf("%s: log entry should exist, instead found: %s", opname, logEntry)
	case logval["result"] != "succeeded" || logval["retriable"] != "" ||
		logval["attempts"] == "" || logval["seconds"] == "":
		t.Errorf("%s: proper log entry should exist, instead found: %s", opname, logEntry)
	default:
		var (
			sec      float32
			attempts int
		)
		cnt, err := fmt.Sscanf(logval["seconds"], "%f", &sec)
		if err != nil || cnt != 1 {
			t.Errorf("%s: seconds not found in log entry: %s", opname, logEntry)
		}
		cnt, err = fmt.Sscanf(logval["attempts"], "%d", &attempts)
		if err != nil || cnt != 1 {
			t.Errorf("%s: attempts not found in log entry: %s", opname, logEntry)
		}

		if attempts != successOn {
			t.Errorf("%s: should have succeeded after %d attempts, log entry shows: %s",
				opname, successOn, logEntry)
		}

		if sec < minSec {
			t.Errorf("%s: elapsed time %4.3f sec outside bounds, should be [%4.3f, %4.3f)",
				opname, sec, minSec, maxSec)
		}
		if sec >= maxSec {
			t.Logf("%s: elapsed time %4.3f sec outside bounds, should be [%4.3f, %4.3f)",
				opname, sec, minSec, maxSec)
		}
	}

	// insure stats are updated correctly (unchanged if no retry occurred,
	// otherwise both are incremented)
	if successOn > 1 {
		retryCntPre++
		retrySuccessCntPre++
	}

	// stats sometimes take a little while to update, so wait a bit if we don't
	// get the right answer on the first try
	for try := 0; try < 500; try++ {
		statMap = stats.Dump()
		retryCntPost, _ = statMap[*reqStat.retryCnt]
		retrySuccessCntPost, _ = statMap[*reqStat.retrySuccessCnt]

		if retryCntPost == retryCntPost && retrySuccessCntPost == retrySuccessCntPre {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if retryCntPost != retryCntPre {
		t.Errorf("%s: stats updated incorrectly: retryOps is %d should be %d",
			opname, retryCntPost, retryCntPre)
	}
	if retrySuccessCntPost != retrySuccessCntPre {
		t.Errorf("%s: stats updated incorrectly: retrySuccessOps is %d should be %d",
			opname, retrySuccessCntPost, retrySuccessCntPre)
	}
}

// Test an operation that ultimately fails after some number of retries
// (possibly 0).  It should fail on the attempt number failOn, after at least
// minSec of delay...but less than maxSec of delay.
//
func testRetryFails(t *testing.T, logcopy *logger.LogTarget,
	opname string, retryObj *RetryCtrl, reqStat requestStatistics,
	request func() (bool, error), failOn int, minSec float32, maxSec float32, retryStr string) {

	var (
		totalEntriesPre     int = logcopy.LogBuf.TotalEntries
		retryCntPre         uint64
		retryCntPost        uint64
		retrySuccessCntPre  uint64
		retrySuccessCntPost uint64
		statMap             map[string]uint64
		logEntry            string
		logval              map[string]string
		err                 error
	)
	statMap = stats.Dump()
	retryCntPre, _ = statMap[*reqStat.retryCnt]
	retrySuccessCntPre, _ = statMap[*reqStat.retrySuccessCnt]

	err = retryObj.RequestWithRetry(request, &opname, &reqStat)
	if err == nil {
		t.Errorf("%s: should have failed, error: %s", opname, err)
	}

	// validate proper log entry found, including number of attempts and
	// elapsed time
	logEntry = logcopy.LogBuf.LogEntries[0]
	logval = parseRetryLogEntry(logEntry)
	switch {
	case logcopy.LogBuf.TotalEntries != totalEntriesPre+1:
		t.Errorf("%s: should have created exactly one log entry", opname)
	case logval == nil:
		t.Errorf("%s: log entry should exist, instead found: %s", opname, logEntry)
	case logval["result"] != "failed" || logval["retriable"] != retryStr ||
		logval["attempts"] == "" || logval["seconds"] == "":
		t.Errorf("%s: proper log entry should exist, instead found: %s", opname, logEntry)
	default:
		var (
			sec      float32
			attempts int
		)
		cnt, err := fmt.Sscanf(logval["seconds"], "%f", &sec)
		if err != nil || cnt != 1 {
			t.Errorf("%s: seconds not found in log entry: %s", opname, logEntry)
		}
		cnt, err = fmt.Sscanf(logval["attempts"], "%d", &attempts)
		if err != nil || cnt != 1 {
			t.Errorf("%s: attempts not found in log entry: %s", opname, logEntry)
		}

		if attempts != failOn {
			t.Errorf("%s: should have failed after %d attempts, log entry shows: %s",
				opname, failOn, logEntry)
		}

		if sec < minSec {
			t.Errorf("%s: elapsed time %4.3f sec outside bounds, should be [%4.3f, %4.3f)",
				opname, sec, minSec, maxSec)
		}
		if sec >= maxSec {
			t.Logf("%s: elapsed time %4.3f sec outside bounds, should be [%4.3f, %4.3f)",
				opname, sec, minSec, maxSec)
		}
	}

	// insure stats are updated correctly: retrySuccessOps is never incremented;
	// retryOps incremented only if retries are enabled
	if retryObj.attemptMax > 0 {
		retryCntPre++
	}

	// stats sometimes take a little while to update, so wait a bit if we don't
	// get the right answer on the first try
	for try := 0; try < 10; try++ {
		statMap = stats.Dump()
		retryCntPost, _ = statMap[*reqStat.retryCnt]
		retrySuccessCntPost, _ = statMap[*reqStat.retrySuccessCnt]

		if retryCntPost == retryCntPost && retrySuccessCntPost == retrySuccessCntPre {
			break
		}
		time.Sleep(time.Second)
	}
	if retryCntPost != retryCntPre {
		t.Errorf("%s: stats updated incorrectly: retryOps is %d should be %d",
			opname, retryCntPost, retryCntPre)
	}
	if retrySuccessCntPost != retrySuccessCntPre {
		t.Errorf("%s: stats updated incorrectly: retrySuccessOps is %d should be %d",
			opname, retrySuccessCntPost, retrySuccessCntPre)
	}
}
