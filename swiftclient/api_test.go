package swiftclient

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/stats"
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
	confStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
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
		"SwiftClient.StarvationCallbackFrequency=100ms",

		"Cluster.WhoAmI=Peer0",

		"Peer:Peer0.ReadCacheQuotaFraction=0.20",

		"FSGlobals.VolumeList=",

		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",

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

		"RamSwiftChaos.ObjectDeleteFailureRate=2",
		"RamSwiftChaos.ObjectGetFailureRate=2",
		"RamSwiftChaos.ObjectHeadFailureRate=2",
		"RamSwiftChaos.ObjectPostFailureRate=2",
		"RamSwiftChaos.ObjectPutFailureRate=2",
	}

	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = logger.Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("logger.Up(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	signalHandlerIsArmed := false
	doneChan := make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon("/dev/null", confStrings, &signalHandlerIsArmed, doneChan, unix.SIGTERM)

	for !signalHandlerIsArmed {
		time.Sleep(100 * time.Millisecond)
	}

	err = stats.Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("stats.Up(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	err = Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("Up(confMap) failed: %v", err)
		t.Fatalf(tErr)
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

	err = Down()
	if nil != err {
		tErr := fmt.Sprintf("Down() failed: %v", err)
		t.Fatalf(tErr)
	}

	err = stats.Down()
	if nil != err {
		tErr := fmt.Sprintf("stats.Down() failed: %v", err)
		t.Fatalf(tErr)
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

	chunkedPutContext, err := ObjectFetchChunkedPutContext("TestAccount", "TestContainer", "FooBar")
	if nil != err {
		tErr := fmt.Sprintf("ObjectFetchChunkedPutContext(\"TestAccount\", \"TestContainer\") failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a chunk for object "FooBar" of []byte{0xAA, 0xBB}

	err = chunkedPutContext.SendChunkAsSlice([]byte{0xAA, 0xBB})
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.SendChunkAsSlice([]byte{0xAA, 0xBB}) failed: %v", err)
		t.Fatalf(tErr)
	}

	// Send a chunk for object "FooBar" of []byte{0xCC, 0xDD, 0xEE}

	err = chunkedPutContext.SendChunkAsSlice([]byte{0xCC, 0xDD, 0xEE})
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.SendChunkAsSlice([]byte{0xCC, 0xDD, 0xEE}) failed: %v", err)
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

	readBuf, err := chunkedPutContext.ReadReturnSlice(uint64(1), uint64(3))
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.ReadReturnSlice(uint64(1), uint64(3)) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, readBuf) {
		t.Fatalf("chunkedPutContext.ReadReturnSlice(uint64(1), uint64(3)) didn't return expected []byte")
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

	getBuf, err := ObjectGetReturnSlice("TestAccount", "TestContainer", "FooBar", uint64(1), uint64(3))
	if nil != err {
		tErr := fmt.Sprintf("ObjectGetReturnSlice(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), uint64(3)) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, getBuf) {
		tErr := fmt.Sprintf("ObjectGetReturnSlice(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(1), uint64(3)) didn't return expected []byte")
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

	// Send a Synchronous DELETE for object "FooBar"

	err = ObjectDeleteSync("TestAccount", "TestContainer", "FooBar")
	if nil != err {
		tErr := fmt.Sprintf("ObjectDeleteSync(\"TestAccount\", \"TestContainer\". \"FooBar\") failed: %v", err)
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

	// Send a Asynchronous DELETE for object "FooBarCopy"

	wgPreCondition := &sync.WaitGroup{}
	wgPostSignal := &sync.WaitGroup{}

	wgPreCondition.Add(1)
	wgPostSignal.Add(1)

	ObjectDeleteAsync("TestAccount", "TestContainer", "FooBarCopy", wgPreCondition, wgPostSignal)

	wgPreCondition.Done()
	wgPostSignal.Wait()

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

	// preserve the original settings
	var (
		chaosSendChunkFailureRate       = globals.chaosSendChunkFailureRate
		chaosFetchChunkedPutFailureRate = globals.chaosFetchChunkedPutFailureRate
		cleanup                         = func() {
			globals.chaosSendChunkFailureRate = chaosSendChunkFailureRate
			globals.chaosFetchChunkedPutFailureRate = chaosFetchChunkedPutFailureRate
		}
	)
	defer cleanup()
	var (
		accountName   = "TestAccount"
		containerName = "TestContainer"
		objNameFmt    = "chunkObj%d"
		objName       string
	)

	// (lack of) headers for putting
	catDogHeaderMap := make(map[string][]string)

	// (re)create the test account and continer

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

	// create an object and write 4 Kbyte to it in 4 chunks with a SendChunk
	// failure every 7th
	globals.chaosSendChunkFailureRate = 7
	globals.chaosFetchChunkedPutFailureRate = 3

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

		err = ObjectDeleteSync(accountName, containerName, objName)
		if nil != err {
			tErr := fmt.Sprintf("ObjectDelete('%s', '%s', '%s') failed: %v",
				accountName, containerName, objName, err)
			t.Fatalf(tErr)
		}
	}

	err = ContainerDelete(accountName, containerName)
	if nil != err {
		tErr := fmt.Sprintf("ContainerDelete('%s', '%s') failed: %v", accountName, containerName, err)
		t.Fatalf(tErr)
	}

	err = AccountDelete(accountName)
	if nil != err {
		tErr := fmt.Sprintf("AccountDelete('%s') failed: %v", accountName, err)
		t.Fatalf(tErr)
	}
}

// write objSize worth of random bytes to the object using nWrite calls to
// SendChunkAsSlice() and then read it back to verify.
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
	chunkedPutContext, err := ObjectFetchChunkedPutContext(accountName, containerName, objName)
	if nil != err {
		tErr := fmt.Sprintf("ObjectFetchChunkedPutContext('%s', '%s', '%s') failed: %v",
			accountName, containerName, objName, err)
		return errors.New(tErr)
	}

	wsz := len(writeBuf) / nwrite
	for off := 0; off < len(writeBuf); off += wsz {
		if off+wsz < objSize {
			err = chunkedPutContext.SendChunkAsSlice(writeBuf[off : off+wsz])
		} else {
			err = chunkedPutContext.SendChunkAsSlice(writeBuf[off:])
		}
		if nil != err {
			tErr := fmt.Sprintf("chunkedPutContext.SendChunkAsSlice(writeBuf[%d:%d]) failed: %v",
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
	readBuf, err = ObjectLoad(accountName, containerName, objName)
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
		`^time="([-:0-9A-Z]+)" level=([a-zA-Z]+) msg="([^"]+)" (error="([^"]+)")? ?function=(\w+) (.*)`)

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

	var (
		retryOps                    = "proxyfs.switclient.test.operations"
		retrySuccessOps             = "proxyfs.switclient.test.success.operations"
		statNm          RetryStatNm = RetryStatNm{retryCnt: &retryOps, retrySuccessCnt: &retrySuccessOps}
		opname          string
		retryObj        *RetryCtrl
	)

	// requests succeeds on first try (no log entry, stats not updated)
	//
	opname = "swiftclient.testRetry.request(1)"
	retryObj = NewRetryCtrl(5, 1*time.Second, 2.0)
	callCnt = 0
	successOn = 1
	unretriableOn = 0 // never happens
	testRetrySucceeds(t, &logcopy, opname, retryObj, statNm, request, successOn, 0.0)

	// request succeeds after 3 retries (4th attempt) which should take 30
	// msec with expBackoff set to 1.0
	//
	opname = "swiftclient.testRetry.request(2)"
	retryObj = NewRetryCtrl(5, 10*time.Millisecond, 1.0)
	callCnt = 0
	successOn = 4
	unretriableOn = 0 // never happens
	testRetrySucceeds(t, &logcopy, opname, retryObj, statNm, request, successOn, 0.030)

	// requests fails after 4 retries (5th attempt) with a retriable error
	// which should take 150 msec with expBackoff set to 2.0
	//
	opname = "swiftclient.testRetry.request(3)"
	retryObj = NewRetryCtrl(4, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0     // no success
	unretriableOn = 0 // no unretriable failure
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 5, 0.150, "retriable")

	// requests fails after 2 retries (3rd attempt) with an unretriable
	// error, which should take 30 msec with expBackoff set to 2.0
	//
	opname = "swiftclient.testRetry.request(4)"
	retryObj = NewRetryCtrl(4, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0 // no success
	unretriableOn = 3
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 3, 0.030, "unretriable")

	// retries disabled, no errors
	//
	opname = "swiftclient.testRetry.request(5)"
	retryObj = NewRetryCtrl(0, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 1 // success on first try
	unretriableOn = 0
	testRetrySucceeds(t, &logcopy, opname, retryObj, statNm, request, successOn, 0.0)

	// retries disabled and request fails with retriable error
	//
	opname = "swiftclient.testRetry.request(6)"
	retryObj = NewRetryCtrl(0, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0 // no success
	unretriableOn = 0
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 1, 0.0, "retriable")

	// retries disabled and request fails withan unretriable error
	//
	opname = "swiftclient.testRetry.request(7)"
	retryObj = NewRetryCtrl(0, 10*time.Millisecond, 2.0)
	callCnt = 0
	successOn = 0 // no success
	unretriableOn = 1
	testRetryFails(t, &logcopy, opname, retryObj, statNm, request, 1, 0.0, "unretriable")
}

// Test an operation that succeeds on attempt number successOn after at least
// totalSec of delay.
//
// successOn may be 1, in which case no log messages should be generated and the
// retry counters are unchanged (because no retries occurred), else we expect
// properly formatted log messages and updated retry counters.
//
func testRetrySucceeds(t *testing.T, logcopy *logger.LogTarget,
	opname string, retryObj *RetryCtrl, retryStatNm RetryStatNm,
	request func() (bool, error), successOn int, totalSec float32) {

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
	retryCntPre, _ = statMap[*retryStatNm.retryCnt]
	retrySuccessCntPre, _ = statMap[*retryStatNm.retrySuccessCnt]

	err = retryObj.RequestWithRetry(request, &opname, &retryStatNm)
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
		// allow upto 20 msec slop for request to complete
		if sec < totalSec || sec > totalSec+0.020 {
			t.Errorf("%s: elapsed time %4.3f sec outside bounds, should be (%4.3f, %4.3f)",
				opname, sec, totalSec, totalSec+0.020)
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
	for try := 0; try < 10; try++ {
		statMap = stats.Dump()
		retryCntPost, _ = statMap[*retryStatNm.retryCnt]
		retrySuccessCntPost, _ = statMap[*retryStatNm.retrySuccessCnt]

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

// Test an operation that ultimately fails after some number of retries
// (possibly 0).  It should fail on the attempt number failOn, after at least
// totalSec of delay.
//
func testRetryFails(t *testing.T, logcopy *logger.LogTarget,
	opname string, retryObj *RetryCtrl, retryStatNm RetryStatNm,
	request func() (bool, error), failOn int, totalSec float32, retryStr string) {

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
	retryCntPre, _ = statMap[*retryStatNm.retryCnt]
	retrySuccessCntPre, _ = statMap[*retryStatNm.retrySuccessCnt]

	err = retryObj.RequestWithRetry(request, &opname, &retryStatNm)
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
		// allow upto 20 msec slop for request to complete
		if sec < totalSec || sec > totalSec+0.020 {
			t.Errorf("%s: elapsed time %4.3f sec outside bounds, should be (%4.3f, %4.3f)",
				opname, sec, totalSec, totalSec+0.020)
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
		retryCntPost, _ = statMap[*retryStatNm.retryCnt]
		retrySuccessCntPost, _ = statMap[*retryStatNm.retrySuccessCnt]

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
