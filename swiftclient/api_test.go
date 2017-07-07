package swiftclient

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/conf"

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
		"SwiftClient.RetryDelay=50ms",
		"SwiftClient.RetryLimit=10",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"Cluster.WhoAmI=Peer0",
		"FSGlobals.VolumeList=",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
	}

	catDogHeaderMap := make(map[string][]string)
	catDogHeaderMap["Cat"] = []string{"Dog"}

	mouseBirdHeaderMap := make(map[string][]string)
	mouseBirdHeaderMap["Mouse"] = []string{"Bird"}

	mouseDeleteHeaderMap := make(map[string][]string)
	mouseDeleteHeaderMap["Mouse"] = []string{""}

	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	signalHandlerIsArmed := false
	doneChan := make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon(confMap, &signalHandlerIsArmed, doneChan)

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

	// Send a PUT for account "TestAccount" and header Cat: Dog

	err = AccountPut("TestAccount", catDogHeaderMap)
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

	// Send a GET for account "TestAccount" expecting header Cat: Dog and containerList []string{"TestContainer"}

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
	if (1 != len(containerList)) || ("TestContainer" != containerList[0]) {
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

	// Retry the chunked PUT for object "FooBar"

	err = chunkedPutContext.Retry()
	if nil != err {
		tErr := fmt.Sprintf("chunkedPutContext.Retry() failed: %v", err)
		t.Fatalf(tErr)
	}

	// Finish the retry'd chunked PUT for object "FooBar"

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

	tailBuf, fooBarSize, err := ObjectTail("TestAccount", "TestContainer", "FooBar", uint64(2))
	if nil != err {
		tErr := fmt.Sprintf("ObjectTail(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(2)) failed: %v", err)
		t.Fatalf(tErr)
	}
	if 0 != bytes.Compare([]byte{0xDD, 0xEE}, tailBuf) {
		tErr := fmt.Sprintf("ObjectTail(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(2)) didn't return expected []byte")
		t.Fatalf(tErr)
	}
	if 5 != fooBarSize {
		tErr := fmt.Sprintf("ObjectTail(\"TestAccount\", \"TestContainer\", \"FooBar\", uint64(2)) didn't return expected object size (got %v)", fooBarSize)
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
