// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/iswift/iswiftpkg"
	"github.com/NVIDIA/proxyfs/retryrpc"
	"github.com/NVIDIA/sortedmap"
)

const (
	testFileInodeExtentMapCacheEvictLowLimit  uint64 = 100
	testFileInodeExtentMapCacheEvictHighLimit uint64 = 110

	testFileInodeExtentMapMaxKeysPerNode = 100
)

type testInodeHeadLayoutEntryV1Struct struct {
	bytesWritten    uint64
	bytesReferenced uint64
}

type testFileInodeStruct struct {
	inodeHeadV1                              *ilayout.InodeHeadV1Struct
	layoutAsMap                              map[uint64]*testInodeHeadLayoutEntryV1Struct // key == ilayout.InodeHeadLayoutEntryV1Struct; value == remaining fields of ilayout.InodeHeadLayoutEntryV1Struct
	superBlockInodeObjectCountAdjustment     int64                                        // to be sent in next PutInodeTableEntriesRequest
	superBlockInodeBytesWrittenAdjustment    int64                                        // to be sent in next PutInodeTableEntriesRequest
	superBlockInodeBytesReferencedAdjustment int64                                        // to be sent in next PutInodeTableEntriesRequest
	dereferencedObjectNumberArray            []uint64                                     // to be sent in next PutInodeTableEntriesRequest
	extentMapCache                           sortedmap.BPlusTreeCache
	extentMap                                sortedmap.BPlusTree
	putObjectNumber                          uint64
	putObjectBuffer                          *bytes.Buffer //                                if nil, testFileInodeStruct not open
}

func newTestFileInode(inodeNumber uint64) (testFileInode *testFileInodeStruct) {
	var (
		timeNow = time.Now()
	)

	testFileInode = &testFileInodeStruct{
		inodeHeadV1: &ilayout.InodeHeadV1Struct{
			InodeNumber:         inodeNumber,
			InodeType:           ilayout.InodeTypeFile,
			LinkTable:           []ilayout.InodeLinkTableEntryStruct{},
			Size:                0,
			ModificationTime:    timeNow,
			StatusChangeTime:    timeNow,
			Mode:                ilayout.InodeModeMask,
			UserID:              0,
			GroupID:             0,
			StreamTable:         []ilayout.InodeStreamTableEntryStruct{},
			PayloadObjectNumber: ilayout.CheckPointObjectNumber,
			PayloadObjectOffset: 0,
			PayloadObjectLength: 0,
			SymLinkTarget:       "",
			Layout:              []ilayout.InodeHeadLayoutEntryV1Struct{},
		},
		layoutAsMap:                              make(map[uint64]*testInodeHeadLayoutEntryV1Struct),
		superBlockInodeObjectCountAdjustment:     0,
		superBlockInodeBytesWrittenAdjustment:    0,
		superBlockInodeBytesReferencedAdjustment: 0,
		dereferencedObjectNumberArray:            []uint64{},
		extentMapCache:                           sortedmap.NewBPlusTreeCache(testFileInodeExtentMapCacheEvictLowLimit, testFileInodeExtentMapCacheEvictHighLimit),
		extentMap:                                nil, // filled in just below
		putObjectNumber:                          ilayout.CheckPointObjectNumber,
		putObjectBuffer:                          nil,
	}

	testFileInode.extentMap = sortedmap.NewBPlusTree(testFileInodeExtentMapMaxKeysPerNode, sortedmap.CompareUint64, testFileInode, testFileInode.extentMapCache)

	return
}

func oldTestFileInode(inodeHeadV1 *ilayout.InodeHeadV1Struct) (testFileInode *testFileInodeStruct, err error) {
	var (
		inodeHeadLayoutEntryV1 ilayout.InodeHeadLayoutEntryV1Struct
	)

	if inodeHeadV1.InodeType != ilayout.InodeTypeFile {
		err = fmt.Errorf("oldTestFileInode() called with inodeHeadV1.InodeType == %v... must be ilayout.InodeTypeFile (%v)", inodeHeadV1.InodeType, ilayout.InodeTypeFile)
		return
	}

	testFileInode = &testFileInodeStruct{
		inodeHeadV1:                              inodeHeadV1,
		layoutAsMap:                              make(map[uint64]*testInodeHeadLayoutEntryV1Struct),
		superBlockInodeObjectCountAdjustment:     0,
		superBlockInodeBytesWrittenAdjustment:    0,
		superBlockInodeBytesReferencedAdjustment: 0,
		dereferencedObjectNumberArray:            []uint64{},
		extentMapCache:                           sortedmap.NewBPlusTreeCache(testFileInodeExtentMapCacheEvictLowLimit, testFileInodeExtentMapCacheEvictHighLimit),
		extentMap:                                nil, // filled in just below
		putObjectNumber:                          ilayout.CheckPointObjectNumber,
		putObjectBuffer:                          nil,
	}

	for _, inodeHeadLayoutEntryV1 = range testFileInode.inodeHeadV1.Layout {
		testFileInode.layoutAsMap[inodeHeadLayoutEntryV1.ObjectNumber] = &testInodeHeadLayoutEntryV1Struct{
			bytesWritten:    inodeHeadLayoutEntryV1.BytesWritten,
			bytesReferenced: inodeHeadLayoutEntryV1.BytesReferenced,
		}
	}

	testFileInode.extentMap, err = sortedmap.OldBPlusTree(inodeHeadV1.PayloadObjectNumber, inodeHeadV1.PayloadObjectOffset, inodeHeadV1.PayloadObjectLength, sortedmap.CompareUint64, testFileInode, testFileInode.extentMapCache)
	if nil != err {
		err = fmt.Errorf("sortedmap.OldBPlusTree() for InodeHeadV1: %#v failed: %v", inodeHeadV1, err)
		return
	}

	err = nil
	return
}

type testByObjectNumber []uint64

func (s testByObjectNumber) Len() int {
	return len(s)
}

func (s testByObjectNumber) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s testByObjectNumber) Less(i, j int) bool {
	return s[i] < s[j]
}

func (testFileInode *testFileInodeStruct) externalizeInodeHeadV1Layout() {
	var (
		inodeHeadLayoutEntryV1StructObjectNumberSlice testByObjectNumber
		layoutIndex                                   int
		objectNumber                                  uint64
	)

	inodeHeadLayoutEntryV1StructObjectNumberSlice = make(testByObjectNumber, len(testFileInode.layoutAsMap))
	testFileInode.inodeHeadV1.Layout = make([]ilayout.InodeHeadLayoutEntryV1Struct, len(testFileInode.layoutAsMap))

	// First fetch the objectNumber's from testFileInode.layoutAsMap
	// Note that iterating over a map could return keys (and their values) in any order

	layoutIndex = 0

	for objectNumber = range testFileInode.layoutAsMap {
		inodeHeadLayoutEntryV1StructObjectNumberSlice[layoutIndex] = objectNumber
		layoutIndex++
	}

	// Next, sort the list of objectNumber's
	// Having the list in sorted order makes comparison against expected results simple
	// Non-test implementations may skip this step

	sort.Sort(testByObjectNumber(inodeHeadLayoutEntryV1StructObjectNumberSlice))

	// Finally, iterate through inodeHeadLayoutEntryV1StructObjectNumberSlice to populate testFileInode.inodeHeadV1.Layout in sorted order

	for layoutIndex, objectNumber = range inodeHeadLayoutEntryV1StructObjectNumberSlice {
		testFileInode.inodeHeadV1.Layout[layoutIndex] = ilayout.InodeHeadLayoutEntryV1Struct{
			ObjectNumber:    objectNumber,
			BytesWritten:    testFileInode.layoutAsMap[objectNumber].bytesWritten,
			BytesReferenced: testFileInode.layoutAsMap[objectNumber].bytesReferenced,
		}
	}
}

func (testFileInode *testFileInodeStruct) openObject(objectNumber uint64) (err error) {
	if testFileInode.putObjectBuffer != nil {
		err = fmt.Errorf("openObject() called while putObjectBuffer set")
		return
	}

	testFileInode.superBlockInodeObjectCountAdjustment = 0
	testFileInode.superBlockInodeBytesWrittenAdjustment = 0
	testFileInode.superBlockInodeBytesReferencedAdjustment = 0
	testFileInode.dereferencedObjectNumberArray = []uint64{}

	testFileInode.putObjectNumber = objectNumber
	testFileInode.putObjectBuffer = new(bytes.Buffer)

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) closeObject() (err error) {
	var (
		putRequestHeaders http.Header
	)

	if testFileInode.putObjectBuffer == nil {
		err = fmt.Errorf("closeObject() called while putObjectBuffer unset")
		return
	}

	putRequestHeaders = make(http.Header)

	putRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	_, _, err = testDoHTTPRequest("PUT", testGlobals.containerURL+"/"+ilayout.GetObjectNameAsString(testFileInode.putObjectNumber), putRequestHeaders, testFileInode.putObjectBuffer, http.StatusCreated)
	if nil != err {
		err = fmt.Errorf("testDoHTTPRequest(\"PUT\", testGlobals.containerURL+\"/\"+ilayout.GetObjectNameAsString(testFileInode.putObjectNumber), putRequestHeaders, &testFileInode.putObjectBuffer, http.StatusCreated) failed: %v", err)
		return
	}

	testFileInode.putObjectBuffer = nil

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) getObjectData(objectNumber uint64, objectOffset uint64, objectLength uint64) (objectData []byte, err error) {
	var (
		getRequestHeaders http.Header
	)

	if objectNumber == ilayout.CheckPointObjectNumber {
		err = fmt.Errorf("getObjectData(objectNumber: %v,,) not supported", objectNumber)
		return
	}

	if objectNumber == testFileInode.putObjectNumber {
		if objectOffset+objectLength > uint64(testFileInode.putObjectBuffer.Len()) {
			err = fmt.Errorf("objectOffset+objectLength [%v] > testFileInode.putObjectBuffer.Len() [%v]", objectOffset+objectLength, testFileInode.putObjectBuffer.Len())
			return
		}

		objectData = testFileInode.putObjectBuffer.Bytes()[objectOffset : objectOffset+objectLength]
	} else {
		getRequestHeaders = make(http.Header)

		getRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}
		getRequestHeaders["Range"] = []string{fmt.Sprintf("bytes=%d-%d", objectOffset, (objectOffset + objectLength - 1))}

		_, objectData, err = testDoHTTPRequest("GET", testGlobals.containerURL+"/"+ilayout.GetObjectNameAsString(objectNumber), getRequestHeaders, nil, http.StatusOK)
		if nil != err {
			err = fmt.Errorf("testDoHTTPRequest(\"GET\", testGlobals.containerURL+\"/\"+ilayout.GetObjectNameAsString(objectNumber: %v, Range: %v), getRequestHeaders, nil, http.StatusOK) failed: %v", objectNumber, getRequestHeaders["Range"][0], err)
			return
		}
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) putObjectData(objectData []byte, trackedInLayout bool) (objectNumber uint64, objectOffset uint64, err error) {
	var (
		ok                         bool
		testInodeHeadLayoutEntryV1 *testInodeHeadLayoutEntryV1Struct
	)

	if testFileInode.putObjectBuffer == nil {
		err = fmt.Errorf("putObjectData() called while putObjectBuffer unset")
		return
	}

	objectNumber = testFileInode.putObjectNumber
	objectOffset = uint64(testFileInode.putObjectBuffer.Len())

	_, err = testFileInode.putObjectBuffer.Write(objectData)
	if nil != err {
		err = fmt.Errorf("testFileInode.putObjectBuffer.Write(objectData) failed: %v", err)
		return
	}

	if trackedInLayout {
		testInodeHeadLayoutEntryV1, ok = testFileInode.layoutAsMap[testFileInode.putObjectNumber]
		if ok {
			testInodeHeadLayoutEntryV1.bytesWritten += uint64(len(objectData))
			testInodeHeadLayoutEntryV1.bytesReferenced += uint64(len(objectData))
		} else {
			testInodeHeadLayoutEntryV1 = &testInodeHeadLayoutEntryV1Struct{
				bytesWritten:    uint64(len(objectData)),
				bytesReferenced: uint64(len(objectData)),
			}

			testFileInode.layoutAsMap[testFileInode.putObjectNumber] = testInodeHeadLayoutEntryV1
		}

		testFileInode.superBlockInodeBytesWrittenAdjustment += int64(len(objectData))
		testFileInode.superBlockInodeBytesReferencedAdjustment += int64(len(objectData))
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) discardObjectData(objectNumber uint64, objectLength uint64) (err error) {
	var (
		ok                         bool
		testInodeHeadLayoutEntryV1 *testInodeHeadLayoutEntryV1Struct
	)

	if testFileInode.putObjectBuffer == nil {
		err = fmt.Errorf("discardObjectData() called while putObjectBuffer unset")
		return
	}

	testInodeHeadLayoutEntryV1, ok = testFileInode.layoutAsMap[objectNumber]
	if !ok {
		err = fmt.Errorf("testFileInode.layoutAsMap[objectNumber] returned !ok")
		return
	}

	if objectLength > testInodeHeadLayoutEntryV1.bytesReferenced {
		err = fmt.Errorf("objectLength [%v] > testInodeHeadLayoutEntryV1.bytesReferenced [%v]", objectLength, testInodeHeadLayoutEntryV1.bytesReferenced)
		return
	}

	testInodeHeadLayoutEntryV1.bytesReferenced -= objectLength
	testFileInode.superBlockInodeBytesReferencedAdjustment -= int64(objectLength)

	if testInodeHeadLayoutEntryV1.bytesReferenced == 0 {
		delete(testFileInode.layoutAsMap, objectNumber)
		testFileInode.superBlockInodeObjectCountAdjustment--
		testFileInode.superBlockInodeBytesWrittenAdjustment -= int64(testInodeHeadLayoutEntryV1.bytesWritten)
		testFileInode.dereferencedObjectNumberArray = append(testFileInode.dereferencedObjectNumberArray, objectNumber)
	} else {
		testFileInode.layoutAsMap[objectNumber] = testInodeHeadLayoutEntryV1
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		fileOffset uint64
		ok         bool
	)

	fileOffset, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("key.(uint64) returned !ok")
		return
	}

	keyAsString = fmt.Sprintf("%016X", fileOffset)

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		extentMapEntryValueV1     *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1JSON []byte
		ok                        bool
	)

	extentMapEntryValueV1, ok = value.(*ilayout.ExtentMapEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("value.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
		return
	}

	extentMapEntryValueV1JSON, err = json.Marshal(extentMapEntryValueV1)
	if nil != err {
		err = fmt.Errorf("json.Marshal(extentMapEntryValueV1) failed: %v", err)
		return
	}

	valueAsString = string(extentMapEntryValueV1JSON[:])

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err = testFileInode.getObjectData(objectNumber, objectOffset, objectLength)
	return
}

func (testFileInode *testFileInodeStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber, objectOffset, err = testFileInode.putObjectData(nodeByteSlice, true)
	return
}

func (testFileInode *testFileInodeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = testFileInode.discardObjectData(objectNumber, objectLength)
	return
}

func (testFileInode *testFileInodeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		fileOffset uint64
		nextPos    int
		ok         bool
	)

	fileOffset, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("key.(uint64) returned !ok")
		return
	}

	packedKey = make([]byte, 8)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedKey, 0, fileOffset)
	if nil != err {
		err = fmt.Errorf("ilayout.PutLEUint64ToBuf(packedKey, 0, fileOffset) failed: %v", err)
		return
	}
	if nextPos != 8 {
		err = fmt.Errorf("ilayout.PutLEUint64ToBuf(packedKey, 0, fileOffset) returned unexpected nextPos: %v", nextPos)
		return
	}

	err = nil
	return
}

func (testFileInode *testFileInodeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		fileOffset uint64
		nextPos    int
	)

	fileOffset, nextPos, err = ilayout.GetLEUint64FromBuf(payloadData, 0)
	if nil != err {
		err = fmt.Errorf("ilayout.GetLEUint64FromBuf(payloadData, 0) failed: %v", err)
		return
	}

	key = fileOffset
	bytesConsumed = uint64(nextPos)
	err = nil
	return
}

func (testFileInode *testFileInodeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		extentMapEntryValueV1 *ilayout.ExtentMapEntryValueV1Struct
		ok                    bool
	)

	extentMapEntryValueV1, ok = value.(*ilayout.ExtentMapEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("value.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
		return
	}

	packedValue, err = extentMapEntryValueV1.MarshalExtentMapEntryValueV1()

	return
}

func (testFileInode *testFileInodeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt    int
		extentMapEntryValueV1 *ilayout.ExtentMapEntryValueV1Struct
	)

	extentMapEntryValueV1, bytesConsumedAsInt, err = ilayout.UnmarshalExtentMapEntryValueV1(payloadData)
	if nil != err {
		err = fmt.Errorf("ilayout.UnmarshalExtentMapEntryValueV1(payloadData) failed: %v", err)
		return
	}

	value = extentMapEntryValueV1
	bytesConsumed = uint64(bytesConsumedAsInt)
	err = nil
	return
}

type testRetryRPCClientCallbacksStruct struct {
	interruptPayloadChan chan []byte
}

func (retryrpcClientCallbacks *testRetryRPCClientCallbacksStruct) Interrupt(payload []byte) {
	retryrpcClientCallbacks.interruptPayloadChan <- payload
}

func TestRetryRPC(t *testing.T) {
	var (
		adjustInodeTableEntryOpenCountRequest  *AdjustInodeTableEntryOpenCountRequestStruct
		adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct
		currentInodeNumberSet                  map[uint64]struct{}
		deleteInodeTableEntryRequest           *DeleteInodeTableEntryRequestStruct
		deleteInodeTableEntryResponse          *DeleteInodeTableEntryResponseStruct
		err                                    error
		fetchNonceRangeRequest                 *FetchNonceRangeRequestStruct
		fetchNonceRangeResponse                *FetchNonceRangeResponseStruct
		fileInodeNumber                        uint64
		fileInodeObjectA                       uint64
		fileInodeObjectB                       uint64
		fileInodeObjectC                       uint64
		flushRequest                           *FlushRequestStruct
		flushResponse                          *FlushResponseStruct
		getInodeTableEntryRequest              *GetInodeTableEntryRequestStruct
		getInodeTableEntryResponse             *GetInodeTableEntryResponseStruct
		inodeHeadV1Buf                         []byte
		leaseRequest                           *LeaseRequestStruct
		leaseResponse                          *LeaseResponseStruct
		mountRequest                           *MountRequestStruct
		mountResponse                          *MountResponseStruct
		ok                                     bool
		postRequestBody                        string
		putInodeTableEntriesRequest            *PutInodeTableEntriesRequestStruct
		putInodeTableEntriesResponse           *PutInodeTableEntriesResponseStruct
		putObjectDataObjectNumber              uint64
		putObjectDataObjectOffset              uint64
		putRequestBody                         string
		renewMountRequest                      *RenewMountRequestStruct
		renewMountResponse                     *RenewMountResponseStruct
		retryrpcClient                         *retryrpc.Client
		retryrpcClientCallbacks                *testRetryRPCClientCallbacksStruct
		testFileInode                          *testFileInodeStruct
		unmountRequest                         *UnmountRequestStruct
		unmountResponse                        *UnmountResponseStruct
	)

	// Setup RetryRPC Client

	retryrpcClientCallbacks = &testRetryRPCClientCallbacksStruct{
		interruptPayloadChan: make(chan []byte),
	}

	// Setup test environment

	testSetup(t, nil, retryrpcClientCallbacks)

	retryrpcClient, err = retryrpc.NewClient(testGlobals.retryrpcClientConfig)
	if nil != err {
		t.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	// Format testVolume

	postRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\",\"AuthToken\":\"%s\"}", testGlobals.containerURL, testGlobals.authToken)

	_, _, err = testDoHTTPRequest("POST", testGlobals.httpServerURL+"/volume", nil, strings.NewReader(postRequestBody), http.StatusCreated)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"POST\", testGlobals.httpServerURL+\"/volume\", nil, strings.NewReader(postRequestBody), http.StatusCreated) failed: %v", err)
	}

	// Start serving testVolume

	putRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\"}", testGlobals.containerURL)

	_, _, err = testDoHTTPRequest("PUT", testGlobals.httpServerURL+"/volume/"+testVolume, nil, strings.NewReader(putRequestBody), http.StatusCreated)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.httpServerURL+\"/volume\"+testVolume, nil, strings.NewReader(putRequestBody), http.StatusCreated) failed: %v", err)
	}

	// Attempt a FetchNonceRange() without a prior Mount()... which should fail (no Mount)

	fetchNonceRangeRequest = &FetchNonceRangeRequestStruct{
		MountID: "", // An invalid MountID... so will not be found in globals.mountMap
	}
	fetchNonceRangeResponse = &FetchNonceRangeResponseStruct{}

	err = retryrpcClient.Send("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"FetchNonceRange()\",,) should have failed")
	}

	// Perform a Mount()

	mountRequest = &MountRequestStruct{
		VolumeName: testVolume,
		AuthToken:  testGlobals.authToken,
	}
	mountResponse = &MountResponseStruct{}

	err = retryrpcClient.Send("Mount", mountRequest, mountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Mount(,)\",,) failed: %v", err)
	}

	// Perform a FetchNonceRange()

	fetchNonceRangeRequest = &FetchNonceRangeRequestStruct{
		MountID: mountResponse.MountID,
	}
	fetchNonceRangeResponse = &FetchNonceRangeResponseStruct{}

	err = retryrpcClient.Send("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"FetchNonceRange()\",,) failed: %v", err)
	}

	// Attempt a GetInodeTableEntry() for RootDirInode... which should fail (no Lease)

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) should have failed")
	}

	// Force a need for a re-auth

	iswiftpkg.ForceReAuth()

	// Attempt a GetInodeTableEntry() for RootDirInode... which should fail (re-auth required)

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) should have failed")
	}

	// Perform a RenewMount()

	err = testDoAuth()
	if nil != err {
		t.Fatalf("testDoAuth() failed: %v", err)
	}

	renewMountRequest = &RenewMountRequestStruct{
		MountID:   mountResponse.MountID,
		AuthToken: testGlobals.authToken,
	}
	renewMountResponse = &RenewMountResponseStruct{}

	err = retryrpcClient.Send("RenewMount", renewMountRequest, renewMountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"RenewMount(,)\",,) failed: %v", err)
	}

	// Fetch a Shared Lease on RootDirInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      ilayout.RootDirInodeNumber,
		LeaseRequestType: LeaseRequestTypeShared,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,ilayout.RootDirInodeNumber,LeaseRequestTypeShared)\",,) failed: %v", err)
	}

	// Perform a GetInodeTableEntry() for RootDirInode

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) failed: %v", err)
	}

	// Attempt a PutInodeTableEntries() for RootDirInode... which should fail (only Shared Lease)

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           ilayout.RootDirInodeNumber,
				InodeHeadObjectNumber: getInodeTableEntryResponse.InodeHeadObjectNumber,
				InodeHeadLength:       getInodeTableEntryResponse.InodeHeadLength,
			},
		},
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) should have failed")
	}

	// Perform a Lease Promote on RootDirInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      ilayout.RootDirInodeNumber,
		LeaseRequestType: LeaseRequestTypePromote,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,ilayout.RootDirInodeNumber,LeaseRequestTypePromote)\",,) failed: %v", err)
	}

	// Perform a PutInodeTableEntries() on RootDirInode

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           ilayout.RootDirInodeNumber,
				InodeHeadObjectNumber: getInodeTableEntryResponse.InodeHeadObjectNumber,
				InodeHeadLength:       getInodeTableEntryResponse.InodeHeadLength,
			},
		},
		SuperBlockInodeObjectCountAdjustment:     0,
		SuperBlockInodeBytesWrittenAdjustment:    0,
		SuperBlockInodeBytesReferencedAdjustment: 0,
		DereferencedObjectNumberArray:            []uint64{},
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) failed: %v", err)
	}

	// Create a FileInode (set LinkCount to 0 & no dir entry)... with small amount of data

	if 2 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeNumber = fetchNonceRangeResponse.NextNonce
	fileInodeObjectA = fileInodeNumber + 1

	fetchNonceRangeResponse.NextNonce += 2
	fetchNonceRangeResponse.NumNoncesFetched -= 2

	testFileInode = newTestFileInode(fileInodeNumber)

	err = testFileInode.openObject(fileInodeObjectA)
	if nil != err {
		t.Fatalf("testFileInode.openObject(fileInodeObjectA) failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData([]byte("ABC"), true)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData([]byte(\"ABC\"), true) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectA {
		t.Fatalf("testFileInode.putObjectData([]byte(\"ABC\"), true) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectA (%v)", putObjectDataObjectNumber, fileInodeObjectA)
	}
	if putObjectDataObjectOffset != 0 {
		t.Fatalf("testFileInode.putObjectData([]byte(\"ABC\"), true) returned unexpected putObjectDataObjectOffset (%v) - expected 0", putObjectDataObjectOffset)
	}

	ok, err = testFileInode.extentMap.Put(uint64(0), &ilayout.ExtentMapEntryValueV1Struct{Length: 3, ObjectNumber: putObjectDataObjectNumber, ObjectOffset: putObjectDataObjectOffset})
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Put(0,) failed: %v", err)
	}
	if !ok {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned !ok")
	}

	testFileInode.inodeHeadV1.PayloadObjectNumber, testFileInode.inodeHeadV1.PayloadObjectOffset, testFileInode.inodeHeadV1.PayloadObjectLength, err = testFileInode.extentMap.Flush(false)
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Flush() failed: %v", err)
	}
	if testFileInode.inodeHeadV1.PayloadObjectNumber != fileInodeObjectA {
		t.Fatalf("testFileInode.extentMap.Flush() returned unexpected PayloadObjectNumber: %v", testFileInode.inodeHeadV1.PayloadObjectNumber)
	}
	if testFileInode.inodeHeadV1.PayloadObjectOffset != 3 {
		t.Fatalf("testFileInode.extentMap.Flush() returned unexpected PayloadObjectOffset: %v", testFileInode.inodeHeadV1.PayloadObjectOffset)
	}
	if testFileInode.inodeHeadV1.PayloadObjectLength != 58 {
		t.Fatalf("testFileInode.extentMap.Flush() returned unexpected PayloadObjectLength: %v", testFileInode.inodeHeadV1.PayloadObjectLength)
	}

	err = testFileInode.extentMap.Prune()
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Prune() failed: %v", err)
	}
	if len(testFileInode.dereferencedObjectNumberArray) != 0 {
		t.Fatalf("len(testFileInode.dereferencedObjectNumberArray) had unexpected length: %v", len(testFileInode.dereferencedObjectNumberArray))
	}

	testFileInode.externalizeInodeHeadV1Layout()
	if len(testFileInode.inodeHeadV1.Layout) != 1 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), len(testFileInode.inodeHeadV1.Layout) was unexpected: %v", len(testFileInode.inodeHeadV1.Layout))
	}
	if testFileInode.inodeHeadV1.Layout[0].ObjectNumber != fileInodeObjectA {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].ObjectNumber was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].ObjectNumber)
	}
	if testFileInode.inodeHeadV1.Layout[0].BytesWritten != 3+58 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].BytesWritten was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].BytesWritten)
	}
	if testFileInode.inodeHeadV1.Layout[0].BytesReferenced != 3+58 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].BytesReferenced was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].BytesReferenced)
	}

	inodeHeadV1Buf, err = testFileInode.inodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		t.Fatalf("testFileInode.inodeHeadV1.MarshalInodeHeadV1() failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData(inodeHeadV1Buf, false)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectA {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectA (%v)", putObjectDataObjectNumber, fileInodeObjectA)
	}
	if putObjectDataObjectOffset != 3+58 {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectOffset (%v) - expected %v", putObjectDataObjectOffset, 3+58)
	}

	err = testFileInode.closeObject()
	if nil != err {
		t.Fatalf("testFileInode.closeObject() failed: %v", err)
	}

	// Fetch an Exclusive Lease on FileInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      fileInodeNumber,
		LeaseRequestType: LeaseRequestTypeExclusive,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,fileInodeNumber,LeaseRequestTypeExclusive)\",,) failed: %v", err)
	}

	// Perform a PutInodeTableEntries() for FileInode

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           fileInodeNumber,
				InodeHeadObjectNumber: fileInodeObjectA,
				InodeHeadLength:       uint64(len(inodeHeadV1Buf)),
			},
		},
		SuperBlockInodeObjectCountAdjustment:     testFileInode.superBlockInodeObjectCountAdjustment,
		SuperBlockInodeBytesWrittenAdjustment:    testFileInode.superBlockInodeBytesWrittenAdjustment,
		SuperBlockInodeBytesReferencedAdjustment: testFileInode.superBlockInodeBytesReferencedAdjustment,
		DereferencedObjectNumberArray:            testFileInode.dereferencedObjectNumberArray,
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) failed: %v", err)
	}

	// Append some data (to new Object) for FileInode... and new stat

	if 1 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeObjectB = fileInodeObjectA + 1

	fetchNonceRangeResponse.NextNonce++
	fetchNonceRangeResponse.NumNoncesFetched--

	err = testFileInode.openObject(fileInodeObjectB)
	if nil != err {
		t.Fatalf("testFileInode.openObject(fileInodeObjectB) failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData([]byte("DEFG"), true)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData([]byte(\"DEFG\"), true) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectB {
		t.Fatalf("testFileInode.putObjectData([]byte(\"DEFG\"), true) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectB (%v)", putObjectDataObjectNumber, fileInodeObjectB)
	}
	if putObjectDataObjectOffset != 0 {
		t.Fatalf("testFileInode.putObjectData([]byte(\"DEFG\"), true) returned unexpected putObjectDataObjectOffset (%v) - expected 0", putObjectDataObjectOffset)
	}

	ok, err = testFileInode.extentMap.Put(uint64(3), &ilayout.ExtentMapEntryValueV1Struct{Length: 4, ObjectNumber: putObjectDataObjectNumber, ObjectOffset: putObjectDataObjectOffset})
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Put(3,) failed: %v", err)
	}
	if !ok {
		t.Fatalf("testFileInode.extentMap.Put(3,) returned !ok")
	}

	testFileInode.inodeHeadV1.PayloadObjectNumber, testFileInode.inodeHeadV1.PayloadObjectOffset, testFileInode.inodeHeadV1.PayloadObjectLength, err = testFileInode.extentMap.Flush(false)
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Flush() failed: %v", err)
	}
	if testFileInode.inodeHeadV1.PayloadObjectNumber != fileInodeObjectB {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectNumber: %v", testFileInode.inodeHeadV1.PayloadObjectNumber)
	}
	if testFileInode.inodeHeadV1.PayloadObjectOffset != 4 {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectOffset: %v", testFileInode.inodeHeadV1.PayloadObjectOffset)
	}
	if testFileInode.inodeHeadV1.PayloadObjectLength != 90 {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectLength: %v", testFileInode.inodeHeadV1.PayloadObjectLength)
	}

	err = testFileInode.extentMap.Prune()
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Prune() failed: %v", err)
	}
	if len(testFileInode.dereferencedObjectNumberArray) != 0 {
		t.Fatalf("len(testFileInode.dereferencedObjectNumberArray) had unexpected length: %v", len(testFileInode.dereferencedObjectNumberArray))
	}

	testFileInode.externalizeInodeHeadV1Layout()
	if len(testFileInode.inodeHeadV1.Layout) != 2 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), len(testFileInode.inodeHeadV1.Layout) was unexpected: %v", len(testFileInode.inodeHeadV1.Layout))
	}
	if testFileInode.inodeHeadV1.Layout[0].ObjectNumber != fileInodeObjectA {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].ObjectNumber was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].ObjectNumber)
	}
	if testFileInode.inodeHeadV1.Layout[0].BytesWritten != 3+58 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].BytesWritten was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].BytesWritten)
	}
	if testFileInode.inodeHeadV1.Layout[0].BytesReferenced != 3 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].BytesReferenced was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].BytesReferenced)
	}
	if testFileInode.inodeHeadV1.Layout[1].ObjectNumber != fileInodeObjectB {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[1].ObjectNumber was unexpected: %v", testFileInode.inodeHeadV1.Layout[1].ObjectNumber)
	}
	if testFileInode.inodeHeadV1.Layout[1].BytesWritten != 4+90 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[1].BytesWritten was unexpected: %v", testFileInode.inodeHeadV1.Layout[1].BytesWritten)
	}
	if testFileInode.inodeHeadV1.Layout[1].BytesReferenced != 4+90 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[1].BytesReferenced was unexpected: %v", testFileInode.inodeHeadV1.Layout[1].BytesReferenced)
	}

	inodeHeadV1Buf, err = testFileInode.inodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		t.Fatalf("testFileInode.inodeHeadV1.MarshalInodeHeadV1() failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData(inodeHeadV1Buf, false)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectB {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectA (%v)", putObjectDataObjectNumber, fileInodeObjectB)
	}
	if putObjectDataObjectOffset != 4+90 {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectOffset (%v) - expected %v", putObjectDataObjectOffset, 4+90)
	}

	err = testFileInode.closeObject()
	if nil != err {
		t.Fatalf("testFileInode.closeObject() failed: %v", err)
	}

	// Perform a PutInodeTableEntries() for FileInode

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           fileInodeNumber,
				InodeHeadObjectNumber: fileInodeObjectB,
				InodeHeadLength:       uint64(len(inodeHeadV1Buf)),
			},
		},
		SuperBlockInodeObjectCountAdjustment:     testFileInode.superBlockInodeObjectCountAdjustment,
		SuperBlockInodeBytesWrittenAdjustment:    testFileInode.superBlockInodeBytesWrittenAdjustment,
		SuperBlockInodeBytesReferencedAdjustment: testFileInode.superBlockInodeBytesReferencedAdjustment,
		DereferencedObjectNumberArray:            testFileInode.dereferencedObjectNumberArray,
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// Overwrite the original data in FileInode (to new Object)... and new stat dereferencing 1st Object

	if 1 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeObjectC = fileInodeObjectB + 1

	fetchNonceRangeResponse.NextNonce++
	fetchNonceRangeResponse.NumNoncesFetched--

	err = testFileInode.openObject(fileInodeObjectC)
	if nil != err {
		t.Fatalf("testFileInode.openObject(fileInodeObjectC) failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData([]byte("abc"), true)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData([]byte(\"abc\"), true) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectC {
		t.Fatalf("testFileInode.putObjectData([]byte(\"abc\"), true) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectC (%v)", putObjectDataObjectNumber, fileInodeObjectC)
	}
	if putObjectDataObjectOffset != 0 {
		t.Fatalf("testFileInode.putObjectData([]byte(\"abc\"), true) returned unexpected putObjectDataObjectOffset (%v) - expected 0", putObjectDataObjectOffset)
	}

	ok, err = testFileInode.extentMap.PatchByKey(uint64(0), &ilayout.ExtentMapEntryValueV1Struct{Length: 3, ObjectNumber: putObjectDataObjectNumber, ObjectOffset: putObjectDataObjectOffset})
	if nil != err {
		t.Fatalf("testFileInode.extentMap.PatchByKey(0,) failed: %v", err)
	}
	if !ok {
		t.Fatalf("testFileInode.extentMap.PatchByKey(0,) returned !ok")
	}

	err = testFileInode.discardObjectData(fileInodeObjectA, 3)
	if nil != err {
		t.Fatalf("testFileInode.discardObjectData(objectNumber: %v, 3) failed: %v", fileInodeObjectA, err)
	}
	if len(testFileInode.dereferencedObjectNumberArray) != 1 {
		t.Fatalf("len(testFileInode.dereferencedObjectNumberArray) had unexpected length: %v", len(testFileInode.dereferencedObjectNumberArray))
	}
	if testFileInode.dereferencedObjectNumberArray[0] != fileInodeObjectA {
		t.Fatalf("testFileInode.dereferencedObjectNumberArray[0] had unexpected objectNumber: %v", testFileInode.dereferencedObjectNumberArray[0])
	}

	testFileInode.inodeHeadV1.PayloadObjectNumber, testFileInode.inodeHeadV1.PayloadObjectOffset, testFileInode.inodeHeadV1.PayloadObjectLength, err = testFileInode.extentMap.Flush(false)
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Flush() failed: %v", err)
	}
	if testFileInode.inodeHeadV1.PayloadObjectNumber != fileInodeObjectC {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectNumber: %v", testFileInode.inodeHeadV1.PayloadObjectNumber)
	}
	if testFileInode.inodeHeadV1.PayloadObjectOffset != 3 {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectOffset: %v", testFileInode.inodeHeadV1.PayloadObjectOffset)
	}
	if testFileInode.inodeHeadV1.PayloadObjectLength != 90 {
		t.Fatalf("testFileInode.extentMap.Put(0,) returned unexpected PayloadObjectLength: %v", testFileInode.inodeHeadV1.PayloadObjectLength)
	}

	err = testFileInode.extentMap.Prune()
	if nil != err {
		t.Fatalf("testFileInode.extentMap.Prune() failed: %v", err)
	}
	if len(testFileInode.dereferencedObjectNumberArray) != 1 {
		t.Fatalf("len(testFileInode.dereferencedObjectNumberArray) had unexpected length: %v", len(testFileInode.dereferencedObjectNumberArray))
	}
	if testFileInode.dereferencedObjectNumberArray[0] != fileInodeObjectA {
		t.Fatalf("testFileInode.dereferencedObjectNumberArray[0] had unexpected objectNumber: %v", testFileInode.dereferencedObjectNumberArray[0])
	}

	testFileInode.externalizeInodeHeadV1Layout()
	if len(testFileInode.inodeHeadV1.Layout) != 2 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), len(testFileInode.inodeHeadV1.Layout) was unexpected: %v", len(testFileInode.inodeHeadV1.Layout))
	}
	if testFileInode.inodeHeadV1.Layout[0].ObjectNumber != fileInodeObjectB {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].ObjectNumber was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].ObjectNumber)
	}
	if testFileInode.inodeHeadV1.Layout[0].BytesWritten != 4+90 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].BytesWritten was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].BytesWritten)
	}
	if testFileInode.inodeHeadV1.Layout[0].BytesReferenced != 4 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[0].BytesReferenced was unexpected: %v", testFileInode.inodeHeadV1.Layout[0].BytesReferenced)
	}
	if testFileInode.inodeHeadV1.Layout[1].ObjectNumber != fileInodeObjectC {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[1].ObjectNumber was unexpected: %v", testFileInode.inodeHeadV1.Layout[1].ObjectNumber)
	}
	if testFileInode.inodeHeadV1.Layout[1].BytesWritten != 3+90 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[1].BytesWritten was unexpected: %v", testFileInode.inodeHeadV1.Layout[1].BytesWritten)
	}
	if testFileInode.inodeHeadV1.Layout[1].BytesReferenced != 3+90 {
		t.Fatalf("following testFileInode.externalizeInodeHeadV1Layout(), testFileInode.inodeHeadV1.Layout[1].BytesReferenced was unexpected: %v", testFileInode.inodeHeadV1.Layout[1].BytesReferenced)
	}

	inodeHeadV1Buf, err = testFileInode.inodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		t.Fatalf("testFileInode.inodeHeadV1.MarshalInodeHeadV1() failed: %v", err)
	}

	putObjectDataObjectNumber, putObjectDataObjectOffset, err = testFileInode.putObjectData(inodeHeadV1Buf, false)
	if nil != err {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) failed: %v", err)
	}
	if putObjectDataObjectNumber != fileInodeObjectC {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectNumber (%v) - expected fileInodeObjectC (%v)", putObjectDataObjectNumber, fileInodeObjectC)
	}
	if putObjectDataObjectOffset != 3+90 {
		t.Fatalf("testFileInode.putObjectData(inodeHeadV1Buf, false) returned unexpected putObjectDataObjectOffset (%v) - expected %v", putObjectDataObjectOffset, 3+90)
	}

	err = testFileInode.closeObject()
	if nil != err {
		t.Fatalf("testFileInode.closeObject() failed: %v", err)
	}

	// Perform a PutInodeTableEntries() for FileInode

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           fileInodeNumber,
				InodeHeadObjectNumber: fileInodeObjectC,
				InodeHeadLength:       uint64(len(inodeHeadV1Buf)),
			},
		},
		SuperBlockInodeObjectCountAdjustment:     testFileInode.superBlockInodeObjectCountAdjustment,
		SuperBlockInodeBytesWrittenAdjustment:    testFileInode.superBlockInodeBytesWrittenAdjustment,
		SuperBlockInodeBytesReferencedAdjustment: testFileInode.superBlockInodeBytesReferencedAdjustment,
		DereferencedObjectNumberArray:            testFileInode.dereferencedObjectNumberArray,
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// Verify that 1st Object for FileInode gets deleted... but not 2nd nor 3rd

	if objectNumberIsPresent(t, mountResponse.MountID, fileInodeObjectA) {
		t.Fatalf("fileInodeObjectA should have been absent")
	}

	if !objectNumberIsPresent(t, mountResponse.MountID, fileInodeObjectB) {
		t.Fatalf("fileInodeObjectB should have been present")
	}

	if !objectNumberIsPresent(t, mountResponse.MountID, fileInodeObjectC) {
		t.Fatalf("fileInodeObjectC should have been present")
	}

	// Perform an AdjustInodeTableEntryOpenCount(+1) for FileInode

	adjustInodeTableEntryOpenCountRequest = &AdjustInodeTableEntryOpenCountRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
		Adjustment:  +1,
	}
	adjustInodeTableEntryOpenCountResponse = &AdjustInodeTableEntryOpenCountResponseStruct{}

	err = retryrpcClient.Send("AdjustInodeTableEntryOpenCount", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"AdjustInodeTableEntryOpenCount(,fileInodeNumber,+1)\",,) failed: %v", err)
	}

	// Perform a DeleteInodeTableEntry() on FileInode

	deleteInodeTableEntryRequest = &DeleteInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
	}
	deleteInodeTableEntryResponse = &DeleteInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("DeleteInodeTableEntry", deleteInodeTableEntryRequest, deleteInodeTableEntryResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"DeleteInodeTableEntry(,fileInodeNumber)\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// Verify that FileInode is still in InodeTable

	currentInodeNumberSet = fetchCurrentInodeNumberInInodeTableSet(t, mountResponse.MountID)

	_, ok = currentInodeNumberSet[fileInodeNumber]
	if !ok {
		t.Fatalf("fileInodeNumber should have been present")
	}

	// Perform an AdjustInodeTableEntryOpenCount(-1) for FileInode

	adjustInodeTableEntryOpenCountRequest = &AdjustInodeTableEntryOpenCountRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
		Adjustment:  -1,
	}
	adjustInodeTableEntryOpenCountResponse = &AdjustInodeTableEntryOpenCountResponseStruct{}

	err = retryrpcClient.Send("AdjustInodeTableEntryOpenCount", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"AdjustInodeTableEntryOpenCount(,fileInodeNumber,-1)\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// Verify that FileInode is no longer in InodeTable and 2nd and 3rd Objects are deleted

	currentInodeNumberSet = fetchCurrentInodeNumberInInodeTableSet(t, mountResponse.MountID)

	_, ok = currentInodeNumberSet[fileInodeNumber]
	if ok {
		t.Fatalf("fileInodeNumber should have been absent")
	}

	if objectNumberIsPresent(t, mountResponse.MountID, fileInodeObjectB) {
		t.Fatalf("fileInodeObjectB should have been absent")
	}

	if objectNumberIsPresent(t, mountResponse.MountID, fileInodeObjectC) {
		t.Fatalf("fileInodeObjectC should have been absent")
	}

	// Perform a Lease Release on FileInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      fileInodeNumber,
		LeaseRequestType: LeaseRequestTypeRelease,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,fileInodeNumber,LeaseRequestTypeRelease)\",,) failed: %v", err)
	}

	// Perform an Unmount()... without first releasing Exclusive Lease on RootDirInode

	unmountRequest = &UnmountRequestStruct{
		MountID: mountResponse.MountID,
	}
	unmountResponse = &UnmountResponseStruct{}

	err = retryrpcClient.Send("Unmount", unmountRequest, unmountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Unmount()\",,) failed: %v", err)
	}

	// Verify that Exclusive Lease on RootDirInode is implicitly released

	currentInodeNumberSet = fetchCurrentInodeNumberInInodeLeaseMapSet(t, mountResponse.MountID)

	_, ok = currentInodeNumberSet[ilayout.RootDirInodeNumber]
	if ok {
		t.Fatalf("ilayout.RootDirInodeNumber should have been absent")
	}

	// Teardown RetryRPC Client

	// TODO: Remove this early exit skipping of following TODOs

	if !ok {
		t.Logf("Exiting TestRetryRPC() early (to skip following TODOs")
		return
	}

	fmt.Print("\n\nReached UNDO A\n")
	retryrpcClient.Close()
	fmt.Print("\n\nReached UNDO B\n")

	// And teardown test environment

	testTeardown(t)
}

func fetchCurrentInodeNumberInInodeLeaseMapSet(t *testing.T, mountID string) (inodeNumberSet map[uint64]struct{}) {
	var (
		inodeNumber uint64
		ok          bool
		testMount   *mountStruct
	)

	inodeNumberSet = make(map[uint64]struct{})

	globals.Lock()

	testMount, ok = globals.mountMap[mountID]
	if !ok {
		t.Fatalf("globals.mountMap[mountID] returned !ok")
	}

	for inodeNumber = range testMount.volume.inodeLeaseMap {
		inodeNumberSet[inodeNumber] = struct{}{}
	}

	globals.Unlock()

	return
}

func fetchCurrentInodeNumberInInodeTableSet(t *testing.T, mountID string) (inodeNumberSet map[uint64]struct{}) {
	var (
		err              error
		inodeNumber      uint64
		inodeNumberAsKey sortedmap.Key
		inodeTableIndex  int
		inodeTableLen    int
		ok               bool
		testMount        *mountStruct
	)

	inodeNumberSet = make(map[uint64]struct{})

	globals.Lock()

	testMount, ok = globals.mountMap[mountID]
	if !ok {
		t.Fatalf("globals.mountMap[mountID] returned !ok")
	}

	inodeTableLen, err = testMount.volume.inodeTable.Len()
	if nil != err {
		t.Fatalf("testMount.volume.inodeTable.Len() failed: %v", err)
	}

	for inodeTableIndex = 0; inodeTableIndex < inodeTableLen; inodeTableIndex++ {
		inodeNumberAsKey, _, ok, err = testMount.volume.inodeTable.GetByIndex(inodeTableIndex)
		inodeNumber, ok = inodeNumberAsKey.(uint64)
		if !ok {
			t.Fatalf("inodeNumberAsKey.(uint64) returned !ok")
		}
		inodeNumberSet[inodeNumber] = struct{}{}
	}

	globals.Unlock()

	return
}

func objectNumberIsPresent(t *testing.T, mountID string, objectNumber uint64) (present bool) {
	var (
		activeDeleteObjectNumber             uint64
		activeDeleteObjectNumberListElement  *list.Element
		err                                  error
		headRequestHeaders                   http.Header
		objectURL                            string
		ok                                   bool
		pendingDeleteObjectNumber            uint64
		pendingDeleteObjectNumberListElement *list.Element
		testMount                            *mountStruct
	)

	globals.Lock()

	testMount, ok = globals.mountMap[mountID]
	if !ok {
		t.Fatal("globals.mountMap[mountID] returned !ok")
	}

	activeDeleteObjectNumberListElement = testMount.volume.activeDeleteObjectNumberList.Front()

	for activeDeleteObjectNumberListElement != nil {
		activeDeleteObjectNumber, ok = activeDeleteObjectNumberListElement.Value.(uint64)
		if !ok {
			t.Fatalf("activeDeleteObjectNumberListElement.Value.(uint64) returned !ok")
		}

		if objectNumber == activeDeleteObjectNumber {
			globals.Unlock()
			present = false
			return
		}

		activeDeleteObjectNumberListElement = activeDeleteObjectNumberListElement.Next()
	}

	pendingDeleteObjectNumberListElement = testMount.volume.pendingDeleteObjectNumberList.Front()

	for pendingDeleteObjectNumberListElement != nil {
		pendingDeleteObjectNumber, ok = pendingDeleteObjectNumberListElement.Value.(uint64)
		if !ok {
			t.Fatalf("pendingDeleteObjectNumberListElement.Value.(uint64) returned !ok")
		}

		if objectNumber == pendingDeleteObjectNumber {
			globals.Unlock()
			present = false
			return
		}

		pendingDeleteObjectNumberListElement = pendingDeleteObjectNumberListElement.Next()
	}

	globals.Unlock()

	headRequestHeaders = make(http.Header)

	headRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	objectURL = testGlobals.containerURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	_, _, err = testDoHTTPRequest("HEAD", objectURL, headRequestHeaders, nil, http.StatusOK)
	if nil == err {
		present = true
		return
	}
	_, _, err = testDoHTTPRequest("HEAD", objectURL, headRequestHeaders, nil, http.StatusNoContent)
	if nil == err {
		present = true
		return
	}
	_, _, err = testDoHTTPRequest("HEAD", objectURL, headRequestHeaders, nil, http.StatusNotFound)
	if nil == err {
		present = false
		return
	}

	t.Fatalf("testDoHTTPRequest(\"HEAD\", objectURL, headRequestHeaders, nil, http.Status{OK|NoContent|NotFound}) all failed")

	return // Though this will never be reached
}
