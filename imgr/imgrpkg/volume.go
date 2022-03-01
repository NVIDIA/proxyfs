// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"bytes"
	"container/list"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/ilayout"
)

const (
	checkPointRandomPathByteSliceLen = 8
)

func startVolumeManagement() (err error) {
	var (
		checkPointIPAddr string
		checkPointURL    string
		customTransport  *http.Transport
		defaultTransport *http.Transport
		ok               bool
		rootCA           []byte
		rootCAPool       *x509.CertPool
		tlsClientConfig  *tls.Config
	)

	if len(globals.config.CheckPointIPAddrs) == 0 {
		globals.checkPointHTTPClient = nil
		globals.checkPointURL = nil
	} else {
		defaultTransport, ok = http.DefaultTransport.(*http.Transport)
		if !ok {
			err = fmt.Errorf("http.DefaultTransport.(*http.Transport) returned !ok")
			return
		}

		if globals.config.CheckPointCACertFilePath == "" {
			tlsClientConfig = defaultTransport.TLSClientConfig
		} else {
			rootCA, err = ioutil.ReadFile(globals.config.CheckPointCACertFilePath)
			if err != nil {
				return
			}
			rootCAPool = x509.NewCertPool()
			ok = rootCAPool.AppendCertsFromPEM(rootCA)
			if !ok {
				err = fmt.Errorf("rootCAPool.AppendCertsFromPEM(rootCA) returned !ok")
				return
			}
			tlsClientConfig = &tls.Config{
				RootCAs: rootCAPool,
			}
		}

		customTransport = &http.Transport{ // Up-to-date as of Golang 1.17
			Proxy:                  defaultTransport.Proxy,
			DialContext:            defaultTransport.DialContext,
			DialTLSContext:         defaultTransport.DialTLSContext,
			TLSClientConfig:        tlsClientConfig,
			TLSHandshakeTimeout:    globals.config.SwiftTimeout,
			DisableKeepAlives:      false,
			DisableCompression:     defaultTransport.DisableCompression,
			MaxIdleConns:           int(globals.config.SwiftConnectionPoolSize),
			MaxIdleConnsPerHost:    int(globals.config.SwiftConnectionPoolSize),
			MaxConnsPerHost:        int(globals.config.SwiftConnectionPoolSize),
			IdleConnTimeout:        globals.config.SwiftTimeout,
			ResponseHeaderTimeout:  globals.config.SwiftTimeout,
			ExpectContinueTimeout:  globals.config.SwiftTimeout,
			TLSNextProto:           defaultTransport.TLSNextProto,
			ProxyConnectHeader:     defaultTransport.ProxyConnectHeader,
			MaxResponseHeaderBytes: defaultTransport.MaxResponseHeaderBytes,
			WriteBufferSize:        0,
			ReadBufferSize:         0,
		}

		globals.checkPointHTTPClient = &http.Client{
			Transport: customTransport,
			Timeout:   globals.config.SwiftTimeout,
		}

		globals.checkPointURL = make([]string, 0, len(globals.config.CheckPointIPAddrs))

		for _, checkPointIPAddr = range globals.config.CheckPointIPAddrs {
			if globals.config.CheckPointCACertFilePath == "" {
				checkPointURL = "http://" + net.JoinHostPort(checkPointIPAddr, fmt.Sprintf("%d", globals.config.CheckPointPort))
			} else {
				checkPointURL = "https://" + net.JoinHostPort(checkPointIPAddr, fmt.Sprintf("%d", globals.config.CheckPointPort))
			}

			globals.checkPointURL = append(globals.checkPointURL, checkPointURL)
		}
	}

	globals.inodeTableCache = sortedmap.NewBPlusTreeCache(globals.config.InodeTableCacheEvictLowLimit, globals.config.InodeTableCacheEvictLowLimit)
	globals.inodeLeaseLRU = list.New()
	globals.volumeMap = sortedmap.NewLLRBTree(sortedmap.CompareString, &globals)
	globals.mountMap = make(map[string]*mountStruct)

	err = nil
	return
}

func stopVolumeManagement() (err error) {
	var (
		checkPointControlWG      *sync.WaitGroup
		checkPointControlWGSlice []*sync.WaitGroup
		ok                       bool
		volume                   *volumeStruct
		volumeAsValue            sortedmap.Value
		volumeMapIndex           int
		volumeMapLen             int
	)

	// TODO: For now, let's just stop all of the CheckPoint activity

	globals.Lock()

	volumeMapLen, err = globals.volumeMap.Len()
	if err != nil {
		globals.Unlock()
		return
	}

	checkPointControlWGSlice = make([]*sync.WaitGroup, 0, volumeMapLen)

	for volumeMapIndex = 0; volumeMapIndex < volumeMapLen; volumeMapIndex++ {
		_, volumeAsValue, ok, err = globals.volumeMap.GetByIndex(volumeMapIndex)
		if err != nil {
			globals.Unlock()
			return
		}
		if !ok {
			err = fmt.Errorf("globals.volumeMap.GetByIndex(volumeMapIndex: %v) (with volumeMapLen: %v) returned !ok", volumeMapIndex, volumeMapLen)
			globals.Unlock()
			return
		}

		volume, ok = volumeAsValue.(*volumeStruct)
		if !ok {
			err = fmt.Errorf("volumeAsValue.(*volumeStruct) returned !ok")
			globals.Unlock()
			return
		}

		if volume.checkPointControlChan != nil {
			close(volume.checkPointControlChan)
			checkPointControlWGSlice = append(checkPointControlWGSlice, &volume.checkPointControlWG)
		}
	}

	globals.Unlock()

	for _, checkPointControlWG = range checkPointControlWGSlice {
		checkPointControlWG.Wait()
	}

	// TODO: For now, just clear out volume-related globals fields

	globals.checkPointHTTPClient = nil
	globals.checkPointURL = nil

	globals.inodeTableCache = nil
	globals.inodeLeaseLRU = nil
	globals.volumeMap = nil
	globals.mountMap = nil

	err = nil
	return
}

func (dummy *globalsStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		ok bool
	)

	keyAsString, ok = key.(string)
	if ok {
		err = nil
	} else {
		err = fmt.Errorf("volumeMap's DumpKey(%v) called for non-string", key)
	}

	return
}

func (dummy *globalsStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok            bool
		valueAsVolume *volumeStruct
	)

	valueAsVolume, ok = value.(*volumeStruct)
	if ok {
		valueAsString = valueAsVolume.storageURL
		err = nil
	} else {
		err = fmt.Errorf("volumeMap's DumpValue(%v) called for non-*volumeStruct", value)
	}

	return
}

func deleteVolume(volumeName string) (err error) {
	var (
		ok             bool
		volumeAsStruct *volumeStruct
		volumeAsValue  sortedmap.Value
	)

	globals.Lock()

	volumeAsValue, ok, err = globals.volumeMap.GetByKey(volumeName)
	if nil != err {
		logFatal(err)
	}
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("volumeName \"%s\" does not exist", volumeName)
		return
	}

	volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
	if !ok {
		logFatalf("globals.volumeMap[\"%s\"] was not a *volumeStruct", volumeName)
	}

	// The following is only temporary...
	// TODO: Actually gracefully unmount clients, block new mounts, and lazily remove it

	if len(volumeAsStruct.mountMap) != 0 {
		logFatalf("No support for deleting actively mounted volume \"%s\"", volumeName)
	}

	volumeAsStruct.activeDeleteObjectWG.Wait()

	ok, err = globals.volumeMap.DeleteByKey(volumeAsStruct.name)
	if nil != err {
		logFatal(err)
	}
	if !ok {
		logFatalf("globals.volumeMap[\"%s\"] suddenly missing", volumeAsStruct.name)
	}

	globals.Unlock()

	err = nil
	return
}

type postVolumeRootDirDirectoryCallbacksStruct struct {
	io.ReadSeeker
	sortedmap.BPlusTreeCallbacks
	objectNumber uint64
	body         []byte
	readPos      int64
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) Read(p []byte) (n int, err error) {
	n = copy(p, postVolumeRootDirDirectoryCallbacks.body[postVolumeRootDirDirectoryCallbacks.readPos:])
	postVolumeRootDirDirectoryCallbacks.readPos += int64(n)

	if postVolumeRootDirDirectoryCallbacks.readPos == int64(len(postVolumeRootDirDirectoryCallbacks.body)) {
		err = io.EOF
	} else {
		err = nil
	}
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) Seek(offset int64, whence int) (int64, error) {
	var (
		newOffset int64
	)

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = postVolumeRootDirDirectoryCallbacks.readPos + offset
	case io.SeekEnd:
		newOffset = postVolumeRootDirDirectoryCallbacks.readPos + offset
	default:
		return 0, fmt.Errorf("invalid whence (%d)", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("resultant offset cannot be negative")
	}
	if newOffset > int64(len(postVolumeRootDirDirectoryCallbacks.body)) {
		return 0, fmt.Errorf("resultant offset cannot be beyond len(body)")
	}

	postVolumeRootDirDirectoryCallbacks.readPos = newOffset

	return newOffset, nil
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber = postVolumeRootDirDirectoryCallbacks.objectNumber
	objectOffset = uint64(len(postVolumeRootDirDirectoryCallbacks.body))

	postVolumeRootDirDirectoryCallbacks.body = append(postVolumeRootDirDirectoryCallbacks.body, nodeByteSlice...)

	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsString string
		nextPos     int
		ok          bool
	)

	keyAsString, ok = key.(string)
	if !ok {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackKey(key:%v) called with non-string", key)
		return
	}

	packedKey = make([]byte, 8+len(keyAsString))

	nextPos, err = ilayout.PutLEStringToBuf(packedKey, 0, keyAsString)
	if nil != err {
		return
	}

	if len(packedKey) != nextPos {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackKey(key:%s) logic error", keyAsString)
		return
	}

	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		nextPos                      int
		ok                           bool
		valueAsDirectoryEntryValueV1 ilayout.DirectoryEntryValueV1Struct
	)

	valueAsDirectoryEntryValueV1, ok = value.(ilayout.DirectoryEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackValue(value:%v) called with non-DirectoryEntryValueV1Struct", value)
		return
	}

	packedValue = make([]byte, 8+1)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedValue, 0, valueAsDirectoryEntryValueV1.InodeNumber)
	if nil != err {
		return
	}

	nextPos, err = ilayout.PutLEUint8ToBuf(packedValue, nextPos, valueAsDirectoryEntryValueV1.InodeType)
	if nil != err {
		return
	}

	if len(packedValue) != nextPos {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackKey(key:%#v) logic error", valueAsDirectoryEntryValueV1)
		return
	}

	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

type postVolumeSuperBlockInodeTableCallbacksStruct struct {
	io.ReadSeeker
	sortedmap.BPlusTreeCallbacks
	objectNumber uint64
	body         []byte
	readPos      int64
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) Read(p []byte) (n int, err error) {
	n = copy(p, postVolumeSuperBlockInodeTableCallbacks.body[postVolumeSuperBlockInodeTableCallbacks.readPos:])
	postVolumeSuperBlockInodeTableCallbacks.readPos += int64(n)

	if postVolumeSuperBlockInodeTableCallbacks.readPos == int64(len(postVolumeSuperBlockInodeTableCallbacks.body)) {
		err = io.EOF
	} else {
		err = nil
	}
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) Seek(offset int64, whence int) (int64, error) {
	var (
		newOffset int64
	)

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = postVolumeSuperBlockInodeTableCallbacks.readPos + offset
	case io.SeekEnd:
		newOffset = postVolumeSuperBlockInodeTableCallbacks.readPos + offset
	default:
		return 0, fmt.Errorf("invalid whence (%d)", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("resultant offset cannot be negative")
	}
	if newOffset > int64(len(postVolumeSuperBlockInodeTableCallbacks.body)) {
		return 0, fmt.Errorf("resultant offset cannot be beyond len(body)")
	}

	postVolumeSuperBlockInodeTableCallbacks.readPos = newOffset

	return newOffset, nil
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber = postVolumeSuperBlockInodeTableCallbacks.objectNumber
	objectOffset = uint64(len(postVolumeSuperBlockInodeTableCallbacks.body))

	postVolumeSuperBlockInodeTableCallbacks.body = append(postVolumeSuperBlockInodeTableCallbacks.body, nodeByteSlice...)

	err = nil
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = nil
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsUint64 uint64
		nextPos     int
		ok          bool
	)

	keyAsUint64, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("(*postVolumeSuperBlockInodeTableCallbacksStruct).PackKey(key:%v) called with non-uint64", key)
		return
	}

	packedKey = make([]byte, 8)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedKey, 0, keyAsUint64)
	if nil != err {
		return
	}

	if len(packedKey) != nextPos {
		err = fmt.Errorf("(*postVolumeSuperBlockInodeTableCallbacksStruct).PackKey(key:%016X) logic error", keyAsUint64)
		return
	}

	err = nil
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		ok                            bool
		valueAsInodeTableEntryValueV1 ilayout.InodeTableEntryValueV1Struct
	)

	valueAsInodeTableEntryValueV1, ok = value.(ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("(*postVolumeSuperBlockInodeTableCallbacksStruct).PackValue(value:%v) called with non-InodeTableEntryValueV1Struct", value)
		return
	}

	packedValue, err = valueAsInodeTableEntryValueV1.MarshalInodeTableEntryValueV1()

	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func postVolume(storageURL string, authToken string) (err error) {
	var (
		checkPointV1                            *ilayout.CheckPointV1Struct
		checkPointV1String                      string
		inodeTable                              sortedmap.BPlusTree
		ok                                      bool
		postVolumeRootDirDirectoryCallbacks     *postVolumeRootDirDirectoryCallbacksStruct
		postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct
		reservedToNonce                         uint64
		rootDirDirectory                        sortedmap.BPlusTree
		rootDirInodeHeadV1                      *ilayout.InodeHeadV1Struct
		rootDirInodeHeadV1Buf                   []byte
		rootDirInodeObjectLength                uint64
		rootDirInodeObjectNumber                uint64
		rootDirInodeObjectOffset                uint64
		superBlockObjectLength                  uint64
		superBlockObjectNumber                  uint64
		superBlockObjectOffset                  uint64
		superBlockV1                            *ilayout.SuperBlockV1Struct
		superBlockV1Buf                         []byte
		timeNow                                 = time.Now()
	)

	// Reserve some Nonce values

	rootDirInodeObjectNumber = ilayout.RootDirInodeNumber + 1
	superBlockObjectNumber = rootDirInodeObjectNumber + 1

	reservedToNonce = superBlockObjectNumber

	// Create RootDirInode

	postVolumeRootDirDirectoryCallbacks = &postVolumeRootDirDirectoryCallbacksStruct{
		objectNumber: rootDirInodeObjectNumber,
		body:         make([]byte, 0),
		readPos:      0,
	}

	rootDirDirectory = sortedmap.NewBPlusTree(
		globals.config.RootDirMaxDirEntriesPerBPlusTreePage,
		sortedmap.CompareString,
		postVolumeRootDirDirectoryCallbacks,
		nil)

	ok, err = rootDirDirectory.Put(
		".",
		ilayout.DirectoryEntryValueV1Struct{
			InodeNumber: ilayout.RootDirInodeNumber,
			InodeType:   ilayout.InodeTypeDir,
		})
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("rootDirDirectory.Put(\".\",) returned !ok")
		return
	}

	ok, err = rootDirDirectory.Put(
		"..",
		ilayout.DirectoryEntryValueV1Struct{
			InodeNumber: ilayout.RootDirInodeNumber,
			InodeType:   ilayout.InodeTypeDir,
		})
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("rootDirDirectory.Put(\".\",) returned !ok")
		return
	}

	_, rootDirInodeObjectOffset, rootDirInodeObjectLength, err = rootDirDirectory.Flush(false)
	if nil != err {
		return
	}

	rootDirInodeHeadV1 = &ilayout.InodeHeadV1Struct{
		InodeNumber: ilayout.RootDirInodeNumber,
		InodeType:   ilayout.InodeTypeDir,
		LinkTable: []ilayout.InodeLinkTableEntryStruct{
			{
				ParentDirInodeNumber: ilayout.RootDirInodeNumber,
				ParentDirEntryName:   ".",
			},
			{
				ParentDirInodeNumber: ilayout.RootDirInodeNumber,
				ParentDirEntryName:   "..",
			},
		},
		Size:                0,
		ModificationTime:    timeNow,
		StatusChangeTime:    timeNow,
		Mode:                ilayout.InodeModeMask,
		UserID:              0,
		GroupID:             0,
		StreamTable:         []ilayout.InodeStreamTableEntryStruct{},
		PayloadObjectNumber: rootDirInodeObjectNumber,
		PayloadObjectOffset: rootDirInodeObjectOffset,
		PayloadObjectLength: rootDirInodeObjectLength,
		SymLinkTarget:       "",
		Layout: []ilayout.InodeHeadLayoutEntryV1Struct{
			{
				ObjectNumber:    rootDirInodeObjectNumber,
				BytesWritten:    uint64(len(postVolumeRootDirDirectoryCallbacks.body)),
				BytesReferenced: uint64(len(postVolumeRootDirDirectoryCallbacks.body)),
			},
		},
	}

	rootDirInodeHeadV1Buf, err = rootDirInodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		return
	}

	postVolumeRootDirDirectoryCallbacks.body = append(postVolumeRootDirDirectoryCallbacks.body, rootDirInodeHeadV1Buf...)

	err = swiftObjectPut(storageURL, authToken, rootDirInodeObjectNumber, postVolumeRootDirDirectoryCallbacks)
	if nil != err {
		return
	}

	// Create SuperBlock

	postVolumeSuperBlockInodeTableCallbacks = &postVolumeSuperBlockInodeTableCallbacksStruct{
		objectNumber: superBlockObjectNumber,
		body:         make([]byte, 0),
		readPos:      0,
	}

	inodeTable = sortedmap.NewBPlusTree(
		globals.config.InodeTableMaxInodesPerBPlusTreePage,
		sortedmap.CompareUint64,
		postVolumeSuperBlockInodeTableCallbacks,
		nil)

	ok, err = inodeTable.Put(
		ilayout.RootDirInodeNumber,
		ilayout.InodeTableEntryValueV1Struct{
			InodeHeadObjectNumber: rootDirInodeObjectNumber,
			InodeHeadLength:       uint64(len(rootDirInodeHeadV1Buf)),
		})
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("inodeTable.Put(RootDirInodeNumber,) returned !ok")
		return
	}

	_, superBlockObjectOffset, superBlockObjectLength, err = inodeTable.Flush(false)
	if nil != err {
		return
	}

	superBlockV1 = &ilayout.SuperBlockV1Struct{
		InodeTableRootObjectNumber: superBlockObjectNumber,
		InodeTableRootObjectOffset: superBlockObjectOffset,
		InodeTableRootObjectLength: superBlockObjectLength,
		InodeTableLayout: []ilayout.InodeTableLayoutEntryV1Struct{
			{
				ObjectNumber:    superBlockObjectNumber,
				BytesWritten:    uint64(len(postVolumeSuperBlockInodeTableCallbacks.body)),
				BytesReferenced: uint64(len(postVolumeSuperBlockInodeTableCallbacks.body)),
			},
		},
		InodeObjectCount:     1,
		InodeBytesWritten:    rootDirInodeHeadV1.Layout[0].BytesWritten,
		InodeBytesReferenced: rootDirInodeHeadV1.Layout[0].BytesReferenced,
	}

	superBlockV1Buf, err = superBlockV1.MarshalSuperBlockV1()
	if nil != err {
		return
	}

	postVolumeSuperBlockInodeTableCallbacks.body = append(postVolumeSuperBlockInodeTableCallbacks.body, superBlockV1Buf...)

	err = swiftObjectPut(storageURL, authToken, superBlockObjectNumber, postVolumeSuperBlockInodeTableCallbacks)
	if nil != err {
		return
	}

	// Create CheckPoint

	checkPointV1 = &ilayout.CheckPointV1Struct{
		Version:                ilayout.CheckPointVersionV1,
		SuperBlockObjectNumber: superBlockObjectNumber,
		SuperBlockLength:       uint64(len(superBlockV1Buf)),
		ReservedToNonce:        reservedToNonce,
	}

	checkPointV1String, err = checkPointV1.MarshalCheckPointV1()
	if nil != err {
		return
	}

	err = checkPointWrite(storageURL, authToken, strings.NewReader(checkPointV1String))
	if nil != err {
		return
	}

	err = nil
	return
}

func putVolume(name string, storageURL string, authToken string) (err error) {
	var (
		ok     bool
		volume *volumeStruct
	)

	volume = &volumeStruct{
		name:                          name,
		dirty:                         false,
		storageURL:                    storageURL,
		authToken:                     authToken,
		mountMap:                      make(map[string]*mountStruct),
		healthyMountList:              list.New(),
		leasesExpiredMountList:        list.New(),
		authTokenExpiredMountList:     list.New(),
		deleting:                      false,
		checkPoint:                    nil,
		superBlock:                    nil,
		inodeTable:                    nil,
		inodeTableLayout:              nil,
		nextNonce:                     0,
		numNoncesReserved:             0,
		activeDeleteObjectNumberList:  list.New(),
		pendingDeleteObjectNumberList: list.New(),
		checkPointControlChan:         nil,
		checkPointObjectNumber:        0,
		checkPointPutObjectBuffer:     nil,
		inodeOpenMap:                  make(map[uint64]*inodeOpenMapElementStruct),
		inodeLeaseMap:                 make(map[uint64]*inodeLeaseStruct),
	}

	globals.Lock()

	ok, err = globals.volumeMap.Put(volume.name, volume)
	if nil != err {
		logFatal(err)
	}

	globals.Unlock()

	if ok {
		err = nil
	} else {
		err = fmt.Errorf("volume \"%s\" already exists", name)
	}

	return
}

func (volume *volumeStruct) checkPointDaemon() {
	var (
		checkPointIntervalTimer *time.Timer
		checkPointResponseChan  chan error
		err                     error
		more                    bool
	)

	for {
		checkPointIntervalTimer = time.NewTimer(globals.config.CheckPointInterval)

		select {
		case <-checkPointIntervalTimer.C:
			err = volume.doCheckPoint()
			if nil != err {
				logWarnf("checkPointIntervalTimer-triggered doCheckPoint() failed: %v", err)
			}
		case checkPointResponseChan, more = <-volume.checkPointControlChan:
			if !checkPointIntervalTimer.Stop() {
				<-checkPointIntervalTimer.C
			}

			if more {
				err = volume.doCheckPoint()
				if nil != err {
					logWarnf("requested doCheckPoint() failed: %v", err)
				}

				checkPointResponseChan <- err
			} else {
				volume.activeDeleteObjectWG.Wait()
				volume.checkPointControlWG.Done()
				runtime.Goexit()
			}
		}
	}
}

func (volume *volumeStruct) doCheckPoint() (err error) {
	var (
		authOK                         bool
		checkPointV1String             string
		deleteObjectNumberListElement  *list.Element
		inodeTableLayoutElement        *inodeTableLayoutElementStruct
		inodeTableRootObjectLength     uint64
		inodeTableRootObjectNumber     uint64
		inodeTableRootObjectOffset     uint64
		lastCheckPointObjectNumber     uint64
		lastCheckPointObjectNumberSeen bool
		objectNumber                   uint64
		ok                             bool
		startTime                      time.Time = time.Now()
		superBlockV1Buf                []byte
	)

	defer func() {
		globals.stats.VolumeCheckPointUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	if !volume.dirty {
		globals.Unlock()
		err = nil
		return
	}

	for volume.numNoncesReserved == 0 {
		volume.nextNonce, volume.numNoncesReserved, err = volume.fetchNonceRangeWhileLocked()
		if nil != err {
			globals.Unlock()
			err = fmt.Errorf("volume.fetchNonceRangeWhileLocked() failed: %v", err)
			return
		}
	}

	lastCheckPointObjectNumber = volume.checkPointObjectNumber
	volume.checkPointObjectNumber = volume.nextNonce
	volume.checkPointPutObjectBuffer = &bytes.Buffer{}

	volume.nextNonce++
	volume.numNoncesReserved--

	inodeTableRootObjectNumber, inodeTableRootObjectOffset, inodeTableRootObjectLength, err = volume.inodeTable.Flush(false)
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("volume.inodeTable.Flush(false) failed: %v", err)
		return
	}

	err = volume.inodeTable.Prune()
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("volume.inodeTable.Prune() failed: %v", err)
		return
	}

	volume.superBlock.InodeTableRootObjectNumber = inodeTableRootObjectNumber
	volume.superBlock.InodeTableRootObjectOffset = inodeTableRootObjectOffset
	volume.superBlock.InodeTableRootObjectLength = inodeTableRootObjectLength

	volume.superBlock.InodeTableLayout = make([]ilayout.InodeTableLayoutEntryV1Struct, 0, len(volume.inodeTableLayout))

	for objectNumber, inodeTableLayoutElement = range volume.inodeTableLayout {
		volume.superBlock.InodeTableLayout = append(volume.superBlock.InodeTableLayout, ilayout.InodeTableLayoutEntryV1Struct{
			ObjectNumber:    objectNumber,
			BytesWritten:    inodeTableLayoutElement.bytesWritten,
			BytesReferenced: inodeTableLayoutElement.bytesReferenced,
		})
	}

	volume.superBlock.PendingDeleteObjectNumberArray = make([]uint64, 0, volume.activeDeleteObjectNumberList.Len()+volume.pendingDeleteObjectNumberList.Len())

	lastCheckPointObjectNumberSeen = false

	deleteObjectNumberListElement = volume.activeDeleteObjectNumberList.Front()

	for nil != deleteObjectNumberListElement {
		objectNumber, ok = deleteObjectNumberListElement.Value.(uint64)
		if !ok {
			err = fmt.Errorf("deleteObjectNumberListElement.Value.(uint64) returned !ok")
			logFatal(err)
		}
		volume.superBlock.PendingDeleteObjectNumberArray = append(volume.superBlock.PendingDeleteObjectNumberArray, objectNumber)
		if objectNumber == lastCheckPointObjectNumber {
			lastCheckPointObjectNumberSeen = true
		}
		deleteObjectNumberListElement = deleteObjectNumberListElement.Next()
	}

	deleteObjectNumberListElement = volume.pendingDeleteObjectNumberList.Front()

	for nil != deleteObjectNumberListElement {
		objectNumber, ok = deleteObjectNumberListElement.Value.(uint64)
		if !ok {
			err = fmt.Errorf("deleteObjectNumberListElement.Value.(uint64) returned !ok")
			logFatal(err)
		}
		volume.superBlock.PendingDeleteObjectNumberArray = append(volume.superBlock.PendingDeleteObjectNumberArray, objectNumber)
		if objectNumber == lastCheckPointObjectNumber {
			lastCheckPointObjectNumberSeen = true
		}
		deleteObjectNumberListElement = deleteObjectNumberListElement.Next()
	}

	if !lastCheckPointObjectNumberSeen {
		_, ok = volume.inodeTableLayout[lastCheckPointObjectNumber]
		if !ok {
			// After the current CheckPoint is posted, we can finally delete the last one's now comp[letely unreferenced Object

			_ = volume.pendingDeleteObjectNumberList.PushBack(lastCheckPointObjectNumber)
			volume.superBlock.PendingDeleteObjectNumberArray = append(volume.superBlock.PendingDeleteObjectNumberArray, lastCheckPointObjectNumber)
		}
	}

	superBlockV1Buf, err = volume.superBlock.MarshalSuperBlockV1()
	if nil != err {
		err = fmt.Errorf("deleteObjectNumberListElement.Value.(uint64) failed: %v", err)
		logFatal(err)
	}

	_, _ = volume.checkPointPutObjectBuffer.Write(superBlockV1Buf)

	volume.checkPoint.SuperBlockObjectNumber = volume.checkPointObjectNumber
	volume.checkPoint.SuperBlockLength = uint64(len(superBlockV1Buf))

	authOK, err = volume.swiftObjectPut(true, volume.checkPointObjectNumber, bytes.NewReader(volume.checkPointPutObjectBuffer.Bytes()))
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectPut(locked==true, volume.checkPointObjectNumber, bytes.NewReader(volume.checkPointPutObjectBuffer.Bytes())) failed: %v", err)
		logFatal(err)
	}
	if !authOK {
		globals.Unlock()
		err = fmt.Errorf("volume.swiftObjectPut(locked==true, volume.checkPointObjectNumber, bytes.NewReader(volume.checkPointPutObjectBuffer.Bytes())) returned !authOK")
		return
	}

	volume.checkPointPutObjectBuffer = nil

	checkPointV1String, err = volume.checkPoint.MarshalCheckPointV1()
	if nil != err {
		err = fmt.Errorf("volume.checkPoint.MarshalCheckPointV1() failed: %v", err)
		logFatal(err)
	}

	authOK, err = volume.checkPointWrite(strings.NewReader(checkPointV1String))
	if nil != err {
		err = fmt.Errorf("volume.checkPointWrite(strings.NewReader(checkPointV1String)) failed: %v", err)
		logFatal(err)
	}
	if !authOK {
		globals.Unlock()
		err = fmt.Errorf("volume.checkPointWrite(strings.NewReader(checkPointV1String)) returned !authOK")
		return
	}

	for (uint32(volume.activeDeleteObjectNumberList.Len()) < globals.config.ParallelObjectDeletePerVolumeLimit) && (volume.pendingDeleteObjectNumberList.Len() > 0) {
		deleteObjectNumberListElement = volume.pendingDeleteObjectNumberList.Front()
		_ = volume.pendingDeleteObjectNumberList.Remove(deleteObjectNumberListElement)
		objectNumber, ok = deleteObjectNumberListElement.Value.(uint64)
		if !ok {
			err = fmt.Errorf("deleteObjectNumberListElement.Value.(uint64) returned !ok")
			logFatal(err)
		}
		deleteObjectNumberListElement = volume.activeDeleteObjectNumberList.PushBack(objectNumber)
		volume.activeDeleteObjectWG.Add(1)
		go volume.doObjectDelete(deleteObjectNumberListElement)
	}

	volume.dirty = false

	globals.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) doObjectDelete(activeDeleteObjectNumberListElement *list.Element) {
	var (
		authOK       bool
		err          error
		objectNumber uint64
		ok           bool
	)

	globals.Lock()

	objectNumber, ok = activeDeleteObjectNumberListElement.Value.(uint64)
	if !ok {
		logFatalf("activeDeleteObjectNumberListElement.Value.(uint64) returned !ok")
	}

	globals.Unlock()

	authOK, err = volume.swiftObjectDelete(false, objectNumber)
	if nil != err {
		logFatalf("volume.swiftObjectDelete(locked: false, objectNumber: %016X) failed: %v", objectNumber, err)
	}

	globals.Lock()

	volume.activeDeleteObjectNumberList.Remove(activeDeleteObjectNumberListElement)

	if !authOK {
		_ = volume.pendingDeleteObjectNumberList.PushBack(objectNumber)
	}

	// The last doObjectDelete() instance to complete, if ever reached, has the
	// responsibility of determining if a new batch should be launched. A curious
	// reverse-chicked-and-egg situation can arrise if the only reason for a subsequent
	// CheckPoint is to update the at the time now empty PendingDeleteObjectNumberArray.
	// Of course this would generate a new entry to delete the just previous CheckPoint.
	// As such, we will only mark the volume dirty if the PendingDeleteObjectNumberArray
	// has at least 2 entries.

	if volume.activeDeleteObjectNumberList.Len() > 0 {
		volume.dirty = true
	} else {
		if volume.pendingDeleteObjectNumberList.Len() >= 2 {
			volume.dirty = true
		}
	}

	globals.Unlock()

	volume.activeDeleteObjectWG.Done()
}

func (volume *volumeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		keyAsInodeNumber uint64
		ok               bool
	)

	keyAsInodeNumber, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("key.(uint64) returned !ok")
		return
	}

	keyAsString = fmt.Sprintf("%016X", keyAsInodeNumber)

	err = nil
	return
}

func (volume *volumeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok                          bool
		valueAsInodeTableEntryValue ilayout.InodeTableEntryValueV1Struct
	)

	valueAsInodeTableEntryValue, ok = value.(ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("value.(ilayout.InodeTableEntryValueV1Struct) returned !ok")
		return
	}

	valueAsString = fmt.Sprintf("[%016X %016X]", valueAsInodeTableEntryValue.InodeHeadObjectNumber, valueAsInodeTableEntryValue.InodeHeadLength)

	err = nil
	return
}

func (volume *volumeStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		authOK bool
	)

	nodeByteSlice, authOK, err = volume.swiftObjectGetRange(true, objectNumber, objectOffset, objectLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetRange(locked==true, objectNumber==0x%016X, objectOffset==0x%016X, objectLength==0x%016X) failed: %v", objectNumber, objectOffset, objectLength, err)
		logError(err)
	} else if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetRange(locked==true, objectNumber==0x%016X, objectOffset==0x%016X, objectLength==0x%016X) returned !authOK", objectNumber, objectOffset, objectLength)
		logWarn(err)
	}

	return
}

func (volume *volumeStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	var (
		inodeTableLayoutElement *inodeTableLayoutElementStruct
		ok                      bool
	)

	if volume.checkPointObjectNumber == 0 {
		err = fmt.Errorf("(*volumeStruct)PutNode() called with volume.checkPointObjectNumber == 0")
		logFatal(err)
	}
	if volume.checkPointPutObjectBuffer == nil {
		err = fmt.Errorf("(*volumeStruct)PutNode() called with volume.checkPointPutObjectBuffer == nil")
		logFatal(err)
	}

	inodeTableLayoutElement, ok = volume.inodeTableLayout[volume.checkPointObjectNumber]
	if ok {
		inodeTableLayoutElement.bytesWritten += uint64(len(nodeByteSlice))
		inodeTableLayoutElement.bytesReferenced += uint64(len(nodeByteSlice))
	} else {
		inodeTableLayoutElement = &inodeTableLayoutElementStruct{
			bytesWritten:    uint64(len(nodeByteSlice)),
			bytesReferenced: uint64(len(nodeByteSlice)),
		}

		volume.inodeTableLayout[volume.checkPointObjectNumber] = inodeTableLayoutElement
	}

	objectNumber = volume.checkPointObjectNumber
	objectOffset = uint64(volume.checkPointPutObjectBuffer.Len())
	_, _ = volume.checkPointPutObjectBuffer.Write(nodeByteSlice)

	err = nil
	return
}

func (volume *volumeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	var (
		inodeTableLayoutElement *inodeTableLayoutElementStruct
		ok                      bool
	)

	inodeTableLayoutElement, ok = volume.inodeTableLayout[objectNumber]
	if !ok {
		err = fmt.Errorf("volume.inodeTableLayout[objectNumber] returned !ok")
		return
	}

	if inodeTableLayoutElement.bytesReferenced < objectLength {
		err = fmt.Errorf("inodeTableLayoutElement.bytesReferenced < objectLength")
		return
	}

	inodeTableLayoutElement.bytesReferenced -= objectLength

	if inodeTableLayoutElement.bytesReferenced == 0 {
		delete(volume.inodeTableLayout, objectNumber)

		// We avoid scheduling the Object for deletion here if it contains the last CheckPoint

		if objectNumber != volume.checkPointObjectNumber {
			_ = volume.pendingDeleteObjectNumberList.PushBack(objectNumber)
		}
	}

	err = nil
	return
}

func (volume *volumeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsUint64 uint64
		nextPos     int
		ok          bool
	)

	keyAsUint64, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("(*volumeStruct).PackKey(key:%v) called with non-uint64", key)
		return
	}

	packedKey = make([]byte, 8)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedKey, 0, keyAsUint64)
	if nil != err {
		return
	}

	if len(packedKey) != nextPos {
		err = fmt.Errorf("(*volumeStruct).PackKey(key:%016X) logic error", keyAsUint64)
		return
	}

	err = nil
	return
}

func (volume *volumeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		nextPos int
	)

	key, nextPos, err = ilayout.GetLEUint64FromBuf(payloadData, 0)
	if (nil == err) && (nextPos != 8) {
		err = fmt.Errorf("ilayout.GetLEUint64FromBuf(payloadData, 0) consumed %v bytes (8 expected)", nextPos)
	}

	bytesConsumed = 8

	return
}

func (volume *volumeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		ok                            bool
		valueAsInodeTableEntryValueV1 ilayout.InodeTableEntryValueV1Struct
	)

	valueAsInodeTableEntryValueV1, ok = value.(ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("(*volumeStruct).PackValue(value:%v) called with non-InodeTableEntryValueV1Struct", value)
		return
	}

	packedValue, err = valueAsInodeTableEntryValueV1.MarshalInodeTableEntryValueV1()

	return
}

func (volume *volumeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt     int
		inodeTableEntryValueV1 *ilayout.InodeTableEntryValueV1Struct
	)

	inodeTableEntryValueV1, bytesConsumedAsInt, err = ilayout.UnmarshalInodeTableEntryValueV1(payloadData)
	if (nil == err) && (bytesConsumedAsInt != 24) {
		err = fmt.Errorf("ilayout.UnmarshalInodeTableEntryValueV1(payloadData) consumed %v bytes (24 expected)", bytesConsumedAsInt)
	}

	value = *inodeTableEntryValueV1
	bytesConsumed = 24

	return
}

func (volume *volumeStruct) fetchCheckPointWhileLocked() (checkPointV1 *ilayout.CheckPointV1Struct, err error) {
	var (
		checkPointV1Buf []byte
	)

	checkPointV1Buf, err = volume.checkPointRead()
	if err != nil {
		return
	}

	checkPointV1, err = ilayout.UnmarshalCheckPointV1(string(checkPointV1Buf[:]))

	return
}

func (volume *volumeStruct) fetchSuperBlockWhileLocked(superBlockObjectNumber uint64, superBlockLength uint64) (superBlockV1 *ilayout.SuperBlockV1Struct, err error) {
	var (
		authOK          bool
		superBlockV1Buf []byte
	)

	superBlockV1Buf, authOK, err = volume.swiftObjectGetTail(true, superBlockObjectNumber, superBlockLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetTail() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetTail() returned !authOK")
		return
	}

	superBlockV1, err = ilayout.UnmarshalSuperBlockV1(superBlockV1Buf)

	return
}

func (volume *volumeStruct) fetchInodeHeadWhileLocked(inodeHeadObjectNumber uint64, inodeHeadLength uint64) (inodeHeadV1 *ilayout.InodeHeadV1Struct, err error) {
	var (
		authOK         bool
		inodeHeadV1Buf []byte
	)

	inodeHeadV1Buf, authOK, err = volume.swiftObjectGetTail(true, inodeHeadObjectNumber, inodeHeadLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetTail() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetTail() returned !authOK")
		return
	}

	inodeHeadV1, err = ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf)

	return
}

func (volume *volumeStruct) statusWhileLocked() (numInodes uint64, objectCount uint64, bytesWritten uint64, bytesReferenced uint64) {
	var (
		err           error
		inodeTableLen int
	)

	inodeTableLen, err = volume.inodeTable.Len()
	if nil != err {
		logFatalf("volume.inodeTable.Len() failed: %v\n", err)
	}
	numInodes = uint64(inodeTableLen)

	objectCount = volume.superBlock.InodeObjectCount
	bytesWritten = volume.superBlock.InodeBytesWritten
	bytesReferenced = volume.superBlock.InodeBytesReferenced

	return
}

func (volume *volumeStruct) fetchNonceRangeWhileLocked() (nextNonce uint64, numNoncesFetched uint64, err error) {
	var (
		authOK                         bool
		nonceUpdatedCheckPoint         *ilayout.CheckPointV1Struct
		nonceUpdatedCheckPointAsString string
	)

	nonceUpdatedCheckPoint = &ilayout.CheckPointV1Struct{}
	*nonceUpdatedCheckPoint = *volume.checkPoint

	nonceUpdatedCheckPoint.ReservedToNonce += globals.config.FetchNonceRangeToReturn

	nextNonce = volume.checkPoint.ReservedToNonce + 1
	numNoncesFetched = globals.config.FetchNonceRangeToReturn

	nonceUpdatedCheckPointAsString, err = nonceUpdatedCheckPoint.MarshalCheckPointV1()
	if nil != err {
		logFatalf("nonceUpdatedCheckPoint.MarshalCheckPointV1() failed: %v", err)
	}

	authOK, err = volume.checkPointWrite(strings.NewReader(nonceUpdatedCheckPointAsString))
	if nil == err {
		if authOK {
			volume.checkPoint = nonceUpdatedCheckPoint
		} else {
			nextNonce = 0
			numNoncesFetched = 0
			err = fmt.Errorf("volume.checkPointWrite(strings.NewReader(nonceUpdatedCheckPointAsString)) returned !authOK")
		}
	} else {
		nextNonce = 0
		numNoncesFetched = 0
		err = fmt.Errorf("volume.checkPointWrite(strings.NewReader(nonceUpdatedCheckPointAsString)) failed: %v", err)
	}

	return
}

func (volume *volumeStruct) removeInodeWhileLocked(inodeNumber uint64) {
	var (
		authOK                        bool
		err                           error
		inodeHeadLayoutEntryV1        ilayout.InodeHeadLayoutEntryV1Struct
		inodeHeadObjectNumberInLayout bool
		inodeHeadV1                   *ilayout.InodeHeadV1Struct
		inodeHeadV1Buf                []byte
		inodeTableEntryValue          ilayout.InodeTableEntryValueV1Struct
		inodeTableEntryValueRaw       sortedmap.Value
		ok                            bool
	)

	inodeTableEntryValueRaw, ok, err = volume.inodeTable.GetByKey(inodeNumber)
	if nil != err {
		logFatalf("volume.inodeTable.GetByKey(inodeNumber) failed: %v", err)
	}
	if !ok {
		logFatalf("volume.inodeTable.GetByKey(inodeNumber) returned !ok")
	}

	inodeTableEntryValue, ok = inodeTableEntryValueRaw.(ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		logFatalf("inodeTableEntryValueRaw.(ilayout.InodeTableEntryValueV1Struct) returned !ok")
	}

	inodeHeadV1Buf, authOK, err = volume.swiftObjectGetTail(true, inodeTableEntryValue.InodeHeadObjectNumber, inodeTableEntryValue.InodeHeadLength)
	if nil != err {
		logFatalf("volume.swiftObjectGetTail(locked==true, inodeTableEntryValue.InodeHeadObjectNumber, inodeTableEntryValue.InodeHeadObjectNumber) failed: %v", err)
	}
	if !authOK {
		logFatalf("volume.swiftObjectGetTail(locked==true, inodeTableEntryValue.InodeHeadObjectNumber, inodeTableEntryValue.InodeHeadObjectNumber) returned !authOK")
	}

	inodeHeadV1, err = ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf)
	if nil != err {
		logFatalf("ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf) failed: %v", err)
	}

	inodeHeadObjectNumberInLayout = false

	for _, inodeHeadLayoutEntryV1 = range inodeHeadV1.Layout {
		_ = volume.pendingDeleteObjectNumberList.PushBack(inodeHeadLayoutEntryV1.ObjectNumber)

		volume.superBlock.InodeObjectCount--
		volume.superBlock.InodeBytesWritten -= inodeHeadLayoutEntryV1.BytesWritten
		volume.superBlock.InodeBytesReferenced -= inodeHeadLayoutEntryV1.BytesReferenced

		if inodeHeadLayoutEntryV1.ObjectNumber == inodeTableEntryValue.InodeHeadObjectNumber {
			inodeHeadObjectNumberInLayout = true
		}
	}

	if !inodeHeadObjectNumberInLayout {
		_ = volume.pendingDeleteObjectNumberList.PushBack(inodeTableEntryValue.InodeHeadObjectNumber)
	}

	ok, err = volume.inodeTable.DeleteByKey(inodeNumber)
	if nil != err {
		logFatalf("volume.inodeTable.DeleteByKey(inodeNumber: %016X) failed: %v", inodeNumber, err)
	}
	if !ok {
		logFatalf("volume.inodeTable.DeleteByKey(inodeNumber: %016X) returned !ok", inodeNumber)
	}
}

func checkAuthToken(storageURL string, authToken string) (authTokenIsAuthorized bool) {
	var (
		err error
	)

	_, err = swiftObjectGet(storageURL, authToken, ilayout.CheckPointObjectNumber)

	authTokenIsAuthorized = (err == nil)

	return
}

func doCheckPointHTTPRequest(httpRequest *http.Request, httpResponse **http.Response, buf *[]byte, err *error, wg *sync.WaitGroup) {
	*httpResponse, *err = globals.checkPointHTTPClient.Do(httpRequest)
	if *err == nil {
		if buf == nil {
			_, *err = ioutil.ReadAll((*httpResponse).Body)
		} else {
			*buf, *err = ioutil.ReadAll((*httpResponse).Body)
		}
		if *err == nil {
			*err = (*httpResponse).Body.Close()
		}
	}

	wg.Done()
}

func fetchCheckPointRandomPath() (checkPointRandomPath string) {
	var (
		checkPointRandomPathAsByte      byte
		checkPointRandomPathAsByteSlice []byte
		err                             error
	)

	checkPointRandomPathAsByteSlice = make([]byte, checkPointRandomPathByteSliceLen)

	_, err = rand.Read(checkPointRandomPathAsByteSlice)
	if err != nil {
		logFatalf("rand.Read(checkPointRandomPathAsByteSlice) failed: %v", err)
	}

	checkPointRandomPath = ""

	for _, checkPointRandomPathAsByte = range checkPointRandomPathAsByteSlice {
		checkPointRandomPath += fmt.Sprintf("%02X", checkPointRandomPathAsByte)
	}

	return
}

func checkPointReadOnce(checkPointObjectURL string, authToken string) (buf []byte, authOK bool, err error) {
	var (
		bufSlice           [][]byte
		checkPointURL      string
		checkPointURLIndex int
		errSlice           []error
		httpRequest        *http.Request
		httpResponse       *http.Response
		httpResponseSlice  []*http.Response
		majority           int
		majorityMap        map[string]int
		majorityMapKey     string
		majorityMapValue   int
		ok                 bool
		wg                 sync.WaitGroup
	)

	httpResponseSlice = make([]*http.Response, len(globals.checkPointURL))
	bufSlice = make([][]byte, len(globals.checkPointURL))
	errSlice = make([]error, len(globals.checkPointURL))

	wg.Add(len(globals.config.CheckPointIPAddrs))

	for checkPointURLIndex, checkPointURL = range globals.checkPointURL {
		httpRequest, err = http.NewRequest("GET", checkPointURL, nil)
		if err != nil {
			return
		}

		httpRequest.Header.Add("X-Storage-Url", checkPointObjectURL)
		httpRequest.Header.Add("X-Auth-Token", authToken)

		go doCheckPointHTTPRequest(httpRequest, &httpResponseSlice[checkPointURLIndex], &bufSlice[checkPointURLIndex], &errSlice[checkPointURLIndex], &wg)
	}

	wg.Wait()

	majorityMap = make(map[string]int)

	for checkPointURLIndex = range globals.checkPointURL {
		if errSlice[checkPointURLIndex] == nil {
			httpResponse = httpResponseSlice[checkPointURLIndex]

			if httpResponse.StatusCode == http.StatusUnauthorized {
				// Special case: any auth failures get reported over errors or majority

				buf = nil
				authOK = false
				err = nil

				return
			}

			if (httpResponse.StatusCode >= 200) && (httpResponse.StatusCode <= 299) {
				majorityMapKey = string(bufSlice[checkPointURLIndex][:])

				majorityMapValue, ok = majorityMap[majorityMapKey]
				if ok {
					majorityMapValue++
				} else {
					majorityMapValue = 1
				}
				majorityMap[majorityMapKey] = majorityMapValue
			}
		}
	}

	// If we reach here, there were no auth failures... but we must search for a majority

	majority = (len(globals.checkPointURL) / 2) + 1

	for majorityMapKey, majorityMapValue = range majorityMap {
		if majorityMapValue >= majority {
			// Success... we can return majorityMapKey as buf right now

			buf = []byte(majorityMapKey)
			authOK = true
			err = nil

			return
		}
	}

	// If we reach here, no majority was reached

	buf = nil
	authOK = true
	err = fmt.Errorf("no majority was reached")

	return
}

func (volume *volumeStruct) checkPointReadOnce(checkPointObjectURL string) (buf []byte, err error) {
	var (
		authOK               bool
		authToken            string
		mount                *mountStruct
		mountListElement     *list.Element
		ok                   bool
		usingVolumeAuthToken bool
	)

	mountListElement = volume.healthyMountList.Front()

	if mountListElement == nil {
		if volume.authToken == "" {
			err = fmt.Errorf("volume.healthyMountList and volume.authToken are empty")
			return
		}

		authToken = volume.authToken
		usingVolumeAuthToken = true
	} else {
		mount, ok = mountListElement.Value.(*mountStruct)
		if !ok {
			logFatalf("mountListElement.Value.(*mountStruct) returned !ok")
		}

		// We know that mount.mountListMembership == onHealthyMountList

		volume.healthyMountList.MoveToBack(mount.mountListElement)

		authToken = mount.authToken
		usingVolumeAuthToken = true
	}

	buf, authOK, err = checkPointReadOnce(checkPointObjectURL, authToken)
	if err == nil {
		if !authOK {
			if usingVolumeAuthToken {
				logWarnf("checkPointReadOnce(checkPointObjectURL,volume.authToken) returned !authOK for volume %s...clearing volume.authToken", volume.name)
				volume.authToken = ""
			} else {
				mount.authTokenExpired = true

				// It's possible that mount has "moved" from volume.healthyMountList

				switch mount.mountListMembership {
				case onHealthyMountList:
					_ = mount.volume.healthyMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onLeasesExpiredMountList:
					_ = mount.volume.leasesExpiredMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onAuthTokenExpiredMountList:
					volume.authTokenExpiredMountList.MoveToBack(mount.mountListElement)
				default:
					logFatalf("mount.mountListMembership (%v) not one of on{Healthy|LeasesExpired|AuthTokenExpired}MountList")
				}
			}

			err = fmt.Errorf("checkPointWriteOnce(checkPointObjectURL, authToken, body) returned !authOK")
		}
	}

	return
}

func checkPointRead(storageURL string, authToken string) (buf []byte, err error) {
	var (
		authOK                   bool
		checkPointObjectURL      string
		nextCheckPointRetryDelay time.Duration
		numCheckPointRetries     uint32
		startTime                time.Time = time.Now()
	)

	defer func() {
		globals.stats.CheckPointReadUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	if globals.checkPointURL == nil {
		buf, err = swiftObjectGet(storageURL, authToken, ilayout.CheckPointObjectNumber)
		return
	}

	checkPointObjectURL = storageURL + "/" + ilayout.GetObjectNameAsString(ilayout.CheckPointObjectNumber)

	nextCheckPointRetryDelay = globals.config.CheckPointRetryDelay

	for numCheckPointRetries = 0; numCheckPointRetries <= globals.config.CheckPointRetryLimit; numCheckPointRetries++ {
		buf, authOK, err = checkPointReadOnce(checkPointObjectURL, authToken)
		if (err == nil) && authOK {
			return
		}

		time.Sleep(nextCheckPointRetryDelay)

		nextCheckPointRetryDelay = time.Duration(float64(nextCheckPointRetryDelay) * globals.config.CheckPointRetryExpBackoff)
	}

	buf = nil
	err = fmt.Errorf("globals.config.CheckPointRetryLimit exceeded")

	return
}

func (volume *volumeStruct) checkPointRead() (buf []byte, err error) {
	var (
		authOK                   bool
		checkPointObjectURL      string
		nextCheckPointRetryDelay time.Duration
		numCheckPointRetries     uint32
		startTime                time.Time = time.Now()
	)

	defer func() {
		globals.stats.CheckPointReadUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	if globals.checkPointURL == nil {
		buf, authOK, err = volume.swiftObjectGet(true, ilayout.CheckPointObjectNumber)
		if (err == nil) && !authOK {
			err = fmt.Errorf("volume.swiftObjectGet(true, ilayout.CheckPointObjectNumber) returned !authOK")
		}
		return
	}

	checkPointObjectURL = volume.storageURL + "/" + ilayout.GetObjectNameAsString(ilayout.CheckPointObjectNumber)

	nextCheckPointRetryDelay = globals.config.CheckPointRetryDelay

	for numCheckPointRetries = 0; numCheckPointRetries <= globals.config.CheckPointRetryLimit; numCheckPointRetries++ {
		buf, err = volume.checkPointReadOnce(checkPointObjectURL)
		if err == nil {
			return
		}

		time.Sleep(nextCheckPointRetryDelay)

		nextCheckPointRetryDelay = time.Duration(float64(nextCheckPointRetryDelay) * globals.config.CheckPointRetryExpBackoff)
	}

	buf = nil
	err = fmt.Errorf("globals.config.CheckPointRetryLimit exceeded")

	return
}

func checkPointWriteOnce(checkPointObjectURL string, authToken string, body io.ReadSeeker) (authOK bool, err error) {
	var (
		bodyAsByteSlice                []byte
		checkPointURL                  string
		checkPointURLIndex             int
		checkPointURLRandomPath        string
		checkPointURLWithRandomPath    string
		checkPointURLWithSuccessfulPUT []string
		errSlice                       []error
		httpRequest                    *http.Request
		httpResponse                   *http.Response
		httpResponseSlice              []*http.Response
		majority                       int
		successfulPOSTs                int
		successfulPUTs                 int
		wg                             sync.WaitGroup
	)

	// Restart body (just in case) and clone it to avoid parallel use of the same io.Reader in http.NewRequest() call below

	body.Seek(0, io.SeekStart)
	bodyAsByteSlice, err = ioutil.ReadAll(body)

	checkPointURLRandomPath = fetchCheckPointRandomPath()

	httpResponseSlice = make([]*http.Response, len(globals.checkPointURL))
	errSlice = make([]error, len(globals.checkPointURL))

	wg.Add(len(globals.config.CheckPointIPAddrs))

	for checkPointURLIndex, checkPointURL = range globals.checkPointURL {
		checkPointURLWithRandomPath = checkPointURL + "/" + checkPointURLRandomPath

		httpRequest, err = http.NewRequest("PUT", checkPointURLWithRandomPath, bytes.NewReader(bodyAsByteSlice))
		if err != nil {
			return
		}

		httpRequest.Header.Add("X-Storage-Url", checkPointObjectURL)
		httpRequest.Header.Add("X-Auth-Token", authToken)

		go doCheckPointHTTPRequest(httpRequest, &httpResponseSlice[checkPointURLIndex], nil, &errSlice[checkPointURLIndex], &wg)
	}

	wg.Wait()

	successfulPUTs = 0
	checkPointURLWithSuccessfulPUT = make([]string, 0, len(globals.checkPointURL))

	for checkPointURLIndex, checkPointURL = range globals.checkPointURL {
		if errSlice[checkPointURLIndex] == nil {
			httpResponse = httpResponseSlice[checkPointURLIndex]

			if httpResponse.StatusCode == http.StatusUnauthorized {
				// Special case: any auth failures get reported over errors or majority

				authOK = false
				err = nil

				return
			}

			if (httpResponse.StatusCode >= 200) && (httpResponse.StatusCode <= 299) {
				successfulPUTs++

				checkPointURLWithSuccessfulPUT = append(checkPointURLWithSuccessfulPUT, checkPointURL)
			}
		}
	}

	// If we reach here, there were no auth failures... but we must search for a majority

	majority = (len(globals.checkPointURL) / 2) + 1

	if len(checkPointURLWithSuccessfulPUT) < majority {
		// No use doing the POST phase since no majority (of those) is possible

		err = fmt.Errorf("no majority was reached during PUT phase")

		return
	}

	wg.Add(len(globals.config.CheckPointIPAddrs))

	for checkPointURLIndex, checkPointURL = range checkPointURLWithSuccessfulPUT {
		checkPointURLWithRandomPath = checkPointURL + "/" + checkPointURLRandomPath

		httpRequest, err = http.NewRequest("POST", checkPointURLWithRandomPath, nil)
		if err != nil {
			return
		}

		httpRequest.Header.Add("X-Storage-Url", checkPointObjectURL)
		httpRequest.Header.Add("X-Auth-Token", authToken)

		go doCheckPointHTTPRequest(httpRequest, &httpResponseSlice[checkPointURLIndex], nil, &errSlice[checkPointURLIndex], &wg)
	}

	wg.Wait()

	successfulPOSTs = 0

	for checkPointURLIndex, checkPointURL = range checkPointURLWithSuccessfulPUT {
		if errSlice[checkPointURLIndex] == nil {
			httpResponse = httpResponseSlice[checkPointURLIndex]

			if httpResponse.StatusCode == http.StatusUnauthorized {
				// Special case: any auth failures get reported over errors or majority

				authOK = false
				err = nil

				return
			}

			if (httpResponse.StatusCode >= 200) && (httpResponse.StatusCode <= 299) {
				successfulPOSTs++
			}
		}
	}

	// If we reach here, there were (again) no auth failures... but we must check majority

	authOK = true

	if successfulPOSTs >= majority {
		err = nil
	} else {
		err = fmt.Errorf("no majority was reached during POST phase")
	}

	return
}

func (volume *volumeStruct) checkPointWriteOnce(checkPointObjectURL string, body io.ReadSeeker) (authOK bool, err error) {
	var (
		authToken            string
		mount                *mountStruct
		mountListElement     *list.Element
		ok                   bool
		usingVolumeAuthToken bool
	)

	mountListElement = volume.healthyMountList.Front()

	if mountListElement == nil {
		if volume.authToken == "" {
			err = fmt.Errorf("volume.healthyMountList and volume.authToken are empty")
			return
		}

		authToken = volume.authToken
		usingVolumeAuthToken = true
	} else {
		mount, ok = mountListElement.Value.(*mountStruct)
		if !ok {
			logFatalf("mountListElement.Value.(*mountStruct) returned !ok")
		}

		// We know that mount.mountListMembership == onHealthyMountList

		volume.healthyMountList.MoveToBack(mount.mountListElement)

		authToken = mount.authToken
		usingVolumeAuthToken = true
	}

	authOK, err = checkPointWriteOnce(checkPointObjectURL, authToken, body)
	if err == nil {
		if !authOK {
			if usingVolumeAuthToken {
				logWarnf("swiftObjectPutOnce(checkPointObjectURL,volume.authToken,body) returned !authOK for volume %s...clearing volume.authToken", volume.name)
				volume.authToken = ""
			} else {
				mount.authTokenExpired = true

				// It's possible that mount has "moved" from volume.healthyMountList

				switch mount.mountListMembership {
				case onHealthyMountList:
					_ = mount.volume.healthyMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onLeasesExpiredMountList:
					_ = mount.volume.leasesExpiredMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onAuthTokenExpiredMountList:
					volume.authTokenExpiredMountList.MoveToBack(mount.mountListElement)
				default:
					logFatalf("mount.mountListMembership (%v) not one of on{Healthy|LeasesExpired|AuthTokenExpired}MountList")
				}
			}
		}
	}

	return
}

func checkPointWrite(storageURL string, authToken string, body io.ReadSeeker) (err error) {
	var (
		authOK                   bool
		checkPointObjectURL      string
		nextCheckPointRetryDelay time.Duration
		numCheckPointRetries     uint32
		startTime                time.Time = time.Now()
	)

	defer func() {
		globals.stats.CheckPointWriteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = swiftObjectPut(storageURL, authToken, ilayout.CheckPointObjectNumber, body)
	if nil != err {
		return
	}

	if globals.checkPointURL == nil {
		return
	}

	checkPointObjectURL = storageURL + "/" + ilayout.GetObjectNameAsString(ilayout.CheckPointObjectNumber)

	nextCheckPointRetryDelay = globals.config.CheckPointRetryDelay

	for numCheckPointRetries = 0; numCheckPointRetries <= globals.config.CheckPointRetryLimit; numCheckPointRetries++ {
		authOK, err = checkPointWriteOnce(checkPointObjectURL, authToken, body)
		if (err == nil) && authOK {
			return
		}

		time.Sleep(nextCheckPointRetryDelay)

		nextCheckPointRetryDelay = time.Duration(float64(nextCheckPointRetryDelay) * globals.config.CheckPointRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.CheckPointRetryLimit exceeded")

	return
}

func (volume *volumeStruct) checkPointWrite(body io.ReadSeeker) (authOK bool, err error) {
	var (
		checkPointObjectURL      string
		nextCheckPointRetryDelay time.Duration
		numCheckPointRetries     uint32
		startTime                time.Time = time.Now()
	)

	defer func() {
		globals.stats.CheckPointWriteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	authOK, err = volume.swiftObjectPut(true, ilayout.CheckPointObjectNumber, body)
	if (err != nil) || !authOK {
		return
	}

	if globals.checkPointURL == nil {
		return
	}

	checkPointObjectURL = volume.storageURL + "/" + ilayout.GetObjectNameAsString(ilayout.CheckPointObjectNumber)

	nextCheckPointRetryDelay = globals.config.CheckPointRetryDelay

	for numCheckPointRetries = 0; numCheckPointRetries <= globals.config.CheckPointRetryLimit; numCheckPointRetries++ {
		authOK, err = volume.checkPointWriteOnce(checkPointObjectURL, body)
		if err == nil {
			// Note: Either value of authOK is appropriate to return here
			return
		}

		time.Sleep(nextCheckPointRetryDelay)

		nextCheckPointRetryDelay = time.Duration(float64(nextCheckPointRetryDelay) * globals.config.CheckPointRetryExpBackoff)
	}

	authOK = false
	err = fmt.Errorf("globals.config.CheckPointRetryLimit exceeded")

	return
}
