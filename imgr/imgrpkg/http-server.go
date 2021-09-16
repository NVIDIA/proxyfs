// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/version"
	"github.com/NVIDIA/sortedmap"
)

const (
	startHTTPServerUpCheckDelay      = 100 * time.Millisecond
	startHTTPServerUpCheckMaxRetries = 10

	httpServerInodeTableCacheLowLimit  = 1000
	httpServerInodeTableCacheHighLimit = 1010
	httpServerDirectoryCacheLowLimit   = 1000
	httpServerDirectoryCacheHighLimit  = 1010
	httpServerExtentMapCacheLowLimit   = 1000
	httpServerExtentMapCacheHighLimit  = 1010
)

func startHTTPServer() (err error) {
	var (
		ipAddrTCPPort                 string
		startHTTPServerUpCheckRetries uint32
	)

	ipAddrTCPPort = net.JoinHostPort(globals.config.PrivateIPAddr, strconv.Itoa(int(globals.config.HTTPServerPort)))

	globals.httpServer = &http.Server{
		Addr:    ipAddrTCPPort,
		Handler: &globals,
	}

	globals.httpServerWG.Add(1)

	go func() {
		var (
			err error
		)

		err = globals.httpServer.ListenAndServe()
		if http.ErrServerClosed != err {
			log.Fatalf("httpServer.ListenAndServe() exited unexpectedly: %v", err)
		}

		globals.httpServerWG.Done()
	}()

	for startHTTPServerUpCheckRetries = 0; startHTTPServerUpCheckRetries < startHTTPServerUpCheckMaxRetries; startHTTPServerUpCheckRetries++ {
		_, err = http.Get("http://" + ipAddrTCPPort + "/version")
		if nil == err {
			return
		}

		time.Sleep(startHTTPServerUpCheckDelay)
	}

	err = fmt.Errorf("startHTTPServerUpCheckMaxRetries (%v) exceeded", startHTTPServerUpCheckMaxRetries)
	return
}

func stopHTTPServer() (err error) {
	err = globals.httpServer.Shutdown(context.TODO())
	if nil == err {
		globals.httpServerWG.Wait()
	}

	return
}

func (dummy *globalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err         error
		requestBody []byte
		requestPath string
	)

	requestPath = strings.TrimRight(request.URL.Path, "/")

	requestBody, err = ioutil.ReadAll(request.Body)
	if nil == err {
		err = request.Body.Close()
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
	} else {
		_ = request.Body.Close()
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	switch request.Method {
	case http.MethodDelete:
		serveHTTPDelete(responseWriter, request, requestPath)
	case http.MethodGet:
		serveHTTPGet(responseWriter, request, requestPath)
	case http.MethodPost:
		serveHTTPPost(responseWriter, request, requestPath, requestBody)
	case http.MethodPut:
		serveHTTPPut(responseWriter, request, requestPath, requestBody)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func serveHTTPDelete(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	switch {
	case strings.HasPrefix(requestPath, "/volume"):
		serveHTTPDeleteOfVolume(responseWriter, request, requestPath)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func serveHTTPDeleteOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	var (
		err       error
		pathSplit []string
		startTime time.Time = time.Now()
	)

	pathSplit = strings.Split(requestPath, "/")

	switch len(pathSplit) {
	case 3:
		defer func() {
			globals.stats.DeleteVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		err = deleteVolume(pathSplit[2])
		if nil == err {
			responseWriter.WriteHeader(http.StatusNoContent)
		} else {
			responseWriter.WriteHeader(http.StatusNotFound)
		}
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
	}
}

func serveHTTPGet(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	switch {
	case "" == requestPath:
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(fmt.Sprintf(indexDotHTMLTemplate, version.ProxyFSVersion)))
	case "/bootstrap.min.css" == requestPath:
		responseWriter.Header().Set("Content-Type", bootstrapDotCSSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(bootstrapDotCSSContent))
	case "/bootstrap.min.js" == requestPath:
		responseWriter.Header().Set("Content-Type", bootstrapDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(bootstrapDotJSContent))
	case "/config" == requestPath:
		serveHTTPGetOfConfig(responseWriter, request)
	case "/index.html" == requestPath:
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(fmt.Sprintf(indexDotHTMLTemplate, version.ProxyFSVersion)))
	case "/jquery.min.js" == requestPath:
		responseWriter.Header().Set("Content-Type", jqueryDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(jqueryDotJSContent))
	case "/jsontree.js" == requestPath:
		responseWriter.Header().Set("Content-Type", jsontreeDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(jsontreeDotJSContent))
	case "/open-iconic/font/css/open-iconic-bootstrap.min.css" == requestPath:
		responseWriter.Header().Set("Content-Type", openIconicBootstrapDotCSSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(openIconicBootstrapDotCSSContent))
	case "/open-iconic/font/fonts/open-iconic.eot" == requestPath:
		responseWriter.Header().Set("Content-Type", openIconicDotEOTContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotEOTContent)
	case "/open-iconic/font/fonts/open-iconic.otf" == requestPath:
		responseWriter.Header().Set("Content-Type", openIconicDotOTFContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotOTFContent)
	case "/open-iconic/font/fonts/open-iconic.svg" == requestPath:
		responseWriter.Header().Set("Content-Type", openIconicDotSVGContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(openIconicDotSVGContent))
	case "/open-iconic/font/fonts/open-iconic.ttf" == requestPath:
		responseWriter.Header().Set("Content-Type", openIconicDotTTFContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotTTFContent)
	case "/open-iconic/font/fonts/open-iconic.woff" == requestPath:
		responseWriter.Header().Set("Content-Type", openIconicDotWOFFContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotWOFFContent)
	case "/popper.min.js" == requestPath:
		responseWriter.Header().Set("Content-Type", popperDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(popperDotJSContent)
	case "/stats" == requestPath:
		serveHTTPGetOfStats(responseWriter, request)
	case "/styles.css" == requestPath:
		responseWriter.Header().Set("Content-Type", stylesDotCSSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(stylesDotCSSContent))
	case "/utils.js" == requestPath:
		responseWriter.Header().Set("Content-Type", utilsDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(utilsDotJSContent))
	case "/version" == requestPath:
		serveHTTPGetOfVersion(responseWriter, request)
	case strings.HasPrefix(requestPath, "/volume"):
		serveHTTPGetOfVolume(responseWriter, request, requestPath)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func serveHTTPGetOfConfig(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		confMapJSON []byte
		err         error
		startTime   time.Time = time.Now()
	)

	defer func() {
		globals.stats.GetConfigUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	confMapJSON, err = json.Marshal(globals.config)
	if nil != err {
		logFatalf("json.Marshal(globals.config) failed: %v", err)
	}

	if strings.Contains(request.Header.Get("Accept"), "text/html") {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write([]byte(fmt.Sprintf(configTemplate, version.ProxyFSVersion, string(confMapJSON[:]))))
		if nil != err {
			logWarnf("responseWriter.Write([]byte(fmt.Sprintf(configTemplate, version.ProxyFSVersion, string(confMapJSON[:])))) failed: %v", err)
		}
	} else {
		responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(confMapJSON)))
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write(confMapJSON)
		if nil != err {
			logWarnf("responseWriter.Write(confMapJSON) failed: %v", err)
		}
	}
}

func serveHTTPGetOfStats(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err           error
		startTime     time.Time = time.Now()
		statsAsString string
	)

	defer func() {
		globals.stats.GetStatsUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	statsAsString = bucketstats.SprintStats(bucketstats.StatFormatParsable1, "*", "*")

	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(statsAsString)))
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	_, err = responseWriter.Write([]byte(statsAsString))
	if nil != err {
		logWarnf("responseWriter.Write([]byte(statsAsString)) failed: %v", err)
	}
}

func serveHTTPGetOfVersion(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err       error
		startTime time.Time
	)

	startTime = time.Now()
	defer func() {
		globals.stats.GetVersionUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(version.ProxyFSVersion)))
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	_, err = responseWriter.Write([]byte(version.ProxyFSVersion))
	if nil != err {
		logWarnf("responseWriter.Write([]byte(statsAsString)) failed: %v", err)
	}
}

type volumeListGETEntryStruct struct {
	Name                   string
	StorageURL             string
	AuthToken              string
	HealthyMounts          uint64
	LeasesExpiredMounts    uint64
	AuthTokenExpiredMounts uint64
}

type volumeOrInodeGETDimensionStruct struct {
	MinKeysPerNode uint64 // only applies to non-Root nodes
	MaxKeysPerNode uint64
	Items          uint64
	Height         uint64
}

type volumeOrInodeGETLayoutEntryStruct struct {
	ObjectName      string // == ilayout.GetObjectNameAsString(ObjectNumber)
	ObjectSize      uint64
	BytesReferenced uint64
}

type volumeGETInodeTableEntryStruct struct {
	InodeNumber         uint64
	InodeHeadObjectName string // == ilayout.GetObjectNameAsString(InodeHeadObjectNumber)
	InodeHeadLength     uint64
}

type httpServerInodeTableWrapperStruct struct {
	volume *volumeStruct
	cache  sortedmap.BPlusTreeCache
	table  sortedmap.BPlusTree
}

type volumeGETStruct struct {
	Name                         string
	StorageURL                   string
	AuthToken                    string
	HealthyMounts                uint64
	LeasesExpiredMounts          uint64
	AuthTokenExpiredMounts       uint64
	SuperBlockObjectName         string // == ilayout.GetObjectNameAsString(SuperBlockObjectNumber)
	SuperBlockLength             uint64
	ReservedToNonce              uint64
	InodeTableMinInodesPerNode   uint64
	InodeTableMaxInodesPerNode   uint64
	InodeTableInodeCount         uint64
	InodeTableHeight             uint64
	InodeTableLayout             []volumeOrInodeGETLayoutEntryStruct
	InodeObjectCount             uint64
	InodeObjectSize              uint64
	InodeBytesReferenced         uint64
	PendingDeleteObjectNameArray []string // == []ilayout.GetObjectNameAsString(PendingDeleteObjectNumber)
	InodeTable                   []volumeGETInodeTableEntryStruct
}

type inodeGETLinkTableEntryStruct struct {
	ParentDirInodeNumber uint64
	ParentDirEntryName   string
}

type inodeGETStreamTableEntryStruct struct {
	Name  string
	Value string // "XX XX .. XX" Hex-displayed bytes
}

type dirInodeGETPayloadEntryStruct struct {
	BaseName    string
	InodeNumber uint64
	InodeType   string // One of "Dir", "File", or "SymLink"
}

type fileInodeGETPayloadEntryStruct struct {
	FileOffset   uint64
	Length       uint64
	ObjectName   string // == ilayout.GetObjectNameAsString(ObjectNumber)
	ObjectOffset uint64
}

type inodeGETStruct struct {
	InodeNumber      uint64
	InodeType        string // == "Dir", "File", or "Symlink"
	LinkTable        []inodeGETLinkTableEntryStruct
	ModificationTime string
	StatusChangeTime string
	Mode             uint16
	UserID           uint64
	GroupID          uint64
	StreamTable      []inodeGETStreamTableEntryStruct
}

type httpServerDirectoryWrapperStruct struct {
	volume *volumeStruct
	cache  sortedmap.BPlusTreeCache
	table  sortedmap.BPlusTree
}

type dirInodeGETStruct struct {
	inodeGETStruct
	MinDirEntriesPerNode uint64
	MaxDirEntriesPerNode uint64
	DirEntryCount        uint64
	DirectoryHeight      uint64
	Payload              []dirInodeGETPayloadEntryStruct
	Layout               []volumeOrInodeGETLayoutEntryStruct
}

type httpServerExtentMapWrapperStruct struct {
	volume *volumeStruct
	cache  sortedmap.BPlusTreeCache
	table  sortedmap.BPlusTree
}

type fileInodeGETStruct struct {
	inodeGETStruct
	Size              uint64
	MinExtentsPerNode uint64
	MaxExtentsPerNode uint64
	ExtentCount       uint64
	ExtentMapHeight   uint64
	Payload           []fileInodeGETPayloadEntryStruct
	Layout            []volumeOrInodeGETLayoutEntryStruct
}

type symLinkInodeGETStruct struct {
	inodeGETStruct
	SymLinkTarget string
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		authOK bool
	)

	nodeByteSlice, authOK, err = inodeTableWrapper.volume.swiftObjectGetRange(objectNumber, objectOffset, objectLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetRange() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetRange() returned !authOK")
		return
	}

	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		nextPos     int
		inodeNumber uint64
	)

	inodeNumber, nextPos, err = ilayout.GetLEUint64FromBuf(payloadData, 0)
	if nil == err {
		key = inodeNumber
		bytesConsumed = uint64(nextPos)
	}

	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (inodeTableWrapper *httpServerInodeTableWrapperStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt     int
		inodeTableEntryValueV1 *ilayout.InodeTableEntryValueV1Struct
	)

	inodeTableEntryValueV1, bytesConsumedAsInt, err = ilayout.UnmarshalInodeTableEntryValueV1(payloadData)
	if nil == err {
		value = inodeTableEntryValueV1
		bytesConsumed = uint64(bytesConsumedAsInt)
	}

	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		authOK bool
	)

	nodeByteSlice, authOK, err = directoryWrapper.volume.swiftObjectGetRange(objectNumber, objectOffset, objectLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetRange() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetRange() returned !authOK")
		return
	}

	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		nextPos  int
		baseName string
	)

	baseName, nextPos, err = ilayout.GetLEStringFromBuf(payloadData, 0)
	if nil == err {
		key = baseName
		bytesConsumed = uint64(nextPos)
	}

	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (directoryWrapper *httpServerDirectoryWrapperStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt    int
		directoryEntryValueV1 *ilayout.DirectoryEntryValueV1Struct
	)

	directoryEntryValueV1, bytesConsumedAsInt, err = ilayout.UnmarshalDirectoryEntryValueV1(payloadData)
	if nil == err {
		value = directoryEntryValueV1
		bytesConsumed = uint64(bytesConsumedAsInt)
	}

	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		authOK bool
	)

	nodeByteSlice, authOK, err = extentMapWrapper.volume.swiftObjectGetRange(objectNumber, objectOffset, objectLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetRange() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetRange() returned !authOK")
		return
	}

	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		nextPos    int
		fileOffset uint64
	)

	fileOffset, nextPos, err = ilayout.GetLEUint64FromBuf(payloadData, 0)
	if nil == err {
		key = fileOffset
		bytesConsumed = uint64(nextPos)
	}

	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (extentMapWrapper *httpServerExtentMapWrapperStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt    int
		extentMapEntryValueV1 *ilayout.ExtentMapEntryValueV1Struct
	)

	extentMapEntryValueV1, bytesConsumedAsInt, err = ilayout.UnmarshalExtentMapEntryValueV1(payloadData)
	if nil == err {
		value = extentMapEntryValueV1
		bytesConsumed = uint64(bytesConsumedAsInt)
	}

	return
}

func serveHTTPGetOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	var (
		acceptHeader                       string
		checkPointV1                       *ilayout.CheckPointV1Struct
		dimensionsReport                   sortedmap.DimensionsReport
		directoryEntryIndex                int
		directoryEntryKeyV1AsKey           sortedmap.Key
		directoryEntryValueV1              *ilayout.DirectoryEntryValueV1Struct
		directoryEntryValueV1AsValue       sortedmap.Value
		directoryWrapper                   *httpServerDirectoryWrapperStruct
		err                                error
		extentMapEntryIndex                int
		extentMapEntryKeyV1AsKey           sortedmap.Key
		extentMapEntryValueV1              *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1AsValue       sortedmap.Value
		extentMapWrapper                   *httpServerExtentMapWrapperStruct
		inodeGET                           interface{}
		inodeGETAsJSON                     []byte
		inodeGETAsHTML                     []byte
		inodeGETLayoutEntryIndex           int
		inodeGETLinkTableEntryIndex        int
		inodeGETStreamTableEntryIndex      int
		inodeGETStreamTableEntryValueByte  byte
		inodeGETStreamTableEntryValueIndex int
		inodeHeadV1                        *ilayout.InodeHeadV1Struct
		inodeNumberAsHexDigits             string
		inodeNumberAsUint64                uint64
		inodeNumberAsKey                   sortedmap.Key
		inodeTableEntryValueV1             *ilayout.InodeTableEntryValueV1Struct
		inodeTableEntryValueV1AsValue      sortedmap.Value
		inodeTableIndex                    int
		inodeTableLayoutIndex              int
		inodeTableWrapper                  *httpServerInodeTableWrapperStruct
		mustBeInode                        string
		ok                                 bool
		pathSplit                          []string
		pendingDeleteObjectNameArrayIndex  int
		requestAuthToken                   string
		startTime                          time.Time = time.Now()
		superBlockV1                       *ilayout.SuperBlockV1Struct
		volumeAsStruct                     *volumeStruct
		volumeAsValue                      sortedmap.Value
		volumeAuthToken                    string
		volumeGET                          *volumeGETStruct
		volumeGETAsHTML                    []byte
		volumeGETAsJSON                    []byte
		volumeListGET                      []*volumeListGETEntryStruct
		volumeListGETIndex                 int
		volumeListGETAsJSON                []byte
		volumeListGETAsHTML                []byte
		volumeListGETLen                   int
		volumeName                         string
	)

	pathSplit = strings.Split(requestPath, "/")

	switch len(pathSplit) {
	case 2:
		// Form: /volume

		defer func() {
			globals.stats.GetVolumeListUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		globals.Lock()

		volumeListGETLen, err = globals.volumeMap.Len()
		if nil != err {
			logFatal(err)
		}

		volumeListGET = make([]*volumeListGETEntryStruct, volumeListGETLen)

		for volumeListGETIndex = 0; volumeListGETIndex < volumeListGETLen; volumeListGETIndex++ {
			_, volumeAsValue, ok, err = globals.volumeMap.GetByIndex(volumeListGETIndex)
			if nil != err {
				logFatal(err)
			}
			if !ok {
				logFatalf("globals.volumeMap[] len (%d) is wrong", volumeListGETLen)
			}

			volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
			if !ok {
				logFatalf("globals.volumeMap[%d] was not a *volumeStruct", volumeListGETIndex)
			}

			volumeListGET[volumeListGETIndex] = &volumeListGETEntryStruct{
				Name:                   volumeAsStruct.name,
				StorageURL:             volumeAsStruct.storageURL,
				AuthToken:              volumeAsStruct.authToken,
				HealthyMounts:          uint64(volumeAsStruct.healthyMountList.Len()),
				LeasesExpiredMounts:    uint64(volumeAsStruct.leasesExpiredMountList.Len()),
				AuthTokenExpiredMounts: uint64(volumeAsStruct.authTokenExpiredMountList.Len()),
			}
		}

		globals.Unlock()

		volumeListGETAsJSON, err = json.Marshal(volumeListGET)
		if nil != err {
			logFatal(err)
		}

		acceptHeader = request.Header.Get("Accept")

		if strings.Contains(acceptHeader, "text/html") {
			volumeListGETAsHTML = []byte(fmt.Sprintf(volumeListTemplate, version.ProxyFSVersion, string(volumeListGETAsJSON)))

			responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(volumeListGETAsHTML)))
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			_, err = responseWriter.Write(volumeListGETAsHTML)
			if nil != err {
				logWarnf("responseWriter.Write(volumeListGETAsHTML) failed: %v", err)
			}
		} else {
			responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(volumeListGETAsJSON)))
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			_, err = responseWriter.Write(volumeListGETAsJSON)
			if nil != err {
				logWarnf("responseWriter.Write(volumeListGETAsJSON) failed: %v", err)
			}
		}
	case 3:
		// Form: /volume/<VolumeName>

		defer func() {
			globals.stats.GetVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		volumeName = pathSplit[2]

		globals.Lock()

		volumeAsValue, ok, err = globals.volumeMap.GetByKey(volumeName)
		if nil != err {
			logFatal(err)
		}

		if ok {
			volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
			if !ok {
				logFatalf("globals.volumeMap[\"%s\"] was not a *volumeStruct", volumeName)
			}

			requestAuthToken = request.Header.Get("X-Auth-Token")
			volumeAuthToken = volumeAsStruct.authToken
			if requestAuthToken != "" {
				volumeAsStruct.authToken = requestAuthToken
			}

			checkPointV1, err = volumeAsStruct.fetchCheckPoint()
			if nil != err {
				volumeAsStruct.authToken = volumeAuthToken
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			superBlockV1, err = volumeAsStruct.fetchSuperBlock(checkPointV1.SuperBlockObjectNumber, checkPointV1.SuperBlockLength)
			if nil != err {
				volumeAsStruct.authToken = volumeAuthToken
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			inodeTableWrapper = &httpServerInodeTableWrapperStruct{
				volume: volumeAsStruct,
				cache:  sortedmap.NewBPlusTreeCache(httpServerInodeTableCacheLowLimit, httpServerInodeTableCacheHighLimit),
			}

			inodeTableWrapper.table, err = sortedmap.OldBPlusTree(superBlockV1.InodeTableRootObjectNumber, superBlockV1.InodeTableRootObjectOffset, superBlockV1.InodeTableRootObjectLength, sortedmap.CompareUint64, inodeTableWrapper, inodeTableWrapper.cache)
			if nil != err {
				volumeAsStruct.authToken = volumeAuthToken
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			dimensionsReport, err = inodeTableWrapper.table.FetchDimensionsReport()
			if nil != err {
				volumeAsStruct.authToken = volumeAuthToken
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			volumeGET = &volumeGETStruct{
				Name:                         volumeAsStruct.name,
				StorageURL:                   volumeAsStruct.storageURL,
				AuthToken:                    volumeAuthToken,
				HealthyMounts:                uint64(volumeAsStruct.healthyMountList.Len()),
				LeasesExpiredMounts:          uint64(volumeAsStruct.leasesExpiredMountList.Len()),
				AuthTokenExpiredMounts:       uint64(volumeAsStruct.authTokenExpiredMountList.Len()),
				SuperBlockObjectName:         ilayout.GetObjectNameAsString(checkPointV1.SuperBlockObjectNumber),
				SuperBlockLength:             checkPointV1.SuperBlockLength,
				ReservedToNonce:              checkPointV1.ReservedToNonce,
				InodeTableMinInodesPerNode:   dimensionsReport.MinKeysPerNode,
				InodeTableMaxInodesPerNode:   dimensionsReport.MaxKeysPerNode,
				InodeTableInodeCount:         dimensionsReport.Items,
				InodeTableHeight:             dimensionsReport.Height,
				InodeTableLayout:             make([]volumeOrInodeGETLayoutEntryStruct, len(superBlockV1.InodeTableLayout)),
				InodeObjectCount:             superBlockV1.InodeObjectCount,
				InodeObjectSize:              superBlockV1.InodeObjectSize,
				InodeBytesReferenced:         superBlockV1.InodeBytesReferenced,
				PendingDeleteObjectNameArray: make([]string, len(superBlockV1.PendingDeleteObjectNumberArray)),
				InodeTable:                   make([]volumeGETInodeTableEntryStruct, dimensionsReport.Items),
			}

			for inodeTableLayoutIndex = range volumeGET.InodeTableLayout {
				volumeGET.InodeTableLayout[inodeTableLayoutIndex].ObjectName = ilayout.GetObjectNameAsString(superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectNumber)
				volumeGET.InodeTableLayout[inodeTableLayoutIndex].ObjectSize = superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectSize
				volumeGET.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced = superBlockV1.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced
			}

			for pendingDeleteObjectNameArrayIndex = range volumeGET.PendingDeleteObjectNameArray {
				volumeGET.PendingDeleteObjectNameArray[pendingDeleteObjectNameArrayIndex] = ilayout.GetObjectNameAsString(superBlockV1.PendingDeleteObjectNumberArray[pendingDeleteObjectNameArrayIndex])
			}

			for inodeTableIndex = range volumeGET.InodeTable {
				inodeNumberAsKey, inodeTableEntryValueV1AsValue, ok, err = inodeTableWrapper.table.GetByIndex(inodeTableIndex)
				if (nil != err) || !ok {
					volumeAsStruct.authToken = volumeAuthToken
					globals.Unlock()
					responseWriter.WriteHeader(http.StatusUnauthorized)
					return
				}

				volumeGET.InodeTable[inodeTableIndex].InodeNumber, ok = inodeNumberAsKey.(uint64)
				if !ok {
					logFatalf("inodeNumberAsKey.(uint64) returned !ok")
				}

				inodeTableEntryValueV1, ok = inodeTableEntryValueV1AsValue.(*ilayout.InodeTableEntryValueV1Struct)
				if !ok {
					logFatalf("inodeTableEntryValueV1AsValue.(*ilayout.InodeTableEntryValueV1Struct) returned !ok")
				}

				volumeGET.InodeTable[inodeTableIndex].InodeHeadObjectName = ilayout.GetObjectNameAsString(inodeTableEntryValueV1.InodeHeadObjectNumber)
				volumeGET.InodeTable[inodeTableIndex].InodeHeadLength = inodeTableEntryValueV1.InodeHeadLength
			}

			volumeAsStruct.authToken = volumeAuthToken

			globals.Unlock()

			volumeGETAsJSON, err = json.Marshal(volumeGET)
			if nil != err {
				logFatal(err)
			}

			acceptHeader = request.Header.Get("Accept")

			if strings.Contains(acceptHeader, "text/html") {
				volumeGETAsHTML = []byte(fmt.Sprintf(volumeTemplate, version.ProxyFSVersion, volumeAsStruct.name, string(volumeGETAsJSON)))

				responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(volumeGETAsHTML)))
				responseWriter.Header().Set("Content-Type", "text/html")
				responseWriter.WriteHeader(http.StatusOK)

				_, err = responseWriter.Write(volumeGETAsHTML)
				if nil != err {
					logWarnf("responseWriter.Write(volumeGETAsHTML) failed: %v", err)
				}
			} else {
				responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(volumeGETAsJSON)))
				responseWriter.Header().Set("Content-Type", "application/json")
				responseWriter.WriteHeader(http.StatusOK)

				_, err = responseWriter.Write(volumeGETAsJSON)
				if nil != err {
					logWarnf("responseWriter.Write(volumeGETAsJSON) failed: %v", err)
				}
			}
		} else {
			globals.Unlock()
			responseWriter.WriteHeader(http.StatusNotFound)
		}
	case 5:
		// Form: /volume/<VolumeName>/inode/<InodeNumber>

		volumeName = pathSplit[2]
		mustBeInode = pathSplit[3]
		inodeNumberAsHexDigits = pathSplit[4]

		switch mustBeInode {
		case "inode":
			inodeNumberAsUint64, err = strconv.ParseUint(inodeNumberAsHexDigits, 16, 64)
			if nil != err {
				responseWriter.WriteHeader(http.StatusBadRequest)
			} else {
				defer func() {
					globals.stats.GetVolumeInodeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
				}()

				globals.Lock()

				volumeAsValue, ok, err = globals.volumeMap.GetByKey(volumeName)
				if nil != err {
					logFatal(err)
				}

				if ok {
					volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
					if !ok {
						logFatalf("globals.volumeMap[\"%s\"] was not a *volumeStruct", volumeName)
					}

					volumeAuthToken = volumeAsStruct.authToken
					requestAuthToken = request.Header.Get("X-Auth-Token")
					if requestAuthToken != "" {
						volumeAsStruct.authToken = requestAuthToken
					}

					checkPointV1, err = volumeAsStruct.fetchCheckPoint()
					if nil != err {
						volumeAsStruct.authToken = volumeAuthToken
						globals.Unlock()
						responseWriter.WriteHeader(http.StatusUnauthorized)
						return
					}

					superBlockV1, err = volumeAsStruct.fetchSuperBlock(checkPointV1.SuperBlockObjectNumber, checkPointV1.SuperBlockLength)
					if nil != err {
						volumeAsStruct.authToken = volumeAuthToken
						globals.Unlock()
						responseWriter.WriteHeader(http.StatusUnauthorized)
						return
					}

					inodeTableWrapper = &httpServerInodeTableWrapperStruct{
						volume: volumeAsStruct,
						cache:  sortedmap.NewBPlusTreeCache(httpServerInodeTableCacheLowLimit, httpServerInodeTableCacheHighLimit),
					}

					inodeTableWrapper.table, err = sortedmap.OldBPlusTree(superBlockV1.InodeTableRootObjectNumber, superBlockV1.InodeTableRootObjectOffset, superBlockV1.InodeTableRootObjectLength, sortedmap.CompareUint64, inodeTableWrapper, inodeTableWrapper.cache)
					if nil != err {
						volumeAsStruct.authToken = volumeAuthToken
						globals.Unlock()
						responseWriter.WriteHeader(http.StatusUnauthorized)
						return
					}

					inodeTableEntryValueV1AsValue, ok, err = inodeTableWrapper.table.GetByKey(inodeNumberAsUint64)
					if nil != err {
						volumeAsStruct.authToken = volumeAuthToken
						globals.Unlock()
						responseWriter.WriteHeader(http.StatusUnauthorized)
						return
					}
					if !ok {
						volumeAsStruct.authToken = volumeAuthToken
						globals.Unlock()
						responseWriter.WriteHeader(http.StatusNotFound)
						return
					}

					inodeTableEntryValueV1, ok = inodeTableEntryValueV1AsValue.(*ilayout.InodeTableEntryValueV1Struct)
					if !ok {
						logFatalf("inodeTableEntryValueV1AsValue.(*ilayout.InodeTableEntryValueV1Struct) returned !ok")
					}

					inodeHeadV1, err = volumeAsStruct.fetchInodeHead(inodeTableEntryValueV1.InodeHeadObjectNumber, inodeTableEntryValueV1.InodeHeadLength)
					if nil != err {
						volumeAsStruct.authToken = volumeAuthToken
						globals.Unlock()
						responseWriter.WriteHeader(http.StatusUnauthorized)
						return
					}

					if inodeNumberAsUint64 != inodeHeadV1.InodeNumber {
						logFatalf("Lookup of Inode 0x%016X returned an Inode with InodeNumber 0x%016X", inodeNumberAsUint64, inodeHeadV1.InodeNumber)
					}

					// Apologies for the hack below due to Golang not supporting full inheritance

					switch inodeHeadV1.InodeType {
					case ilayout.InodeTypeDir:
						directoryWrapper = &httpServerDirectoryWrapperStruct{
							volume: volumeAsStruct,
							cache:  sortedmap.NewBPlusTreeCache(httpServerDirectoryCacheLowLimit, httpServerDirectoryCacheHighLimit),
						}

						directoryWrapper.table, err = sortedmap.OldBPlusTree(inodeHeadV1.PayloadObjectNumber, inodeHeadV1.PayloadObjectOffset, inodeHeadV1.PayloadObjectLength, sortedmap.CompareString, directoryWrapper, directoryWrapper.cache)
						if nil != err {
							volumeAsStruct.authToken = volumeAuthToken
							globals.Unlock()
							responseWriter.WriteHeader(http.StatusUnauthorized)
							return
						}

						dimensionsReport, err = directoryWrapper.table.FetchDimensionsReport()
						if nil != err {
							volumeAsStruct.authToken = volumeAuthToken
							globals.Unlock()
							responseWriter.WriteHeader(http.StatusUnauthorized)
							return
						}

						inodeGET = &dirInodeGETStruct{
							inodeGETStruct{
								InodeNumber:      inodeHeadV1.InodeNumber,
								InodeType:        "Dir",
								LinkTable:        make([]inodeGETLinkTableEntryStruct, len(inodeHeadV1.LinkTable)),
								ModificationTime: inodeHeadV1.ModificationTime.Format(time.RFC3339),
								StatusChangeTime: inodeHeadV1.StatusChangeTime.Format(time.RFC3339),
								Mode:             inodeHeadV1.Mode,
								UserID:           inodeHeadV1.UserID,
								GroupID:          inodeHeadV1.GroupID,
								StreamTable:      make([]inodeGETStreamTableEntryStruct, len(inodeHeadV1.StreamTable)),
							},
							dimensionsReport.MinKeysPerNode,
							dimensionsReport.MaxKeysPerNode,
							dimensionsReport.Items,
							dimensionsReport.Height,
							make([]dirInodeGETPayloadEntryStruct, dimensionsReport.Items),
							make([]volumeOrInodeGETLayoutEntryStruct, len(inodeHeadV1.Layout)),
						}

						for inodeGETLinkTableEntryIndex = range inodeHeadV1.LinkTable {
							inodeGET.(*dirInodeGETStruct).LinkTable[inodeGETLinkTableEntryIndex].ParentDirInodeNumber = inodeHeadV1.LinkTable[inodeGETLinkTableEntryIndex].ParentDirInodeNumber
							inodeGET.(*dirInodeGETStruct).LinkTable[inodeGETLinkTableEntryIndex].ParentDirEntryName = inodeHeadV1.LinkTable[inodeGETLinkTableEntryIndex].ParentDirEntryName
						}

						for inodeGETStreamTableEntryIndex = range inodeHeadV1.StreamTable {
							inodeGET.(*dirInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Name = inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Name
							if len(inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Value) == 0 {
								inodeGET.(*dirInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value = ""
							} else {
								for inodeGETStreamTableEntryValueIndex, inodeGETStreamTableEntryValueByte = range inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Value {
									if inodeGETStreamTableEntryValueIndex == 0 {
										inodeGET.(*dirInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value = fmt.Sprintf("%02X", inodeGETStreamTableEntryValueByte)
									} else {
										inodeGET.(*dirInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value += fmt.Sprintf(" %02X", inodeGETStreamTableEntryValueByte)
									}
								}
							}
						}

						for directoryEntryIndex = range inodeGET.(*dirInodeGETStruct).Payload {
							directoryEntryKeyV1AsKey, directoryEntryValueV1AsValue, ok, err = directoryWrapper.table.GetByIndex(directoryEntryIndex)
							if (nil != err) || !ok {
								volumeAsStruct.authToken = volumeAuthToken
								globals.Unlock()
								responseWriter.WriteHeader(http.StatusUnauthorized)
								return
							}

							inodeGET.(*dirInodeGETStruct).Payload[directoryEntryIndex].BaseName, ok = directoryEntryKeyV1AsKey.(string)
							if !ok {
								logFatalf("directoryEntryKeyV1AsKey.(string) returned !ok")
							}

							directoryEntryValueV1, ok = directoryEntryValueV1AsValue.(*ilayout.DirectoryEntryValueV1Struct)
							if !ok {
								logFatalf("directoryEntryValueV1AsValue.(*ilayout.DirectoryEntryValueV1Struct) returned !ok")
							}

							inodeGET.(*dirInodeGETStruct).Payload[directoryEntryIndex].InodeNumber = directoryEntryValueV1.InodeNumber

							switch directoryEntryValueV1.InodeType {
							case ilayout.InodeTypeDir:
								inodeGET.(*dirInodeGETStruct).Payload[directoryEntryIndex].InodeType = "Dir"
							case ilayout.InodeTypeFile:
								inodeGET.(*dirInodeGETStruct).Payload[directoryEntryIndex].InodeType = "File"
							case ilayout.InodeTypeSymLink:
								inodeGET.(*dirInodeGETStruct).Payload[directoryEntryIndex].InodeType = "SymLink"
							default:
								logFatalf("Directory entry in DirInode 0x%016X contains unknown InodeType: 0x%02X", inodeGET.(*dirInodeGETStruct).Payload[directoryEntryIndex].InodeNumber, directoryEntryValueV1.InodeType)
							}
						}

						for inodeGETLayoutEntryIndex = range inodeHeadV1.Layout {
							inodeGET.(*dirInodeGETStruct).Layout[inodeGETLayoutEntryIndex].ObjectName = ilayout.GetObjectNameAsString(inodeHeadV1.Layout[inodeGETLayoutEntryIndex].ObjectNumber)
							inodeGET.(*dirInodeGETStruct).Layout[inodeGETLayoutEntryIndex].ObjectSize = inodeHeadV1.Layout[inodeGETLayoutEntryIndex].ObjectSize
							inodeGET.(*dirInodeGETStruct).Layout[inodeGETLayoutEntryIndex].BytesReferenced = inodeHeadV1.Layout[inodeGETLayoutEntryIndex].BytesReferenced
						}
					case ilayout.InodeTypeFile:
						extentMapWrapper = &httpServerExtentMapWrapperStruct{
							volume: volumeAsStruct,
							cache:  sortedmap.NewBPlusTreeCache(httpServerExtentMapCacheLowLimit, httpServerExtentMapCacheHighLimit),
						}

						extentMapWrapper.table, err = sortedmap.OldBPlusTree(inodeHeadV1.PayloadObjectNumber, inodeHeadV1.PayloadObjectOffset, inodeHeadV1.PayloadObjectLength, sortedmap.CompareUint64, extentMapWrapper, extentMapWrapper.cache)
						if nil != err {
							volumeAsStruct.authToken = volumeAuthToken
							globals.Unlock()
							responseWriter.WriteHeader(http.StatusUnauthorized)
							return
						}

						dimensionsReport, err = extentMapWrapper.table.FetchDimensionsReport()
						if nil != err {
							volumeAsStruct.authToken = volumeAuthToken
							globals.Unlock()
							responseWriter.WriteHeader(http.StatusUnauthorized)
							return
						}

						inodeGET = &fileInodeGETStruct{
							inodeGETStruct{
								InodeNumber:      inodeHeadV1.InodeNumber,
								InodeType:        "File",
								LinkTable:        make([]inodeGETLinkTableEntryStruct, len(inodeHeadV1.LinkTable)),
								ModificationTime: inodeHeadV1.ModificationTime.Format(time.RFC3339),
								StatusChangeTime: inodeHeadV1.StatusChangeTime.Format(time.RFC3339),
								Mode:             inodeHeadV1.Mode,
								UserID:           inodeHeadV1.UserID,
								GroupID:          inodeHeadV1.GroupID,
								StreamTable:      make([]inodeGETStreamTableEntryStruct, len(inodeHeadV1.StreamTable)),
							},
							inodeHeadV1.Size,
							dimensionsReport.MinKeysPerNode,
							dimensionsReport.MaxKeysPerNode,
							dimensionsReport.Items,
							dimensionsReport.Height,
							make([]fileInodeGETPayloadEntryStruct, dimensionsReport.Items),
							make([]volumeOrInodeGETLayoutEntryStruct, len(inodeHeadV1.Layout)),
						}

						for inodeGETLinkTableEntryIndex = range inodeHeadV1.LinkTable {
							inodeGET.(*fileInodeGETStruct).LinkTable[inodeGETLinkTableEntryIndex].ParentDirInodeNumber = inodeHeadV1.LinkTable[inodeGETLinkTableEntryIndex].ParentDirInodeNumber
							inodeGET.(*fileInodeGETStruct).LinkTable[inodeGETLinkTableEntryIndex].ParentDirEntryName = inodeHeadV1.LinkTable[inodeGETLinkTableEntryIndex].ParentDirEntryName
						}

						for inodeGETStreamTableEntryIndex = range inodeHeadV1.StreamTable {
							inodeGET.(*fileInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Name = inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Name
							if len(inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Value) == 0 {
								inodeGET.(*fileInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value = ""
							} else {
								for inodeGETStreamTableEntryValueIndex, inodeGETStreamTableEntryValueByte = range inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Value {
									if inodeGETStreamTableEntryValueIndex == 0 {
										inodeGET.(*fileInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value = fmt.Sprintf("%02X", inodeGETStreamTableEntryValueByte)
									} else {
										inodeGET.(*fileInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value += fmt.Sprintf(" %02X", inodeGETStreamTableEntryValueByte)
									}
								}
							}
						}

						for extentMapEntryIndex = range inodeGET.(*fileInodeGETStruct).Payload {
							extentMapEntryKeyV1AsKey, extentMapEntryValueV1AsValue, ok, err = extentMapWrapper.table.GetByIndex(extentMapEntryIndex)
							if (nil != err) || !ok {
								volumeAsStruct.authToken = volumeAuthToken
								globals.Unlock()
								responseWriter.WriteHeader(http.StatusUnauthorized)
								return
							}

							inodeGET.(*fileInodeGETStruct).Payload[extentMapEntryIndex].FileOffset, ok = extentMapEntryKeyV1AsKey.(uint64)
							if !ok {
								logFatalf("extentMapEntryKeyV1AsKey.(uint64) returned !ok")
							}

							extentMapEntryValueV1, ok = extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct)
							if !ok {
								logFatalf("extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
							}

							inodeGET.(*fileInodeGETStruct).Payload[extentMapEntryIndex].Length = extentMapEntryValueV1.Length
							inodeGET.(*fileInodeGETStruct).Payload[extentMapEntryIndex].ObjectName = ilayout.GetObjectNameAsString(extentMapEntryValueV1.ObjectNumber)
							inodeGET.(*fileInodeGETStruct).Payload[extentMapEntryIndex].ObjectOffset = extentMapEntryValueV1.ObjectOffset
						}

						for inodeGETLayoutEntryIndex = range inodeHeadV1.Layout {
							inodeGET.(*dirInodeGETStruct).Layout[inodeGETLayoutEntryIndex].ObjectName = ilayout.GetObjectNameAsString(inodeHeadV1.Layout[inodeGETLayoutEntryIndex].ObjectNumber)
							inodeGET.(*dirInodeGETStruct).Layout[inodeGETLayoutEntryIndex].ObjectSize = inodeHeadV1.Layout[inodeGETLayoutEntryIndex].ObjectSize
							inodeGET.(*dirInodeGETStruct).Layout[inodeGETLayoutEntryIndex].BytesReferenced = inodeHeadV1.Layout[inodeGETLayoutEntryIndex].BytesReferenced
						}
					case ilayout.InodeTypeSymLink:
						inodeGET = &symLinkInodeGETStruct{
							inodeGETStruct{
								InodeNumber:      inodeHeadV1.InodeNumber,
								InodeType:        "SymLink",
								LinkTable:        make([]inodeGETLinkTableEntryStruct, len(inodeHeadV1.LinkTable)),
								ModificationTime: inodeHeadV1.ModificationTime.Format(time.RFC3339),
								StatusChangeTime: inodeHeadV1.StatusChangeTime.Format(time.RFC3339),
								Mode:             inodeHeadV1.Mode,
								UserID:           inodeHeadV1.UserID,
								GroupID:          inodeHeadV1.GroupID,
								StreamTable:      make([]inodeGETStreamTableEntryStruct, len(inodeHeadV1.StreamTable)),
							},
							inodeHeadV1.SymLinkTarget,
						}

						for inodeGETLinkTableEntryIndex = range inodeHeadV1.LinkTable {
							inodeGET.(*symLinkInodeGETStruct).LinkTable[inodeGETLinkTableEntryIndex].ParentDirInodeNumber = inodeHeadV1.LinkTable[inodeGETLinkTableEntryIndex].ParentDirInodeNumber
							inodeGET.(*symLinkInodeGETStruct).LinkTable[inodeGETLinkTableEntryIndex].ParentDirEntryName = inodeHeadV1.LinkTable[inodeGETLinkTableEntryIndex].ParentDirEntryName
						}

						for inodeGETStreamTableEntryIndex = range inodeHeadV1.StreamTable {
							inodeGET.(*symLinkInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Name = inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Name
							if len(inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Value) == 0 {
								inodeGET.(*symLinkInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value = ""
							} else {
								for inodeGETStreamTableEntryValueIndex, inodeGETStreamTableEntryValueByte = range inodeHeadV1.StreamTable[inodeGETStreamTableEntryIndex].Value {
									if inodeGETStreamTableEntryValueIndex == 0 {
										inodeGET.(*symLinkInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value = fmt.Sprintf("%02X", inodeGETStreamTableEntryValueByte)
									} else {
										inodeGET.(*symLinkInodeGETStruct).StreamTable[inodeGETStreamTableEntryIndex].Value += fmt.Sprintf(" %02X", inodeGETStreamTableEntryValueByte)
									}
								}
							}
						}
					default:
						logFatalf("Inode 0x%016X contains unknown InodeType: 0x%02X", inodeNumberAsUint64, inodeHeadV1.InodeType)
					}

					volumeAsStruct.authToken = volumeAuthToken

					globals.Unlock()

					inodeGETAsJSON, err = json.Marshal(inodeGET)
					if nil != err {
						logFatal(err)
					}

					acceptHeader = request.Header.Get("Accept")

					if strings.Contains(acceptHeader, "text/html") {
						inodeGETAsHTML = []byte(fmt.Sprintf(inodeTemplate, version.ProxyFSVersion, volumeAsStruct.name, inodeHeadV1.InodeNumber, string(inodeGETAsJSON)))

						responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(inodeGETAsHTML)))
						responseWriter.Header().Set("Content-Type", "text/html")
						responseWriter.WriteHeader(http.StatusOK)

						_, err = responseWriter.Write(inodeGETAsHTML)
						if nil != err {
							logWarnf("responseWriter.Write(inodeGETAsHTML) failed: %v", err)
						}
					} else {
						responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(inodeGETAsJSON)))
						responseWriter.Header().Set("Content-Type", "application/json")
						responseWriter.WriteHeader(http.StatusOK)

						_, err = responseWriter.Write(inodeGETAsJSON)
						if nil != err {
							logWarnf("responseWriter.Write(inodeGETAsJSON) failed: %v", err)
						}
					}
				} else {
					globals.Unlock()
					responseWriter.WriteHeader(http.StatusNotFound)
				}
			}
		default:
			responseWriter.WriteHeader(http.StatusBadRequest)
		}
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
	}
}

func serveHTTPPost(responseWriter http.ResponseWriter, request *http.Request, requestPath string, requestBody []byte) {
	switch {
	case requestPath == "/volume":
		serveHTTPPostOfVolume(responseWriter, request, requestBody)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

type serveHTTPPostOfVolumeRequestBodyAsJSONStruct struct {
	StorageURL string
	AuthToken  string
}

func serveHTTPPostOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestBody []byte) {
	var (
		err               error
		requestBodyAsJSON serveHTTPPostOfVolumeRequestBodyAsJSONStruct
		startTime         time.Time = time.Now()
	)

	defer func() {
		globals.stats.PostVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = json.Unmarshal(requestBody, &requestBodyAsJSON)
	if nil != err {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	err = postVolume(requestBodyAsJSON.StorageURL, requestBodyAsJSON.AuthToken)
	if nil == err {
		responseWriter.WriteHeader(http.StatusCreated)
	} else {
		responseWriter.WriteHeader(http.StatusConflict)
	}
}

func serveHTTPPut(responseWriter http.ResponseWriter, request *http.Request, requestPath string, requestBody []byte) {
	switch {
	case strings.HasPrefix(requestPath, "/volume"):
		serveHTTPPutOfVolume(responseWriter, request, requestPath, requestBody)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

type serveHTTPPutOfVolumeRequestBodyAsJSONStruct struct {
	StorageURL string
	AuthToken  string
}

func serveHTTPPutOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestPath string, requestBody []byte) {
	var (
		err               error
		pathSplit         []string
		requestBodyAsJSON serveHTTPPutOfVolumeRequestBodyAsJSONStruct
		startTime         time.Time = time.Now()
	)

	pathSplit = strings.Split(requestPath, "/")

	switch len(pathSplit) {
	case 3:
		defer func() {
			globals.stats.PutVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		err = json.Unmarshal(requestBody, &requestBodyAsJSON)
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		err = putVolume(pathSplit[2], requestBodyAsJSON.StorageURL, requestBodyAsJSON.AuthToken)
		if nil == err {
			responseWriter.WriteHeader(http.StatusCreated)
		} else {
			responseWriter.WriteHeader(http.StatusConflict)
		}
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
	}
}
