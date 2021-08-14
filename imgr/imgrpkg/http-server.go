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

	volumeGETInodeTableCacheLowLimit  = 1000
	volumeGETInodeTableCacheHighLimit = 1010
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
		_, err = http.Get("http://" + ipAddrTCPPort + "/config")
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
		startTime time.Time
	)

	startTime = time.Now()

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
		startTime   time.Time
	)

	startTime = time.Now()
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
		startTime     time.Time
		statsAsString string
	)

	startTime = time.Now()
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

type volumeMiniGETStruct struct {
	Name                   string
	StorageURL             string
	AuthToken              string
	HealthyMounts          uint64
	LeasesExpiredMounts    uint64
	AuthTokenExpiredMounts uint64
}

type volumeFullGETInodeTableLayoutEntryStruct struct {
	ObjectName      string // == ilayout.GetObjectNameAsString(ObjectNumber)
	ObjectSize      uint64
	BytesReferenced uint64
}

type volumeFullGETInodeTableEntryStruct struct {
	InodeNumber         uint64
	InodeHeadObjectName string // == ilayout.GetObjectNameAsString(InodeHeadObjectNumber)
	InodeHeadLength     uint64
}

type volumeFullGETStruct struct {
	volume                       *volumeStruct // will not be encoded by json.Marshal()
	Name                         string
	StorageURL                   string
	AuthToken                    string
	HealthyMounts                uint64
	LeasesExpiredMounts          uint64
	AuthTokenExpiredMounts       uint64
	SuperBlockObjectName         string // == ilayout.GetObjectNameAsString(SuperBlockObjectNumber)
	SuperBlockLength             uint64
	ReservedToNonce              uint64
	InodeTableLayout             []volumeFullGETInodeTableLayoutEntryStruct
	InodeObjectCount             uint64
	InodeObjectSize              uint64
	InodeBytesReferenced         uint64
	PendingDeleteObjectNameArray []string // == []ilayout.GetObjectNameAsString(PendingDeleteObjectNumber)
	InodeTable                   []volumeFullGETInodeTableEntryStruct
}

type inodeGETLinkTableEntryStruct struct {
	ParentDirInodeNumber uint64
	ParentDirEntryName   uint64
}

type inodeGETStreamTableEntryStruct struct {
	Name  string
	Value string // "XX XX .. XX" Hex-displayed bytes
}

type dirInodeGETPayloadEntryStruct struct {
	BaseName    string
	InodeNumber uint64
	InodeType   string // One of "InodeTypeDir", "InodeTypeFile", or "InodeTypeSymLink"
}

type fileInodeGETPayloadEntryStruct struct {
	FileOffset   uint64
	Length       uint64
	ObjectName   string // == ilayout.GetObjectNameAsString(ObjectNumber)
	ObjectOffset uint64
}

type inodeGETLayoutEntryStruct struct {
	ObjectName      string // == ilayout.GetObjectNameAsString(ObjectNumber)
	ObjectSize      uint64
	BytesReferenced uint64
}

type dirInodeGETStruct struct {
	InodeNumber      uint64
	InodeType        string // == "InodeTypeDir"
	LinkTable        []inodeGETLinkTableEntryStruct
	ModificationTime string
	StatusChangeTime string
	Mode             uint16
	UserID           uint64
	GroupID          uint64
	StreamTable      []inodeGETStreamTableEntryStruct
	Payload          []dirInodeGETPayloadEntryStruct
	Layout           []inodeGETLayoutEntryStruct
}

type fileInodeGETStruct struct {
	InodeNumber      uint64
	InodeType        string // == "InodeTypeFile"
	LinkTable        []inodeGETLinkTableEntryStruct
	Size             string
	ModificationTime string
	StatusChangeTime string
	Mode             uint16
	UserID           uint64
	GroupID          uint64
	StreamTable      []inodeGETStreamTableEntryStruct
	Payload          []fileInodeGETPayloadEntryStruct
	Layout           []inodeGETLayoutEntryStruct
}

type symLinkInodeGETStruct struct {
	InodeNumber      uint64
	InodeType        string // == "InodeTypeSymLink"
	LinkTable        []inodeGETLinkTableEntryStruct
	ModificationTime string
	StatusChangeTime string
	Mode             uint16
	UserID           uint64
	GroupID          uint64
	StreamTable      []inodeGETStreamTableEntryStruct
	SymLinkTarget    string
}

func (volumeGET *volumeFullGETStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (volumeGET *volumeFullGETStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (volumeGET *volumeFullGETStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		authOK bool
	)

	nodeByteSlice, authOK, err = volumeGET.volume.swiftObjectGetRange(objectNumber, objectOffset, objectLength)
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

func (volumeGET *volumeFullGETStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (volumeGET *volumeFullGETStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (volumeGET *volumeFullGETStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (volumeGET *volumeFullGETStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
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

func (volumeGET *volumeFullGETStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (volumeGET *volumeFullGETStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
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

func serveHTTPGetOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	var (
		checkPointV1 *ilayout.CheckPointV1Struct
		err          error
		// inodeHeadV1 *ilayout.InodeHeadV1Struct
		inodeNumberAsHexDigits            string
		inodeNumberAsUint64               uint64
		inodeNumberAsKey                  sortedmap.Key
		inodeTable                        sortedmap.BPlusTree
		inodeTableCache                   sortedmap.BPlusTreeCache
		inodeTableEntryValueV1            *ilayout.InodeTableEntryValueV1Struct
		inodeTableEntryValueV1AsValue     sortedmap.Value
		inodeTableIndex                   int
		inodeTableLayoutIndex             int
		inodeTableLen                     int
		mustBeInode                       string
		ok                                bool
		pathSplit                         []string
		pendingDeleteObjectNameArrayIndex int
		startTime                         time.Time
		superBlockV1                      *ilayout.SuperBlockV1Struct
		toReturnTODO                      string
		volumeAsStruct                    *volumeStruct
		volumeAsValue                     sortedmap.Value
		volumeGET                         *volumeFullGETStruct
		volumeGETAsJSON                   []byte
		volumeGETList                     []*volumeMiniGETStruct
		volumeGETListIndex                int
		volumeGETListAsJSON               []byte
		volumeGETListLen                  int
		volumeName                        string
	)

	startTime = time.Now()

	pathSplit = strings.Split(requestPath, "/")

	switch len(pathSplit) {
	case 2:
		// Form: /volume

		defer func() {
			globals.stats.GetVolumeListUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		globals.Lock()

		volumeGETListLen, err = globals.volumeMap.Len()
		if nil != err {
			logFatal(err)
		}

		volumeGETList = make([]*volumeMiniGETStruct, volumeGETListLen)

		for volumeGETListIndex = 0; volumeGETListIndex < volumeGETListLen; volumeGETListIndex++ {
			_, volumeAsValue, ok, err = globals.volumeMap.GetByIndex(volumeGETListIndex)
			if nil != err {
				logFatal(err)
			}
			if !ok {
				logFatalf("globals.volumeMap[] len (%d) is wrong", volumeGETListLen)
			}

			volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
			if !ok {
				logFatalf("globals.volumeMap[%d] was not a *volumeStruct", volumeGETListIndex)
			}

			volumeGETList[volumeGETListIndex] = &volumeMiniGETStruct{
				Name:                   volumeAsStruct.name,
				StorageURL:             volumeAsStruct.storageURL,
				AuthToken:              volumeAsStruct.authToken,
				HealthyMounts:          uint64(volumeAsStruct.healthyMountList.Len()),
				LeasesExpiredMounts:    uint64(volumeAsStruct.leasesExpiredMountList.Len()),
				AuthTokenExpiredMounts: uint64(volumeAsStruct.authTokenExpiredMountList.Len()),
			}
		}

		globals.Unlock()

		volumeGETListAsJSON, err = json.Marshal(volumeGETList)
		if nil != err {
			logFatal(err)
		}

		responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(volumeGETListAsJSON)))
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write(volumeGETListAsJSON)
		if nil != err {
			logWarnf("responseWriter.Write(volumeGETListAsJSON) failed: %v", err)
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

			checkPointV1, err = volumeAsStruct.fetchCheckPoint()
			if nil != err {
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			superBlockV1, err = volumeAsStruct.fetchSuperBlock(checkPointV1.SuperBlockObjectNumber, checkPointV1.SuperBlockLength)
			if nil != err {
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			volumeGET = &volumeFullGETStruct{
				volume:                       volumeAsStruct,
				Name:                         volumeAsStruct.name,
				StorageURL:                   volumeAsStruct.storageURL,
				AuthToken:                    volumeAsStruct.authToken,
				HealthyMounts:                uint64(volumeAsStruct.healthyMountList.Len()),
				LeasesExpiredMounts:          uint64(volumeAsStruct.leasesExpiredMountList.Len()),
				AuthTokenExpiredMounts:       uint64(volumeAsStruct.authTokenExpiredMountList.Len()),
				SuperBlockObjectName:         ilayout.GetObjectNameAsString(checkPointV1.SuperBlockObjectNumber),
				SuperBlockLength:             checkPointV1.SuperBlockLength,
				ReservedToNonce:              checkPointV1.ReservedToNonce,
				InodeTableLayout:             make([]volumeFullGETInodeTableLayoutEntryStruct, len(superBlockV1.InodeTableLayout)),
				InodeObjectCount:             superBlockV1.InodeObjectCount,
				InodeObjectSize:              superBlockV1.InodeObjectSize,
				InodeBytesReferenced:         superBlockV1.InodeBytesReferenced,
				PendingDeleteObjectNameArray: make([]string, len(superBlockV1.PendingDeleteObjectNumberArray)),
				// InodeTable filled after the BPlusTree has been loaded later
			}

			for inodeTableLayoutIndex = range volumeGET.InodeTableLayout {
				volumeGET.InodeTableLayout[inodeTableLayoutIndex].ObjectName = ilayout.GetObjectNameAsString(superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectNumber)
				volumeGET.InodeTableLayout[inodeTableLayoutIndex].ObjectSize = superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectSize
				volumeGET.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced = superBlockV1.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced
			}

			for pendingDeleteObjectNameArrayIndex = range volumeGET.PendingDeleteObjectNameArray {
				volumeGET.PendingDeleteObjectNameArray[pendingDeleteObjectNameArrayIndex] = ilayout.GetObjectNameAsString(superBlockV1.PendingDeleteObjectNumberArray[pendingDeleteObjectNameArrayIndex])
			}

			inodeTableCache = sortedmap.NewBPlusTreeCache(volumeGETInodeTableCacheLowLimit, volumeGETInodeTableCacheHighLimit)

			inodeTable, err = sortedmap.OldBPlusTree(superBlockV1.InodeTableRootObjectNumber, superBlockV1.InodeTableRootObjectOffset, superBlockV1.InodeTableRootObjectLength, sortedmap.CompareUint64, volumeGET, inodeTableCache)
			if nil != err {
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			inodeTableLen, err = inodeTable.Len()
			if nil != err {
				globals.Unlock()
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}

			volumeGET.InodeTable = make([]volumeFullGETInodeTableEntryStruct, inodeTableLen)

			for inodeTableIndex = range volumeGET.InodeTable {
				inodeNumberAsKey, inodeTableEntryValueV1AsValue, ok, err = inodeTable.GetByIndex(inodeTableIndex)
				if (nil != err) || !ok {
					globals.Unlock()
					responseWriter.WriteHeader(http.StatusUnauthorized)
					return
				}

				volumeGET.InodeTable[inodeTableIndex].InodeNumber, ok = inodeNumberAsKey.(uint64)
				if !ok {
					globals.Unlock()
					responseWriter.WriteHeader(http.StatusUnauthorized)
					return
				}

				inodeTableEntryValueV1, ok = inodeTableEntryValueV1AsValue.(*ilayout.InodeTableEntryValueV1Struct)
				if !ok {
					globals.Unlock()
					responseWriter.WriteHeader(http.StatusUnauthorized)
					return
				}

				volumeGET.InodeTable[inodeTableIndex].InodeHeadObjectName = ilayout.GetObjectNameAsString(inodeTableEntryValueV1.InodeHeadObjectNumber)
				volumeGET.InodeTable[inodeTableIndex].InodeHeadLength = inodeTableEntryValueV1.InodeHeadLength
			}

			globals.Unlock()

			volumeGETAsJSON, err = json.Marshal(volumeGET)
			if nil != err {
				logFatal(err)
			}

			responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(volumeGETAsJSON)))
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			_, err = responseWriter.Write(volumeGETAsJSON)
			if nil != err {
				logWarnf("responseWriter.Write(volumeGETAsJSON) failed: %v", err)
			}
		} else {
			globals.Unlock()
			responseWriter.WriteHeader(http.StatusNotFound)
		}
	case 5:
		// Form: /volume/<VolumeName>/inode/<InodeNumberAsHexDigits>

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

					toReturnTODO = fmt.Sprintf("TODO: GET /volume/%s/inode/%016X", volumeAsStruct.name, inodeNumberAsUint64)

					globals.Unlock()

					responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(toReturnTODO)))
					responseWriter.Header().Set("Content-Type", "text/plain")
					responseWriter.WriteHeader(http.StatusOK)

					_, err = responseWriter.Write([]byte(toReturnTODO))
					if nil != err {
						logWarnf("responseWriter.Write([]byte(toReturnTODO)) failed: %v", err)
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
	case "/volume" == requestPath:
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
		startTime         time.Time
	)

	startTime = time.Now()

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
		startTime         time.Time
	)

	startTime = time.Now()

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
