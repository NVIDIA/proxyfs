// Package ramswift provides an in-memory emulation of the Swift object storage
// API, which can be run as a goroutine from another package, or as a standalone
// binary.
package ramswift

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/utils"
)

type swiftAccountStruct struct {
	sync.Mutex         //                    protects swiftAccountStruct.headers & swiftContainerTree
	name               string
	headers            http.Header
	swiftContainerTree sortedmap.LLRBTree // key is swiftContainerStruct.name, value is *swiftContainerStruct
}

type swiftAccountContext struct {
	swiftAccount *swiftAccountStruct
}

type swiftContainerStruct struct {
	sync.Mutex      //                     protects swiftContainerStruct.headers & swiftContainerStruct.swiftObjectTree
	name            string
	swiftAccount    *swiftAccountStruct // back-reference to swiftAccountStruct
	headers         http.Header
	swiftObjectTree sortedmap.LLRBTree //  key is swiftObjectStruct.name, value is *swiftObjectStruct
}

type swiftContainerContext struct {
	swiftContainer *swiftContainerStruct
}

type swiftObjectStruct struct {
	sync.Mutex     //                       protects swiftObjectStruct.contents
	name           string
	swiftContainer *swiftContainerStruct // back-reference to swiftContainerStruct
	contents       []byte
}

type methodStruct struct {
	delete uint64
	get    uint64
	head   uint64
	post   uint64
	put    uint64
}

type globalsStruct struct {
	sync.Mutex                      // protects globalsStruct.swiftAccountMap
	whoAmI                          string
	noAuthTCPPort                   uint16
	noAuthAddr                      string
	swiftAccountMap                 map[string]*swiftAccountStruct // key is swiftAccountStruct.name, value is *swiftAccountStruct
	accountMethodCount              methodStruct
	containerMethodCount            methodStruct
	objectMethodCount               methodStruct
	chaosFailureHTTPStatus          int
	accountMethodChaosFailureRate   methodStruct // fail method if corresponding accountMethodCount divisible by accountMethodChaosFailureRate
	containerMethodChaosFailureRate methodStruct // fail method if corresponding containerMethodCount divisible by containerMethodChaosFailureRate
	objectMethodChaosFailureRate    methodStruct // fail method if corresponding objectMethodCount divisible by objectMethodChaosFailureRate
	maxAccountNameLength            uint64
	maxContainerNameLength          uint64
	maxObjectNameLength             uint64
	accountListingLimit             uint64
	containerListingLimit           uint64
}

var globals *globalsStruct

type httpRequestHandler struct{}

type rangeStruct struct {
	startOffset uint64
	stopOffset  uint64
}

type stringSet map[string]bool

var headerNameIgnoreSet = stringSet{"Accept-Encoding": true, "User-Agent": true, "Content-Length": true}

func (context *swiftAccountContext) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString, ok := key.(string)
	if ok {
		err = nil
	} else {
		err = fmt.Errorf("swiftAccountContext.DumpKey() could not parse key as a string")
	}
	return
}

func (context *swiftAccountContext) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	swiftContainer, ok := value.(*swiftContainerStruct)
	if !ok {
		err = fmt.Errorf("swiftAccountContext.DumpValue() could not parse key as a *swiftContainerStruct")
		return
	}
	valueAsString = fmt.Sprintf("@%p: %#v", swiftContainer, swiftContainer)
	err = nil
	return
}

func (context *swiftContainerContext) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString, ok := key.(string)
	if ok {
		err = nil
	} else {
		err = fmt.Errorf("swiftContainerContext.DumpKey() could not parse key as a string")
	}
	return
}

func (context *swiftContainerContext) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	swiftObject, ok := value.(*swiftObjectStruct)
	if !ok {
		err = fmt.Errorf("swiftContainerContext.DumpValue() could not parse key as a *swiftObjectStruct")
		return
	}
	valueAsString = fmt.Sprintf("@%p: %#v", swiftObject, swiftObject)
	err = nil
	return
}

func parsePath(request *http.Request) (infoOnly bool, swiftAccountName string, swiftContainerName string, swiftObjectName string) {
	infoOnly = false
	swiftAccountName = ""
	swiftContainerName = ""
	swiftObjectName = ""

	if "/info" == request.URL.Path {
		infoOnly = true
		return
	}

	if strings.HasPrefix(request.URL.Path, "/v1/") {
		pathSplit := strings.SplitN(request.URL.Path[4:], "/", 3)
		swiftAccountName = pathSplit[0]
		if 1 == len(pathSplit) {
			swiftContainerName = ""
			swiftObjectName = ""
		} else {
			swiftContainerName = pathSplit[1]
			if 2 == len(pathSplit) {
				swiftObjectName = ""
			} else {
				swiftObjectName = pathSplit[2]
			}
		}
	}

	return
}

// parseRangeHeader takes an HTTP Range header (e.g. bytes=100-200, bytes=10-20,30-40) and returns a slice of start and stop indices if any.
func parseRangeHeader(request *http.Request, objectLen int) (ranges []rangeStruct, err error) {
	var (
		off                    int
		rangeHeaderValueSuffix string
		rangeString            string
		rangesStrings          []string
		rangesStringsIndex     int
		startOffset            int64
		stopOffset             int64
	)

	rangeHeaderValue := request.Header.Get("Range")
	if "" == rangeHeaderValue {
		ranges = make([]rangeStruct, 0)
		err = nil
		return
	}

	if !strings.HasPrefix(rangeHeaderValue, "bytes=") {
		err = fmt.Errorf("rangeHeaderValue (%v) does not start with expected \"bytes=\"", rangeHeaderValue)
		return
	}

	rangeHeaderValueSuffix = rangeHeaderValue[len("bytes="):]

	rangesStrings = strings.SplitN(rangeHeaderValueSuffix, ",", 2)

	ranges = make([]rangeStruct, len(rangesStrings))

	for rangesStringsIndex, rangeString = range rangesStrings {
		rangeStringSlice := strings.SplitN(rangeString, "-", 2)
		if 2 != len(rangeStringSlice) {
			err = fmt.Errorf("rangeHeaderValue (%v) malformed", rangeHeaderValue)
			return
		}
		if "" == rangeStringSlice[0] {
			startOffset = int64(-1)
		} else {
			off, err = strconv.Atoi(rangeStringSlice[0])
			if nil != err {
				err = fmt.Errorf("rangeHeaderValue (%v) malformed (strconv.Atoi() failure: %v)", rangeHeaderValue, err)
				return
			}
			startOffset = int64(off)
		}

		if "" == rangeStringSlice[1] {
			stopOffset = int64(-1)
		} else {
			off, err = strconv.Atoi(rangeStringSlice[1])
			if nil != err {
				err = fmt.Errorf("rangeHeaderValue (%v) malformed (strconv.Atoi() failure: %v)", rangeHeaderValue, err)
				return
			}
			stopOffset = int64(off)
		}

		if ((0 > startOffset) && (0 > stopOffset)) || (startOffset > stopOffset) {
			err = fmt.Errorf("rangeHeaderValue (%v) malformed", rangeHeaderValue)
			return
		}

		if startOffset < 0 {
			startOffset = int64(objectLen) - stopOffset
			if startOffset < 0 {
				err = fmt.Errorf("rangeHeaderValue (%v) malformed...computed startOffset negative", rangeHeaderValue)
				return
			}
			stopOffset = int64(objectLen - 1)
		} else if stopOffset < 0 {
			stopOffset = int64(objectLen - 1)
		} else {
			if stopOffset > int64(objectLen-1) {
				stopOffset = int64(objectLen - 1)
			}
		}

		ranges[rangesStringsIndex].startOffset = uint64(startOffset)
		ranges[rangesStringsIndex].stopOffset = uint64(stopOffset)
	}

	err = nil
	return
}

func locateSwiftAccount(swiftAccountName string) (swiftAccount *swiftAccountStruct, errno syscall.Errno) {
	globals.Lock()
	swiftAccount, ok := globals.swiftAccountMap[swiftAccountName]
	if !ok {
		globals.Unlock()
		errno = unix.ENOENT
		return
	}
	globals.Unlock()
	errno = 0
	return
}

func createSwiftAccount(swiftAccountName string) (swiftAccount *swiftAccountStruct, errno syscall.Errno) {
	globals.Lock()
	_, ok := globals.swiftAccountMap[swiftAccountName]
	if ok {
		globals.Unlock()
		errno = unix.EEXIST
		return
	}
	context := &swiftAccountContext{}
	swiftAccount = &swiftAccountStruct{
		name:               swiftAccountName,
		headers:            make(http.Header),
		swiftContainerTree: sortedmap.NewLLRBTree(sortedmap.CompareString, context),
	}
	context.swiftAccount = swiftAccount
	globals.swiftAccountMap[swiftAccountName] = swiftAccount
	globals.Unlock()
	errno = 0
	return
}

func createOrLocateSwiftAccount(swiftAccountName string) (swiftAccount *swiftAccountStruct, wasCreated bool) {
	globals.Lock()
	swiftAccount, ok := globals.swiftAccountMap[swiftAccountName]
	if ok {
		wasCreated = false
	} else {
		context := &swiftAccountContext{}
		swiftAccount = &swiftAccountStruct{
			name:               swiftAccountName,
			headers:            make(http.Header),
			swiftContainerTree: sortedmap.NewLLRBTree(sortedmap.CompareString, context),
		}
		context.swiftAccount = swiftAccount
		globals.swiftAccountMap[swiftAccountName] = swiftAccount
		wasCreated = true
	}
	globals.Unlock()
	return
}

func deleteSwiftAccount(swiftAccountName string, force bool) (errno syscall.Errno) {
	globals.Lock()
	swiftAccount, ok := globals.swiftAccountMap[swiftAccountName]
	if ok {
		if force {
			// ok if account contains data... we'll forget it
		} else {
			swiftAccount.Lock()
			swiftswiftAccountContainerCount, nonShadowingErr := swiftAccount.swiftContainerTree.Len()
			if nil != nonShadowingErr {
				panic(nonShadowingErr)
			}
			if 0 != swiftswiftAccountContainerCount {
				swiftAccount.Unlock()
				globals.Unlock()
				errno = unix.ENOTEMPTY
				return
			}
			swiftAccount.Unlock()
		}
		delete(globals.swiftAccountMap, swiftAccountName)
	} else {
		globals.Unlock()
		errno = unix.ENOENT
		return
	}
	globals.Unlock()
	errno = 0
	return
}

func locateSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (swiftContainer *swiftContainerStruct, errno syscall.Errno) {
	swiftAccount.Lock()
	swiftContainerAsValue, ok, err := swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftContainer = swiftContainerAsValue.(*swiftContainerStruct)
	} else {
		swiftAccount.Unlock()
		errno = unix.ENOENT
		return
	}
	swiftAccount.Unlock()
	errno = 0
	return
}

func createSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (swiftContainer *swiftContainerStruct, errno syscall.Errno) {
	swiftAccount.Lock()
	_, ok, err := swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftAccount.Unlock()
		errno = unix.EEXIST
		return
	} else {
		context := &swiftContainerContext{}
		swiftContainer = &swiftContainerStruct{
			name:            swiftContainerName,
			swiftAccount:    swiftAccount,
			headers:         make(http.Header),
			swiftObjectTree: sortedmap.NewLLRBTree(sortedmap.CompareString, context),
		}
		context.swiftContainer = swiftContainer
		_, err = swiftAccount.swiftContainerTree.Put(swiftContainerName, swiftContainer)
		if nil != err {
			panic(err)
		}
	}
	swiftAccount.Unlock()
	errno = 0
	return
}

func createOrLocateSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (swiftContainer *swiftContainerStruct, wasCreated bool) {
	swiftAccount.Lock()
	swiftContainerAsValue, ok, err := swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftContainer = swiftContainerAsValue.(*swiftContainerStruct)
		wasCreated = false
	} else {
		context := &swiftContainerContext{}
		swiftContainer = &swiftContainerStruct{
			name:            swiftContainerName,
			swiftAccount:    swiftAccount,
			headers:         make(http.Header),
			swiftObjectTree: sortedmap.NewLLRBTree(sortedmap.CompareString, context),
		}
		context.swiftContainer = swiftContainer
		_, err = swiftAccount.swiftContainerTree.Put(swiftContainerName, swiftContainer)
		if nil != err {
			panic(err)
		}
		wasCreated = true
	}
	swiftAccount.Unlock()
	return
}

func deleteSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (errno syscall.Errno) {
	swiftAccount.Lock()
	swiftContainerAsValue, ok, err := swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftContainer := swiftContainerAsValue.(*swiftContainerStruct)
		swiftContainer.Lock()
		swiftContainerObjectCount, nonShadowingErr := swiftContainer.swiftObjectTree.Len()
		if nil != nonShadowingErr {
			panic(nonShadowingErr)
		}
		if 0 != swiftContainerObjectCount {
			swiftContainer.Unlock()
			swiftAccount.Unlock()
			errno = unix.ENOTEMPTY
			return
		}
		swiftContainer.Unlock()
		_, err = swiftAccount.swiftContainerTree.DeleteByKey(swiftContainerName)
		if nil != err {
			panic(err)
		}
	} else {
		swiftAccount.Unlock()
		errno = unix.ENOENT
		return
	}
	swiftAccount.Unlock()
	errno = 0
	return
}

func locateSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (swiftObject *swiftObjectStruct, errno syscall.Errno) {
	swiftContainer.Lock()
	swiftObjectAsValue, ok, err := swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftObject = swiftObjectAsValue.(*swiftObjectStruct)
	} else {
		swiftContainer.Unlock()
		errno = unix.ENOENT
		return
	}
	swiftContainer.Unlock()
	errno = 0
	return
}

func createSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (swiftObject *swiftObjectStruct, errno syscall.Errno) {
	swiftContainer.Lock()
	_, ok, err := swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftContainer.Unlock()
		errno = unix.EEXIST
		return
	} else {
		swiftObject = &swiftObjectStruct{name: swiftObjectName, swiftContainer: swiftContainer, contents: []byte{}}
		_, err = swiftContainer.swiftObjectTree.Put(swiftObjectName, swiftObject)
		if nil != err {
			panic(err)
		}
	}
	swiftContainer.Unlock()
	errno = 0
	return
}

func createOrLocateSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (swiftObject *swiftObjectStruct, wasCreated bool) {
	swiftContainer.Lock()
	swiftObjectAsValue, ok, err := swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftObject = swiftObjectAsValue.(*swiftObjectStruct)
		wasCreated = false
	} else {
		swiftObject = &swiftObjectStruct{name: swiftObjectName, swiftContainer: swiftContainer, contents: []byte{}}
		_, err = swiftContainer.swiftObjectTree.Put(swiftObjectName, swiftObject)
		if nil != err {
			panic(err)
		}
		wasCreated = true
	}
	swiftContainer.Unlock()
	return
}

func deleteSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (errno syscall.Errno) {
	swiftContainer.Lock()
	_, ok, err := swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		_, err = swiftContainer.swiftObjectTree.DeleteByKey(swiftObjectName)
		if nil != err {
			panic(err)
		}
	} else {
		swiftContainer.Unlock()
		errno = unix.ENOENT
		return
	}
	swiftContainer.Unlock()
	errno = 0
	return
}

func doDelete(responseWriter http.ResponseWriter, request *http.Request) {
	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName := parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		if "" == swiftContainerName {
			// DELETE SwiftAccount
			globals.Lock()
			globals.accountMethodCount.delete++
			if (0 != globals.accountMethodChaosFailureRate.delete) && (0 == globals.accountMethodCount.delete%globals.accountMethodChaosFailureRate.delete) {
				globals.Unlock()
				responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
			} else {
				globals.Unlock()
				errno := deleteSwiftAccount(swiftAccountName, false)
				switch errno {
				case 0:
					responseWriter.WriteHeader(http.StatusNoContent)
				case unix.ENOENT:
					responseWriter.WriteHeader(http.StatusNotFound)
				case unix.ENOTEMPTY:
					responseWriter.WriteHeader(http.StatusConflict)
				default:
					err := fmt.Errorf("deleteSwiftAccount(\"%v\", false) returned unexpected errno: %v", swiftAccountName, errno)
					panic(err)
				}
			}
		} else {
			// DELETE SwiftContainer or SwiftObject
			swiftAccount, errno := locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftObjectName {
					// DELETE SwiftContainer
					globals.Lock()
					globals.containerMethodCount.delete++
					if (0 != globals.containerMethodChaosFailureRate.delete) && (0 == globals.containerMethodCount.delete%globals.containerMethodChaosFailureRate.delete) {
						globals.Unlock()
						responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
					} else {
						globals.Unlock()
						errno := deleteSwiftContainer(swiftAccount, swiftContainerName)
						switch errno {
						case 0:
							responseWriter.WriteHeader(http.StatusNoContent)
						case unix.ENOENT:
							responseWriter.WriteHeader(http.StatusNotFound)
						case unix.ENOTEMPTY:
							responseWriter.WriteHeader(http.StatusConflict)
						default:
							err := fmt.Errorf("deleteSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
							panic(err)
						}
					}
				} else {
					// DELETE SwiftObject
					globals.Lock()
					globals.objectMethodCount.delete++
					if (0 != globals.objectMethodChaosFailureRate.delete) && (0 == globals.objectMethodCount.delete%globals.objectMethodChaosFailureRate.delete) {
						globals.Unlock()
						responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
					} else {
						globals.Unlock()
						swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
						switch errno {
						case 0:
							errno := deleteSwiftObject(swiftContainer, swiftObjectName)
							switch errno {
							case 0:
								responseWriter.WriteHeader(http.StatusNoContent)
							case unix.ENOENT:
								responseWriter.WriteHeader(http.StatusNotFound)
							default:
								err := fmt.Errorf("deleteSwiftObject(\"%v\") returned unexpected errno: %v", swiftObjectName, errno)
								panic(err)
							}
						case unix.ENOENT:
							responseWriter.WriteHeader(http.StatusNotFound)
						default:
							err := fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
							panic(err)
						}
					}
				}
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusNotFound)
			default:
				err := fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		}
	}
}

func doGet(responseWriter http.ResponseWriter, request *http.Request) {
	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName := parsePath(request)
	if infoOnly {
		_, _ = responseWriter.Write(utils.StringToByteSlice("{"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"swift\": {"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"max_account_name_length\": " + strconv.Itoa(int(globals.maxAccountNameLength)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"max_container_name_length\": " + strconv.Itoa(int(globals.maxContainerNameLength)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"max_object_name_length\": " + strconv.Itoa(int(globals.maxObjectNameLength)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"account_listing_limit\": " + strconv.Itoa(int(globals.accountListingLimit)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"container_listing_limit\": " + strconv.Itoa(int(globals.containerListingLimit))))
		_, _ = responseWriter.Write(utils.StringToByteSlice("}"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("}"))
	} else {
		if "" == swiftAccountName {
			responseWriter.WriteHeader(http.StatusForbidden)
		} else {
			swiftAccount, errno := locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftContainerName {
					// GET SwiftAccount
					globals.Lock()
					globals.accountMethodCount.get++
					if (0 != globals.accountMethodChaosFailureRate.get) && (0 == globals.accountMethodCount.get%globals.accountMethodChaosFailureRate.get) {
						globals.Unlock()
						responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
					} else {
						globals.Unlock()
						swiftAccount.Lock()
						for headerName, headerValueSlice := range swiftAccount.headers {
							for _, headerValue := range headerValueSlice {
								responseWriter.Header().Add(headerName, headerValue)
							}
						}
						numContainers, err := swiftAccount.swiftContainerTree.Len()
						if nil != err {
							panic(err)
						}
						if 0 == numContainers {
							responseWriter.WriteHeader(http.StatusNoContent)
						} else {
							marker := ""
							markerSlice, ok := request.URL.Query()["marker"]
							if ok && (0 < len(markerSlice)) {
								marker = markerSlice[0]
							}
							containerIndex, found, err := swiftAccount.swiftContainerTree.BisectRight(marker)
							if nil != err {
								panic(err)
							}
							if found {
								containerIndex++
							}
							if containerIndex < numContainers {
								containerIndexLimit := numContainers
								if (containerIndexLimit - containerIndex) > int(globals.accountListingLimit) {
									containerIndexLimit = containerIndex + int(globals.accountListingLimit)
								}
								for containerIndex < containerIndexLimit {
									swiftContainerNameAsKey, _, _, err := swiftAccount.swiftContainerTree.GetByIndex(containerIndex)
									if nil != err {
										panic(err)
									}
									swiftContainerName := swiftContainerNameAsKey.(string)
									_, _ = responseWriter.Write(utils.StringToByteSlice(swiftContainerName))
									_, _ = responseWriter.Write([]byte{'\n'})
									containerIndex++
								}
							} else {
								responseWriter.WriteHeader(http.StatusNoContent)
							}
						}
						swiftAccount.Unlock()
					}
				} else {
					// GET SwiftContainer or SwiftObject
					swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
					switch errno {
					case 0:
						if "" == swiftObjectName {
							// GET SwiftContainer
							globals.Lock()
							globals.containerMethodCount.get++
							if (0 != globals.containerMethodChaosFailureRate.get) && (0 == globals.containerMethodCount.get%globals.containerMethodChaosFailureRate.get) {
								globals.Unlock()
								responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
							} else {
								globals.Unlock()
								swiftContainer.Lock()
								for headerName, headerValueSlice := range swiftContainer.headers {
									for _, headerValue := range headerValueSlice {
										responseWriter.Header().Add(headerName, headerValue)
									}
								}
								numObjects, err := swiftContainer.swiftObjectTree.Len()
								if nil != err {
									panic(err)
								}
								if 0 == numObjects {
									responseWriter.WriteHeader(http.StatusNoContent)
								} else {
									marker := ""
									markerSlice, ok := request.URL.Query()["marker"]
									if ok && (0 < len(markerSlice)) {
										marker = markerSlice[0]
									}
									objectIndex, found, err := swiftContainer.swiftObjectTree.BisectRight(marker)
									if nil != err {
										panic(err)
									}
									if found {
										objectIndex++
									}
									if objectIndex < numObjects {
										objectIndexLimit := numObjects
										if (objectIndexLimit - objectIndex) > int(globals.containerListingLimit) {
											objectIndexLimit = objectIndex + int(globals.containerListingLimit)
										}
										for objectIndex < objectIndexLimit {
											swiftObjectNameAsKey, _, _, err := swiftContainer.swiftObjectTree.GetByIndex(objectIndex)
											if nil != err {
												panic(err)
											}
											swiftObjectName := swiftObjectNameAsKey.(string)
											_, _ = responseWriter.Write(utils.StringToByteSlice(swiftObjectName))
											_, _ = responseWriter.Write([]byte{'\n'})
											objectIndex++
										}
									} else {
										responseWriter.WriteHeader(http.StatusNoContent)
									}
								}
								swiftContainer.Unlock()
							}
						} else {
							// GET SwiftObject
							globals.Lock()
							globals.objectMethodCount.get++
							if (0 != globals.objectMethodChaosFailureRate.get) && (0 == globals.objectMethodCount.get%globals.objectMethodChaosFailureRate.get) {
								globals.Unlock()
								responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
							} else {
								globals.Unlock()
								swiftObject, errno := locateSwiftObject(swiftContainer, swiftObjectName)
								switch errno {
								case 0:
									swiftObject.Lock()
									ranges, err := parseRangeHeader(request, len(swiftObject.contents))
									if nil == err {
										switch len(ranges) {
										case 0:
											responseWriter.Header().Add("Content-Type", "application/octet-stream")
											responseWriter.WriteHeader(http.StatusOK)
											_, _ = responseWriter.Write(swiftObject.contents)
										case 1:
											responseWriter.Header().Add("Content-Type", "application/octet-stream")
											responseWriter.Header().Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ranges[0].startOffset, ranges[0].stopOffset, len(swiftObject.contents)))
											responseWriter.WriteHeader(http.StatusPartialContent)
											_, _ = responseWriter.Write(swiftObject.contents[ranges[0].startOffset:(ranges[0].stopOffset + 1)])
										default:
											boundaryString := fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())
											responseWriter.Header().Add("Content-Type", fmt.Sprintf("multipart/byteranges; boundary=%v", boundaryString))
											responseWriter.WriteHeader(http.StatusPartialContent)
											for _, rS := range ranges {
												_, _ = responseWriter.Write([]byte("--" + boundaryString + "\r\n"))
												_, _ = responseWriter.Write([]byte("Content-Type: application/octet-stream\r\n"))
												_, _ = responseWriter.Write([]byte(fmt.Sprintf("Content-Range: bytes %d-%d/%d\r\n", rS.startOffset, rS.stopOffset, len(swiftObject.contents))))
												_, _ = responseWriter.Write([]byte("\r\n"))
												_, _ = responseWriter.Write(swiftObject.contents[rS.startOffset:(rS.stopOffset + 1)])
												_, _ = responseWriter.Write([]byte("\r\n"))
											}
											_, _ = responseWriter.Write([]byte("--" + boundaryString + "--"))
										}
									} else {
										responseWriter.WriteHeader(http.StatusBadRequest)
									}
									swiftObject.Unlock()
								case unix.ENOENT:
									responseWriter.WriteHeader(http.StatusNotFound)
								default:
									err := fmt.Errorf("locateSwiftObject(\"%v\") returned unexpected errno: %v", swiftObjectName, errno)
									panic(err)
								}
							}
						}
					case unix.ENOENT:
						responseWriter.WriteHeader(http.StatusNotFound)
					default:
						err := fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
						panic(err)
					}
				}
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusNotFound)
			default:
				err := fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		}
	}
}

func doHead(responseWriter http.ResponseWriter, request *http.Request) {
	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName := parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		swiftAccount, errno := locateSwiftAccount(swiftAccountName)
		switch errno {
		case 0:
			if "" == swiftContainerName {
				// HEAD SwiftAccount
				globals.Lock()
				globals.accountMethodCount.head++
				if (0 != globals.accountMethodChaosFailureRate.head) && (0 == globals.accountMethodCount.head%globals.accountMethodChaosFailureRate.head) {
					globals.Unlock()
					responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
				} else {
					globals.Unlock()
					swiftAccount.Lock()
					for headerName, headerValueSlice := range swiftAccount.headers {
						for _, headerValue := range headerValueSlice {
							responseWriter.Header().Add(headerName, headerValue)
						}
					}
					swiftAccount.Unlock()
					responseWriter.WriteHeader(http.StatusNoContent)
				}
			} else {
				// HEAD SwiftContainer or SwiftObject
				swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
				switch errno {
				case 0:
					if "" == swiftObjectName {
						// HEAD SwiftContainer
						globals.Lock()
						globals.containerMethodCount.head++
						if (0 != globals.containerMethodChaosFailureRate.head) && (0 == globals.containerMethodCount.head%globals.containerMethodChaosFailureRate.head) {
							globals.Unlock()
							responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
						} else {
							globals.Unlock()
							swiftContainer.Lock()
							for headerName, headerValueSlice := range swiftContainer.headers {
								for _, headerValue := range headerValueSlice {
									responseWriter.Header().Add(headerName, headerValue)
								}
							}
							swiftContainer.Unlock()
							responseWriter.WriteHeader(http.StatusNoContent)
						}
					} else {
						// HEAD SwiftObject
						globals.Lock()
						globals.objectMethodCount.head++
						if (0 != globals.objectMethodChaosFailureRate.head) && (0 == globals.objectMethodCount.head%globals.objectMethodChaosFailureRate.head) {
							globals.Unlock()
							responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
						} else {
							globals.Unlock()
							swiftObject, errno := locateSwiftObject(swiftContainer, swiftObjectName)
							switch errno {
							case 0:
								responseWriter.Header().Set("Content-Length", strconv.Itoa(len(swiftObject.contents)))
								responseWriter.WriteHeader(http.StatusOK)
							case unix.ENOENT:
								responseWriter.WriteHeader(http.StatusNotFound)
							default:
								err := fmt.Errorf("locateSwiftObject(\"%v\") returned unexpected errno: %v", swiftObjectName, errno)
								panic(err)
							}
						}
					}
				case unix.ENOENT:
					responseWriter.WriteHeader(http.StatusNotFound)
				default:
					err := fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
					panic(err)
				}
			}
		case unix.ENOENT:
			responseWriter.WriteHeader(http.StatusNotFound)
		default:
			err := fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
			panic(err)
		}
	}
}

func doPost(responseWriter http.ResponseWriter, request *http.Request) {
	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName := parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		swiftAccount, errno := locateSwiftAccount(swiftAccountName)
		switch errno {
		case 0:
			if "" == swiftContainerName {
				// POST SwiftAccount
				globals.Lock()
				globals.accountMethodCount.post++
				if (0 != globals.accountMethodChaosFailureRate.post) && (0 == globals.accountMethodCount.post%globals.accountMethodChaosFailureRate.post) {
					globals.Unlock()
					responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
				} else {
					globals.Unlock()
					swiftAccount.Lock()
					for headerName, headerValueSlice := range request.Header {
						_, ignoreHeader := headerNameIgnoreSet[headerName]
						if !ignoreHeader {
							headerValueSliceLen := len(headerValueSlice)
							if 0 < headerValueSliceLen {
								swiftAccount.headers[headerName] = make([]string, 0, headerValueSliceLen)
								for _, headerValue := range headerValueSlice {
									if 0 < len(headerValue) {
										swiftAccount.headers[headerName] = append(swiftAccount.headers[headerName], headerValue)
									}
								}
								if 0 == len(swiftAccount.headers[headerName]) {
									delete(swiftAccount.headers, headerName)
								}
							}
						}
					}
					swiftAccount.Unlock()
					responseWriter.WriteHeader(http.StatusNoContent)
				}
			} else {
				// POST SwiftContainer or SwiftObject
				swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
				switch errno {
				case 0:
					if "" == swiftObjectName {
						// POST SwiftContainer
						globals.Lock()
						globals.containerMethodCount.post++
						if (0 != globals.containerMethodChaosFailureRate.post) && (0 == globals.containerMethodCount.post%globals.containerMethodChaosFailureRate.post) {
							globals.Unlock()
							responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
						} else {
							globals.Unlock()
							swiftContainer.Lock()
							for headerName, headerValueSlice := range request.Header {
								_, ignoreHeader := headerNameIgnoreSet[headerName]
								if !ignoreHeader {
									headerValueSliceLen := len(headerValueSlice)
									if 0 < headerValueSliceLen {
										swiftContainer.headers[headerName] = make([]string, 0, headerValueSliceLen)
										for _, headerValue := range headerValueSlice {
											if 0 < len(headerValue) {
												swiftContainer.headers[headerName] = append(swiftContainer.headers[headerName], headerValue)
											}
										}
										if 0 == len(swiftContainer.headers[headerName]) {
											delete(swiftContainer.headers, headerName)
										}
									}
								}
							}
							swiftContainer.Unlock()
							responseWriter.WriteHeader(http.StatusNoContent)
						}
					} else {
						// POST SwiftObject
						globals.Lock()
						globals.objectMethodCount.post++
						if (0 != globals.objectMethodChaosFailureRate.post) && (0 == globals.objectMethodCount.post%globals.objectMethodChaosFailureRate.post) {
							globals.Unlock()
							responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
						} else {
							globals.Unlock()
							responseWriter.WriteHeader(http.StatusForbidden)
						}
					}
				case unix.ENOENT:
					responseWriter.WriteHeader(http.StatusNotFound)
				default:
					err := fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
					panic(err)
				}
			}
		case unix.ENOENT:
			responseWriter.WriteHeader(http.StatusNotFound)
		default:
			err := fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
			panic(err)
		}
	}
}

func doPut(responseWriter http.ResponseWriter, request *http.Request) {
	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName := parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		if "" == swiftContainerName {
			// PUT SwiftAccount
			globals.Lock()
			globals.accountMethodCount.put++
			if (0 != globals.accountMethodChaosFailureRate.put) && (0 == globals.accountMethodCount.put%globals.accountMethodChaosFailureRate.put) {
				globals.Unlock()
				responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
			} else {
				globals.Unlock()
				swiftAccount, wasCreated := createOrLocateSwiftAccount(swiftAccountName)
				swiftAccount.Lock()
				if wasCreated {
					swiftAccount.headers = make(http.Header)
				}
				for headerName, headerValueSlice := range request.Header {
					_, ignoreHeader := headerNameIgnoreSet[headerName]
					if !ignoreHeader {
						headerValueSliceLen := len(headerValueSlice)
						if 0 < headerValueSliceLen {
							swiftAccount.headers[headerName] = make([]string, 0, headerValueSliceLen)
							for _, headerValue := range headerValueSlice {
								if 0 < len(headerValue) {
									swiftAccount.headers[headerName] = append(swiftAccount.headers[headerName], headerValue)
								}
							}
							if 0 == len(swiftAccount.headers[headerName]) {
								delete(swiftAccount.headers, headerName)
							}
						}
					}
				}
				swiftAccount.Unlock()
				if wasCreated {
					responseWriter.WriteHeader(http.StatusCreated)
				} else {
					responseWriter.WriteHeader(http.StatusAccepted)
				}
			}
		} else {
			// PUT SwiftContainer or SwiftObject
			swiftAccount, errno := locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftObjectName {
					// PUT SwiftContainer
					globals.Lock()
					globals.containerMethodCount.put++
					if (0 != globals.containerMethodChaosFailureRate.put) && (0 == globals.containerMethodCount.put%globals.containerMethodChaosFailureRate.put) {
						globals.Unlock()
						responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)
					} else {
						globals.Unlock()
						swiftContainer, wasCreated := createOrLocateSwiftContainer(swiftAccount, swiftContainerName)
						swiftContainer.Lock()
						if wasCreated {
							swiftContainer.headers = make(http.Header)
						}
						for headerName, headerValueSlice := range request.Header {
							_, ignoreHeader := headerNameIgnoreSet[headerName]
							if !ignoreHeader {
								headerValueSliceLen := len(headerValueSlice)
								if 0 < headerValueSliceLen {
									swiftContainer.headers[headerName] = make([]string, 0, headerValueSliceLen)
									for _, headerValue := range headerValueSlice {
										if 0 < len(headerValue) {
											swiftContainer.headers[headerName] = append(swiftContainer.headers[headerName], headerValue)
										}
									}
									if 0 == len(swiftContainer.headers[headerName]) {
										delete(swiftContainer.headers, headerName)
									}
								}
							}
						}
						swiftContainer.Unlock()
						if wasCreated {
							responseWriter.WriteHeader(http.StatusCreated)
						} else {
							responseWriter.WriteHeader(http.StatusAccepted)
						}
					}
				} else {
					// PUT SwiftObject
					globals.Lock()
					globals.objectMethodCount.put++
					if (0 != globals.objectMethodChaosFailureRate.put) && (0 == globals.objectMethodCount.put%globals.objectMethodChaosFailureRate.put) {
						globals.Unlock()
						responseWriter.WriteHeader(globals.chaosFailureHTTPStatus)

						// consume the PUT so the sender can finish sending
						// and read the response
						_, _ = ioutil.ReadAll(request.Body)
					} else {
						globals.Unlock()
						swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
						switch errno {
						case 0:
							swiftObject, wasCreated := createOrLocateSwiftObject(swiftContainer, swiftObjectName)
							swiftObject.Lock()
							swiftObject.contents, _ = ioutil.ReadAll(request.Body)
							swiftObject.Unlock()
							if wasCreated {
								responseWriter.WriteHeader(http.StatusCreated)
							} else {
								responseWriter.WriteHeader(http.StatusAccepted)
							}
						case unix.ENOENT:
							responseWriter.WriteHeader(http.StatusForbidden)
						default:
							err := fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
							panic(err)
						}
					}
				}
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusForbidden)
			default:
				err := fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		}
	}
}

func (h httpRequestHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodDelete:
		doDelete(responseWriter, request)
	case http.MethodGet:
		doGet(responseWriter, request)
	case http.MethodHead:
		doHead(responseWriter, request)
	case http.MethodPost:
		doPost(responseWriter, request)
	case http.MethodPut:
		doPut(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func serveNoAuthSwift(confMap conf.ConfMap) {
	var (
		err                    error
		errno                  syscall.Errno
		primaryPeerList        []string
		swiftAccountName       string
		volumeGroupNameList    []string
		volumeGroupName        string
		volumeGroupSectionName string
		volumeName             string
		volumeNameList         []string
		volumeSectionName      string
	)

	// Fetch and configure volumes for which "we" are the PrimaryPeer

	volumeGroupNameList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != err {
		log.Fatalf("failed fetch of FSGlobals.VolumeGroupList: %v", err)
	}

	for _, volumeGroupName = range volumeGroupNameList {
		volumeGroupSectionName = "VolumeGroup:" + volumeGroupName

		primaryPeerList, err = confMap.FetchOptionValueStringSlice(volumeGroupSectionName, "PrimaryPeer")
		if nil != err {
			log.Fatalf("failed fetch of %v.PrimaryPeer: %v", volumeGroupSectionName, err)
		}
		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if globals.whoAmI != primaryPeerList[0] {
				continue
			}
		} else {
			log.Fatalf("fetch of %v.PrimaryPeer returned multiple values", volumeGroupSectionName)
		}

		volumeNameList, err = confMap.FetchOptionValueStringSlice(volumeGroupSectionName, "VolumeList")
		if nil != err {
			log.Fatalf("failed fetch of %v.VolumeList: %v", volumeGroupSectionName, err)
		}

		for _, volumeName = range volumeNameList {
			volumeSectionName = "Volume:" + volumeName

			swiftAccountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
			if nil != err {
				log.Fatalf("failed fetch of %v.AccountName: %v", volumeSectionName, err)
			}

			_, errno = createSwiftAccount(swiftAccountName)
			if 0 != errno {
				log.Fatalf("failed create of %v: %v", swiftAccountName, err)
			}
		}
	}

	// Fetch chaos settings

	fetchChaosSettings(confMap)

	// Fetch responses for GETs on /info

	fetchSwiftInfo(confMap)

	// Launch HTTP Server on the requested noAuthTCPPort

	http.ListenAndServe(globals.noAuthAddr, httpRequestHandler{})
}

func updateConf(confMap conf.ConfMap) {
	var (
		err                         error
		noAuthTCPPortUpdate         uint16
		ok                          bool
		primaryPeerList             []string
		swiftAccountNameListCurrent []string        // element == swiftAccountName
		swiftAccountNameListUpdate  map[string]bool // key     == swiftAccountName; value is ignored
		swiftAccountName            string
		volumeListUpdate            []string
		volumeName                  string
		volumeSectionName           string
		whoAmIUpdate                string
	)

	// First validate "we" didn't change

	whoAmIUpdate, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		log.Fatalf("failed fetch of Cluster.WhoAmI: %v", err)
	}
	if whoAmIUpdate != globals.whoAmI {
		log.Fatal("update of whoAmI not allowed")
	}

	noAuthTCPPortUpdate, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		log.Fatalf("failed fetch of Swift.NoAuthTCPPort: %v", err)
	}
	if noAuthTCPPortUpdate != globals.noAuthTCPPort {
		log.Fatal("update of noAuthTCPPort not allowed")
	}

	// Compute current list of accounts being served

	swiftAccountNameListCurrent = make([]string, 0)

	globals.Lock()

	for swiftAccountName = range globals.swiftAccountMap {
		swiftAccountNameListCurrent = append(swiftAccountNameListCurrent, swiftAccountName)
	}

	globals.Unlock()

	// Fetch list of accounts for which "we" are the PrimaryPeer

	volumeListUpdate, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		log.Fatalf("failed fetch of FSGlobals.VolumeList: %v", err)
	}

	swiftAccountNameListUpdate = make(map[string]bool)

	for _, volumeName = range volumeListUpdate {
		volumeSectionName = "Volume:" + volumeName
		primaryPeerList, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "PrimaryPeer")
		if nil != err {
			log.Fatalf("failed fetch of %v.PrimaryPeer: %v", volumeSectionName, err)
		}
		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if globals.whoAmI != primaryPeerList[0] {
				continue
			}
		} else {
			log.Fatalf("fetch of %v.PrimaryPeer returned multiple values", volumeSectionName)
		}
		swiftAccountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
		if nil != err {
			log.Fatalf("failed fetch of %v.AccountName: %v", volumeSectionName, err)
		}
		swiftAccountNameListUpdate[swiftAccountName] = true
	}

	// Delete accounts not found in accountListUpdate

	for _, swiftAccountName = range swiftAccountNameListCurrent {
		_, ok = swiftAccountNameListUpdate[swiftAccountName]
		if !ok {
			_ = deleteSwiftAccount(swiftAccountName, true)
		}
	}

	// Add accounts in accountListUpdate not found in globals.swiftAccountMap

	for swiftAccountName = range swiftAccountNameListUpdate {
		_, _ = createOrLocateSwiftAccount(swiftAccountName)
	}

	// Fetch (potentially updated) chaos settings

	fetchChaosSettings(confMap)

	// Fetch (potentially updated) responses for GETs on /info

	fetchSwiftInfo(confMap)
}

func fetchChaosSettings(confMap conf.ConfMap) {
	var (
		chaosFailureHTTPStatus uint16
		err                    error
	)

	chaosFailureHTTPStatus, err = confMap.FetchOptionValueUint16("RamSwiftChaos", "FailureHTTPStatus")
	if nil == err {
		globals.chaosFailureHTTPStatus = int(chaosFailureHTTPStatus)
	} else {
		globals.chaosFailureHTTPStatus = http.StatusInternalServerError
	}

	globals.accountMethodChaosFailureRate.delete, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "AccountDeleteFailureRate")
	if nil != err {
		globals.accountMethodChaosFailureRate.delete = 0
	}
	globals.accountMethodChaosFailureRate.get, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "AccountGetFailureRate")
	if nil != err {
		globals.accountMethodChaosFailureRate.get = 0
	}
	globals.accountMethodChaosFailureRate.head, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "AccountHeadFailureRate")
	if nil != err {
		globals.accountMethodChaosFailureRate.head = 0
	}
	globals.accountMethodChaosFailureRate.post, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "AccountPostFailureRate")
	if nil != err {
		globals.accountMethodChaosFailureRate.post = 0
	}
	globals.accountMethodChaosFailureRate.put, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "AccountPutFailureRate")
	if nil != err {
		globals.accountMethodChaosFailureRate.put = 0
	}

	globals.containerMethodChaosFailureRate.delete, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ContainerDeleteFailureRate")
	if nil != err {
		globals.containerMethodChaosFailureRate.delete = 0
	}
	globals.containerMethodChaosFailureRate.get, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ContainerGetFailureRate")
	if nil != err {
		globals.containerMethodChaosFailureRate.get = 0
	}
	globals.containerMethodChaosFailureRate.head, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ContainerHeadFailureRate")
	if nil != err {
		globals.containerMethodChaosFailureRate.head = 0
	}
	globals.containerMethodChaosFailureRate.post, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ContainerPostFailureRate")
	if nil != err {
		globals.containerMethodChaosFailureRate.post = 0
	}
	globals.containerMethodChaosFailureRate.put, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ContainerPutFailureRate")
	if nil != err {
		globals.containerMethodChaosFailureRate.put = 0
	}

	globals.objectMethodChaosFailureRate.delete, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ObjectDeleteFailureRate")
	if nil != err {
		globals.objectMethodChaosFailureRate.delete = 0
	}
	globals.objectMethodChaosFailureRate.get, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ObjectGetFailureRate")
	if nil != err {
		globals.objectMethodChaosFailureRate.get = 0
	}
	globals.objectMethodChaosFailureRate.head, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ObjectHeadFailureRate")
	if nil != err {
		globals.objectMethodChaosFailureRate.head = 0
	}
	globals.objectMethodChaosFailureRate.post, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ObjectPostFailureRate")
	if nil != err {
		globals.objectMethodChaosFailureRate.post = 0
	}
	globals.objectMethodChaosFailureRate.put, err = confMap.FetchOptionValueUint64("RamSwiftChaos", "ObjectPutFailureRate")
	if nil != err {
		globals.objectMethodChaosFailureRate.put = 0
	}
}

func fetchSwiftInfo(confMap conf.ConfMap) {
	var (
		err error
	)

	maxIntAsUint64 := uint64(^uint(0) >> 1)

	globals.maxAccountNameLength, err = confMap.FetchOptionValueUint64("RamSwiftInfo", "MaxAccountNameLength")
	if nil != err {
		log.Fatalf("failed fetch of RamSwiftInfo.MaxAccountNameLength: %v", err)
	}
	if globals.maxAccountNameLength > maxIntAsUint64 {
		log.Fatal("RamSwiftInfo.MaxAccountNameLength too large... must fit in a Go int")
	}
	globals.maxContainerNameLength, err = confMap.FetchOptionValueUint64("RamSwiftInfo", "MaxContainerNameLength")
	if nil != err {
		log.Fatalf("failed fetch of RamSwiftInfo.MaxContainerNameLength: %v", err)
	}
	if globals.maxContainerNameLength > maxIntAsUint64 {
		log.Fatal("RamSwiftInfo.MaxContainerNameLength too large... must fit in a Go int")
	}
	globals.maxObjectNameLength, err = confMap.FetchOptionValueUint64("RamSwiftInfo", "MaxObjectNameLength")
	if nil != err {
		log.Fatalf("failed fetch of RamSwiftInfo.MaxObjectNameLength: %v", err)
	}
	if globals.maxObjectNameLength > maxIntAsUint64 {
		log.Fatal("RamSwiftInfo.MaxObjectNameLength too large... must fit in a Go int")
	}
	globals.accountListingLimit, err = confMap.FetchOptionValueUint64("RamSwiftInfo", "AccountListingLimit")
	if nil != err {
		log.Fatalf("failed fetch of RamSwiftInfo.AccountListingLimit: %v", err)
	}
	if globals.accountListingLimit > maxIntAsUint64 {
		log.Fatal("RamSwiftInfo.AccountListingLimit too large... must fit in a Go int")
	}
	globals.containerListingLimit, err = confMap.FetchOptionValueUint64("RamSwiftInfo", "ContainerListingLimit")
	if nil != err {
		log.Fatalf("failed fetch of RamSwiftInfo.ContainerListingLimit: %v", err)
	}
	if globals.containerListingLimit > maxIntAsUint64 {
		log.Fatal("RamSwiftInfo.ContainerListingLimit too large... must fit in a Go int")
	}
}

func Daemon(confFile string, confStrings []string, signalHandlerIsArmedWG *sync.WaitGroup, doneChan chan bool, signals ...os.Signal) {
	var (
		confMap        conf.ConfMap
		err            error
		resp           *http.Response
		signalChan     chan os.Signal
		signalReceived os.Signal
	)

	// Initialization

	globals = &globalsStruct{}

	globals.swiftAccountMap = make(map[string]*swiftAccountStruct)

	// Compute confMap

	confMap, err = conf.MakeConfMapFromFile(confFile)
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(confStrings)
	if nil != err {
		log.Fatalf("failed to apply config overrides: %v", err)
	}

	// Find out who "we" are

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		log.Fatalf("failed fetch of Cluster.WhoAmI: %v", err)
	}

	globals.noAuthTCPPort, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		log.Fatalf("failed fetch of Swift.NoAuthTCPPort: %v", err)
	}

	globals.noAuthAddr = "127.0.0.1:" + strconv.Itoa(int(globals.noAuthTCPPort))

	// Kick off NoAuth Swift Proxy Emulator

	go serveNoAuthSwift(confMap)

	// Wait for serveNoAuthSwift() to begin serving

	for {
		resp, err = http.Get("http://" + globals.noAuthAddr + "/info")
		if nil != err {
			log.Printf("failed GET of \"/info\": %v", err)
			continue
		}
		if http.StatusOK == resp.StatusCode {
			break
		}
		log.Printf("GET of \"/info\" returned Status %v", resp.Status)
		time.Sleep(100 * time.Millisecond)
	}

	// Arm signal handler used to indicate termination and wait on it
	//
	// Note: signalled chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, signals...)

	if nil != signalHandlerIsArmedWG {
		signalHandlerIsArmedWG.Done()
	}

	// Await a signal - reloading confFile each SIGHUP - exiting otherwise

	for {
		signalReceived = <-signalChan

		if unix.SIGHUP == signalReceived {
			// recompute confMap and re-apply

			confMap, err = conf.MakeConfMapFromFile(confFile)
			if nil != err {
				log.Fatalf("failed to load updated config: %v", err)
			}

			err = confMap.UpdateFromStrings(confStrings)
			if nil != err {
				log.Fatalf("failed to reapply config overrides: %v", err)
			}

			updateConf(confMap)
		} else {
			// signalReceived either SIGINT or SIGTERM... so just exit

			globals = nil

			doneChan <- true

			return
		}
	}
}
