// Package ramswift provides an in-memory emulation of the Swift object storage
// API, which can be run as a goroutine from another package, or as a standalone
// binary.
package ramswift

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/conf"
	"github.com/swiftstack/sortedmap"

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

type globalsStruct struct {
	sync.Mutex             // protects globalsStruct.swiftAccountMap
	whoAmI                 string
	noAuthTCPPort          uint16
	swiftAccountMap        map[string]*swiftAccountStruct // key is swiftAccountStruct.name, value is *swiftAccountStruct
	maxAccountNameLength   uint64
	maxContainerNameLength uint64
	maxObjectNameLength    uint64
}

var globals = globalsStruct{swiftAccountMap: make(map[string]*swiftAccountStruct)}

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

// Take an HTTP Range header (e.g. bytes=100-200) and return its start and end indices (e.g. 100 and 200).
//
// Does not handle multiple ranges (e.g. bytes=1000-2000,8000-8999). Returns an error on malformed header despite RFC
// 7233 saying to ignore them; this is because ramswift is only ever used with ProxyFS generating requests, and ProxyFS
// should never generate a malformed Range header.
func parseRangeHeader(request *http.Request) (rangeHeaderPresent bool, startOffset int64, stopOffset int64, err error) {
	var off int
	rangeHeaderValue := request.Header.Get("Range")
	if "" == rangeHeaderValue {
		rangeHeaderPresent = false
		err = nil
	} else {
		rangeHeaderPresent = true
		if strings.HasPrefix(rangeHeaderValue, "bytes=") {
			rangeHeaderValueSuffix := rangeHeaderValue[len("bytes="):]
			rangeSlice := strings.SplitN(rangeHeaderValueSuffix, "-", 2)
			if 2 == len(rangeSlice) {
				if "" == rangeSlice[0] {
					startOffset = int64(-1)
				} else {
					off, err = strconv.Atoi(rangeSlice[0])
					if err != nil {
						return
					}
					startOffset = int64(off)
				}

				if "" == rangeSlice[1] {
					stopOffset = int64(-1)
				} else {
					off, err = strconv.Atoi(rangeSlice[1])
					if err != nil {
						return
					}
					stopOffset = int64(off)
				}

				if startOffset < 0 && stopOffset < 0 {
					err = fmt.Errorf("rangeHeaderValue (%v) malformed", rangeHeaderValue)
				}
			} else {
				err = fmt.Errorf("rangeHeaderValue (%v) malformed", rangeHeaderValue)
			}
		} else {
			err = fmt.Errorf("rangeHeaderValue (%v) does not start with expected \"bytes=\"", rangeHeaderValue)
		}
	}
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

func deleteSwiftAccount(swiftAccountName string) (errno syscall.Errno) {
	globals.Lock()
	swiftAccount, ok := globals.swiftAccountMap[swiftAccountName]
	if ok {
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
			errno := deleteSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				responseWriter.WriteHeader(http.StatusNoContent)
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusNotFound)
			case unix.ENOTEMPTY:
				responseWriter.WriteHeader(http.StatusConflict)
			default:
				err := fmt.Errorf("deleteSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		} else {
			// DELETE SwiftContainer or SwiftObject
			swiftAccount, errno := locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftObjectName {
					// DELETE SwiftContainer
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
				} else {
					// DELETE SwiftObject
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
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"max_object_name_length\": " + strconv.Itoa(int(globals.maxObjectNameLength))))
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
						for containerIndex := 0; containerIndex < numContainers; containerIndex++ {
							swiftContainerNameAsKey, _, _, err := swiftAccount.swiftContainerTree.GetByIndex(containerIndex)
							if nil != err {
								panic(err)
							}
							swiftContainerName := swiftContainerNameAsKey.(string)
							_, _ = responseWriter.Write(utils.StringToByteSlice(swiftContainerName))
							_, _ = responseWriter.Write([]byte{'\n'})
						}
					}
					swiftAccount.Unlock()
				} else {
					// GET SwiftContainer or SwiftObject
					swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
					switch errno {
					case 0:
						if "" == swiftObjectName {
							// GET SwiftContainer
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
								for objectIndex := 0; objectIndex < numObjects; objectIndex++ {
									swiftObjectNameAsKey, _, _, err := swiftContainer.swiftObjectTree.GetByIndex(objectIndex)
									if nil != err {
										panic(err)
									}
									swiftObjectName := swiftObjectNameAsKey.(string)
									_, _ = responseWriter.Write(utils.StringToByteSlice(swiftObjectName))
									_, _ = responseWriter.Write([]byte{'\n'})
								}
							}
							swiftContainer.Unlock()
						} else {
							// GET SwiftObject
							swiftObject, errno := locateSwiftObject(swiftContainer, swiftObjectName)
							switch errno {
							case 0:
								swiftObject.Lock()
								rangeHeaderPresent, startOffset, stopOffset, err := parseRangeHeader(request)
								if nil == err {
									if rangeHeaderPresent {
										// A negative offset indicates it was absent from the HTTP Range header in the
										// request. At least one offset is present.
										if startOffset < 0 {
											// e.g. bytes=-100
											startOffset = int64(len(swiftObject.contents)) - stopOffset
											if startOffset < 0 {
												// happens if you ask for the last N+K bytes of an N-byte file
												startOffset = 0
											}
											stopOffset = int64(len(swiftObject.contents) - 1)
										} else if stopOffset < 0 {
											// e.g. bytes=100-
											stopOffset = int64(len(swiftObject.contents) - 1)
										} else {
											// TODO: we'll probably want to handle ranges that are off the end of the
											// object with a 416 response, like if someone asks for "bytes=100-200" of a
											// 50-byte object. We haven't needed it yet, though.
											if stopOffset > int64(len(swiftObject.contents)-1) {
												stopOffset = int64(len(swiftObject.contents) - 1)
											}
										}

										responseWriter.Header().Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startOffset, stopOffset, len(swiftObject.contents)))
										responseWriter.WriteHeader(http.StatusPartialContent)
										_, _ = responseWriter.Write(swiftObject.contents[startOffset:(stopOffset + 1)])

									} else {
										_, _ = responseWriter.Write(swiftObject.contents)
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
				swiftAccount.Lock()
				for headerName, headerValueSlice := range swiftAccount.headers {
					for _, headerValue := range headerValueSlice {
						responseWriter.Header().Add(headerName, headerValue)
					}
				}
				swiftAccount.Unlock()
				responseWriter.WriteHeader(http.StatusNoContent)
			} else {
				// HEAD SwiftContainer or SwiftObject
				swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
				switch errno {
				case 0:
					if "" == swiftObjectName {
						// HEAD SwiftContainer
						swiftContainer.Lock()
						for headerName, headerValueSlice := range swiftContainer.headers {
							for _, headerValue := range headerValueSlice {
								responseWriter.Header().Add(headerName, headerValue)
							}
						}
						swiftContainer.Unlock()
						responseWriter.WriteHeader(http.StatusNoContent)
					} else {
						// HEAD SwiftObject
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
			} else {
				// POST SwiftContainer or SwiftObject
				swiftContainer, errno := locateSwiftContainer(swiftAccount, swiftContainerName)
				switch errno {
				case 0:
					if "" == swiftObjectName {
						// POST SwiftContainer
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
					} else {
						// POST SwiftObject
						responseWriter.WriteHeader(http.StatusForbidden)
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
		} else {
			// PUT SwiftContainer or SwiftObject
			swiftAccount, errno := locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftObjectName {
					// PUT SwiftContainer
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
				} else {
					// PUT SwiftObject
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
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusForbidden)
			default:
				err := fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		}
	}
}

type httpRequestHandler struct{}

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
		err              error
		errno            syscall.Errno
		primaryPeer      string
		swiftAccountName string
		volumeList       []string
	)

	// Find out who "we" are

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		log.Fatalf("failed fetch of Cluster.WhoAmI: %v", err)
	}

	globals.noAuthTCPPort, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		log.Fatalf("failed fetch of Swift.NoAuthTCPPort: %v", err)
	}

	// Fetch and configure volumes for which "we" are the PrimaryPeer

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		log.Fatalf("failed fetch of FSGlobals.VolumeList: %v", err)
	}

	for _, volumeSectionName := range volumeList {
		primaryPeer, err = confMap.FetchOptionValueString(volumeSectionName, "PrimaryPeer")
		if nil != err {
			log.Fatalf("failed fetch of %v.PrimaryPeer: %v", volumeSectionName, err)
		}
		if 0 != strings.Compare(globals.whoAmI, primaryPeer) {
			continue
		}
		swiftAccountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
		if nil != err {
			log.Fatalf("failed fetch of %v.AccountName: %v", volumeSectionName, err)
		}
		_, errno = createSwiftAccount(swiftAccountName)
		if 0 != errno {
			log.Fatalf("failed create of %v: %v", swiftAccountName, err)
		}
	}

	// Fetch responses for GETs on /info

	fetchSwiftInfo(confMap)

	// Launch HTTP Server on the requested noAuthTCPPort

	http.ListenAndServe("127.0.0.1:"+strconv.Itoa(int(globals.noAuthTCPPort)), httpRequestHandler{})
}

func updateConf(confMap conf.ConfMap) {
	var (
		err error
		//errno                      syscall.Errno
		noAuthTCPPortUpdate         uint16
		primaryPeer                 string
		swiftAccountNameListCurrent []string        // element == swiftAccountName
		swiftAccountNameListUpdate  map[string]bool // key     == swiftAccountName; value is ignored
		swiftAccountName            string
		volumeListUpdate            []string
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

	for _, volumeSectionName := range volumeListUpdate {
		primaryPeer, err = confMap.FetchOptionValueString(volumeSectionName, "PrimaryPeer")
		if nil != err {
			log.Fatalf("failed fetch of %v.PrimaryPeer: %v", volumeSectionName, err)
		}
		if 0 != strings.Compare(globals.whoAmI, primaryPeer) {
			continue
		}
		swiftAccountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
		if nil != err {
			log.Fatalf("failed fetch of %v.AccountName: %v", volumeSectionName, err)
		}
		swiftAccountNameListUpdate[swiftAccountName] = true
	}

	// Delete accounts not found in accountListUpdate

	for _, swiftAccountName = range swiftAccountNameListCurrent {
		_ = deleteSwiftAccount(swiftAccountName)
	}

	// Add accounts in accountListUpdate not found in globals.swiftAccountMap

	for swiftAccountName = range swiftAccountNameListUpdate {
		_, _ = createOrLocateSwiftAccount(swiftAccountName)
	}

	// Fetch (potentially updated) responses for GETs on /info

	fetchSwiftInfo(confMap)
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
}

func Daemon(confFile string, confStrings []string, signalHandlerIsArmed *bool, doneChan chan bool) {
	var (
		confMap        conf.ConfMap
		err            error
		signalChan     chan os.Signal
		signalReceived os.Signal
	)

	// Compute confMap

	confMap, err = conf.MakeConfMapFromFile(confFile)
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(confStrings)
	if nil != err {
		log.Fatalf("failed to apply config overrides: %v", err)
	}

	// Kick off NoAuth Swift Proxy Emulator

	go serveNoAuthSwift(confMap)

	// Arm signal handler used to indicate termination and wait on it
	//
	// Note: signalled chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	if nil != signalHandlerIsArmed {
		*signalHandlerIsArmed = true
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

			doneChan <- true

			return
		}
	}
}
