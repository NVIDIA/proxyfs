package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
)

const (
	fileWrite  = "write"
	fileStat   = "stat"
	fileRead   = "read"
	fileDelete = "delete"
)

type workerStruct struct {
	sync.Mutex

	name string

	methodList []string // One or more of:
	//                       fileWrite   http.MethodPut
	//                       fileStat    http.MethodHead
	//                       fileRead    http.MethodGet
	//                       fileDelete  http.MethodDelete

	mountPoint     string // Typically corresponds to swiftAccount
	directory      string // Typically matches swiftContainer
	fileBlockCount uint64 // Typically,
	fileBlockSize  uint64 //   (fileBlockCount * fileBlockSize) == objectSize

	swiftProxyURL               string
	swiftAuthUser               string
	swiftAuthKey                string
	swiftAccount                string // Typically corresponds to mountPoint
	swiftContainer              string // Typically matches directory
	swiftContainerStoragePolicy string
	objectSize                  uint64 // Typically matches (fileBlockCount * fileBlockSize)

	iterations uint64
	numThreads uint64

	methodListIncludesFileMethod  bool
	methodListIncludesSwiftMethod bool

	fileWriteBuffer []byte
	swiftPutBuffer  []byte

	swiftAuthToken                string
	swiftAuthTokenUpdateWaitGroup *sync.WaitGroup
	authorizationsPerformed       uint64

	nextIteration            uint64
	iterationsCompleted      uint64
	priorIterationsCompleted uint64
}

type globalsStruct struct {
	sync.Mutex
	optionDestroy    bool
	optionFormat     bool
	logFile          *os.File
	workersDoneGate  sync.WaitGroup
	workersReadyGate sync.WaitGroup
	workersGoGate    sync.WaitGroup
	liveThreadCount  uint64
	elapsedTime      time.Duration
}

var globals globalsStruct

type workerIntervalValues struct {
	iterationsCompleted uint64
	iterationsDelta     uint64
}

var columnTitles = []string{"ElapsedTime", "Completed", "Delta"}

func main() {
	var (
		args                        []string
		confMap                     conf.ConfMap
		displayInterval             time.Duration
		err                         error
		iterationsCompleted         uint64
		iterationsDelta             uint64
		logHeaderLine               string
		logPath                     string
		method                      string
		methodListIncludesFileWrite bool
		methodListIncludesSwiftPut  bool
		optionString                string
		worker                      *workerStruct
		workerArray                 []*workerStruct
		workerName                  string
		workerList                  []string
		workerSectionName           string
		workersIntervals            []workerIntervalValues
	)

	// Parse arguments

	args = os.Args[1:]

	// Check that os.Args[1] was supplied... it might be a .conf or an option list (followed by a .conf)
	if 0 == len(args) {
		log.Fatalf("No .conf file specified")
	}

	if "-" == args[0][:1] {
		// Peel off option list from args
		optionString = args[0]
		args = args[1:]

		// Check that os.Args[2]-specified (and required) .conf was supplied
		if 0 == len(args) {
			log.Fatalf("No .conf file specified")
		}

		switch optionString {
		case "-F":
			globals.optionFormat = true
			globals.optionDestroy = false
		case "-D":
			globals.optionFormat = false
			globals.optionDestroy = true
		case "-DF":
			globals.optionFormat = true
			globals.optionDestroy = true
		case "-FD":
			globals.optionFormat = true
			globals.optionDestroy = true
		default:
			log.Fatalf("unexpected option supplied: %s", optionString)
		}
	} else {
		globals.optionDestroy = false
		globals.optionFormat = false
	}

	confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Update confMap with any extra os.Args supplied
	err = confMap.UpdateFromStrings(args[1:])
	if nil != err {
		log.Fatalf("Failed to load config overrides: %v", err)
	}

	// Process resultant confMap

	workerList, err = confMap.FetchOptionValueStringSlice("LoadParameters", "WorkerList")
	if nil != err {
		log.Fatalf("Fetching LoadParameters.WorkerList failed: failed: %v", err)
	}
	if 0 == len(workerList) {
		log.Fatalf("LoadParameters.WorkerList must not be empty")
	}
	displayInterval, err = confMap.FetchOptionValueDuration("LoadParameters", "DisplayInterval")
	if nil != err {
		log.Fatalf("Fetching LoadParameters.DisplayInterval failed: %v", err)
	}
	logPath, err = confMap.FetchOptionValueString("LoadParameters", "LogPath")
	if nil != err {
		log.Fatalf("Fetching LoadParameters.LogPath failed: %v", err)
	}

	workerArray = make([]*workerStruct, 0, len(workerList))

	if globals.optionDestroy && !globals.optionFormat {
		// We will be exiting once optionDestroy is complete
	} else {
		globals.logFile, err = os.Create(logPath)
		if nil != err {
			log.Fatalf("os.Create(\"%s\") failed: %v", logPath, err)
		}

		globals.liveThreadCount = 0

		globals.workersGoGate.Add(1)
	}

	for _, workerName = range workerList {
		worker = &workerStruct{name: workerName}

		workerSectionName = "Worker:" + workerName

		worker.methodList, err = confMap.FetchOptionValueStringSlice(workerSectionName, "MethodList")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueStringSlice(\"%s\", \"MethodList\") failed: %v", workerSectionName, err)
		}
		if 0 == len(worker.methodList) {
			log.Fatalf("%v.MethodList must not be empty", workerSectionName)
		}

		worker.methodListIncludesFileMethod = false
		worker.methodListIncludesSwiftMethod = false

		methodListIncludesFileWrite = false
		methodListIncludesSwiftPut = false

		for _, method = range worker.methodList {
			switch method {
			case fileWrite:
				worker.methodListIncludesFileMethod = true
				methodListIncludesFileWrite = true
			case fileStat:
				worker.methodListIncludesFileMethod = true
			case fileRead:
				worker.methodListIncludesFileMethod = true
			case fileDelete:
				worker.methodListIncludesFileMethod = true
			case http.MethodPut:
				worker.methodListIncludesSwiftMethod = true
				methodListIncludesSwiftPut = true
			case http.MethodHead:
				worker.methodListIncludesSwiftMethod = true
			case http.MethodGet:
				worker.methodListIncludesSwiftMethod = true
			case http.MethodDelete:
				worker.methodListIncludesSwiftMethod = true
			default:
				log.Fatalf("Method %s in %s.MethodList not supported", method, workerSectionName)
			}
		}

		if worker.methodListIncludesFileMethod {
			worker.mountPoint, err = confMap.FetchOptionValueString(workerSectionName, "MountPoint")
			if nil != err {
				log.Fatalf("Fetching %s.MountPoint failed: %v", workerSectionName, err)
			}
			worker.directory, err = confMap.FetchOptionValueString(workerSectionName, "Directory")
			if nil != err {
				log.Fatalf("Fetching %s.Directory failed: %v", workerSectionName, err)
			}
			worker.fileBlockCount, err = confMap.FetchOptionValueUint64(workerSectionName, "FileBlockCount")
			if nil != err {
				log.Fatalf("Fetching %s.FileBlockCount failed: %v", workerSectionName, err)
			}
			worker.fileBlockSize, err = confMap.FetchOptionValueUint64(workerSectionName, "FileBlockSize")
			if nil != err {
				log.Fatalf("Fetching %s.FileBlockSize failed: %v", workerSectionName, err)
			}

			if methodListIncludesFileWrite {
				worker.fileWriteBuffer = make([]byte, worker.fileBlockSize)
				rand.Read(worker.fileWriteBuffer)
			}
		}

		if worker.methodListIncludesSwiftMethod {
			worker.swiftProxyURL, err = confMap.FetchOptionValueString(workerSectionName, "SwiftProxyURL")
			if nil != err {
				log.Fatalf("Fetching %s.SwiftProxyURL failed: %v", workerSectionName, err)
			}
			worker.swiftAuthUser, err = confMap.FetchOptionValueString(workerSectionName, "SwiftAuthUser")
			if nil != err {
				log.Fatalf("Fetching %s.SwiftAuthUser failed: %v", workerSectionName, err)
			}
			worker.swiftAuthKey, err = confMap.FetchOptionValueString(workerSectionName, "SwiftAuthKey")
			if nil != err {
				log.Fatalf("Fetching %s.SwiftAuthKey failed: %v", workerSectionName, err)
			}
			worker.swiftAccount, err = confMap.FetchOptionValueString(workerSectionName, "SwiftAccount")
			if nil != err {
				log.Fatalf("Fetching %s.SwiftAccount failed: %v", workerSectionName, err)
			}
			worker.swiftContainer, err = confMap.FetchOptionValueString(workerSectionName, "SwiftContainer")
			if nil != err {
				log.Fatalf("Fetching %s.SwiftContainer failed: %v", workerSectionName, err)
			}
			worker.swiftContainerStoragePolicy, err = confMap.FetchOptionValueString(workerSectionName, "SwiftContainerStoragePolicy")
			if nil != err {
				log.Fatalf("Fetching %s.SwiftContainerStoragePolicy failed: %v", workerSectionName, err)
			}
			worker.objectSize, err = confMap.FetchOptionValueUint64(workerSectionName, "ObjectSize")
			if nil != err {
				log.Fatalf("Fetching %s.ObjectSize failed: %v", workerSectionName, err)
			}

			if methodListIncludesSwiftPut {
				worker.swiftPutBuffer = make([]byte, worker.objectSize)
				rand.Read(worker.swiftPutBuffer)
			}
		}

		worker.iterations, err = confMap.FetchOptionValueUint64(workerSectionName, "Iterations")
		if nil != err {
			log.Fatalf("Fetching %s.Iterations failed: %v", workerSectionName, err)
		}
		worker.numThreads, err = confMap.FetchOptionValueUint64(workerSectionName, "NumThreads")
		if nil != err {
			log.Fatalf("Fetching %s.NumThreads failed: %v", workerSectionName, err)
		}

		workerArray = append(workerArray, worker)

		if globals.optionDestroy && !globals.optionFormat {
			globals.workersDoneGate.Add(1)
		} else {
			globals.workersReadyGate.Add(1)
		}

		go worker.workerLauncher()
	}

	if globals.optionDestroy && !globals.optionFormat {
		// Wait for optionDestroy to be completed and exit
		globals.workersDoneGate.Wait()
		os.Exit(0)
	}

	// Wait for all workers to be ready

	globals.workersReadyGate.Wait()

	// Print column heads

	fmt.Printf(" ElapsedTime      ")
	for _, worker = range workerArray {
		fmt.Printf("     %-20s", worker.name)
	}
	fmt.Println()

	// Log column heads

	logHeaderLine = ""
	for _, worker = range workerArray {
		for range columnTitles {
			if "" == logHeaderLine {
				logHeaderLine = worker.name
			} else {
				logHeaderLine += "," + worker.name
			}
		}
	}
	_, _ = globals.logFile.WriteString(logHeaderLine + "\n")

	logHeaderLine = ""
	for _, worker = range workerArray {
		if "" == logHeaderLine {
			logHeaderLine = strings.Join(columnTitles, ",")
		} else {
			logHeaderLine += "," + strings.Join(columnTitles, ",")
		}
	}
	_, _ = globals.logFile.WriteString(logHeaderLine + "\n")

	// Kick off workers (and their threads)

	globals.workersGoGate.Done()

	// Display ongoing results until globals.liveThreadCount == 0

	globals.elapsedTime = time.Duration(0)

	for {
		time.Sleep(displayInterval)

		globals.elapsedTime += displayInterval

		workersIntervals = make([]workerIntervalValues, 0, len(workerArray))

		for _, worker = range workerArray {
			worker.Lock()
			iterationsCompleted = worker.iterationsCompleted
			iterationsDelta = iterationsCompleted - worker.priorIterationsCompleted
			worker.priorIterationsCompleted = iterationsCompleted
			worker.Unlock()

			workersIntervals = append(workersIntervals, workerIntervalValues{iterationsCompleted, iterationsDelta})
		}

		printPlainTextInterval(workersIntervals)
		logCSVInterval(workersIntervals)

		globals.Lock()
		if 0 == globals.liveThreadCount {
			globals.Unlock()
			// All threads exited, so just exit
			os.Exit(0)
		}
		globals.Unlock()
	}
}

func printPlainTextInterval(workersIntervals []workerIntervalValues) {
	var (
		elapsedTimeString         string
		iterationsCompletedString string
		iterationsDeltaString     string
		workerInterval            workerIntervalValues
	)

	elapsedTimeString = fmt.Sprintf("%v", globals.elapsedTime)

	fmt.Printf("%10s", elapsedTimeString)

	for _, workerInterval = range workersIntervals {
		iterationsCompletedString = fmt.Sprintf("%d", workerInterval.iterationsCompleted)
		iterationsDeltaString = fmt.Sprintf("(+%d)", workerInterval.iterationsDelta)

		fmt.Printf("  %12s %-10s", iterationsCompletedString, iterationsDeltaString)
	}

	fmt.Println()
}

func logCSVInterval(workersIntervals []workerIntervalValues) {
	var (
		csvString             string
		workerInterval        workerIntervalValues
		workerIntervalStrings []string
	)

	for _, workerInterval = range workersIntervals {
		csvString = fmt.Sprintf("%v,%v,%v", globals.elapsedTime.Seconds(), workerInterval.iterationsCompleted, workerInterval.iterationsDelta)
		workerIntervalStrings = append(workerIntervalStrings, csvString)
	}

	_, _ = globals.logFile.WriteString(strings.Join(workerIntervalStrings, ",") + "\n")
}

func (worker *workerStruct) fetchNextIteration() (iteration uint64, allDone bool) {
	worker.Lock()
	if worker.nextIteration < worker.iterations {
		iteration = worker.nextIteration
		worker.nextIteration = iteration + 1
		allDone = false
	} else {
		allDone = true
	}
	worker.Unlock()
	return
}

func (worker *workerStruct) incIterationsCompleted() {
	worker.Lock()
	worker.iterationsCompleted++
	worker.Unlock()
}

func (worker *workerStruct) workerLauncher() {
	var (
		containerDeleteRequest  *http.Request
		containerDeleteResponse *http.Response
		containerGetBody        []byte
		containerGetRequest     *http.Request
		containerGetResponse    *http.Response
		containerPutRequest     *http.Request
		containerPutResponse    *http.Response
		containerURL            string
		err                     error
		httpClient              *http.Client
		objectDeleteRequest     *http.Request
		objectDeleteResponse    *http.Response
		objectList              []string
		objectName              string
		objectURL               string
		path                    string
		swiftAuthToken          string
		threadIndex             uint64
	)

	if worker.methodListIncludesFileMethod {
		path = worker.mountPoint + "/" + worker.directory
	}
	if worker.methodListIncludesSwiftMethod {
		worker.authorizationsPerformed = 0

		worker.updateSwiftAuthToken()

		httpClient = &http.Client{}

		containerURL = fmt.Sprintf("%sv1/%s/%s", worker.swiftProxyURL, worker.swiftAccount, worker.swiftContainer)
	}

	if globals.optionDestroy {
		if worker.methodListIncludesFileMethod {
			err = os.RemoveAll(path)
			if nil != err {
				log.Fatalf("os.RemoveAll(\"%s\") failed: %v", path, err)
			}
		} else { // worker.methodListIncludesSwiftMethod must be true
			// First, empty the containerURL

		optionDestroyLoop:
			for {
				swiftAuthToken = worker.fetchSwiftAuthToken()
				containerGetRequest, err = http.NewRequest("GET", containerURL, nil)
				if nil != err {
					log.Fatalf("http.NewRequest(\"GET\", \"%s\", nil) failed: %v", containerURL, err)
				}
				containerGetRequest.Header.Set("X-Auth-Token", swiftAuthToken)
				containerGetResponse, err = httpClient.Do(containerGetRequest)
				if nil != err {
					log.Fatalf("httpClient.Do(containerGetRequest) failed: %v", err)
				}
				containerGetBody, err = ioutil.ReadAll(containerGetResponse.Body)
				containerGetResponse.Body.Close()
				if nil != err {
					log.Fatalf("ioutil.ReadAll(containerGetResponse.Body) failed: %v", err)
				}

				switch containerGetResponse.StatusCode {
				case http.StatusOK:
					objectList = strings.Split(string(containerGetBody[:]), "\n")

					if 0 == len(objectList) {
						break optionDestroyLoop
					}

					for _, objectName = range objectList {
						objectURL = containerURL + "/" + objectName

					objectDeleteRetryLoop:
						for {
							swiftAuthToken = worker.fetchSwiftAuthToken()
							objectDeleteRequest, err = http.NewRequest("DELETE", objectURL, nil)
							if nil != err {
								log.Fatalf("http.NewRequest(\"DELETE\", \"%s\", nil) failed: %v", objectURL, err)
							}
							objectDeleteRequest.Header.Set("X-Auth-Token", swiftAuthToken)
							objectDeleteResponse, err = httpClient.Do(objectDeleteRequest)
							if nil != err {
								log.Fatalf("httpClient.Do(objectDeleteRequest) failed: %v", err)
							}
							_, err = ioutil.ReadAll(objectDeleteResponse.Body)
							objectDeleteResponse.Body.Close()
							if nil != err {
								log.Fatalf("ioutil.ReadAll(objectDeleteResponse.Body) failed: %v", err)
							}

							switch objectDeleteResponse.StatusCode {
							case http.StatusNoContent:
								break objectDeleteRetryLoop
							case http.StatusNotFound:
								// Object apparently already deleted... Container just doesn't know it yet
								break objectDeleteRetryLoop
							case http.StatusUnauthorized:
								worker.updateSwiftAuthToken()
							default:
								log.Fatalf("DELETE response had unexpected status: %s (%d)", objectDeleteResponse.Status, objectDeleteResponse.StatusCode)
							}
						}
					}
				case http.StatusNoContent:
					break optionDestroyLoop
				case http.StatusNotFound:
					// Container apparently already deleted...
					break optionDestroyLoop
				default:
					log.Fatalf("GET response had unexpected status: %s (%d)", containerGetResponse.Status, containerGetResponse.StatusCode)
				}
			}

			// Now, delete containerURL

		containerDeleteRetryLoop:
			for {
				swiftAuthToken = worker.fetchSwiftAuthToken()
				containerDeleteRequest, err = http.NewRequest("DELETE", containerURL, nil)
				if nil != err {
					log.Fatalf("http.NewRequest(\"DELETE\", \"%s\", nil) failed: %v", containerURL, err)
				}
				containerDeleteRequest.Header.Set("X-Auth-Token", swiftAuthToken)
				containerDeleteResponse, err = httpClient.Do(containerDeleteRequest)
				if nil != err {
					log.Fatalf("httpClient.Do(containerDeleteRequest) failed: %v", err)
				}
				_, err = ioutil.ReadAll(containerDeleteResponse.Body)
				containerDeleteResponse.Body.Close()
				if nil != err {
					log.Fatalf("ioutil.ReadAll(containerDeleteResponse.Body) failed: %v", err)
				}

				switch containerDeleteResponse.StatusCode {
				case http.StatusNoContent:
					break containerDeleteRetryLoop
				case http.StatusNotFound:
					// Container apparently already deleted...
					break containerDeleteRetryLoop
				case http.StatusUnauthorized:
					worker.updateSwiftAuthToken()
				default:
					log.Fatalf("DELETE response had unexpected status: %s (%d)", containerDeleteResponse.Status, containerDeleteResponse.StatusCode)
				}
			}
		}

		if !globals.optionFormat {
			// We are just going to exit if all we were asked to do was optionDestroy
			globals.workersDoneGate.Done()
			return
		}
	}

	if globals.optionFormat {
		if worker.methodListIncludesFileMethod {
			err = os.Mkdir(path, os.ModePerm)
			if nil != err {
				log.Fatalf("os.Mkdir(\"%s\", os.ModePerm) failed: %v", path, err)
			}
		} else { //worker.methodListIncludesSwiftMethod must be true
		containerPutRetryLoop:
			for {
				swiftAuthToken = worker.fetchSwiftAuthToken()
				containerPutRequest, err = http.NewRequest("PUT", containerURL, nil)
				if nil != err {
					log.Fatalf("http.NewRequest(\"PUT\", \"%s\", nil) failed: %v", containerURL, err)
				}
				containerPutRequest.Header.Set("X-Auth-Token", swiftAuthToken)
				containerPutRequest.Header.Set("X-Storage-Policy", worker.swiftContainerStoragePolicy)
				containerPutResponse, err = httpClient.Do(containerPutRequest)
				if nil != err {
					log.Fatalf("httpClient.Do(containerPutRequest) failed: %v", err)
				}
				_, err = ioutil.ReadAll(containerPutResponse.Body)
				containerPutResponse.Body.Close()
				if nil != err {
					log.Fatalf("ioutil.ReadAll(containerPutResponse.Body) failed: %v", err)
				}

				switch containerPutResponse.StatusCode {
				case http.StatusCreated:
					break containerPutRetryLoop
				case http.StatusNoContent:
					worker.updateSwiftAuthToken()
				default:
					log.Fatalf("PUT response had unexpected status: %s (%d)", containerPutResponse.Status, containerPutResponse.StatusCode)
				}
			}
		}
	}

	worker.nextIteration = 0
	worker.iterationsCompleted = 0
	worker.priorIterationsCompleted = 0

	for threadIndex = 0; threadIndex < worker.numThreads; threadIndex++ {
		globals.workersReadyGate.Add(1)

		go worker.workerThreadLauncher()
	}

	globals.workersReadyGate.Done()
}

func (worker *workerStruct) workerThreadLauncher() {
	var (
		abandonedIteration bool
		allDone            bool
		doSwiftMethod      bool
		err                error
		file               *os.File
		fileBlockIndex     uint64
		fileReadAt         uint64
		fileReadBuffer     []byte
		httpClient         *http.Client
		iteration          uint64
		method             string
		methodRequest      *http.Request
		methodResponse     *http.Response
		path               string
		swiftAuthToken     string
		swiftPutReader     *bytes.Reader
		url                string
	)

	globals.Lock()
	globals.liveThreadCount++
	globals.Unlock()

	if worker.methodListIncludesFileMethod {
		fileReadBuffer = make([]byte, worker.fileBlockSize)
	}
	if worker.methodListIncludesSwiftMethod {
		httpClient = &http.Client{}
	}

	globals.workersReadyGate.Done()

	globals.workersGoGate.Wait()

	defer func() {
		globals.Lock()
		globals.liveThreadCount--
		globals.Unlock()
	}()

	iteration, allDone = worker.fetchNextIteration()
	if allDone {
		return
	}

	for {
		if worker.methodListIncludesFileMethod {
			path = fmt.Sprintf("%s/%s/%016X", worker.mountPoint, worker.directory, iteration)
		}
		if worker.methodListIncludesSwiftMethod {
			url = fmt.Sprintf("%sv1/%s/%s/%016X", worker.swiftProxyURL, worker.swiftAccount, worker.swiftContainer, iteration)
		}

		abandonedIteration = false

		for _, method = range worker.methodList {
			if !abandonedIteration {
				switch method {
				case fileWrite:
					doSwiftMethod = false
					file, err = os.Create(path)
					if nil == err {
						fileBlockIndex = 0
						for (nil == err) && (fileBlockIndex < worker.fileBlockCount) {
							_, err = file.Write(worker.fileWriteBuffer)
							fileBlockIndex++
						}
						if nil == err {
							err = file.Close()
							if nil != err {
								abandonedIteration = true
							}
						} else {
							abandonedIteration = true
							_ = file.Close()
						}
					} else {
						abandonedIteration = true
					}
				case fileStat:
					doSwiftMethod = false
					file, err = os.Open(path)
					if nil == err {
						_, err = file.Stat()
						if nil == err {
							err = file.Close()
							if nil != err {
								abandonedIteration = true
							}
						} else {
							abandonedIteration = true
							_ = file.Close()
						}
					} else {
						abandonedIteration = true
					}
				case fileRead:
					doSwiftMethod = false
					file, err = os.Open(path)
					if nil == err {
						fileBlockIndex = 0
						fileReadAt = 0
						for (nil == err) && (fileBlockIndex < worker.fileBlockCount) {
							_, err = file.ReadAt(fileReadBuffer, int64(fileReadAt))
							fileBlockIndex++
							fileReadAt += worker.fileBlockSize
						}
						if nil == err {
							err = file.Close()
							if nil != err {
								abandonedIteration = true
							}
						} else {
							abandonedIteration = true
							_ = file.Close()
						}
					} else {
						abandonedIteration = true
					}
				case fileDelete:
					doSwiftMethod = false
					err = os.Remove(path)
					if nil == err {
						abandonedIteration = true
					}
				case http.MethodPut:
					doSwiftMethod = true
				case http.MethodHead:
					doSwiftMethod = true
				case http.MethodGet:
					doSwiftMethod = true
				case http.MethodDelete:
					doSwiftMethod = true
				default:
					log.Fatalf("Method %s in Worker:%s.MethodList not supported", method, worker.name)
				}

				for doSwiftMethod {
					swiftAuthToken = worker.fetchSwiftAuthToken()

					switch method {
					case http.MethodPut:
						swiftPutReader = bytes.NewReader(worker.swiftPutBuffer)
						methodRequest, err = http.NewRequest("PUT", url, swiftPutReader)
						if nil != err {
							log.Fatalf("http.NewRequest(\"PUT\", \"%s\", swiftPutReader) failed: %v", url, err)
						}
					case http.MethodHead:
						methodRequest, err = http.NewRequest("HEAD", url, nil)
						if nil != err {
							log.Fatalf("http.NewRequest(\"HEAD\", \"%s\", nil) failed: %v", url, err)
						}
					case http.MethodGet:
						methodRequest, err = http.NewRequest("GET", url, nil)
						if nil != err {
							log.Fatalf("http.NewRequest(\"GET\", \"%s\", nil) failed: %v", url, err)
						}
					case http.MethodDelete:
						methodRequest, err = http.NewRequest("DELETE", url, nil)
						if nil != err {
							log.Fatalf("http.NewRequest(\"DELETE\", \"%s\", nil) failed: %v", url, err)
						}
					default:
						log.Fatalf("Method %s in Worker:%s.MethodList not supported", method, worker.name)
					}

					methodRequest.Header.Set("X-Auth-Token", swiftAuthToken)
					methodResponse, err = httpClient.Do(methodRequest)
					if nil != err {
						log.Fatalf("httpClient.Do(methodRequest) failed: %v", err)
					}
					_, err = ioutil.ReadAll(methodResponse.Body)
					methodResponse.Body.Close()
					if nil != err {
						log.Fatalf("ioutil.ReadAll(methodResponse.Body) failed: %v", err)
					}
					switch methodResponse.StatusCode {
					case http.StatusOK:
						doSwiftMethod = false
					case http.StatusNoContent:
						doSwiftMethod = false
					case http.StatusNotFound:
						doSwiftMethod = false
						abandonedIteration = true
					case http.StatusCreated:
						doSwiftMethod = false
					case http.StatusUnauthorized:
						worker.updateSwiftAuthToken()
					default:
						log.Fatalf("%s response for url \"%s\" had unexpected status: %s (%d)", method, url, methodResponse.Status, methodResponse.StatusCode)
					}
				}
			}
		}

		if !abandonedIteration {
			worker.incIterationsCompleted()
		}

		iteration, allDone = worker.fetchNextIteration()
		if allDone {
			return
		}
	}
}

func (worker *workerStruct) fetchSwiftAuthToken() (swiftAuthToken string) {
	var (
		swiftAuthTokenUpdateWaitGroup *sync.WaitGroup
	)

	for {
		worker.Lock()

		swiftAuthTokenUpdateWaitGroup = worker.swiftAuthTokenUpdateWaitGroup

		if nil == swiftAuthTokenUpdateWaitGroup {
			swiftAuthToken = worker.swiftAuthToken
			worker.Unlock()
			return
		}

		worker.Unlock()

		swiftAuthTokenUpdateWaitGroup.Wait()
	}
}

func (worker *workerStruct) updateSwiftAuthToken() {
	var (
		err         error
		getRequest  *http.Request
		getResponse *http.Response
		httpClient  *http.Client
	)

	worker.Lock()

	if nil != worker.swiftAuthTokenUpdateWaitGroup {
		worker.Unlock()

		_ = worker.fetchSwiftAuthToken()

		return
	}

	worker.swiftAuthTokenUpdateWaitGroup = &sync.WaitGroup{}
	worker.swiftAuthTokenUpdateWaitGroup.Add(1)

	worker.Unlock()

	getRequest, err = http.NewRequest("GET", worker.swiftProxyURL+"auth/v1.0", nil)
	if nil != err {
		log.Fatalf("http.NewRequest(\"GET\", \"%sauth/v1.0\", nil) failed: %v", worker.swiftProxyURL, err)
	}

	getRequest.Header.Set("X-Auth-User", worker.swiftAuthUser)
	getRequest.Header.Set("X-Auth-Key", worker.swiftAuthKey)

	httpClient = &http.Client{}

	getResponse, err = httpClient.Do(getRequest)
	if nil != err {
		log.Fatalf("httpClient.Do(getRequest) failed: %v", err)
	}
	_, err = ioutil.ReadAll(getResponse.Body)
	getResponse.Body.Close()
	if nil != err {
		log.Fatalf("ioutil.ReadAll(getResponse.Body) failed: %v", err)
	}

	worker.Lock()

	worker.swiftAuthToken = getResponse.Header.Get("X-Auth-Token")

	worker.authorizationsPerformed++

	worker.swiftAuthTokenUpdateWaitGroup.Done()

	worker.swiftAuthTokenUpdateWaitGroup = nil

	worker.Unlock()
}
