package main

import (
	"bytes"
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

type workerStruct struct {
	sync.Mutex
	name                          string
	directory                     string
	swiftAuthUser                 string
	swiftAuthKey                  string
	swiftAccount                  string
	swiftContainer                string
	swiftContainerStoragePolicy   string
	numThreads                    uint16
	swiftAuthToken                string
	swiftAuthTokenUpdateWaitGroup *sync.WaitGroup
	authorizationsPerformed       uint64
	nextMethodNumberToStart       uint64
	methodsCompleted              uint64
	priorMethodsCompleted         uint64
}

type workerIntervalValues struct {
	methodsCompleted uint64
	methodsDelta     uint64
}

const (
	fileDelete = "delete"
	fileRead   = "read"
	fileStat   = "stat"
	fileWrite  = "write"
)

var (
	displayInterval  time.Duration
	elapsedTime      time.Duration
	fileBlockCount   uint64
	fileBlockSize    uint64
	fileMethod       string // One of fileWrite, fileStat, fileRead, fileDelete
	fileWriteBuffer  []byte
	mountPoint       string
	optionDestroy    bool
	optionFormat     bool
	outputFormat     string
	swiftMethod      string // One of http.MethodGet, http.MethodHead, http.MethodPut, or http.MethodDelete
	swiftObjectSize  uint64
	swiftProxyURL    string
	swiftPutBuffer   []byte
	workerArray      []*workerStruct
	workerMap        map[string]*workerStruct
	workersGoGate    sync.WaitGroup
	workersReadyGate sync.WaitGroup
)

var columnTitles = []string{"ElapsedTime", "Completed", "Delta"}

func main() {
	var (
		args              []string
		confMap           conf.ConfMap
		err               error
		fileWorkerList    []string
		infoResponse      *http.Response
		methodsCompleted  uint64
		methodsDelta      uint64
		optionString      string
		swiftWorkerList   []string
		worker            *workerStruct
		workerName        string
		workerSectionName string
		workersIntervals  []workerIntervalValues
	)

	// Parse arguments

	args = os.Args[1:]

	// Check that os.Args[1] was supplied... it might be a .conf or an option list (followed by a .conf)
	if 0 == len(args) {
		log.Fatalf("no .conf file specified")
	}

	optionDestroy = false
	optionFormat = false

	if "-" == args[0][:1] {
		// Peel off option list from args
		optionString = args[0]
		args = args[1:]

		// Check that os.Args[2]-specified (and required) .conf was supplied
		if 0 == len(args) {
			log.Fatalf("no .conf file specified")
		}

		switch optionString {
		case "-F":
			optionFormat = true
			optionDestroy = false
		case "-D":
			optionFormat = false
			optionDestroy = true
		case "-DF":
			optionFormat = true
			optionDestroy = true
		case "-FD":
			optionFormat = true
			optionDestroy = true
		default:
			log.Fatalf("unexpected option supplied: %s", optionString)
		}
	} else {
		optionDestroy = false
		optionFormat = false
	}

	confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	// Update confMap with any extra os.Args supplied
	err = confMap.UpdateFromStrings(args[1:])
	if nil != err {
		log.Fatalf("failed to load config overrides: %v", err)
	}

	// Process resultant confMap

	fileWorkerList, err = confMap.FetchOptionValueStringSlice("LoadParameters", "FileWorkerList")
	if nil != err {
		log.Fatalf("confMap.FetchOptionValueStringSlice(\"LoadParameters\", \"FileWorkerList\") failed: %v", err)
	}

	if 0 < len(fileWorkerList) {
		fileMethod, err = confMap.FetchOptionValueString("LoadParameters", "FileMethod")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueStringSlice(\"LoadParameters\", \"FileMethod\") failed: %v", err)
		}

		mountPoint, err = confMap.FetchOptionValueString("LoadParameters", "MountPoint")
		if nil != err {
			log.Fatalf("FileMethod %s requires specifying LoadParameters.MountPoint (err: %v)", fileMethod, err)
		}
		fileBlockCount, err = confMap.FetchOptionValueUint64("LoadParameters", "FileBlockCount")
		if nil != err {
			log.Fatalf("FileMethod %s requires specifying LoadParameters.FileBlockCount (err: %v)", fileMethod, err)
		}
		fileBlockSize, err = confMap.FetchOptionValueUint64("LoadParameters", "FileBlockSize")
		if nil != err {
			log.Fatalf("FileMethod %s requires specifying LoadParameters.FileBlockSize (err: %v)", fileMethod, err)
		}

		switch fileMethod {
		case fileWrite:
			fileWriteBuffer = make([]byte, fileBlockSize)
		case fileStat:
		case fileRead:
		case fileDelete:
		default:
			log.Fatalf("FileMethod %s not supported", fileMethod)
		}
	}

	swiftWorkerList, err = confMap.FetchOptionValueStringSlice("LoadParameters", "SwiftWorkerList")
	if nil != err {
		log.Fatalf("confMap.FetchOptionValueStringSlice(\"LoadParameters\", \"SwiftWorkerList\") failed: %v", err)
	}

	if 0 < len(swiftWorkerList) {
		swiftMethod, err = confMap.FetchOptionValueString("LoadParameters", "SwiftMethod")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueStringSlice(\"LoadParameters\", \"SwiftMethod\") failed: %v", err)
		}

		swiftObjectSize, err = confMap.FetchOptionValueUint64("LoadParameters", "SwiftObjectSize")
		if nil != err {
			log.Fatalf("SwiftMethod %s requires specifying LoadParameters.SwiftObjectSize (err: %v)", swiftMethod, err)
		}
		swiftProxyURL, err = confMap.FetchOptionValueString("LoadParameters", "SwiftProxy")
		if nil != err {
			log.Fatalf("SwiftMethod %s requires specifying LoadParameters.SwiftProxy (err: %v)", swiftMethod, err)
		}

		switch swiftMethod {
		case http.MethodPut:
			swiftPutBuffer = make([]byte, swiftObjectSize)
		case http.MethodHead:
		case http.MethodGet:
		case http.MethodDelete:
		default:
			log.Fatalf("SwiftMethod %s not supported", swiftMethod)
		}

		infoResponse, err = http.Get(swiftProxyURL + "info")
		if nil != err {
			log.Fatalf("http.Get(\"%sinfo\") failed: %v", swiftProxyURL, err)
		}
		_, err = ioutil.ReadAll(infoResponse.Body)
		infoResponse.Body.Close()
		if nil != err {
			log.Fatalf("ioutil.ReadAll(infoResponse.Body) failed: %v", err)
		}
	}

	if (0 == len(fileWorkerList)) && (0 == len(swiftWorkerList)) {
		log.Fatalf("FileWorkerList & SwiftWorkerList must not both be empty")
	}

	displayInterval, err = confMap.FetchOptionValueDuration("LoadParameters", "DisplayInterval")
	if nil != err {
		log.Fatalf("confMap.FetchOptionValueDuration(\"LoadParameters\", \"DisplayInterval\") failed: %v", err)
	}

	outputFormat, err = confMap.FetchOptionValueString("LoadParameters", "OutputFormat")
	if nil != err {
		log.Fatalf("confMap.FetchOptionValueString(\"LoadParameters\", \"OutputFormat\") failed: %v", err)
	}

	workerArray = make([]*workerStruct, 0, len(fileWorkerList)+len(swiftWorkerList))
	workerMap = make(map[string]*workerStruct)

	workersGoGate.Add(1)

	for _, workerName = range fileWorkerList {
		worker = &workerStruct{
			name: workerName,
			nextMethodNumberToStart: 0,
			methodsCompleted:        0,
			priorMethodsCompleted:   0,
		}

		workerSectionName = "Worker:" + workerName

		worker.directory, err = confMap.FetchOptionValueString(workerSectionName, "Directory")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueString(\"%s\", \"Directory\") failed: %v", workerSectionName, err)
		}
		worker.numThreads, err = confMap.FetchOptionValueUint16(workerSectionName, "NumThreads")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueUint16(\"%s\", \"NumThreads\") failed: %v", workerSectionName, err)
		}

		workerArray = append(workerArray, worker)
		workerMap[workerName] = worker

		workersReadyGate.Add(1)

		go worker.fileWorkerLauncher()
	}

	for _, workerName = range swiftWorkerList {
		worker = &workerStruct{
			name:                          workerName,
			swiftAuthToken:                "",
			swiftAuthTokenUpdateWaitGroup: nil,
			authorizationsPerformed:       0,
			nextMethodNumberToStart:       0,
			methodsCompleted:              0,
			priorMethodsCompleted:         0,
		}

		workerSectionName = "Worker:" + workerName

		worker.swiftAuthUser, err = confMap.FetchOptionValueString(workerSectionName, "SwiftUser")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueString(\"%s\", \"SwiftUser\") failed: %v", workerSectionName, err)
		}
		worker.swiftAuthKey, err = confMap.FetchOptionValueString(workerSectionName, "SwiftKey")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueString(\"%s\", \"SwiftKey\") failed: %v", workerSectionName, err)
		}
		worker.swiftAccount, err = confMap.FetchOptionValueString(workerSectionName, "SwiftAccount")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueString(\"%s\", \"SwiftAccount\") failed: %v", workerSectionName, err)
		}
		worker.swiftContainer, err = confMap.FetchOptionValueString(workerSectionName, "SwiftContainer")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueString(\"%s\", \"SwiftContainer\") failed: %v", workerSectionName, err)
		}
		worker.numThreads, err = confMap.FetchOptionValueUint16(workerSectionName, "NumThreads")
		if nil != err {
			log.Fatalf("confMap.FetchOptionValueUint16(\"%s\", \"NumThreads\") failed: %v", workerSectionName, err)
		}

		if optionFormat {
			worker.swiftContainerStoragePolicy, err = confMap.FetchOptionValueString(workerSectionName, "SwiftContainerStoragePolicy")
			if nil != err {
				log.Fatalf("confMap.FetchOptionValueString(\"%s\", \"SwiftContainerStoragePolicy\") failed: %v", workerSectionName, err)
			}
		}

		workerArray = append(workerArray, worker)
		workerMap[workerName] = worker

		workersReadyGate.Add(1)

		go worker.swiftWorkerLauncher()
	}

	workersReadyGate.Wait()

	if optionDestroy && !optionFormat {
		// We are just going to exit if all we were asked to do was optionDestroy

		os.Exit(0)
	}

	// Print workerName's as column heads

	switch outputFormat {
	case "text":
		printPlainTextWorkerNames()
	case "csv":
		printCSVWorkerNames()
		printCSVColumnTitles()
	default:
		log.Fatalf("OutputFormat %s not supported", outputFormat)
	}

	// Workers (or, rather, all their Threads) are ready to go... so kick off the test

	workersGoGate.Done()

	elapsedTime = time.Duration(0)

	for {
		time.Sleep(displayInterval)
		elapsedTime += displayInterval
		workersIntervals = nil

		for _, worker = range workerArray {
			worker.Lock()
			methodsCompleted = worker.methodsCompleted
			methodsDelta = methodsCompleted - worker.priorMethodsCompleted
			worker.priorMethodsCompleted = methodsCompleted
			worker.Unlock()

			workersIntervals = append(workersIntervals, workerIntervalValues{methodsCompleted, methodsDelta})
		}

		if "text" == outputFormat {
			printPlainTextInterval(workersIntervals)
		} else { // "csv" == outputFormat
			printCSVInterval(workersIntervals)
		}
	}
}

func printPlainTextWorkerNames() {
	var (
		worker *workerStruct
	)

	fmt.Printf(" ElapsedTime      ")

	for _, worker = range workerArray {
		fmt.Printf("     %-20s", worker.name)
	}

	fmt.Println()
}

func printCSVWorkerNames() {
	var (
		firstRowValues []string
		worker         *workerStruct
	)

	for _, worker = range workerArray {
		for range columnTitles {
			firstRowValues = append(firstRowValues, worker.name)
		}
	}

	fmt.Println(strings.Join(firstRowValues, ","))
}

func printCSVColumnTitles() {
	var (
		i int
	)

	fmt.Printf(strings.Join(columnTitles, ","))

	for i = 1; i < len(workerArray); i++ {
		fmt.Printf(",%v", strings.Join(columnTitles, ","))
	}

	fmt.Println()
}

func printPlainTextInterval(workersIntervals []workerIntervalValues) {
	var (
		elapsedTimeString      string
		methodsCompletedString string
		methodsDeltaString     string
		workerInterval         workerIntervalValues
	)

	elapsedTimeString = fmt.Sprintf("%v", elapsedTime)

	fmt.Printf("%10s", elapsedTimeString)

	for _, workerInterval = range workersIntervals {
		methodsCompletedString = fmt.Sprintf("%d", workerInterval.methodsCompleted)
		methodsDeltaString = fmt.Sprintf("(+%d)", workerInterval.methodsDelta)

		fmt.Printf("  %12s %-10s", methodsCompletedString, methodsDeltaString)
	}

	fmt.Println()
}

func printCSVInterval(workersIntervals []workerIntervalValues) {
	var (
		workerInterval        workerIntervalValues
		workerIntervalStrings []string
	)

	for _, workerInterval = range workersIntervals {
		workerIntervalStrings = append(workerIntervalStrings, workerInterval.toCSV())
	}

	fmt.Println(strings.Join(workerIntervalStrings, ","))
}

func (workerInterval workerIntervalValues) toCSV() (csvString string) {
	csvString = fmt.Sprintf("%v,%v,%v", elapsedTime.Seconds(), workerInterval.methodsCompleted, workerInterval.methodsDelta)

	return
}

func (worker *workerStruct) fileWorkerLauncher() {
	var (
		err         error
		path        string
		threadIndex uint16
	)

	path = mountPoint + "/" + worker.directory

	if optionDestroy {
		err = os.RemoveAll(path)
		if nil != err {
			log.Fatalf("os.RemoveAll(\"%s\") failed: %v", path, err)
		}

		if !optionFormat {
			// We are just going to exit if all we were asked to do was optionDestroy
			workersReadyGate.Done()
			return
		}
	}

	if optionFormat {
		err = os.Mkdir(path, os.ModePerm)
		if nil != err {
			log.Fatalf("os.Mkdir(\"%s\", os.ModePerm) failed: %v", path, err)
		}
	}

	for threadIndex = 0; threadIndex < worker.numThreads; threadIndex++ {
		workersReadyGate.Add(1)

		go worker.fileWorkerThreadLauncher()
	}

	workersReadyGate.Done()

	workersGoGate.Wait()
}

func (worker *workerStruct) fileWorkerThreadLauncher() {
	var (
		err                 error
		file                *os.File
		fileBlockIndex      uint64
		fileReadAt          uint64
		fileReadBuffer      []byte
		methodNumberToStart uint64
		path                string
	)

	fileReadBuffer = make([]byte, fileBlockSize)

	workersReadyGate.Done()

	workersGoGate.Wait()

	methodNumberToStart = worker.fetchNextMethodNumberToStart()

	for {
		path = fmt.Sprintf("%s/%s/%016X", mountPoint, worker.directory, methodNumberToStart)

		switch fileMethod {
		case fileWrite:
			file, err = os.Create(path)
			if nil == err {
				fileBlockIndex = uint64(0)
				for (nil == err) && (fileBlockIndex < fileBlockCount) {
					_, err = file.Write(fileWriteBuffer)
					fileBlockIndex++
				}
				if nil == err {
					err = file.Close()
					if nil == err {
						worker.incMethodsCompleted()
					}
				} else {
					_ = file.Close()
				}
			}
		case fileStat:
			file, err = os.Open(path)
			if nil == err {
				_, err = file.Stat()
				if nil == err {
					err = file.Close()
					if nil == err {
						worker.incMethodsCompleted()
					}
				} else {
					_ = file.Close()
				}
			}
		case fileRead:
			file, err = os.Open(path)
			if nil == err {
				fileBlockIndex = uint64(0)
				fileReadAt = uint64(0)
				for (nil == err) && (fileBlockIndex < fileBlockCount) {
					_, err = file.ReadAt(fileReadBuffer, int64(fileReadAt))
					fileBlockIndex++
					fileReadAt += fileBlockSize
				}
				if nil == err {
					err = file.Close()
					if nil == err {
						worker.incMethodsCompleted()
					}
				} else {
					_ = file.Close()
				}
			}
		case fileDelete:
			err = os.Remove(path)
			if nil == err {
				worker.incMethodsCompleted()
			}
		default:
			log.Fatalf("FileMethod %s not supported", fileMethod)
		}

		methodNumberToStart = worker.fetchNextMethodNumberToStart()
	}
}

func (worker *workerStruct) swiftWorkerLauncher() {
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
		swiftAuthToken          string
		threadIndex             uint16
	)

	worker.updateSwiftAuthToken()

	httpClient = &http.Client{}

	containerURL = fmt.Sprintf("%sv1/%s/%s", swiftProxyURL, worker.swiftAccount, worker.swiftContainer)

	if optionDestroy {
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

		if !optionFormat {
			// We are just going to exit if all we were asked to do was optionDestroy
			workersReadyGate.Done()
			return
		}
	}

	if optionFormat {
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

	for threadIndex = 0; threadIndex < worker.numThreads; threadIndex++ {
		workersReadyGate.Add(1)

		go worker.swiftWorkerThreadLauncher()
	}

	workersReadyGate.Done()

	workersGoGate.Wait()
}

func (worker *workerStruct) swiftWorkerThreadLauncher() {
	var (
		err                 error
		httpClient          *http.Client
		methodNumberToStart uint64
		methodRequest       *http.Request
		methodResponse      *http.Response
		swiftAuthToken      string
		swiftPutReader      *bytes.Reader
		url                 string
	)

	httpClient = &http.Client{}

	workersReadyGate.Done()

	workersGoGate.Wait()

	methodNumberToStart = worker.fetchNextMethodNumberToStart()

	for {
		url = fmt.Sprintf("%sv1/%s/%s/%016X", swiftProxyURL, worker.swiftAccount, worker.swiftContainer, methodNumberToStart)

		swiftAuthToken = worker.fetchSwiftAuthToken()

		switch swiftMethod {
		case http.MethodPut:
			swiftPutReader = bytes.NewReader(swiftPutBuffer)
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
			log.Fatalf("SwiftMethod %s not supported", swiftMethod)
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
			worker.incMethodsCompleted()
			methodNumberToStart = worker.fetchNextMethodNumberToStart()
		case http.StatusNoContent:
			worker.incMethodsCompleted()
			methodNumberToStart = worker.fetchNextMethodNumberToStart()
		case http.StatusNotFound:
			methodNumberToStart = worker.fetchNextMethodNumberToStart()
		case http.StatusCreated:
			worker.incMethodsCompleted()
			methodNumberToStart = worker.fetchNextMethodNumberToStart()
		case http.StatusUnauthorized:
			worker.updateSwiftAuthToken()
		default:
			log.Fatalf("%s response for url \"%s\" had unexpected status: %s (%d)", swiftMethod, url, methodResponse.Status, methodResponse.StatusCode)
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

	getRequest, err = http.NewRequest("GET", swiftProxyURL+"auth/v1.0", nil)
	if nil != err {
		log.Fatalf("http.NewRequest(\"GET\", \"%sauth/v1.0\", nil) failed: %v", swiftProxyURL, err)
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

func (worker *workerStruct) fetchNextMethodNumberToStart() (methodNumberToStart uint64) {
	worker.Lock()
	methodNumberToStart = worker.nextMethodNumberToStart
	worker.nextMethodNumberToStart = methodNumberToStart + 1
	worker.Unlock()
	return
}

func (worker *workerStruct) incMethodsCompleted() {
	worker.Lock()
	worker.methodsCompleted++
	worker.Unlock()
}
