// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"golang.org/x/sys/unix"
)

const (
	// PFSAgentReadyAttemptLimit defines the number of times pfsagentd-init
	// will poll for the successful launching of each child pfsagentd instance
	//
	PFSAgentReadyAttemptLimit = uint32(100)

	// PFSAgentReadyRetryDelay is the delay between polling attempts by
	// pfsagentd-init of each child pfsagentd instance
	//
	PFSAgentReadyRetryDelay = "100ms"
)

type pfsagentdInstanceStruct struct {
	confFileName       string        //
	confMap            conf.ConfMap  //
	httpServerHostPort string        //
	stopChan           chan struct{} // Closed by main() to tell (*pfsagentdInstanceStruct).daemon() to exit
}

type globalsStruct struct {
	parentEntrypoint         []string                   // A non-existent or empty PFSAGENT_PARENT_ENTRYPOINT will result in []string{}
	parentCmd                []string                   // A non-existent or empty PFSAGENT_PARENT_CMD will result in []string{}
	parentWorkingDir         string                     // A non-existent or empty PFSAGENT_PARENT_WORKING_DIR will result in "/"
	pfsagentdReadyRetryDelay time.Duration              //
	pfsagentdInstance        []*pfsagentdInstanceStruct //
	pfsagentdInstanceUpChan  chan struct{}              // Signaled by each pfsagentdInstance once they are up
	childErrorChan           chan struct{}              // Signaled by an unexpectedly exiting child to
	//                                                       indicate that the entire container should exit
	applicationCleanExitChan  chan struct{}  //            Signaled by applicationDaemon() on clean exit
	applicationDaemonStopChan chan struct{}  //            Closed by main() to tell applicationDaemon() to exit
	signalChan                chan os.Signal //            Signaled by OS or Container Framework to trigger exit
	pfsagentdInstanceWG       sync.WaitGroup //
	applicationDaemonWG       sync.WaitGroup //
}

var globals globalsStruct

func main() {
	var (
		err                                 error
		fileInfo                            os.FileInfo
		fileInfoName                        string
		fileInfoSlice                       []os.FileInfo
		parentCmdBuf                        string
		parentEntrypointBuf                 string
		pfsagentdInstance                   *pfsagentdInstanceStruct
		pfsagentdInstanceFUSEMountPointPath string
		pfsagentdInstanceHTTPServerIPAddr   string
		pfsagentdInstanceHTTPServerTCPPort  uint16
		pfsagentdInstanceUpCount            int
	)

	log.Printf("Launch of %s\n", os.Args[0])

	parentEntrypointBuf = os.Getenv("PFSAGENT_PARENT_ENTRYPOINT")
	if (parentEntrypointBuf == "") || (parentEntrypointBuf == "null") {
		globals.parentEntrypoint = make([]string, 0)
	} else {
		globals.parentEntrypoint = make([]string, 0)
		err = json.Unmarshal([]byte(parentEntrypointBuf), &globals.parentEntrypoint)
		if err != nil {
			log.Fatalf("Couldn't parse PFSAGENT_PARENT_ENTRYPOINT: \"%s\"\n", parentEntrypointBuf)
		}
	}

	parentCmdBuf = os.Getenv("PFSAGENT_PARENT_CMD")
	if (parentCmdBuf == "") || (parentCmdBuf == "null") {
		globals.parentCmd = make([]string, 0)
	} else {
		globals.parentCmd = make([]string, 0)
		err = json.Unmarshal([]byte(parentCmdBuf), &globals.parentCmd)
		if err != nil {
			log.Fatalf("Couldn't parse PFSAGENT_PARENT_CMD: \"%s\"\n", parentCmdBuf)
		}
	}

	globals.parentWorkingDir = os.Getenv("PFSAGENT_PARENT_WORKING_DIR")
	if globals.parentWorkingDir == "" {
		globals.parentWorkingDir = "/"
	}

	fileInfoSlice, err = ioutil.ReadDir(".")
	if err != nil {
		log.Fatalf("Couldn't read directory: %v\n", err)
	}

	globals.pfsagentdReadyRetryDelay, err = time.ParseDuration(PFSAgentReadyRetryDelay)
	if err != nil {
		log.Fatalf("Couldn't parse PFSAgentReadyRetryDelay\n")
	}

	globals.pfsagentdInstance = make([]*pfsagentdInstanceStruct, 0)

	for _, fileInfo = range fileInfoSlice {
		fileInfoName = fileInfo.Name()
		if strings.HasPrefix(fileInfoName, "pfsagent.conf_") {
			if fileInfoName != "pfsagent.conf_TEMPLATE" {
				pfsagentdInstance = &pfsagentdInstanceStruct{
					confFileName: fileInfoName,
					stopChan:     make(chan struct{}),
				}
				globals.pfsagentdInstance = append(globals.pfsagentdInstance, pfsagentdInstance)
			}
		}
	}

	globals.pfsagentdInstanceUpChan = make(chan struct{}, len(globals.pfsagentdInstance))
	globals.childErrorChan = make(chan struct{}, len(globals.pfsagentdInstance)+1)

	globals.signalChan = make(chan os.Signal, 1)
	signal.Notify(globals.signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	globals.pfsagentdInstanceWG.Add(len(globals.pfsagentdInstance))

	for _, pfsagentdInstance = range globals.pfsagentdInstance {
		pfsagentdInstance.confMap, err = conf.MakeConfMapFromFile(pfsagentdInstance.confFileName)
		if err != nil {
			log.Fatalf("Unable to parse %s: %v\n", pfsagentdInstance.confFileName, err)
		}

		pfsagentdInstanceFUSEMountPointPath, err = pfsagentdInstance.confMap.FetchOptionValueString("Agent", "FUSEMountPointPath")
		if err != nil {
			log.Fatalf("In %s, parsing [Agent]FUSEMountPointPath failed: %v\n", pfsagentdInstance.confFileName, err)
		}

		err = os.MkdirAll(pfsagentdInstanceFUSEMountPointPath, 0777)
		if err != nil {
			log.Fatalf("For %s, could not create directory path %s: %v\n", pfsagentdInstance.confFileName, pfsagentdInstanceFUSEMountPointPath, err)
		}

		pfsagentdInstanceHTTPServerIPAddr, err = pfsagentdInstance.confMap.FetchOptionValueString("Agent", "HTTPServerIPAddr")
		if err != nil {
			log.Fatalf("In %s, parsing [Agent]HTTPServerIPAddr failed: %v\n", pfsagentdInstance.confFileName, err)
		}
		if pfsagentdInstanceHTTPServerIPAddr == "0.0.0.0" {
			pfsagentdInstanceHTTPServerIPAddr = "127.0.0.1"
		} else if pfsagentdInstanceHTTPServerIPAddr == "::" {
			pfsagentdInstanceHTTPServerIPAddr = "::1"
		}

		pfsagentdInstanceHTTPServerTCPPort, err = pfsagentdInstance.confMap.FetchOptionValueUint16("Agent", "HTTPServerTCPPort")
		if err != nil {
			log.Fatalf("In %s, parsing [Agent]HTTPServerTCPPort failed: %v\n", pfsagentdInstance.confFileName, err)
		}

		pfsagentdInstance.httpServerHostPort = net.JoinHostPort(pfsagentdInstanceHTTPServerIPAddr, strconv.Itoa(int(pfsagentdInstanceHTTPServerTCPPort)))

		go pfsagentdInstance.daemon()
	}

	if len(globals.pfsagentdInstance) > 0 {
		pfsagentdInstanceUpCount = 0

		for {
			select {
			case _ = <-globals.pfsagentdInstanceUpChan:
				pfsagentdInstanceUpCount++
				if pfsagentdInstanceUpCount == len(globals.pfsagentdInstance) {
					goto pfsagentdInstanceUpComplete
				}
			case _ = <-globals.childErrorChan:
				for _, pfsagentdInstance = range globals.pfsagentdInstance {
					close(pfsagentdInstance.stopChan)
				}

				globals.pfsagentdInstanceWG.Wait()

				log.Fatalf("Abnormal exit during PFSAgent Instances creation\n")
			case _ = <-globals.signalChan:
				for _, pfsagentdInstance = range globals.pfsagentdInstance {
					close(pfsagentdInstance.stopChan)
				}

				globals.pfsagentdInstanceWG.Wait()

				log.Printf("Signaled exit during PFSAgent Instances creation\n")
				os.Exit(0)
			}
		}

	pfsagentdInstanceUpComplete:
	}

	globals.applicationCleanExitChan = make(chan struct{}, 1)
	globals.applicationDaemonStopChan = make(chan struct{})

	globals.applicationDaemonWG.Add(1)

	go applicationDaemon()

	for {
		select {
		case _ = <-globals.childErrorChan:
			close(globals.applicationDaemonStopChan)

			globals.applicationDaemonWG.Wait()

			for _, pfsagentdInstance = range globals.pfsagentdInstance {
				close(pfsagentdInstance.stopChan)
			}

			globals.pfsagentdInstanceWG.Wait()

			log.Fatalf("Abnormal exit after PFSAgent Instances creation\n")
		case _ = <-globals.applicationCleanExitChan:
			globals.applicationDaemonWG.Wait()

			for _, pfsagentdInstance = range globals.pfsagentdInstance {
				close(pfsagentdInstance.stopChan)
			}

			globals.pfsagentdInstanceWG.Wait()

			log.Printf("Normal exit do to application clean exit\n")
			os.Exit(0)
		case _ = <-globals.signalChan:
			close(globals.applicationDaemonStopChan)

			globals.applicationDaemonWG.Wait()

			for _, pfsagentdInstance = range globals.pfsagentdInstance {
				close(pfsagentdInstance.stopChan)
			}

			globals.pfsagentdInstanceWG.Wait()

			log.Printf("Signaled exit after PFSAgent Instances creation\n")
			os.Exit(0)
		}
	}
}

func (pfsagentdInstance *pfsagentdInstanceStruct) daemon() {
	var (
		cmd          *exec.Cmd
		cmdExitChan  chan struct{}
		err          error
		getResponse  *http.Response
		readyAttempt uint32
	)

	cmd = exec.Command("pfsagentd", pfsagentdInstance.confFileName)

	err = cmd.Start()
	if err != nil {
		globals.childErrorChan <- struct{}{}
		globals.pfsagentdInstanceWG.Done()
		runtime.Goexit()
	}

	readyAttempt = 1

	for {
		getResponse, err = http.Get("http://" + pfsagentdInstance.httpServerHostPort + "/version")
		if err == nil {
			_, err = ioutil.ReadAll(getResponse.Body)
			if err != nil {
				log.Fatalf("Failure to read GET Response Body: %v\n", err)
			}
			err = getResponse.Body.Close()
			if err != nil {
				log.Fatalf("Failure to close GET Response Body: %v\n", err)
			}

			if getResponse.StatusCode == http.StatusOK {
				break
			}
		}

		readyAttempt++

		if readyAttempt > PFSAgentReadyAttemptLimit {
			log.Fatalf("Ready attempt limit exceeded for pfsagentd instance %s\n", pfsagentdInstance.confFileName)
		}

		time.Sleep(globals.pfsagentdReadyRetryDelay)
	}

	globals.pfsagentdInstanceUpChan <- struct{}{}

	cmdExitChan = make(chan struct{}, 1)
	go watchCmdDaemon(cmd, cmdExitChan)

	for {
		select {
		case _ = <-cmdExitChan:
			globals.childErrorChan <- struct{}{}
			globals.pfsagentdInstanceWG.Done()
			runtime.Goexit()
		case _, _ = <-pfsagentdInstance.stopChan:
			err = cmd.Process.Signal(unix.SIGTERM)
			if err == nil {
				_ = cmd.Wait()
			}
			globals.pfsagentdInstanceWG.Done()
			runtime.Goexit()
		}
	}
}

func applicationDaemon() {
	var (
		cmd         *exec.Cmd
		cmdArgs     []string
		cmdExitChan chan struct{}
		cmdPath     string
		err         error
	)

	cmdArgs = make([]string, 0)

	if len(globals.parentEntrypoint) > 0 {
		cmdPath = globals.parentEntrypoint[0]
		cmdArgs = globals.parentEntrypoint[1:]

		if len(os.Args) > 1 {
			cmdArgs = append(cmdArgs, os.Args[1:]...)
		} else {
			cmdArgs = append(cmdArgs, globals.parentCmd...)
		}
	} else { // len(globals.parentEntrypoint) == 0
		if len(os.Args) > 1 {
			cmdPath = os.Args[1]
			cmdArgs = os.Args[2:]
		} else {
			if len(globals.parentCmd) > 0 {
				cmdPath = globals.parentCmd[0]
				cmdArgs = globals.parentCmd[1:]
			} else {
				cmdPath = "/bin/sh"
				cmdArgs = make([]string, 0)
			}
		}
	}

	cmd = exec.Command(cmdPath, cmdArgs...)

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Start()
	if err != nil {
		globals.childErrorChan <- struct{}{}
		globals.applicationDaemonWG.Done()
		runtime.Goexit()
	}

	cmdExitChan = make(chan struct{}, 1)
	go watchCmdDaemon(cmd, cmdExitChan)

	for {
		select {
		case _ = <-cmdExitChan:
			if cmd.ProcessState.ExitCode() == 0 {
				globals.applicationCleanExitChan <- struct{}{}
			} else {
				globals.childErrorChan <- struct{}{}
			}
			globals.applicationDaemonWG.Done()
			runtime.Goexit()
		case _, _ = <-globals.applicationDaemonStopChan:
			globals.applicationDaemonWG.Done()
			runtime.Goexit()
		}
	}
}

func watchCmdDaemon(cmd *exec.Cmd, cmdExitChan chan struct{}) {
	_ = cmd.Wait()

	cmdExitChan <- struct{}{}
}
