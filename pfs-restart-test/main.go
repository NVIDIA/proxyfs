// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"golang.org/x/sys/unix"
)

const (
	compareChunkSize    = 65536
	dstDirPerm          = os.FileMode(0700)
	dstFilePerm         = os.FileMode(0600)
	startupPollingDelay = 100 * time.Millisecond
)

type argsStruct struct {
	Volume        string
	UseRAMSwift   bool
	NumLoops      uint64
	NumCopies     uint64
	NumOverwrites uint64
	SkipWrites    bool
	DirPath       string
}

var (
	args                             argsStruct
	volumeGroupOfVolume              string
	checkpointInterval               time.Duration
	dirIndexAdvancePerNonSkippedLoop uint64
	fuseMountPointName               string
	proxyfsdCmd                      *exec.Cmd
	proxyfsdVersionURL               string
	ramswiftCmd                      *exec.Cmd
	testConfFileName                 string
)

func main() {
	var (
		loopIndex uint64
	)

	setup()

	dirIndexAdvancePerNonSkippedLoop = args.NumCopies - args.NumOverwrites

	for loopIndex = 0; loopIndex < args.NumLoops; loopIndex++ {
		log.Printf("Loop # %d\n", loopIndex)
		doMount()
		if 0 < loopIndex {
			doCompares(loopIndex)
			if (0 != dirIndexAdvancePerNonSkippedLoop) && (!args.SkipWrites || (0 == loopIndex%2)) {
				doDeletions(loopIndex)
			}
		}
		if !args.SkipWrites || (0 == loopIndex%2) {
			doWrites(loopIndex)
		}
		doUnmount()
	}

	if !args.SkipWrites || (0 != args.NumLoops%2) {
		log.Printf("Final Compare\n")
		doMount()
		doCompares(args.NumLoops)
		doUnmount()
	}

	teardown()
}

func setup() {
	var (
		confFile                string
		confOverride            string
		confOverrides           []string
		err                     error
		getInfoResponse         *http.Response
		getInfoURL              string
		mkproxyfsCmd            *exec.Cmd
		noAuthTCPPort           uint16
		primaryPeerSlice        []string
		proxyfsdPrivateIPAddr   string
		proxyfsdTCPPortAsString string
		proxyfsdTCPPortAsUint16 uint16
		testConfFile            *os.File
		testConfMap             conf.ConfMap
		volumeGroupList         []string
		volumeGroupListElement  string
		volumeGroup             string
		volumeList              []string
		volumeListElement       string
		whoAmI                  string
	)

	// Parse arguments

	if 2 > len(os.Args) {
		fmt.Printf("%v <confFile> [<confOverride>]*\n", os.Args[0])
		os.Exit(1)
	}

	confFile = os.Args[1]
	confOverrides = os.Args[2:]

	testConfMap, err = conf.MakeConfMapFromFile(confFile)
	if nil != err {
		log.Fatalf("confFile (\"%s\") not parseable: %v", confFile, err)
	}

	for _, confOverride = range confOverrides {
		err = testConfMap.UpdateFromString(confOverride)
		if nil != err {
			log.Fatalf("confOverride (\"%s\") not parseable: %v", confOverride, err)
		}
	}

	args.Volume, err = testConfMap.FetchOptionValueString("RestartTest", "Volume")
	if nil != err {
		log.Fatalf("unable to fetch RestartTest.Volume: %v", err)
	}

	args.UseRAMSwift, err = testConfMap.FetchOptionValueBool("RestartTest", "UseRAMSwift")
	if nil != err {
		log.Fatalf("unable to fetch RestartTest.UseRAMSwift: %v", err)
	}

	args.NumLoops, err = testConfMap.FetchOptionValueUint64("RestartTest", "NumLoops")
	if nil != err {
		log.Fatalf("unable to fetch RestartTest.NumLoops: %v", err)
	}
	if 0 == args.NumLoops {
		log.Fatalf("RestartTest.NumLoops (%v) not valid", args.NumLoops)
	}

	args.NumCopies, err = testConfMap.FetchOptionValueUint64("RestartTest", "NumCopies")
	if nil != err {
		log.Fatalf("unable to fetch RestartTest.NumCopies: %v", err)
	}
	if 0 == args.NumCopies {
		log.Fatalf("RestartTest.NumCopies (%v) not valid", args.NumCopies)
	}

	args.NumOverwrites, err = testConfMap.FetchOptionValueUint64("RestartTest", "NumOverwrites")
	if nil != err {
		log.Fatalf("unable to fetch RestartTest.NumOverwrites: %v", err)
	}
	if args.NumCopies < args.NumOverwrites {
		log.Fatalf("RestartTest.NumOverwrites (%v) must be <= RestartTest.NumCopies (%v)", args.NumOverwrites, args.NumCopies)
	}

	args.SkipWrites, err = testConfMap.FetchOptionValueBool("RestartTest", "SkipWrites")
	if nil != err {
		log.Fatalf("unable to fetch RestartTest.SkipWrites: %v", err)
	}

	args.DirPath, err = testConfMap.FetchOptionValueString("RestartTest", "DirPath")
	if nil != err {
		log.Fatalf("unable to fetch RestartTest.DirPath: %v", err)
	}

	// Fetch (RAMSwift &) ProxyFS values from testConfMap and validate Volume is served by this Peer

	whoAmI, err = testConfMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		log.Fatalf("unable to fetch Cluster.WhoAmI: %v", err)
	}

	volumeGroupList, err = testConfMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != err {
		log.Fatalf("unable to fetch FSGlobals.VolumeGroupList: %v", err)
	}

	for _, volumeGroupListElement = range volumeGroupList {
		primaryPeerSlice, err = testConfMap.FetchOptionValueStringSlice("VolumeGroup:"+volumeGroupListElement, "PrimaryPeer")
		if nil != err {
			log.Fatalf("unable to fetch VolumeGroup:%s.PrimaryPeer: %v", volumeGroupListElement, err)
		}
		if (1 == len(primaryPeerSlice)) && (whoAmI == primaryPeerSlice[0]) {
			volumeList, err = testConfMap.FetchOptionValueStringSlice("VolumeGroup:"+volumeGroupListElement, "VolumeList")
			if nil != err {
				log.Fatalf("unable to fetch VolumeGroup:%s.VolumeList: %v", volumeGroupListElement, err)
			}
			for _, volumeListElement = range volumeList {
				if volumeListElement == args.Volume {
					volumeGroup = volumeGroupListElement
					goto FoundVolume
				}
			}
		}
	}

	log.Fatalf("unable to find Volume (\"%s\") in list of served Volumes for this peer (\"%s\")", args.Volume, whoAmI)

FoundVolume:

	proxyfsdPrivateIPAddr, err = testConfMap.FetchOptionValueString("Peer:"+whoAmI, "PrivateIPAddr")
	if nil != err {
		log.Fatalf("unable to fetch Peer:%s.PrivateIPAddr: %v", whoAmI, err)
	}
	proxyfsdTCPPortAsUint16, err = testConfMap.FetchOptionValueUint16("HTTPServer", "TCPPort")
	if nil != err {
		log.Fatalf("unable to fetch HTTPServer.TCPPort: %v", err)
	}
	proxyfsdTCPPortAsString = fmt.Sprintf("%d", proxyfsdTCPPortAsUint16)
	proxyfsdVersionURL = "http://" + net.JoinHostPort(proxyfsdPrivateIPAddr, proxyfsdTCPPortAsString) + "/version"

	testConfMap["FSGlobals"]["VolumeGroupList"] = []string{volumeGroup}
	testConfMap["VolumeGroup:"+volumeGroup]["VolumeList"] = []string{args.Volume}

	fuseMountPointName, err = testConfMap.FetchOptionValueString("Volume:"+args.Volume, "FUSEMountPointName")
	if nil != err {
		log.Fatalf("unable to fetch Volume:%s.FUSEMountPointName: %v", args.Volume, err)
	}

	noAuthTCPPort, err = testConfMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		log.Fatalf("unable to fetch SwiftClient.NoAuthTCPPort: %v", err)
	}

	// Construct ConfFile to pass to ramswift, mkproxyfs, and proxyfsd

	testConfFile, err = ioutil.TempFile("", "pfs-fsck-*.conf")
	if nil != err {
		log.Fatalf("unable to create testConfFile: %v", err)
	}
	testConfFileName = testConfFile.Name()
	_, err = testConfFile.Write([]byte(testConfMap.Dump()))
	if nil != err {
		log.Fatalf("unable to populate testConfFile (\"%s\"): %v", testConfFileName, err)
	}
	err = testConfFile.Close()
	if nil != err {
		log.Fatalf("unable to close testConfFile (\"%s\"): %v", testConfFileName, err)
	}

	// Start-up ramswift if necessary

	if args.UseRAMSwift {
		ramswiftCmd = exec.Command("ramswift", testConfFileName)

		err = ramswiftCmd.Start()
		if nil != err {
			log.Fatalf("unable to start ramswift: %v", err)
		}

		getInfoURL = "http://127.0.0.1:" + strconv.Itoa(int(noAuthTCPPort)) + "/info"

		for {
			getInfoResponse, err = http.Get(getInfoURL)
			if nil == err {
				_, err = ioutil.ReadAll(getInfoResponse.Body)
				if nil != err {
					log.Fatalf("unable to read getInfoResponse.Body: %v", err)
				}
				err = getInfoResponse.Body.Close()
				if nil != err {
					log.Fatalf("unable to close getInfoResponse.Body: %v", err)
				}
				if http.StatusOK == getInfoResponse.StatusCode {
					break
				}
			}
			time.Sleep(startupPollingDelay)
		}
	}

	// Format Volume

	mkproxyfsCmd = exec.Command("mkproxyfs", "-F", args.Volume, testConfFileName, "SwiftClient.RetryLimit=0")

	err = mkproxyfsCmd.Run()
	if nil != err {
		log.Fatalf("mkproxyfs -F Volume \"%s\" failed: %v", args.Volume, err)
	}
}

func doMount() {
	var (
		err                error
		getVersionResponse *http.Response
	)

	// Start-up proxyfsd

	proxyfsdCmd = exec.Command("proxyfsd", testConfFileName)

	err = proxyfsdCmd.Start()
	if nil != err {
		log.Fatalf("unable to start proxyfsd: %v", err)
	}

	for {
		getVersionResponse, err = http.Get(proxyfsdVersionURL)
		if nil == err {
			_, err = ioutil.ReadAll(getVersionResponse.Body)
			if nil != err {
				log.Fatalf("unable to read getVersionResponse.Body: %v", err)
			}
			err = getVersionResponse.Body.Close()
			if nil != err {
				log.Fatalf("unable to close getVersionResponse.Body: %v", err)
			}
			if http.StatusOK == getVersionResponse.StatusCode {
				break
			}
		}
		time.Sleep(startupPollingDelay)
	}
}

func doCompares(loopIndex uint64) {
	var (
		dirIndex            uint64
		dirIndexEnd         uint64
		dirIndexStart       uint64
		dstDirPath          string
		nonSkippedLoopIndex uint64
	)

	if args.SkipWrites {
		nonSkippedLoopIndex = (loopIndex + 1) / 2
	} else {
		nonSkippedLoopIndex = loopIndex
	}

	dirIndexStart = (nonSkippedLoopIndex - 1) * dirIndexAdvancePerNonSkippedLoop
	dirIndexEnd = dirIndexStart + (args.NumCopies - 1)

	for dirIndex = dirIndexStart; dirIndex <= dirIndexEnd; dirIndex++ {
		dstDirPath = fmt.Sprintf(fuseMountPointName+"/%016X", dirIndex)
		compareDir(args.DirPath, dstDirPath)
	}
}

func compareDir(srcDirPath string, dstDirPath string) {
	var (
		dirEntryListIndex     int
		dstDirEntry           os.FileInfo
		dstDirEntryList       []os.FileInfo
		dstDirEntryListPruned []os.FileInfo
		dstDirEntryPath       string
		err                   error
		srcDirEntry           os.FileInfo
		srcDirEntryList       []os.FileInfo
		srcDirEntryListPruned []os.FileInfo
		srcDirEntryPath       string
	)

	srcDirEntryList, err = ioutil.ReadDir(srcDirPath)
	if nil != err {
		log.Fatalf("unable to read srcDirPath (\"%s\"): %v", srcDirPath, err)
	}
	dstDirEntryList, err = ioutil.ReadDir(dstDirPath)
	if nil != err {
		log.Fatalf("unable to read dstDirPath (\"%s\"): %v", dstDirPath, err)
	}

	srcDirEntryListPruned = make([]os.FileInfo, 0, len(srcDirEntryList))
	dstDirEntryListPruned = make([]os.FileInfo, 0, len(dstDirEntryList))

	for _, srcDirEntry = range srcDirEntryList {
		if ".fseventsd" != srcDirEntry.Name() {
			srcDirEntryListPruned = append(srcDirEntryListPruned, srcDirEntry)
		}
	}
	for _, dstDirEntry = range dstDirEntryList {
		if ".fseventsd" != dstDirEntry.Name() {
			dstDirEntryListPruned = append(dstDirEntryListPruned, dstDirEntry)
		}
	}

	if len(srcDirEntryListPruned) != len(dstDirEntryListPruned) {
		log.Fatalf("found unexpected discrepency between directory contents between \"%s\" & \"%s\"", srcDirPath, dstDirPath)
	}

	for dirEntryListIndex = 0; dirEntryListIndex < len(srcDirEntryListPruned); dirEntryListIndex++ { // || len(dstDirEntryListPruned)
		srcDirEntry = srcDirEntryListPruned[dirEntryListIndex]
		dstDirEntry = dstDirEntryListPruned[dirEntryListIndex]
		srcDirEntryPath = srcDirPath + "/" + srcDirEntry.Name()
		dstDirEntryPath = dstDirPath + "/" + dstDirEntry.Name()
		if srcDirEntry.IsDir() {
			if !dstDirEntry.IsDir() {
				log.Fatalf("dstDirPath/%s was not expected to be a dir", dstDirEntryPath)
			}
			compareDir(srcDirEntryPath, dstDirEntryPath)
		} else { // !srcDirEntry.IsDir()
			if dstDirEntry.IsDir() {
				log.Fatalf("dstDirPath/%s was expected to be a dir", dstDirEntryPath)
			}
			compareFile(srcDirEntryPath, dstDirEntryPath)
		}
	}
}

func compareFile(srcFilePath string, dstFilePath string) {
	var (
		err         error
		dstFile     *os.File
		dstFileBuf  []byte
		dstFileLen  int
		dstFileStat os.FileInfo
		fileLen     int64
		fileOffset  int64
		srcFile     *os.File
		srcFileBuf  []byte
		srcFileLen  int
		srcFileStat os.FileInfo
	)

	srcFile, err = os.Open(srcFilePath)
	if nil != err {
		log.Fatalf("unable to open srcFilePath (\"%s\"): %v", srcFilePath, err)
	}
	srcFileStat, err = srcFile.Stat()
	if nil != err {
		log.Fatalf("unable to stat srcFilePath (\"%s\"): %v", srcFilePath, err)
	}
	dstFile, err = os.Open(dstFilePath)
	if nil != err {
		log.Fatalf("unable to open dstFilePath (\"%s\"): %v", dstFilePath, err)
	}
	dstFileStat, err = dstFile.Stat()
	if nil != err {
		log.Fatalf("unable to stat srcFilePath (\"%s\"): %v", srcFilePath, err)
	}

	if srcFileStat.Size() != dstFileStat.Size() {
		log.Fatalf("srcFilePath (\"%s\") and dstFilePath (\"%s\") sizes done match", srcFilePath, dstFilePath)
	}

	srcFileBuf = make([]byte, compareChunkSize, compareChunkSize)
	dstFileBuf = make([]byte, compareChunkSize, compareChunkSize)

	fileLen = srcFileStat.Size() // || dstFileStat.Size()
	fileOffset = 0

	for fileLen < fileOffset {
		srcFileLen, err = srcFile.Read(srcFileBuf)
		if nil != err {
			log.Fatalf("unable to read srcFilePath (\"%s\"): %v", srcFilePath, err)
		}
		dstFileLen, err = srcFile.Read(dstFileBuf)
		if nil != err {
			log.Fatalf("unable to read dstFilePath (\"%s\"): %v", dstFilePath, err)
		}

		if srcFileLen != dstFileLen {
			log.Fatalf("unexpectedly read differing buffer sizes from srcFilePath (\"%s\") [%d] & dstFilePath (\"%s\") [%d]", srcFilePath, srcFileLen, dstFilePath, dstFileLen)
		}

		if 0 == bytes.Compare(srcFileBuf[:srcFileLen], dstFileBuf[:srcFileLen]) {
			log.Fatalf("unexpectedly read differing buffer contents from srcFilePath (\"%s\") and dstFilePath (\"%s\") at offset %d", srcFilePath, dstFilePath, fileOffset)
		}

		fileOffset += int64(srcFileLen) // || int64(dstFileLen)
	}

	err = srcFile.Close()
	if nil != err {
		log.Fatalf("unable to close srcFilePath (\"%s\"): %v", srcFilePath, err)
	}
	err = dstFile.Close()
	if nil != err {
		log.Fatalf("unable to close dstFilePath (\"%s\"): %v", dstFilePath, err)
	}
}

func doDeletions(loopIndex uint64) {
	var (
		dstDirPath          string
		dirIndex            uint64
		dirIndexEnd         uint64
		dirIndexStart       uint64
		err                 error
		nonSkippedLoopIndex uint64
	)

	if args.SkipWrites {
		nonSkippedLoopIndex = (loopIndex + 1) / 2
	} else {
		nonSkippedLoopIndex = loopIndex
	}

	dirIndexStart = (nonSkippedLoopIndex - 1) * dirIndexAdvancePerNonSkippedLoop
	dirIndexEnd = dirIndexStart + (dirIndexAdvancePerNonSkippedLoop - 1)

	for dirIndex = dirIndexStart; dirIndex <= dirIndexEnd; dirIndex++ {
		dstDirPath = fmt.Sprintf(fuseMountPointName+"/%016X", dirIndex)
		err = os.RemoveAll(dstDirPath)
		if nil != err {
			log.Fatalf("unable to remove dstDirPath (\"%s\"): %v", dstDirPath, err)
		}
	}
}

func doWrites(loopIndex uint64) {
	var (
		dirIndex            uint64
		dirIndexEnd         uint64
		dirIndexStart       uint64
		dstDirPath          string
		nonSkippedLoopIndex uint64
		numOverwritesLeft   uint64
	)

	if args.SkipWrites {
		nonSkippedLoopIndex = loopIndex / 2
	} else {
		nonSkippedLoopIndex = loopIndex
	}

	dirIndexStart = nonSkippedLoopIndex * dirIndexAdvancePerNonSkippedLoop
	dirIndexEnd = dirIndexStart + (args.NumCopies - 1)

	if 0 == loopIndex {
		numOverwritesLeft = 0
	} else {
		numOverwritesLeft = args.NumOverwrites
	}

	for dirIndex = dirIndexStart; dirIndex <= dirIndexEnd; dirIndex++ {
		dstDirPath = fmt.Sprintf(fuseMountPointName+"/%016X", dirIndex)
		if 0 < numOverwritesLeft {
			cloneDir(args.DirPath, dstDirPath, true)
			numOverwritesLeft--
		} else {
			cloneDir(args.DirPath, dstDirPath, false)
		}
	}
}

func cloneDir(srcDirPath string, dstDirPath string, overwrite bool) {
	var (
		dstDirEntryPath string
		err             error
		srcDirEntry     os.FileInfo
		srcDirEntryList []os.FileInfo
		srcDirEntryPath string
	)

	if !overwrite {
		err = os.Mkdir(dstDirPath, dstDirPerm)
		if nil != err {
			log.Fatalf("unable to create dstDirPath (\"%s\"): %v", dstDirPath, err)
		}
	}

	srcDirEntryList, err = ioutil.ReadDir(srcDirPath)
	if nil != err {
		log.Fatalf("unable to read srcDirPath (\"%s\"): %v", srcDirPath, err)
	}

	for _, srcDirEntry = range srcDirEntryList {
		if ("." != srcDirEntry.Name()) && (".." != srcDirEntry.Name()) {
			srcDirEntryPath = srcDirPath + "/" + srcDirEntry.Name()
			dstDirEntryPath = dstDirPath + "/" + srcDirEntry.Name()
			if srcDirEntry.IsDir() {
				cloneDir(srcDirEntryPath, dstDirEntryPath, overwrite)
			} else {
				cloneFile(srcDirEntryPath, dstDirEntryPath, overwrite)
			}
		}
	}
}

func cloneFile(srcFilePath string, dstFilePath string, overwrite bool) {
	var (
		err     error
		dstFile *os.File
		srcFile *os.File
	)

	srcFile, err = os.Open(srcFilePath)
	if nil != err {
		log.Fatalf("unable to open srcFilePath (\"%s\"): %v", srcFilePath, err)
	}

	if overwrite {
		dstFile, err = os.OpenFile(dstFilePath, os.O_WRONLY|os.O_TRUNC, dstFilePerm)
		if nil != err {
			log.Fatalf("unable to open existing dstFilePath (\"%s\"): %v", dstFilePath, err)
		}
	} else {
		dstFile, err = os.OpenFile(dstFilePath, os.O_WRONLY|os.O_EXCL|os.O_CREATE, dstFilePerm)
		if nil != err {
			log.Fatalf("unable to create dstFilePath (\"%s\"): %v", dstFilePath, err)
		}
	}

	_, err = io.Copy(dstFile, srcFile)
	if nil != err {
		log.Fatalf("unable to copp srcFilePath (\"%s\") to dstFilePath (\"%s\"): %v", srcFilePath, dstFilePath, err)
	}

	err = dstFile.Close()
	if nil != err {
		log.Fatalf("unable to close dstFilePath (\"%s\"): %v", dstFilePath, err)
	}

	err = srcFile.Close()
	if nil != err {
		log.Fatalf("unable to close srcFilePath (\"%s\"): %v", srcFilePath, err)
	}
}

func doUnmount() {
	var (
		err error
	)

	// Stop proxyfsd

	err = proxyfsdCmd.Process.Signal(unix.SIGTERM)
	if nil != err {
		log.Fatalf("unable to send SIGTERM to proxyfsd: %v", err)
	}

	err = proxyfsdCmd.Wait()
	if nil != err {
		log.Printf("failure waiting for proxyfsd to exit: %v", err)
	}
}

func teardown() {
	var (
		err error
	)

	// Stop ramswift if started earlier

	if args.UseRAMSwift {
		err = ramswiftCmd.Process.Signal(unix.SIGTERM)
		if nil != err {
			log.Fatalf("unable to send SIGTERM to ramswift: %v", err)
		}

		err = ramswiftCmd.Wait()
		if nil != err {
			log.Fatalf("failure waiting for ramswift to exit: %v", err)
		}
	}

	// Clean-up

	err = os.Remove(testConfFileName)
	if nil != err {
		log.Fatalf("unable to delete testConfFile (\"%s\"): %v", testConfFileName, err)
	}
}
