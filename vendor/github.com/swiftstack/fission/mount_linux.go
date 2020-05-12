package fission

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	recvmsgFlags   int = 0
	recvmsgOOBSize     = 32
	recvmsgPSize       = 4

	devLinuxFusePath = "/dev/fuse"
)

func (volume *volumeStruct) DoMount() (err error) {
	var (
		allowOtherOption         string
		childOpenFDs             []int
		fsnameOption             string
		fuseSubtypeOption        string
		fusermountChildWriteFile *os.File
		fusermountLineCount      uint32
		fusermountParentReadFile *os.File
		fusermountProgramPath    string
		fusermountSocketPair     [2]int
		gid                      int
		gidOption                string
		mountCmd                 *exec.Cmd
		mountCmdStderrPipe       io.ReadCloser
		mountCmdStdoutPipe       io.ReadCloser
		mountOptions             string
		recvmsgOOB               [recvmsgOOBSize]byte
		recvmsgOOBN              int
		recvmsgP                 [recvmsgPSize]byte
		rootMode                 uint32
		rootModeOption           string
		scanPipeWaitGroup        sync.WaitGroup
		socketControlMessages    []syscall.SocketControlMessage
		syscallRecvmsgAborted    bool
		uid                      int
		uidOption                string
		unmountCmd               *exec.Cmd
	)

	fusermountProgramPath, err = exec.LookPath("fusermount")
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() unable to find program `fusermount`: %v", volume.volumeName, err)
		return
	}

	unmountCmd = exec.Command(fusermountProgramPath, "-u", volume.mountpointDirPath)
	_, _ = unmountCmd.CombinedOutput()

	fusermountSocketPair, err = syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET, 0)
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() unable to create socketpairFDs: %v", volume.volumeName, err)
		return
	}

	fusermountChildWriteFile = os.NewFile(uintptr(fusermountSocketPair[0]), "fusermountChildWriteFile")
	fusermountParentReadFile = os.NewFile(uintptr(fusermountSocketPair[1]), "fusermountParentReadFile")

	syscallRecvmsgAborted = false

	defer func() {
		var (
			localErr error
		)

		if !syscallRecvmsgAborted {
			localErr = fusermountChildWriteFile.Close()
			if nil != localErr {
				volume.logger.Printf("Volume %s DoMount() unable to close fusermountChildWriteFile: %v", volume.volumeName, err)
			}
		}

		localErr = fusermountParentReadFile.Close()
		if nil != localErr {
			volume.logger.Printf("Volume %s DoMount() unable to close fusermountParentReadFile: %v", volume.volumeName, err)
		}
	}()

	rootMode = syscall.S_IFDIR
	rootModeOption = fmt.Sprintf("rootmode=%o", rootMode)

	uid = syscall.Geteuid()
	gid = syscall.Getegid()

	uidOption = fmt.Sprintf("user_id=%d", uid)
	gidOption = fmt.Sprintf("group_id=%d", gid)
	allowOtherOption = "allow_other"
	fsnameOption = "fsname=" + volume.volumeName

	mountOptions = rootModeOption +
		"," + uidOption +
		"," + gidOption +
		"," + allowOtherOption +
		"," + fsnameOption

	if "" != volume.fuseSubtype {
		fuseSubtypeOption = "subtype=" + volume.fuseSubtype
		mountOptions += "," + fuseSubtypeOption
	}

	mountCmd = &exec.Cmd{
		Path: fusermountProgramPath,
		Args: []string{
			fusermountProgramPath,
			"-o", mountOptions,
			volume.mountpointDirPath,
		},
		Env:          append(os.Environ(), "_FUSE_COMMFD=3"),
		Dir:          "",
		Stdin:        nil,
		Stdout:       nil, // This will be redirected to mountCmdStdoutPipe below
		Stderr:       nil, // This will be redirected to mountCmdStderrPipe below
		ExtraFiles:   []*os.File{fusermountChildWriteFile},
		SysProcAttr:  nil,
		Process:      nil,
		ProcessState: nil,
	}

	scanPipeWaitGroup.Add(2)

	mountCmdStdoutPipe, err = mountCmd.StdoutPipe()
	if nil == err {
		go volume.scanPipe("mountCmdStdoutPipe", mountCmdStdoutPipe, nil, &scanPipeWaitGroup)
	} else {
		volume.logger.Printf("Volume %s DoMount() unable to create mountCmd.StdoutPipe: %v", volume.volumeName, err)
		return
	}

	mountCmdStderrPipe, err = mountCmd.StderrPipe()
	if nil == err {
		go volume.scanPipe("mountCmdStderrPipe", mountCmdStderrPipe, &fusermountLineCount, &scanPipeWaitGroup)
	} else {
		volume.logger.Printf("Volume %s DoMount() unable to create mountCmd.StderrPipe: %v", volume.volumeName, err)
		return
	}

	err = mountCmd.Start()
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() unable to mountCmd.Start(): %v", volume.volumeName, err)
		return
	}

	go volume.awaitScanPipe(&scanPipeWaitGroup, &fusermountLineCount, &syscallRecvmsgAborted, fusermountChildWriteFile)

	_, recvmsgOOBN, _, _, err = syscall.Recvmsg(
		int(fusermountParentReadFile.Fd()),
		recvmsgP[:],
		recvmsgOOB[:],
		recvmsgFlags)
	if syscallRecvmsgAborted {
		err = fmt.Errorf("Volume %s DoMount() failed", volume.volumeName)
		return
	}
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() got error from fusermount: %v", volume.volumeName, err)
		return
	}

	socketControlMessages, err = syscall.ParseSocketControlMessage(recvmsgOOB[:recvmsgOOBN])
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() unable to syscall.ParseSocketControlMessage(): %v", volume.volumeName, err)
		return
	}
	if 1 != len(socketControlMessages) {
		volume.logger.Printf("Volume %s DoMount() got unexpected len(socketControlMessages): %v", volume.volumeName, len(socketControlMessages))
		return
	}

	childOpenFDs, err = syscall.ParseUnixRights(&socketControlMessages[0])
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() unable to syscall.ParseUnixRights(): %v", volume.volumeName, err)
		return
	}
	if 1 != len(childOpenFDs) {
		volume.logger.Printf("Volume %s DoMount() got unexpected len(childOpenFDs): %v", volume.volumeName, len(childOpenFDs))
		return
	}

	volume.devFuseFD = childOpenFDs[0]
	volume.devFuseFile = os.NewFile(uintptr(volume.devFuseFD), devLinuxFusePath)

	volume.devFuseFDReaderWG.Add(1)
	go volume.devFuseFDReader()

	err = mountCmd.Wait()
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() got error from fusermount: %v", volume.volumeName, err)
		return
	}

	volume.logger.Printf("Volume %s mounted on mountpoint %s", volume.volumeName, volume.mountpointDirPath)

	err = nil
	return
}

func (volume *volumeStruct) DoUnmount() (err error) {
	var (
		fusermountProgramPath    string
		unmountCmd               *exec.Cmd
		unmountCmdCombinedOutput []byte
	)

	fusermountProgramPath, err = exec.LookPath("fusermount")
	if nil != err {
		volume.logger.Printf("DoUnmount() unable to find program `fusermount`: %v", err)
		return
	}

	unmountCmd = exec.Command(fusermountProgramPath, "-u", volume.mountpointDirPath)
	unmountCmdCombinedOutput, err = unmountCmd.CombinedOutput()
	if nil != err {
		volume.logger.Printf("DoUnmount() unable to unmount %s (%v): %s", volume.volumeName, err, string(unmountCmdCombinedOutput[:]))
		return
	}

	err = syscall.Close(volume.devFuseFD)
	if nil != err {
		volume.logger.Printf("DoUnmount() unable to close /dev/fuse: %v", err)
		return
	}

	volume.devFuseFDReaderWG.Wait()

	volume.logger.Printf("Volume %s unmounted from mountpoint %s", volume.volumeName, volume.mountpointDirPath)

	err = nil
	return
}

func (volume *volumeStruct) scanPipe(name string, pipe io.ReadCloser, lineCount *uint32, wg *sync.WaitGroup) {
	var (
		pipeScanner *bufio.Scanner
	)

	pipeScanner = bufio.NewScanner(pipe)

	for pipeScanner.Scan() {
		if nil != lineCount {
			atomic.AddUint32(lineCount, 1)
		}
		volume.logger.Printf("Volume %s DoMount() %s: %s", volume.volumeName, name, pipeScanner.Text())
	}

	wg.Done()
}

func (volume *volumeStruct) awaitScanPipe(wg *sync.WaitGroup, lineCount *uint32, syscallRecvmsgAborted *bool, fusermountChildWriteFile *os.File) {
	var (
		err error
	)

	wg.Wait()

	if 0 != *lineCount {
		*syscallRecvmsgAborted = true

		volume.logger.Printf("Volume %s DoMount() got error(s) from fusermount - exiting", volume.volumeName)

		err = syscall.Close(int(fusermountChildWriteFile.Fd()))
		if nil != err {
			volume.logger.Printf("DoMount() unable to close fusermountChildWriteFile: %v", err)
		}
	}
}
