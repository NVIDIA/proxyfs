package fission

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

const (
	recvmsgFlags   int = 0
	recvmsgOOBSize     = 32
	recvmsgPSize       = 4
)

func (volume *volumeStruct) DoMount() (err error) {
	var (
		allowOtherOption         string
		fuseSubtypeMountOption   string
		fusermountChildWriteFile *os.File
		fusermountParentReadFile *os.File
		fusermountProgramPath    string
		fusermountSocketPair     [2]int
		gid                      int
		gidMountOption           string
		mountCmd                 *exec.Cmd
		mountCmdCombinedOutput   bytes.Buffer
		mountOptions             string
		childOpenFDs             []int
		recvmsgOOB               [recvmsgOOBSize]byte
		recvmsgOOBN              int
		recvmsgP                 [recvmsgPSize]byte
		rootMode                 uint32
		rootModeMountOption      string
		socketControlMessages    []syscall.SocketControlMessage
		uid                      int
		uidMountOption           string
		unmountCmd               *exec.Cmd
	)

	fusermountProgramPath, err = exec.LookPath("fusermount")
	if nil != err {
		volume.logger.Printf("DoMount() unable to find program `fusermount`: %v", err)
		return
	}

	unmountCmd = exec.Command(fusermountProgramPath, "-u", volume.mountpointDirPath)
	_, _ = unmountCmd.CombinedOutput()

	fusermountSocketPair, err = syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET, 0)
	if nil != err {
		volume.logger.Printf("DoMount() unable to create socketpairFDs: %v", err)
		return
	}

	fusermountChildWriteFile = os.NewFile(uintptr(fusermountSocketPair[0]), "fusermountChildWriteFile")
	fusermountParentReadFile = os.NewFile(uintptr(fusermountSocketPair[1]), "fusermountParentReadFile")

	defer func() {
		err = fusermountChildWriteFile.Close()
		if nil != err {
			volume.logger.Printf("DoMount() unable to close fusermountChildWriteFile: %v", err)
		}
		err = fusermountParentReadFile.Close()
		if nil != err {
			volume.logger.Printf("DoMount() unable to close fusermountParentReadFile: %v", err)
		}
	}()

	rootMode = syscall.S_IFDIR
	rootModeMountOption = fmt.Sprintf("rootmode=%o", rootMode)

	uid = syscall.Geteuid()
	gid = syscall.Getegid()

	uidMountOption = fmt.Sprintf("user_id=%d", uid)
	gidMountOption = fmt.Sprintf("group_id=%d", gid)
	allowOtherOption = "allow_other"

	mountOptions = rootModeMountOption +
		"," + uidMountOption +
		"," + gidMountOption +
		"," + allowOtherOption

	if "" != volume.fuseSubtype {
		fuseSubtypeMountOption = "subtype=" + volume.fuseSubtype
		mountOptions += "," + fuseSubtypeMountOption
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
		Stdout:       &mountCmdCombinedOutput,
		Stderr:       &mountCmdCombinedOutput,
		ExtraFiles:   []*os.File{fusermountChildWriteFile},
		SysProcAttr:  nil,
		Process:      nil,
		ProcessState: nil,
	}

	err = mountCmd.Start()
	if nil != err {
		volume.logger.Printf("DoMount() unable to mountCmd.Start(): %v", err)
		return
	}

	_, recvmsgOOBN, _, _, err = syscall.Recvmsg(
		int(fusermountParentReadFile.Fd()),
		recvmsgP[:],
		recvmsgOOB[:],
		recvmsgFlags)
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

	volume.devFuseFDReaderWG.Add(1)
	go volume.devFuseFDReader()

	err = mountCmd.Wait()
	if nil != err {
		volume.logger.Printf("Volume %s DoMount() got error (%v) from fusermount: %s", volume.volumeName, err, mountCmdCombinedOutput.String())
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
