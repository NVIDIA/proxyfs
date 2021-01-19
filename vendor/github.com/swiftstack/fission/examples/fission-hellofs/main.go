// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/fission"
)

const (
	fuseSubtype = "fission-hellofs"

	initOutFlagsNearlyAll = uint32(0) |
		fission.InitFlagsAsyncRead |
		fission.InitFlagsFileOps |
		fission.InitFlagsAtomicOTrunc |
		fission.InitFlagsBigWrites |
		fission.InitFlagsAutoInvalData |
		fission.InitFlagsDoReadDirPlus |
		fission.InitFlagsReaddirplusAuto |
		fission.InitFlagsParallelDirops |
		fission.InitFlagsMaxPages |
		fission.InitFlagsExplicitInvalData

	initOutMaxBackgound         = uint16(100)
	initOutCongestionThreshhold = uint16(0)
	initOutMaxWrite             = uint32(128 * 1024) // 128KiB... the max write size in Linux FUSE at this time

	attrBlkSize = 4096

	accessROK = syscall.S_IROTH // surprisingly not defined as syscall.R_OK
	accessWOK = syscall.S_IWOTH // surprisingly not defined as syscall.W_OK
	accessXOK = syscall.S_IXOTH // surprisingly not defined as syscall.X_OK

	accessMask       = syscall.S_IRWXO // used to mask Owner, Group, or Other RWX bits
	accessOwnerShift = 6
	accessGroupShift = 3
	accessOtherShift = 0

	rootInodeIno  = uint64(1)
	helloInodeIno = uint64(2)
)

var ( // these are really const but Go won't let us do this
	rootDirDotName    = []byte(".")
	rootDirDotDotName = []byte("..")

	helloFileName      = []byte("hello")
	helloInodeFileData = []byte("Hello World\n")
)

type globalsStruct struct {
	programPath    string
	mountPoint     string
	programName    string
	volumeName     string
	logger         *log.Logger
	errChan        chan error
	rootInodeAttr  *fission.Attr
	helloInodeAttr *fission.Attr
	dirEnt         []fission.DirEnt
	dirEntPlus     []fission.DirEntPlus
	volume         fission.Volume
}

var globals globalsStruct

func main() {
	var (
		err             error
		signalChan      chan os.Signal
		unixTimeNowNSec uint32
		unixTimeNowSec  uint64
	)

	if 2 != len(os.Args) {
		fmt.Printf("Usage: %s <mount_point>\n", os.Args[0])
		os.Exit(0)
	}

	globals.programPath = os.Args[0]
	globals.mountPoint = os.Args[1]

	globals.programName = path.Base(globals.programPath)
	globals.volumeName = path.Base(globals.mountPoint)

	globals.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime) // |log.Lmicroseconds|log.Lshortfile

	globals.errChan = make(chan error, 1)

	unixTimeNowSec, unixTimeNowNSec = unixTimeNow()

	globals.rootInodeAttr = &fission.Attr{
		Ino:       rootInodeIno,
		Size:      0,
		Blocks:    0,
		ATimeSec:  unixTimeNowSec,
		MTimeSec:  unixTimeNowSec,
		CTimeSec:  unixTimeNowSec,
		ATimeNSec: unixTimeNowNSec,
		MTimeNSec: unixTimeNowNSec,
		CTimeNSec: unixTimeNowNSec,
		Mode:      uint32(syscall.S_IFDIR | syscall.S_IRUSR | syscall.S_IXUSR | syscall.S_IRGRP | syscall.S_IXGRP | syscall.S_IROTH | syscall.S_IXOTH),
		NLink:     2,
		UID:       0,
		GID:       0,
		RDev:      0,
		BlkSize:   attrBlkSize,
		Padding:   0,
	}

	globals.helloInodeAttr = &fission.Attr{
		Ino:       helloInodeIno,
		Size:      uint64(len(helloInodeFileData)),
		Blocks:    1,
		ATimeSec:  unixTimeNowSec,
		MTimeSec:  unixTimeNowSec,
		CTimeSec:  unixTimeNowSec,
		ATimeNSec: unixTimeNowNSec,
		MTimeNSec: unixTimeNowNSec,
		CTimeNSec: unixTimeNowNSec,
		Mode:      uint32(syscall.S_IFREG | syscall.S_IRUSR | syscall.S_IRGRP | syscall.S_IROTH),
		NLink:     1,
		UID:       0,
		GID:       0,
		RDev:      0,
		BlkSize:   attrBlkSize,
		Padding:   0,
	}

	globals.dirEnt = []fission.DirEnt{
		fission.DirEnt{
			Ino:     globals.rootInodeAttr.Ino,
			Off:     1,
			NameLen: uint32(len(rootDirDotName)),
			Type:    globals.rootInodeAttr.Mode & syscall.S_IFMT,
			Name:    rootDirDotName,
		},
		fission.DirEnt{
			Ino:     globals.rootInodeAttr.Ino,
			Off:     2,
			NameLen: uint32(len(rootDirDotDotName)),
			Type:    globals.rootInodeAttr.Mode & syscall.S_IFMT,
			Name:    rootDirDotDotName,
		},
		fission.DirEnt{
			Ino:     globals.helloInodeAttr.Ino,
			Off:     3,
			NameLen: uint32(len(helloFileName)),
			Type:    globals.helloInodeAttr.Mode & syscall.S_IFMT,
			Name:    helloFileName,
		},
	}

	globals.dirEntPlus = []fission.DirEntPlus{
		fission.DirEntPlus{
			EntryOut: fission.EntryOut{
				NodeID:         globals.rootInodeAttr.Ino,
				Generation:     0,
				EntryValidSec:  0,
				AttrValidSec:   0,
				EntryValidNSec: 0,
				AttrValidNSec:  0,
				Attr: fission.Attr{
					Ino:       globals.rootInodeAttr.Ino,
					Size:      globals.rootInodeAttr.Size,
					Blocks:    globals.rootInodeAttr.Blocks,
					ATimeSec:  globals.rootInodeAttr.ATimeSec,
					MTimeSec:  globals.rootInodeAttr.MTimeSec,
					CTimeSec:  globals.rootInodeAttr.CTimeSec,
					ATimeNSec: globals.rootInodeAttr.ATimeNSec,
					MTimeNSec: globals.rootInodeAttr.MTimeNSec,
					CTimeNSec: globals.rootInodeAttr.CTimeNSec,
					Mode:      globals.rootInodeAttr.Mode,
					NLink:     globals.rootInodeAttr.NLink,
					UID:       globals.rootInodeAttr.UID,
					GID:       globals.rootInodeAttr.GID,
					RDev:      globals.rootInodeAttr.RDev,
					BlkSize:   globals.rootInodeAttr.BlkSize,
					Padding:   globals.rootInodeAttr.Padding,
				},
			},
			DirEnt: fission.DirEnt{
				Ino:     globals.rootInodeAttr.Ino,
				Off:     1,
				NameLen: uint32(len(rootDirDotName)),
				Type:    globals.rootInodeAttr.Mode & syscall.S_IFMT,
				Name:    rootDirDotName,
			},
		},
		fission.DirEntPlus{
			EntryOut: fission.EntryOut{
				NodeID:         globals.rootInodeAttr.Ino,
				Generation:     0,
				EntryValidSec:  0,
				AttrValidSec:   0,
				EntryValidNSec: 0,
				AttrValidNSec:  0,
				Attr: fission.Attr{
					Ino:       globals.rootInodeAttr.Ino,
					Size:      globals.rootInodeAttr.Size,
					Blocks:    globals.rootInodeAttr.Blocks,
					ATimeSec:  globals.rootInodeAttr.ATimeSec,
					MTimeSec:  globals.rootInodeAttr.MTimeSec,
					CTimeSec:  globals.rootInodeAttr.CTimeSec,
					ATimeNSec: globals.rootInodeAttr.ATimeNSec,
					MTimeNSec: globals.rootInodeAttr.MTimeNSec,
					CTimeNSec: globals.rootInodeAttr.CTimeNSec,
					Mode:      globals.rootInodeAttr.Mode,
					NLink:     globals.rootInodeAttr.NLink,
					UID:       globals.rootInodeAttr.UID,
					GID:       globals.rootInodeAttr.GID,
					RDev:      globals.rootInodeAttr.RDev,
					BlkSize:   globals.rootInodeAttr.BlkSize,
					Padding:   globals.rootInodeAttr.Padding,
				},
			},
			DirEnt: fission.DirEnt{
				Ino:     globals.rootInodeAttr.Ino,
				Off:     2,
				NameLen: uint32(len(rootDirDotDotName)),
				Type:    globals.rootInodeAttr.Mode & syscall.S_IFMT,
				Name:    rootDirDotDotName,
			},
		},
		fission.DirEntPlus{
			EntryOut: fission.EntryOut{
				NodeID:         globals.helloInodeAttr.Ino,
				Generation:     0,
				EntryValidSec:  0,
				AttrValidSec:   0,
				EntryValidNSec: 0,
				AttrValidNSec:  0,
				Attr: fission.Attr{
					Ino:       globals.helloInodeAttr.Ino,
					Size:      globals.helloInodeAttr.Size,
					Blocks:    globals.helloInodeAttr.Blocks,
					ATimeSec:  globals.helloInodeAttr.ATimeSec,
					MTimeSec:  globals.helloInodeAttr.MTimeSec,
					CTimeSec:  globals.helloInodeAttr.CTimeSec,
					ATimeNSec: globals.helloInodeAttr.ATimeNSec,
					MTimeNSec: globals.helloInodeAttr.MTimeNSec,
					CTimeNSec: globals.helloInodeAttr.CTimeNSec,
					Mode:      globals.helloInodeAttr.Mode,
					NLink:     globals.helloInodeAttr.NLink,
					UID:       globals.helloInodeAttr.UID,
					GID:       globals.helloInodeAttr.GID,
					RDev:      globals.helloInodeAttr.RDev,
					BlkSize:   globals.helloInodeAttr.BlkSize,
					Padding:   globals.helloInodeAttr.Padding,
				},
			},
			DirEnt: fission.DirEnt{
				Ino:     globals.helloInodeAttr.Ino,
				Off:     3,
				NameLen: uint32(len(helloFileName)),
				Type:    globals.helloInodeAttr.Mode & syscall.S_IFMT,
				Name:    helloFileName,
			},
		},
	}

	globals.volume = fission.NewVolume(globals.volumeName, globals.mountPoint, fuseSubtype, initOutMaxWrite, &globals, globals.logger, globals.errChan)

	err = globals.volume.DoMount()
	if nil != err {
		globals.logger.Printf("fission.DoMount() failed: %v", err)
		os.Exit(1)
	}

	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	select {
	case _ = <-signalChan:
		// Normal termination due to one of the above registered signals
	case err = <-globals.errChan:
		// Unexpected exit of /dev/fuse read loop since it's before we call DoUnmount()
		globals.logger.Printf("unexpected exit of /dev/fuse read loop: %v", err)
	}

	err = globals.volume.DoUnmount()
	if nil != err {
		globals.logger.Printf("fission.DoUnmount() failed: %v", err)
		os.Exit(2)
	}
}

func unixTimeToGoTime(unixTimeSec uint64, unixTimeNSec uint32) (goTime time.Time) {
	goTime = time.Unix(int64(unixTimeSec), int64(unixTimeNSec))
	return
}

func goTimeToUnixTime(goTime time.Time) (unixTimeSec uint64, unixTimeNSec uint32) {
	var (
		unixTime uint64
	)
	unixTime = uint64(goTime.UnixNano())
	unixTimeSec = unixTime / 1e9
	unixTimeNSec = uint32(unixTime - (unixTimeSec * 1e9))
	return
}

func unixTimeNow() (unixTimeNowSec uint64, unixTimeNowNSec uint32) {
	unixTimeNowSec, unixTimeNowNSec = goTimeToUnixTime(time.Now())
	return
}

func cloneByteSlice(inBuf []byte) (outBuf []byte) {
	outBuf = make([]byte, len(inBuf))
	if 0 != len(inBuf) {
		_ = copy(outBuf, inBuf)
	}
	return
}
