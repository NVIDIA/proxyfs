package platform

import (
	"encoding/binary"

	"golang.org/x/sys/unix"
)

const (
	swiftAccountCheckpointHeaderName = "X-Account-Meta-Checkpoint"
)

func MemSize() (memSize uint64) {
	var (
		err          error
		sysctlReturn string
	)

	sysctlReturn, err = unix.Sysctl("hw.memsize")
	if nil != err {
		panic(err)
	}

	sysctlReturn += "\x00"

	memSize = uint64(binary.LittleEndian.Uint64([]byte(sysctlReturn)))

	return
}
