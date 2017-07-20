package platform

import (
	"syscall"
)

const (
	swiftAccountCheckpointHeaderName = "X-Account-Meta-Checkpoint"
)

func MemSize() (memSize uint64) {
	var (
		err     error
		sysinfo syscall.Sysinfo_t
	)

	err = syscall.Sysinfo(&sysinfo)
	if nil != err {
		panic(err)
	}

	memSize = sysinfo.Totalram

	return
}
