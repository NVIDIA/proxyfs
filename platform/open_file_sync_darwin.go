package platform

import (
	"fmt"
	"os"
	"syscall"
)

// OpenFileSync that ensures reads and writes are not cached and also that writes
// are not reported as complete until the data and metadata are persisted.
//
// Note that the request for no caching will only be honored if the file has
// not already entered the cache at the time of the call to OpenFile.
func OpenFileSync(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	var (
		errno        syscall.Errno
		modifiedFlag int
	)

	modifiedFlag = flag
	modifiedFlag |= syscall.O_SYNC // writes are not complete until data & metadata is persisted

	file, err = os.OpenFile(name, modifiedFlag, perm)
	if nil != err {
		return
	}

	_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, uintptr(file.Fd()), syscall.F_NOCACHE, 1)
	if 0 != errno {
		err = fmt.Errorf("fcntl(,F_NOCACHE,1) returned non-zero errno: %v", errno)
		_ = file.Close()
		file = nil
	}

	return
}
