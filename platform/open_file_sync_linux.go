package platform

import (
	"os"
	"syscall"
)

// OpenFile that ensures reads and writes are not cached and also that writes
// are not reported as complete until the data and metadata are persisted.
//
// Note that buffers must be aligned on the platform's buffer cache boundary
// in order for the cache bypass mode to function.
func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	var (
		modifiedFlag int
	)

	modifiedFlag = flag
	modifiedFlag |= syscall.O_DIRECT // aligned reads & writes DMA'd directly to/from user memory
	modifiedFlag |= syscall.O_SYNC   // writes are not complete until data & metadata is persisted

	file, err = os.OpenFile(name, modifiedFlag, perm)

	return
}
