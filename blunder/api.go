// Package blunder provides error-handling wrappers
//
// These wrappers allow callers to provide additional information in Go errors
// while still conforming to the Go error interface.
//
// This package provides APIs to add errno and HTTP status information to regular Go errors.
//
// This package is currently implemented on top of the ansel1/merry package:
//   https://github.com/ansel1/merry
//
//   merry comes with built-in support for adding information to errors:
//    - stacktraces
//    - overriding the error message
//    - HTTP status codes
//    - end user error messages
//    - your own additional information
//
//   From merry godoc:
//     You can add any context information to an error with `e = merry.WithValue(e, "code", 12345)`
//     You can retrieve that value with `v, _ := merry.Value(e, "code").(int)`
//
// FUTURE: Create APIs that add error information without doing a stacktrace.
//         We probably want to do this, but not for thin spike.
//
//         Currently, the merry package always adds a stack trace.
//         However a recent change to the merry package permits disabling stacktrace.

package blunder

import (
	"fmt"

	"github.com/ansel1/merry"
	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/logger"
)

// Error constants to be used in the ProxyFS namespace.
//
// There are two groups of constants:
//  - constants that correspond to linux/POSIX errnos as defined in errno.h
//  - ProxyFS-specific constants for errors not covered in the errno space
//
// The linux/POSIX-related constants should be used in cases where there is a clear
// mapping to these errors. Using these constants makes it easier to map errors for
// use by our JSON RPC functionality.
//
//
// NOTE: unix.Errno is used here because they are errno constants that exist in Go-land.
//       This type consists of an unsigned number describing an error condition. It implements
//       the error interface; we need to cast it to an int to get the errno value.
//
type FsError int

// The following line of code is a directive to go generate that tells it to create a
// file called fserror_string.go that implements the .String() method for type FsError.
//go:generate stringer -type=FsError

const (
	// Errors that map to linux/POSIX errnos as defined in errno.h
	//
	NotPermError          FsError = FsError(int(unix.EPERM))        // Operation not permitted
	NotFoundError         FsError = FsError(int(unix.ENOENT))       // No such file or directory
	IOError               FsError = FsError(int(unix.EIO))          // I/O error
	ReadOnlyError         FsError = FsError(int(unix.EROFS))        // Read-only file system
	TooBigError           FsError = FsError(int(unix.E2BIG))        // Argument list too long
	TooManyArgsError      FsError = FsError(int(unix.E2BIG))        // Arg list too long
	BadFileError          FsError = FsError(int(unix.EBADF))        // Bad file number
	TryAgainError         FsError = FsError(int(unix.EAGAIN))       // Try again
	OutOfMemoryError      FsError = FsError(int(unix.ENOMEM))       // Out of memory
	PermDeniedError       FsError = FsError(int(unix.EACCES))       // Permission denied
	BadAddressError       FsError = FsError(int(unix.EFAULT))       // Bad address
	DevBusyError          FsError = FsError(int(unix.EBUSY))        // Device or resource busy
	FileExistsError       FsError = FsError(int(unix.EEXIST))       // File exists
	NoDeviceError         FsError = FsError(int(unix.ENODEV))       // No such device
	NotDirError           FsError = FsError(int(unix.ENOTDIR))      // Not a directory
	IsDirError            FsError = FsError(int(unix.EISDIR))       // Is a directory
	InvalidArgError       FsError = FsError(int(unix.EINVAL))       // Invalid argument
	TableOverflowError    FsError = FsError(int(unix.ENFILE))       // File table overflow
	TooManyOpenFilesError FsError = FsError(int(unix.EMFILE))       // Too many open files
	FileTooLargeError     FsError = FsError(int(unix.EFBIG))        // File too large
	NoSpaceError          FsError = FsError(int(unix.ENOSPC))       // No space left on device
	BadSeekError          FsError = FsError(int(unix.ESPIPE))       // Illegal seek
	TooManyLinksError     FsError = FsError(int(unix.EMLINK))       // Too many links
	OutOfRangeError       FsError = FsError(int(unix.ERANGE))       // Math result not representable
	NameTooLongError      FsError = FsError(int(unix.ENAMETOOLONG)) // File name too long
	NoLocksError          FsError = FsError(int(unix.ENOLCK))       // No record locks available
	NotImplementedError   FsError = FsError(int(unix.ENOSYS))       // Function not implemented
	NotEmptyError         FsError = FsError(int(unix.ENOTEMPTY))    // Directory not empty
	TooManySymlinksError  FsError = FsError(int(unix.ELOOP))        // Too many symbolic links encountered
	NotSupportedError     FsError = FsError(int(unix.ENOTSUP))      // Operation not supported
	NoDataError           FsError = FsError(int(unix.ENODATA))      // No data available
	TimedOut              FsError = FsError(int(unix.ETIMEDOUT))    // Connection Timed Out
)

// Errors that map to constants already defined above
const (
	NotActiveError        FsError = NotFoundError
	BadMountIDError       FsError = InvalidArgError
	BadMountVolumeError   FsError = InvalidArgError
	NotFileError          FsError = IsDirError
	SegNumNotIntError     FsError = IOError
	SegNotFoundError      FsError = IOError
	SegReadError          FsError = IOError
	InodeFlushError       FsError = IOError
	BtreeDeleteError      FsError = IOError
	BtreePutError         FsError = IOError
	BtreeLenError         FsError = IOError
	FileWriteError        FsError = IOError
	GetMetadataError      FsError = IOError
	NotSymlinkError       FsError = InvalidArgError
	IsSymlinkError        FsError = InvalidArgError
	LinkDirError          FsError = NotPermError
	BadHTTPDeleteError    FsError = IOError
	BadHTTPGetError       FsError = IOError
	BadHTTPHeadError      FsError = IOError
	BadHTTPPutError       FsError = IOError
	InvalidInodeTypeError FsError = InvalidArgError
	InvalidFileModeError  FsError = InvalidArgError
	InvalidUserIDError    FsError = InvalidArgError
	InvalidGroupIDError   FsError = InvalidArgError
	StreamNotFound        FsError = NoDataError
	AccountNotModifiable  FsError = NotPermError
	OldMetaDataDifferent  FsError = TryAgainError
)

// Success error (sounds odd, no? - perhaps this could be renamed "NotAnError"?)
const SuccessError FsError = 0

const ( // reset iota to 0
	// Errors that are internal/specific to ProxyFS
	UnpackError FsError = 1000 + iota
	PackError
	CorruptInodeError
	NotAnObjectError
)

// Default errno values for success and failure
const successErrno = 0
const failureErrno = -1

// Value returns the int value for the specified FsError constant
func (err FsError) Value() int {
	return int(err)
}

// NewError creates a new merry/blunder.FsError-annotated error using the given
// format string and arguments.
func NewError(errValue FsError, format string, a ...interface{}) error {
	return merry.WrapSkipping(fmt.Errorf(format, a...), 1).WithValue("errno", int(errValue))
}

// AddError is used to add FS error detail to a Go error.
//
// NOTE: Checks whether the error value has already been set
//       Note that by default merry will replace the old with the new.
//
func AddError(e error, errValue FsError) error {
	if e == nil {
		// Error hasn't been allocated yet; need to create one
		//
		// Usually we wouldn't want to mess with a nil error, but the caller of
		// this function obviously intends to make this a non-nil error.
		//
		// It's recommended that the caller create an error with some context
		// in the error string first, but we don't want to silently not work
		// if they forget to do that.
		//
		return merry.New("regular error").WithValue("errno", int(errValue))
	}

	// Make the error "merry", adding stack trace as well as errno value.
	// This is done all in one line because the merry APIs create a new error each time.

	// For now, check and log if an errno has already been added to
	// this error, to help debugging in the cases where this was not intentional.
	prevValue := Errno(e)
	if prevValue != successErrno && prevValue != failureErrno {
		logger.Warnf("replacing error value %v with value %v for error %v.\n", prevValue, int(errValue), e)
	}

	return merry.WrapSkipping(e, 1).WithValue("errno", int(errValue))
}

func hasErrnoValue(e error) bool {
	// If the "errno" key/value was not present, merry.Value returns nil.
	tmp := merry.Value(e, "errno")
	if tmp != nil {
		return true
	}

	return false
}

func AddHTTPCode(e error, statusCode int) error {
	if e == nil {
		// Error hasn't been allocated yet; need to create one
		//
		// Usually we wouldn't want to mess with a nil error, but the caller of
		// this function obviously intends to make this a non-nil error.
		//
		// It's recommended that the caller create an error with some context
		// in the error string first, but we don't want to silently not work
		// if they forget to do that.
		//
		return merry.New("HTTP error").WithHTTPCode(statusCode)
	}

	// Make the error "merry", adding stack trace as well as errno value.
	// This is done all in one line because the merry APIs create a new error each time.
	return merry.WrapSkipping(e, 1).WithHTTPCode(statusCode)
}

// Errno extracts errno from the error, if it was previously wrapped.
// Otherwise a default value is returned.
//
func Errno(e error) int {
	if e == nil {
		// nil error = success
		return successErrno
	}

	// If the "errno" key/value was not present, merry.Value returns nil.
	var errno = failureErrno
	tmp := merry.Value(e, "errno")
	if tmp != nil {
		errno = tmp.(int)
	}

	return errno
}

func ErrorString(e error) string {
	if e == nil {
		return ""
	}

	// Get the regular error string
	errPlusVal := e.Error()

	// Add the error value to it, if set
	var errno = failureErrno
	tmp := merry.Value(e, "errno")
	if tmp != nil {
		errno = tmp.(int)
		errPlusVal = fmt.Sprintf("%s. Error Value: %v\n", errPlusVal, errno)
	}

	return errPlusVal
}

// Check if an error matches a particular FsError
//
// NOTE: Because the value of the underlying errno is used to do this check, one cannot
//       use this API to distinguish between FsErrors that use the same errno value.
//       IOW, it can't tell the difference between InvalidFileModeError/BadMountIDError/InvalidArgError,
//       since they all use unix.EINVAL as their underlying errno value.
//
func Is(e error, theError FsError) bool {
	return Errno(e) == theError.Value()
}

// Check if an error is NOT a particular FsError
func IsNot(e error, theError FsError) bool {
	return Errno(e) != theError.Value()
}

// Check if an error is the success FsError
func IsSuccess(e error) bool {
	return Errno(e) == successErrno
}

// Check if an error is NOT the success FsError
func IsNotSuccess(e error) bool {
	return Errno(e) != successErrno
}

func ErrorUpdate(e error, currentVal FsError, changeToVal FsError) error {
	errVal := Errno(e)

	if errVal == int(currentVal) {
		fmt.Printf("blunder.ErrorUpdate: errVal was %d, changing to %d.\n", errVal, int(changeToVal))
		// Change to the new value
		return merry.Wrap(e).WithValue("errno", int(changeToVal))
	}

	return e
}

// HTTPCode wraps merry.HTTPCode, which returns the HTTP status code. Default value is 500.
func HTTPCode(e error) int {
	return merry.HTTPCode(e)
}

// Location returns the file and line number of the code that generated the error.
// Returns zero values if e has no stacktrace.
func Location(e error) (file string, line int) {
	file, line = merry.Location(e)
	return
}

// SourceLine returns the string representation of Location's result
// Returns empty stringif e has no stacktrace.
func SourceLine(e error) string {
	return merry.SourceLine(e)
}

// Details wraps merry.Details, which returns all error details including stacktrace in a string.
func Details(e error) string {
	return merry.Details(e)
}

// Stacktrace wraps merry.Stacktrace, which returns error stacktrace (if set) in a string.
func Stacktrace(e error) string {
	return merry.Stacktrace(e)
}
