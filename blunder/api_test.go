package blunder

import (
	"fmt"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"
)

var testConfMap conf.ConfMap

func testSetup(t *testing.T) {
	var (
		err             error
		testConfStrings []string
	)

	testConfStrings = []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"Logging.LogFilePath=/dev/null",
		"Cluster.WhoAmI=nobody",
		"FSGlobals.VolumeGroupList=",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	err = transitions.Up(testConfMap)
	if nil != err {
		t.Fatalf("transitions.Up() failed: %v", err)
	}
}

func testTeardown(t *testing.T) {
	var (
		err error
	)

	err = transitions.Down(testConfMap)
	if nil != err {
		t.Fatalf("transitions.Down() failed: %v", err)
	}
}

func TestValues(t *testing.T) {
	errConstant := NotPermError
	expectedValue := int(unix.EPERM)
	if errConstant.Value() != expectedValue {
		logger.Fatalf("Error, %s != %d", errConstant.String(), expectedValue)
	}
	// XXX TODO: add the rest of the values
}

func checkValue(testInfo string, actualVal int, expectedVal int) bool {
	if actualVal != expectedVal {
		logger.Fatalf("Error, %s value was %d, expected %d", testInfo, actualVal, expectedVal)
		return false
	}
	return true
}

func TestDefaultErrno(t *testing.T) {
	testSetup(t)

	// Nil error test
	var err error

	// Now try to get error val out of err. We should get a default value, since error value hasn't been set.
	errno := Errno(err)

	// Since err is nil, the default value should be successErrno
	checkValue("nil error", errno, successErrno)

	// IsSuccess should return true and IsNotSuccess should return false
	if !IsSuccess(err) {
		logger.Fatalf("Error, IsSuccess() returned false for error %v (errno %v)", ErrorString(err), Errno(err))
	}
	if IsNotSuccess(err) {
		logger.Fatalf("Error, IsNotSuccess() returned true for error %v", ErrorString(err))
	}

	// Non-nil error test
	err = fmt.Errorf("This is an ordinary error")

	// Since err is non-nil, the default value should be failureErrno (-1)
	errno = Errno(err)
	checkValue("non-nil error", errno, failureErrno)

	// IsSuccess should return false and IsNotSuccess should return true
	if IsSuccess(err) {
		logger.Fatalf("Error, IsSuccess() returned true for error %v (errno %v)", ErrorString(err), Errno(err))
	}
	if !IsNotSuccess(err) {
		logger.Fatalf("Error, IsNotSuccess() returned false for error %v", ErrorString(err))
	}

	// Specific error test
	err = AddError(err, InvalidArgError)
	errno = Errno(err)
	checkValue("specific error", errno, InvalidArgError.Value())

	testTeardown(t)
}

func TestAddValue(t *testing.T) {
	testSetup(t)

	// Add value to a nil error (not recommended as a strategy, but it needs to work anyway)
	var err error
	err = AddError(err, ReadOnlyError)
	errno := Errno(err)
	checkValue("specific error", errno, ReadOnlyError.Value())
	if !hasErrnoValue(err) {
		logger.Fatalf("Error, hasErrnoValue returned false for error %v", ErrorString(err))
	}
	// Validate the Is* APIs on what started as a nil error
	if !Is(err, ReadOnlyError) {
		logger.Fatalf("Error, Is() returned false for error %v is NameTooLongError", ErrorString(err))
	}
	if Is(err, NotFoundError) {
		logger.Fatalf("Error, Is() returned true for error %v is IsDirError", ErrorString(err))
	}
	if !IsNot(err, InvalidArgError) {
		logger.Fatalf("Error, IsNot() returned false for error %v is IsDirError", ErrorString(err))
	}
	if IsSuccess(err) {
		logger.Fatalf("Error, IsSuccess() returned true for error %v", ErrorString(err))
	}
	if !IsNotSuccess(err) {
		logger.Fatalf("Error, IsNotSuccess() returned false for error %v", ErrorString(err))
	}

	// Add value to a non-nil error
	err = fmt.Errorf("This is an ordinary error")
	err = AddError(err, NameTooLongError)
	errno = Errno(err)
	checkValue("specific error", errno, NameTooLongError.Value())
	if !hasErrnoValue(err) {
		logger.Fatalf("Error, hasErrnoValue returned false for error %v", ErrorString(err))
	}
	// Validate the Is* APIs on what started as a non-nil error
	if !Is(err, NameTooLongError) {
		logger.Fatalf("Error, Is() returned false for error %v is NameTooLongError", ErrorString(err))
	}
	if Is(err, IsDirError) {
		logger.Fatalf("Error, Is() returned true for error %v is IsDirError", ErrorString(err))
	}
	if !IsNot(err, IsDirError) {
		logger.Fatalf("Error, IsNot() returned false for error %v is IsDirError", ErrorString(err))
	}
	if IsSuccess(err) {
		logger.Fatalf("Error, IsSuccess() returned true for error %v", ErrorString(err))
	}
	if !IsNotSuccess(err) {
		logger.Fatalf("Error, IsNotSuccess() returned false for error %v", ErrorString(err))
	}

	// Add a different value to a non-nil error
	err = AddError(err, ReadOnlyError)
	errno = Errno(err)
	checkValue("specific error", errno, ReadOnlyError.Value())
	if !hasErrnoValue(err) {
		logger.Fatalf("Error, hasErrnoValue returned false for error %v", ErrorString(err))
	}
	if !Is(err, ReadOnlyError) {
		logger.Fatalf("Error, Is() returned false for error %v is NameTooLongError", ErrorString(err))
	}

	testTeardown(t)
}

func TestHTTPCode(t *testing.T) {
	testSetup(t)

	// Nil error test
	// Add http code to a nil error (not recommended as a strategy, but it needs to work anyway)
	var err error

	// Now try to get http code out of err. We should get a default value, since error value hasn't been set.
	code := HTTPCode(err)

	// Since err is nil, the default value should be 200 OK
	checkValue("nil error", code, 200)

	// Non-nil error test
	err = fmt.Errorf("This is an ordinary error")

	// Err is non-nil but http code is not set, the default value should be 500
	code = HTTPCode(err)
	checkValue("non-nil error", code, 500)

	// Specific error test
	err = AddHTTPCode(err, 400)
	code = HTTPCode(err)
	checkValue("specific error", code, 400)

	testTeardown(t)
}
