package evtlog

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"
)

// #include <errno.h>
// #include <stdint.h>
// #include <sys/ipc.h>
// #include <sys/shm.h>
// #include <sys/types.h>
//
// uintptr_t shmat_returning_uintptr(int shmid, uintptr_t shmaddr, int shmflg) {
//     void *shmat_return;
//     shmat_return = shmat(shmid, (void *)shmaddr, shmflg);
//     return (uintptr_t)shmat_return;
// }
//
// int shmdt_returning_errno(uintptr_t shmaddr) {
//     int shmdt_return;
//     shmdt_return = shmdt((void *)shmaddr);
//     if (0 == shmdt_return) {
//         return 0;
//     } else {
//         return errno;
//     }
// }
import "C"

type globalsStruct struct {
	sync.Mutex              // While there can only ever be a single Consumer, multiple Producers are possible (within the same process)
	eventLogEnabled         bool
	eventLogBufferKey       uint64
	eventLogBufferLength    uint64
	eventLogLockMinBackoff  time.Duration
	eventLogLockMaxBackoff  time.Duration
	shmKey                  C.key_t
	shmSize                 C.size_t
	shmID                   C.int
	shmAddr                 C.uintptr_t
	shmKnownToBeInitialized bool // Iindicates that this instance "knows" initialization has completed
}

var globals globalsStruct

type eventLogConfigSettings struct {
	eventLogEnabled        bool
	eventLogBufferKey      uint64
	eventLogBufferLength   uint64
	eventLogLockMinBackoff time.Duration
	eventLogLockMaxBackoff time.Duration
}

func init() {
	transitions.Register("evtlog", &globals)
}

// Extract the settings from the confMap and perform minimal sanity checking
//
func parseConfMap(confMap conf.ConfMap) (settings eventLogConfigSettings, err error) {
	settings.eventLogEnabled, err = confMap.FetchOptionValueBool("EventLog", "Enabled")
	if (nil != err) || !settings.eventLogEnabled {
		// ignore parsing errors and treat this as logging disabled
		settings.eventLogEnabled = false
		err = nil
		return
	}

	settings.eventLogBufferKey, err = confMap.FetchOptionValueUint64("EventLog", "BufferKey")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueUint32(\"EventLog\", \"BufferKey\") failed: %v", err)
		return
	}

	settings.eventLogBufferLength, err = confMap.FetchOptionValueUint64("EventLog", "BufferLength")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueUint64(\"EventLog\", \"BufferLength\") failed: %v", err)
		return
	}
	if 0 != (globals.eventLogBufferLength % 4) {
		err = fmt.Errorf("confMap.FetchOptionValueUint64(\"EventLog\", \"BufferLength\") not divisible by 4")
		return
	}

	settings.eventLogLockMinBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MinBackoff")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MinBackoff\") failed: %v", err)
		return
	}

	settings.eventLogLockMaxBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MaxBackoff")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MaxBackoff\") failed: %v", err)
		return
	}

	return
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	var settings eventLogConfigSettings

	settings, err = parseConfMap(confMap)
	if nil != err {
		return
	}

	logger.Infof("evtlog.Up(): event logging is %v", settings.eventLogEnabled)

	globals.eventLogEnabled = settings.eventLogEnabled
	if !globals.eventLogEnabled {
		return
	}

	globals.eventLogBufferKey = settings.eventLogBufferKey
	globals.eventLogBufferLength = settings.eventLogBufferLength
	globals.eventLogLockMinBackoff = settings.eventLogLockMinBackoff
	globals.eventLogLockMaxBackoff = settings.eventLogLockMaxBackoff

	err = enableLogging()
	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (dummy *globalsStruct) Signaled(confMap conf.ConfMap) (err error) {
	var settings eventLogConfigSettings

	settings, err = parseConfMap(confMap)
	if nil != err {
		return
	}

	logger.Infof("evtlog.ExpandAndResume(): event logging is %v was %v", settings.eventLogEnabled, globals.eventLogEnabled)

	if !settings.eventLogEnabled {
		if !globals.eventLogEnabled {
			// was disabled and still is; no work to do
			return
		}

		// was enabled but is now disabled
		err = disableLogging()
		return
	}

	// event logging will be enabled
	//
	// if it was enabled previously certain settings cannot be changed
	if globals.eventLogEnabled {
		if settings.eventLogBufferKey != globals.eventLogBufferKey {
			err = fmt.Errorf("confMap[EventLog][BufferKey] not modifable without a restart")
			return
		}
		if settings.eventLogBufferLength != globals.eventLogBufferLength {
			err = fmt.Errorf("confMap[EventLog][BufferLength] not modifable without a restart")
			return
		}
	}

	globals.eventLogEnabled = settings.eventLogEnabled
	globals.eventLogBufferKey = settings.eventLogBufferKey
	globals.eventLogBufferLength = settings.eventLogBufferLength
	globals.eventLogLockMinBackoff = settings.eventLogLockMinBackoff
	globals.eventLogLockMaxBackoff = settings.eventLogLockMaxBackoff

	err = enableLogging()
	return
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	if globals.eventLogEnabled {
		err = disableLogging()
	}

	return
}

func enableLogging() (err error) {
	var (
		rmidResult                    C.int
		errno                         error
		sharedMemoryObjectPermissions C.int
	)

	globals.shmKey = C.key_t(globals.eventLogBufferKey)
	globals.shmSize = C.size_t(globals.eventLogBufferLength)

	sharedMemoryObjectPermissions = 0 |
		syscall.S_IRUSR |
		syscall.S_IWUSR |
		syscall.S_IRGRP |
		syscall.S_IWGRP |
		syscall.S_IROTH |
		syscall.S_IWOTH

	globals.shmID, errno = C.shmget(globals.shmKey, globals.shmSize, C.IPC_CREAT|sharedMemoryObjectPermissions)

	if C.int(-1) == globals.shmID {
		globals.eventLogEnabled = false
		err = fmt.Errorf("C.shmget(globals.shmKey, globals.shmSize, C.IPC_CREAT) failed with errno: %v", errno)
		return
	}

	globals.shmAddr = C.shmat_returning_uintptr(globals.shmID, C.uintptr_t(0), C.int(0))

	if ^C.uintptr_t(0) == globals.shmAddr {
		globals.eventLogEnabled = false
		rmidResult = C.shmctl(globals.shmID, C.IPC_RMID, nil)
		if C.int(-1) == rmidResult {
			err = fmt.Errorf("C.shmat_returning_uintptr(globals.shmID, C.uintptr_t(0), C.int(0)) then C.shmctl(globals.shmID, C.IPC_RMID, nil) failed")
			return
		}
		err = fmt.Errorf("C.shmat_returning_uintptr(globals.shmID, C.uintptr_t(0), C.int(0)) failed")
		return
	}

	globals.eventLogEnabled = true

	err = nil
	return
}

func disableLogging() (err error) {
	var (
		shmdtErrnoReturn syscall.Errno
		shmdtIntReturn   C.int
	)

	globals.eventLogEnabled = false

	shmdtIntReturn = C.shmdt_returning_errno(globals.shmAddr)
	if C.int(0) != shmdtIntReturn {
		shmdtErrnoReturn = syscall.Errno(shmdtIntReturn)
		err = fmt.Errorf("C.shmdt() returned non-zero failure... errno: %v", shmdtErrnoReturn.Error())
		return
	}

	err = nil
	return
}
