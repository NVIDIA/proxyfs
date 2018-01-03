package evtlog

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
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
	eventLogBufferKey       uint32
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

// Up initializes the package and must successfully return before any API functions are invoked
func Up(confMap conf.ConfMap) (err error) {
	globals.eventLogEnabled, err = confMap.FetchOptionValueBool("EventLog", "Enabled")
	if (nil != err) || !globals.eventLogEnabled {
		// Unless successfully read as `True`, disable event logging and return success
		globals.eventLogEnabled = false
		err = nil
		return
	}

	globals.eventLogBufferKey, err = confMap.FetchOptionValueUint32("EventLog", "BufferKey")
	if nil != err {
		globals.eventLogEnabled = false
		err = fmt.Errorf("confMap.FetchOptionValueUint32(\"EventLog\", \"BufferKey\") failed: %v", err)
		return
	}

	globals.eventLogBufferLength, err = confMap.FetchOptionValueUint64("EventLog", "BufferLength")
	if nil != err {
		globals.eventLogEnabled = false
		err = fmt.Errorf("confMap.FetchOptionValueUint64(\"EventLog\", \"BufferLength\") failed: %v", err)
		return
	}
	if 0 != (globals.eventLogBufferLength % 4) {
		globals.eventLogEnabled = false
		err = fmt.Errorf("confMap.FetchOptionValueUint64(\"EventLog\", \"BufferLength\") not divisible by 4")
		return
	}

	globals.eventLogLockMinBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MinBackoff")
	if nil != err {
		globals.eventLogEnabled = false
		err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MinBackoff\") failed: %v", err)
		return
	}

	globals.eventLogLockMaxBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MaxBackoff")
	if nil != err {
		globals.eventLogEnabled = false
		err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MaxBackoff\") failed: %v", err)
		return
	}

	err = enableLogging()

	return
}

// PauseAndContract pauses the evtlog package and applies any removals from the supplied confMap
func PauseAndContract(confMap conf.ConfMap) (err error) {
	// Nothing to do here

	err = nil
	return
}

// ExpandAndResume applies any additions from the supplied confMap and resumes the evtlog package
func ExpandAndResume(confMap conf.ConfMap) (err error) {
	var (
		newEventLogBufferKey    uint32
		newEventLogBufferLength uint64
		newEventLogEnabled      bool
	)

	if globals.eventLogEnabled {
		newEventLogEnabled, err = confMap.FetchOptionValueBool("EventLog", "Enabled")
		if (nil != err) || !globals.eventLogEnabled {
			// Unless successfully read as `True`, disable event logging and return success
			newEventLogEnabled = false
		}

		if newEventLogEnabled {
			// Ensure EventLog name & size didn't change

			newEventLogBufferKey, err = confMap.FetchOptionValueUint32("EventLog", "BufferKey")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueUint32(\"EventLog\", \"BufferKey\") failed: %v", err)
				return
			}
			if newEventLogBufferKey != globals.eventLogBufferKey {
				err = fmt.Errorf("confMap[EventLog][BufferKey] not modifyable without a restart")
				return
			}

			newEventLogBufferLength, err = confMap.FetchOptionValueUint64("EventLog", "BufferLength")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueUint64(\"EventLog\", \"BufferLength\") failed: %v", err)
				return
			}
			if newEventLogBufferLength != globals.eventLogBufferLength {
				err = fmt.Errorf("confMap[EventLog][BufferLength] not modifyable without a restart")
				return
			}

			// Fetch remaining EventLog parameters

			globals.eventLogLockMinBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MinBackoff")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MinBackoff\") failed: %v", err)
				return
			}

			globals.eventLogLockMaxBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MaxBackoff")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MaxBackoff\") failed: %v", err)
				return
			}
		} else {
			err = disableLogging()
		}
	} else {
		newEventLogEnabled, err = confMap.FetchOptionValueBool("EventLog", "Enabled")
		if (nil != err) || !globals.eventLogEnabled {
			// Unless successfully read as `True`, disable event logging and return success
			newEventLogEnabled = false
			err = nil
		}

		if newEventLogEnabled {
			globals.eventLogBufferKey, err = confMap.FetchOptionValueUint32("EventLog", "BufferKey")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueUint32(\"EventLog\", \"BufferKey\") failed: %v", err)
				return
			}

			globals.eventLogBufferLength, err = confMap.FetchOptionValueUint64("EventLog", "BufferLength")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueUint64(\"EventLog\", \"BufferLength\") failed: %v", err)
				return
			}

			globals.eventLogLockMinBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MinBackoff")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MinBackoff\") failed: %v", err)
				return
			}

			globals.eventLogLockMaxBackoff, err = confMap.FetchOptionValueDuration("EventLog", "MaxBackoff")
			if nil != err {
				err = fmt.Errorf("confMap.FetchOptionValueDuration(\"EventLog\", \"MaxBackoff\") failed: %v", err)
				return
			}

			err = enableLogging()
		}
	}

	return
}

// Down terminates event logging and should only be called once no API functions are active or subsequently invoked
func Down() (err error) {
	if globals.eventLogEnabled {
		err = disableLogging()
	} else {
		err = nil
	}

	return
}

func enableLogging() (err error) {
	var (
		ipc_rmid_result C.int
	)

	globals.shmSize = C.size_t(globals.eventLogBufferLength)

	globals.shmID = C.shmget(globals.shmKey, globals.shmSize, C.IPC_CREAT|syscall.S_IRUSR|syscall.S_IWUSR)

	if C.int(-1) == globals.shmID {
		globals.eventLogEnabled = false
		err = fmt.Errorf("C.shmget(globals.shmKey, globals.shmSize, C.IPC_CREAT) failed")
		return
	}

	globals.shmAddr = C.shmat_returning_uintptr(globals.shmID, C.uintptr_t(0), C.int(0))

	if ^C.uintptr_t(0) == globals.shmAddr {
		globals.eventLogEnabled = false
		ipc_rmid_result = C.shmctl(globals.shmID, C.IPC_RMID, nil)
		if C.int(-1) == ipc_rmid_result {
			err = fmt.Errorf("C.shmat_returning_uintptr(globals.shmID, C.uintptr_t(0), C.int(0)) then C.shmctl(globals.shmID, C.IPC_RMID, nil) failed")
			return
		}
		err = fmt.Errorf("C.shmat_returning_uintptr(globals.shmID, C.uintptr_t(0), C.int(0)) failed")
		return
	}

	ipc_rmid_result = C.shmctl(globals.shmID, C.IPC_RMID, nil)

	if C.int(-1) == ipc_rmid_result {
		globals.eventLogEnabled = false
		err = fmt.Errorf("C.shmctl(globals.shmID, C.IPC_RMID, nil) failed")
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
