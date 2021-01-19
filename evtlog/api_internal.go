// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package evtlog

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"
	"unsafe"
)

// #include <stdint.h>
// #include <string.h>
// #include <sys/shm.h>
// #include <sys/types.h>
//
// uint8_t read_uint8(uintptr_t shmaddr, uint64_t offset) {
//     uint8_t *src_ptr;
//
//     src_ptr = (uint8_t *)shmaddr + offset;
//
//     return *src_ptr;
// }
//
// uint32_t read_uint32(uintptr_t shmaddr, uint64_t offset) {
//     uint32_t *src_ptr;
//
//     src_ptr = (uint32_t *)((uint8_t *)shmaddr + offset);
//
//     return *src_ptr;
// }
//
// uint64_t read_uint64(uintptr_t shmaddr, uint64_t offset) {
//     uint64_t *src_ptr;
//
//     src_ptr = (uint64_t *)((uint8_t *)shmaddr + offset);
//
//     return *src_ptr;
// }
//
// void read_buf(uintptr_t shmaddr, uint64_t offset, uint64_t length, uintptr_t buf) {
//     uint8_t *src_ptr;
//
//     src_ptr = (uint8_t *)shmaddr + offset;
//
//     (void)memcpy((void *)buf, (const void *)src_ptr, (size_t)length);
// }
//
// void write_uint8(uintptr_t shmaddr, uint64_t offset, uint8_t value) {
//     uint8_t *dst_ptr;
//
//     dst_ptr = (uint8_t *)shmaddr + offset;
//
//     *dst_ptr = value;
// }
//
// void write_uint32(uintptr_t shmaddr, uint64_t offset, uint32_t value) {
//     uint32_t *dst_ptr;
//
//     dst_ptr = (uint32_t *)((uint8_t *)shmaddr + offset);
//
//     *dst_ptr = value;
// }
//
// void write_uint64(uintptr_t shmaddr, uint64_t offset, uint64_t value) {
//     uint64_t *dst_ptr;
//
//     dst_ptr = (uint64_t *)((uint8_t *)shmaddr + offset);
//
//     *dst_ptr = value;
// }
//
// void write_buf(uintptr_t shmaddr, uint64_t offset, uint64_t length, uintptr_t buf) {
//     uint8_t *dst_ptr;
//
//     dst_ptr = (uint8_t *)shmaddr + offset;
//
//     (void)memcpy((void *)dst_ptr, (const void *)buf, (size_t)length);
// }
import "C"

// Shared memory object has the following format:
//
//   type sharedMemoryStruct struct {
//       producerActive     uint8  // 0 if Producer is not currently requesting or holding the lock; otherwise 1
//       consumerActive     uint8  // 0 if Consumer is not currently requesting or holding the lock; otherwise 1
//       producerNext       uint8  // 1 if Producer is "next"; 0 if Consumer is "next"
//       nextProducerOffset uint64 // where next call to record() will place Record
//       nextConsumerOffset uint64 // where next call to retieve() will read a Record from
//       numBufferWraps     uint64 // number of times the Producer had to wrap back to offsetToFirstRecordEver
//       numDroppedRecords  uint64 // number of times the Consumer fell too far behind Producer
//   }
//
// Synchronization uses Peterson's algorithm (simple one... for just two "threads")

const (
	offsetToProducerActive     = C.uint64_t(0x00)
	offsetToConsumerActive     = C.uint64_t(0x01)
	offsetToProducerNext       = C.uint64_t(0x02)
	offsetToNextProducerOffset = C.uint64_t(0x08)
	offsetToNextConsumerOffset = C.uint64_t(0x10)
	offsetToNumBufferWraps     = C.uint64_t(0x18)
	offsetToNumDroppedRecords  = C.uint64_t(0x20)
	offsetToFirstRecordEver    = C.uint64_t(0x28) // contents (possibly not on Record boundary) will wrap to here also
)

// Each Record has the following format:
//
//   type recordStruct struct {
//       recordLength uint32             // includes recordLength field... and what follows
//       unixNano     uint64             // time.Now().UnixNano() of event
//       formatType   FormatType         // indicates the format of the event
//       // For each format specifier in event[formatType].patternType"
//       //   %s:
//       stringLength uint32             // number of UTF-8 bytes in stringBytes array
//       stringBytes  [stringLength]byte // bytes of the UTF-8 string (not terminated)
//       //   %03d:
//       u32          uint32             // 32-bit unsigned value (though <= 999)
//       //   %08X:
//       u32          uint32             // 32-bit unsigned value
//       //   %016X:
//       u64          uint64             // 64-bit unsigned value
//   }

func backoff() {
	var (
		backoffDuration         time.Duration
		err                     error
		mustBeLessThanBigIntPtr *big.Int
		u64BigIntPtr            *big.Int
	)

	mustBeLessThanBigIntPtr = big.NewInt(int64(globals.eventLogLockMaxBackoff - globals.eventLogLockMinBackoff + 1))
	u64BigIntPtr, err = rand.Int(rand.Reader, mustBeLessThanBigIntPtr)
	if nil != err {
		log.Fatal(err)
	}
	backoffDuration = globals.eventLogLockMinBackoff + time.Duration(u64BigIntPtr.Uint64())

	time.Sleep(backoffDuration)
}

func ensureSharedMemoryIsInitialized() {
	var (
		nextProducerOffset C.uint64_t
		nextConsumerOffset C.uint64_t
	)

	nextProducerOffset = C.read_uint64(globals.shmAddr, offsetToNextProducerOffset)
	if C.uint64_t(0) == nextProducerOffset {
		C.write_uint64(globals.shmAddr, offsetToNextProducerOffset, offsetToFirstRecordEver)
	}

	nextConsumerOffset = C.read_uint64(globals.shmAddr, offsetToNextConsumerOffset)
	if C.uint64_t(0) == nextConsumerOffset {
		C.write_uint64(globals.shmAddr, offsetToNextConsumerOffset, offsetToFirstRecordEver)
	}

	globals.shmKnownToBeInitialized = true
}

func getProducerLock() {
	var (
		consumerActive C.uint8_t
		producerNext   C.uint8_t
	)

	// Lock out other Producers (all must be within this process)

	globals.Lock()

	// Perform 2-thread acquire() from Peterson's algorithm

	C.write_uint8(globals.shmAddr, offsetToProducerActive, 1)
	C.write_uint8(globals.shmAddr, offsetToProducerNext, 1)

	consumerActive = C.read_uint8(globals.shmAddr, offsetToConsumerActive)
	producerNext = C.read_uint8(globals.shmAddr, offsetToProducerNext)

	for (C.uint8_t(1) == consumerActive) && (C.uint8_t(1) == producerNext) {
		backoff()

		consumerActive = C.read_uint8(globals.shmAddr, offsetToConsumerActive)
		producerNext = C.read_uint8(globals.shmAddr, offsetToProducerNext)
	}

	if !globals.shmKnownToBeInitialized {
		ensureSharedMemoryIsInitialized()
	}
}

func freeProducerLock() {
	// Indicate this Producer is no longer active

	C.write_uint8(globals.shmAddr, offsetToProducerActive, 0)

	// Unlock to allow other Producers (all within this process) to proceed

	globals.Unlock() // Unlock to allow other Producers (all within this process) to proceed
}

func getConsumerLock() {
	var (
		producerActive C.uint8_t
		producerNext   C.uint8_t
	)

	// Perform 2-thread acquire() from Peterson's algorithm

	C.write_uint8(globals.shmAddr, offsetToConsumerActive, 1)
	C.write_uint8(globals.shmAddr, offsetToProducerNext, 0)

	producerActive = C.read_uint8(globals.shmAddr, offsetToProducerActive)
	producerNext = C.read_uint8(globals.shmAddr, offsetToProducerNext)

	for (C.uint8_t(1) == producerActive) && (C.uint8_t(0) == producerNext) {
		backoff()

		producerActive = C.read_uint8(globals.shmAddr, offsetToProducerActive)
		producerNext = C.read_uint8(globals.shmAddr, offsetToProducerNext)
	}

	if !globals.shmKnownToBeInitialized {
		ensureSharedMemoryIsInitialized()
	}
}

func freeConsumerLock() {
	// Indicate this Consumer is no longer active

	C.write_uint8(globals.shmAddr, offsetToConsumerActive, 0)
}

func record(formatType FormatType, args ...interface{}) {
	var (
		arg0Len            uint32
		arg1Len            uint32
		arg1Index          uint32
		arg2Len            uint32
		discardLength      C.uint32_t
		err                error
		nextProducerOffset C.uint64_t
		nextConsumerOffset C.uint64_t
		numBufferWraps     C.uint64_t
		numDroppedRecords  C.uint64_t
		pattern            patternType
		record             []byte
		recordLength       uint32
		recordPosition     uint32
		room               uint64
		wrapAmount         uint64
	)

	// Format record

	if formatType >= formatTypeCount {
		err = fmt.Errorf("Unrecognized formatType: %v", formatType)
		panic(err)
	}

	pattern = event[formatType].patternType

	switch pattern {
	case patternFixed:
		if len(args) != 0 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 //              formatType
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
	case patternS:
		if len(args) != 1 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len //    args[0] %s
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
	case patternS03D:
		if len(args) != 2 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 //              args[1] %03d
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[1].(uint32))
	case patternS08X:
		if len(args) != 2 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 //              args[1] %08X
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[1].(uint32))
	case patternS016X:
		if len(args) != 2 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			8 //              args[1] %016X
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[1].(uint64))
	case patternS016X016X:
		if len(args) != 3 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			8 + //            args[1] %016X
			8 //              args[2] %016X
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[1].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[2].(uint64))
	case patternS016X016X016X:
		if len(args) != 4 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			8 + //            args[1] %016X
			8 + //            args[2] %016X
			8 //              args[3] %016X
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[1].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[2].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[3].(uint64))
	case patternS016Xslice:
		if len(args) != 2 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].([]uint64)))
		recordLength = 4 + //    recordLength
			8 + //               unixNano
			4 + //               formatType
			4 + arg0Len + //     args[0] %s
			4 + (arg1Len * 8) // args[1] [%016X]
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		for arg1Index = uint32(0); arg1Index < arg1Len; arg1Index++ {
			binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[1].([]uint64)[arg1Index])
			recordPosition += 8
		}
	case patternS016XS:
		if len(args) != 3 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg2Len = uint32(len(args[2].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			8 + //            args[1] %016X
			4 + arg2Len //    args[2] %s
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[1].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg2Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg2Len], args[2].(string))
	case patternSS:
		if len(args) != 2 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len //    args[1] %s
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
	case patternSS03D:
		if len(args) != 3 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len + //  args[1] %s
			4 //              args[2] %03d
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
		recordPosition += arg1Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[2].(uint32))
	case patternSS016X1X:
		if len(args) != 4 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len + //  args[1] %s
			8 + //            args[2] %016X
			4 //              args[3] %1X
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
		recordPosition += arg1Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[2].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[3].(uint32))
	case patternSS016X1X1X:
		if len(args) != 5 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len + //  args[1] %s
			8 + //            args[2] %016X
			4 + //            args[3] %1X
			4 //              args[4] %1X
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
		recordPosition += arg1Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[2].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[3].(uint32))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[4].(uint32))
	case patternSSS:
		if len(args) != 3 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		arg2Len = uint32(len(args[2].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len + //  args[1] %s
			4 + arg2Len //    args[2] %s
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
		recordPosition += arg1Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg2Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg2Len], args[2].(string))
	case patternSSS03D:
		if len(args) != 4 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		arg2Len = uint32(len(args[2].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len + //  args[1] %s
			4 + arg2Len + //  args[2] %s
			4 //              args[3] %03d
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
		recordPosition += arg1Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg2Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg2Len], args[2].(string))
		recordPosition += arg2Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[3].(uint32))
	case patternSSS016X03D:
		if len(args) != 5 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		arg2Len = uint32(len(args[2].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len + //  args[1] %s
			4 + arg2Len + //  args[2] %s
			8 + //            args[3] %016X
			4 //              args[4] %03d
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
		recordPosition += arg1Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg2Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg2Len], args[2].(string))
		recordPosition += arg2Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[3].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[4].(uint32))
	case patternSSS016X016X03D:
		if len(args) != 6 {
			err = fmt.Errorf("Unexpected number of arguments (%v) for formatType %v", len(args), formatType)
			panic(err)
		}
		arg0Len = uint32(len(args[0].(string)))
		arg1Len = uint32(len(args[1].(string)))
		arg2Len = uint32(len(args[2].(string)))
		recordLength = 4 + // recordLength
			8 + //            unixNano
			4 + //            formatType
			4 + arg0Len + //  args[0] %s
			4 + arg1Len + //  args[1] %s
			4 + arg2Len + //  args[2] %s
			8 + //            args[3] %016X
			8 + //            args[4] %016X
			4 //              args[5] %03d
		recordLength = (recordLength + 3) & ^uint32(3) // round up so that a uint32 is never split during wrapping
		record = make([]byte, recordLength)
		recordPosition = 0
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], recordLength)
		recordPosition += 4
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], uint64(time.Now().UnixNano()))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], uint32(formatType))
		recordPosition += 4
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg0Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg0Len], args[0].(string))
		recordPosition += arg0Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg1Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg1Len], args[1].(string))
		recordPosition += arg1Len
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], arg2Len)
		recordPosition += 4
		copy(record[recordPosition:recordPosition+arg2Len], args[2].(string))
		recordPosition += arg2Len
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[3].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint64(record[recordPosition:recordPosition+8], args[4].(uint64))
		recordPosition += 8
		binary.LittleEndian.PutUint32(record[recordPosition:recordPosition+4], args[5].(uint32))
	default:
		err = fmt.Errorf("Unrecognized patternType (%v) for formatType %v", pattern, formatType)
		panic(err)
	}

	// Insert record in shared memory object at nextProducerOffset

	getProducerLock()

	nextProducerOffset = C.read_uint64(globals.shmAddr, offsetToNextProducerOffset)
	nextConsumerOffset = C.read_uint64(globals.shmAddr, offsetToNextConsumerOffset)

	// Ensure there is room for this record

	if nextProducerOffset == nextConsumerOffset { // Empty, not full
		room = globals.eventLogBufferLength - uint64(offsetToFirstRecordEver) - 4
	} else if nextProducerOffset < nextConsumerOffset {
		room = uint64(nextConsumerOffset-nextProducerOffset) - 4
	} else { // nextProducerOffset > nextConsumerOffset
		room = uint64(nextConsumerOffset) + (globals.eventLogBufferLength - uint64(offsetToFirstRecordEver)) - uint64(nextProducerOffset) - 4
	}

	for room < uint64(recordLength) {
		discardLength = C.read_uint32(globals.shmAddr, nextConsumerOffset)
		nextConsumerOffset += C.uint64_t(discardLength)
		if nextConsumerOffset >= C.uint64_t(globals.eventLogBufferLength) {
			nextConsumerOffset -= C.uint64_t(globals.eventLogBufferLength) - offsetToFirstRecordEver
		}
		C.write_uint64(globals.shmAddr, offsetToNextConsumerOffset, nextConsumerOffset)
		numDroppedRecords = C.read_uint64(globals.shmAddr, offsetToNumDroppedRecords)
		numDroppedRecords++
		C.write_uint64(globals.shmAddr, offsetToNumDroppedRecords, numDroppedRecords)
		room += uint64(discardLength)
	}

	// Copy record to shared memory object honoring any required wrap

	if (uint64(nextProducerOffset) + uint64(recordLength)) <= globals.eventLogBufferLength {
		// No wrap needed to "fit" record
		C.write_buf(globals.shmAddr, nextProducerOffset, C.uint64_t(uint64(recordLength)), C.uintptr_t(uintptr(unsafe.Pointer(&record[0]))))
		nextProducerOffset += C.uint64_t(uint64(recordLength))
		if nextProducerOffset == C.uint64_t(globals.eventLogBufferLength) {
			// But the next record will start at the beginning
			nextProducerOffset = offsetToFirstRecordEver
			numBufferWraps = C.read_uint64(globals.shmAddr, offsetToNumBufferWraps)
			numBufferWraps++
			C.write_uint64(globals.shmAddr, offsetToNumBufferWraps, numBufferWraps)
		}
	} else {
		// Some portion of record must wrap to the beginning
		wrapAmount = uint64(nextProducerOffset) + uint64(recordLength) - globals.eventLogBufferLength
		C.write_buf(globals.shmAddr, nextProducerOffset, C.uint64_t(uint64(recordLength)-wrapAmount), C.uintptr_t(uintptr(unsafe.Pointer(&record[0]))))
		nextProducerOffset = offsetToFirstRecordEver
		C.write_buf(globals.shmAddr, nextProducerOffset, C.uint64_t(wrapAmount), C.uintptr_t(uintptr(unsafe.Pointer(&record[uint64(recordLength)-wrapAmount]))))
		nextProducerOffset += C.uint64_t(wrapAmount)
		numBufferWraps = C.read_uint64(globals.shmAddr, offsetToNumBufferWraps)
		numBufferWraps++
		C.write_uint64(globals.shmAddr, offsetToNumBufferWraps, numBufferWraps)
	}

	C.write_uint64(globals.shmAddr, offsetToNextProducerOffset, nextProducerOffset)

	freeProducerLock()
}

func retrieve() (formattedRecord string, numDroppedRecords uint64) {
	var (
		arg0String                    string
		arg0StringLen                 uint32
		arg1String                    string
		arg1StringLen                 uint32
		arg1U32                       uint32
		arg1U64                       uint64
		arg1U64Slice                  []uint64
		arg1U64SliceIndex             uint32
		arg1U64SliceLen               uint32
		arg2String                    string
		arg2StringLen                 uint32
		arg2U32                       uint32
		arg2U64                       uint64
		arg3U32                       uint32
		arg3U64                       uint64
		arg4U32                       uint32
		arg4U64                       uint64
		arg5U32                       uint32
		err                           error
		formatStringLeftBracketIndex  int
		formatStringRightBracketIndex int
		formatType                    FormatType
		nextProducerOffset            C.uint64_t
		nextConsumerOffset            C.uint64_t
		pattern                       patternType
		record                        []byte
		recordLength                  C.uint32_t
		recordPosition                uint32
		replicatedFormatStringSubset  string
		timestamp                     time.Time
		unixNano                      uint64
		unixNanoSecPart               int64
		unixNanoNSecPart              int64
		wrapAmount                    uint64
	)

	// Fetch record (if any)

	getConsumerLock()

	numDroppedRecords = uint64(C.read_uint64(globals.shmAddr, offsetToNumDroppedRecords))
	if 0 != numDroppedRecords {
		C.write_uint64(globals.shmAddr, offsetToNumDroppedRecords, C.uint64_t(0))
	}

	nextProducerOffset = C.read_uint64(globals.shmAddr, offsetToNextProducerOffset)
	nextConsumerOffset = C.read_uint64(globals.shmAddr, offsetToNextConsumerOffset)

	if nextProducerOffset == nextConsumerOffset {
		// No record available - so just return ""
		freeConsumerLock()
		formattedRecord = ""
		return
	}

	recordLength = C.read_uint32(globals.shmAddr, nextConsumerOffset)

	record = make([]byte, recordLength)

	if uint64(nextConsumerOffset+C.uint64_t(recordLength)) <= globals.eventLogBufferLength {
		C.read_buf(globals.shmAddr, nextConsumerOffset, C.uint64_t(recordLength), C.uintptr_t(uintptr(unsafe.Pointer(&record[0]))))
		nextConsumerOffset += C.uint64_t(recordLength)
		if nextConsumerOffset == C.uint64_t(globals.eventLogBufferLength) {
			nextConsumerOffset = offsetToFirstRecordEver
		}
	} else {
		wrapAmount = uint64(nextConsumerOffset) + uint64(C.uint64_t(recordLength)) - globals.eventLogBufferLength
		C.read_buf(globals.shmAddr, nextConsumerOffset, C.uint64_t(recordLength)-C.uint64_t(wrapAmount), C.uintptr_t(uintptr(unsafe.Pointer(&record[0]))))
		nextConsumerOffset = offsetToFirstRecordEver
		C.read_buf(globals.shmAddr, nextConsumerOffset, C.uint64_t(wrapAmount), C.uintptr_t(uintptr(unsafe.Pointer(&record[uint64(C.uint64_t(recordLength))-wrapAmount]))))
		nextConsumerOffset += C.uint64_t(wrapAmount)
	}

	C.write_uint64(globals.shmAddr, offsetToNextConsumerOffset, nextConsumerOffset)

	freeConsumerLock()

	// Decode/format record

	recordPosition = 4 // No need to re-decode recordLength

	unixNano = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
	recordPosition += 8
	unixNanoSecPart = int64(unixNano / 1000000000)
	unixNanoNSecPart = int64(unixNano % 1000000000)
	timestamp = time.Unix(unixNanoSecPart, unixNanoNSecPart)

	formatType = FormatType(binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4]))
	recordPosition += 4

	if formatType >= formatTypeCount {
		err = fmt.Errorf("Unrecognized formatType: %v", formatType)
		panic(err)
	}

	pattern = event[formatType].patternType

	switch pattern {
	case patternFixed:
		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat))
	case patternS:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String)
	case patternS03D:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1U32)
	case patternS08X:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1U32)
	case patternS016X:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1U64)
	case patternS016X016X:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8

		arg2U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1U64, arg2U64)
	case patternS016X016X016X:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8

		arg2U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8

		arg3U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1U64, arg2U64, arg3U64)
	case patternS016Xslice:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1U64SliceLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1U64Slice = make([]uint64, arg1U64SliceLen)
		for arg1U64SliceIndex = uint32(0); arg1U64SliceIndex < arg1U64SliceLen; arg1U64SliceIndex++ {
			arg1U64Slice[arg1U64SliceIndex] = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
			recordPosition += 8
		}

		formatStringLeftBracketIndex = strings.Index(event[formatType].formatString, "[")
		formatStringRightBracketIndex = strings.Index(event[formatType].formatString, "]")

		formattedRecord = fmt.Sprintf(event[formatType].formatString[:formatStringLeftBracketIndex+1], timestamp.Format(patternTimestampFormat), arg0String)

		if 0 < arg1U64SliceLen {
			replicatedFormatStringSubset = event[formatType].formatString[formatStringLeftBracketIndex+1 : formatStringRightBracketIndex]
			formattedRecord = formattedRecord + fmt.Sprintf(replicatedFormatStringSubset, arg1U64Slice[0])
			for arg1U64SliceIndex = uint32(1); arg1U64SliceIndex < arg1U64SliceLen; arg1U64SliceIndex++ {
				formattedRecord = formattedRecord + " " + fmt.Sprintf(replicatedFormatStringSubset, arg1U64Slice[arg1U64SliceIndex])
			}
		}

		formattedRecord = formattedRecord + fmt.Sprintf(event[formatType].formatString[formatStringRightBracketIndex:])
	case patternS016XS:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8

		arg2StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg2String = string(record[recordPosition : recordPosition+arg2StringLen])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1U64, arg2String)
	case patternSS:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String)
	case patternSS03D:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])
		recordPosition += arg1StringLen

		arg2U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String, arg2U32)
	case patternSS016X1X:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])
		recordPosition += arg1StringLen

		arg2U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8
		arg3U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String, arg2U64, arg3U32)
	case patternSS016X1X1X:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])
		recordPosition += arg1StringLen

		arg2U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8
		arg3U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg4U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String, arg2U64, arg3U32, arg4U32)
	case patternSSS:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])
		recordPosition += arg1StringLen

		arg2StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg2String = string(record[recordPosition : recordPosition+arg2StringLen])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String, arg2String)
	case patternSSS03D:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])
		recordPosition += arg1StringLen

		arg2StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg2String = string(record[recordPosition : recordPosition+arg2StringLen])
		recordPosition += arg2StringLen

		arg3U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String, arg2String, arg3U32)
	case patternSSS016X03D:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])
		recordPosition += arg1StringLen

		arg2StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg2String = string(record[recordPosition : recordPosition+arg2StringLen])
		recordPosition += arg2StringLen

		arg3U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8

		arg4U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String, arg2String, arg3U64, arg4U32)
	case patternSSS016X016X03D:
		arg0StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg0String = string(record[recordPosition : recordPosition+arg0StringLen])
		recordPosition += arg0StringLen

		arg1StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg1String = string(record[recordPosition : recordPosition+arg1StringLen])
		recordPosition += arg1StringLen

		arg2StringLen = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])
		recordPosition += 4
		arg2String = string(record[recordPosition : recordPosition+arg2StringLen])
		recordPosition += arg2StringLen

		arg3U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8

		arg4U64 = binary.LittleEndian.Uint64(record[recordPosition : recordPosition+8])
		recordPosition += 8

		arg5U32 = binary.LittleEndian.Uint32(record[recordPosition : recordPosition+4])

		formattedRecord = fmt.Sprintf(event[formatType].formatString, timestamp.Format(patternTimestampFormat), arg0String, arg1String, arg2String, arg3U64, arg4U64, arg5U32)
	default:
		err = fmt.Errorf("Unrecognized patternType (%v) for formatType %v", pattern, formatType)
		panic(err)
	}

	return
}

func markForDeletion() {
	var (
		err        error
		errno      error
		rmidResult C.int
	)

	rmidResult, errno = C.shmctl(globals.shmID, C.IPC_RMID, nil)

	if C.int(-1) == rmidResult {
		globals.eventLogEnabled = false
		err = fmt.Errorf("C.shmctl(globals.shmID, C.IPC_RMID, nil) failed with errno: %v", errno)
		panic(err)
	}
}
