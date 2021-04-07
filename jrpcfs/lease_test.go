// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/retryrpc"
)

const (
	testRpcLeaseDelayAfterSendingRequest            = 10 * time.Millisecond
	testRpcLeaseDelayBeforeSendingRequest           = 10 * time.Millisecond
	testRpcLeaseRetryRPCDeadlineIO                  = "60s"
	testRpcLeaseRetryRPCKeepAlivePeriod             = "60s"
	testRpcLeaseMultiFirstInodeNumber        int64  = 1
	testRpcLeaseMultiNumInstances            int    = 5
	testRpcLeaseSingleInodeNumber            int64  = 1
	testRpcLeaseSingleNumInstances           int    = 101 // Must be >= 4
	testRpcLeaseTimeFormat                          = "15:04:05.000"
	testRpcLeaseLocalClientID                uint64 = 0
	testRpcLeaseShortcutIPAddrPort                  = "127.0.0.1:24680"
	testRpcLeaseShortcutClientConnMaxRetries        = 10
	testRpcLeaseShortcutClientConnRetryDelay        = "100ms"
)

var (
	testRpcLeaseRequestLetters   = [5]string{"S", "P", "E", "D", "R"}
	testRpcLeaseReplyLetters     = [6]string{"D", "S", "P", "E", "D", "R"}
	testRpcLeaseInterruptLetters = [3]string{"U", "D", "R"}
	testRpcLeaseLogVerbosely     bool
)

type testRpcLeaseClientStruct struct {
	instance         int
	inodeNumber      int64
	chIn             chan LeaseRequestType // close it to terminate testRpcLeaseClient instance
	chOut            chan interface{}      // either a LeaseReplyType or an RPCInterruptType
	alreadyUnmounted bool                  // if true, no RpcUnmount will be issued
	wg               *sync.WaitGroup       // signaled when testRpcLeaseClient instance exits
	t                *testing.T
}

type benchmarkRpcShortcutRequestMethodOnlyStruct struct { // preceeded by a uint32 encoding length in LittleEndian form
	Method string
}

type benchmarkRpcShortcutLeaseRequestStruct struct { // preceeded by a uint32 encoding length in LittleEndian form
	Method  string
	Request *LeaseRequest
}

type benchmarkRpcShortcutMountByAccountNameRequestStruct struct { // preceeded by a uint32 encoding length in LittleEndian form
	Method  string
	Request *MountByAccountNameRequest
}

type benchmarkRpcShortcutUnmountRequestStruct struct { // preceeded by a uint32 encoding length in LittleEndian form
	Method  string
	Request *UnmountRequest
}

type benchmarkRpcShortcutLeaseReplyStruct struct { // preceeded by a uint32 encoding length in LittleEndian form
	Err   error
	Reply *LeaseReply
}

type benchmarkRpcShortcutMountByAccountNameReplyStruct struct { // preceeded by a uint32 encoding length in LittleEndian form
	Err   error
	Reply *MountByAccountNameReply
}

type benchmarkRpcShortcutUnmountReplyStruct struct { // preceeded by a uint32 encoding length in LittleEndian form
	Err   error
	Reply *Reply
}

// benchmarkRpcLeaseCustomRequest's are laid out as follows:
//
//   RequestLength uint32 - [LittleEndian] does not include RequestLength nor RequestType
//   RequestType   uint32 - [LittleEndian] one of benchmarkRpcLeaseCustomRequestType*
//   Request       []byte - optimally packed request of the corresponding type

// benchmarkRpcLeaseCustomReply's are laid out as follows:
//
//   ReplyLenth uint32 - does not include ReplyLength
//   Reply      []byte - optimally packed RPC error string followed by the reply of the corresponding type

// In each of the marshaled forms, strings consist of:
//
//   StringLength uint32 - [LittleEndian] length of UTF-8 string
//   StringBuf    []byte - UTF-8 string

const (
	benchmarkRpcLeaseCustomRequestTypeMountByAccountName uint32 = iota
	benchmarkRpcLeaseCustomRequestTypeLease
	benchmarkRpcLeaseCustomRequestTypeUnmount
)

func benchmarkRpcLeaseCustomMountByAccountNameRequestMarshal(mountByAccountNameRequest *MountByAccountNameRequest) (mountByAccountNameRequestBuf []byte) {
	var (
		accountNameLen = uint32(len(mountByAccountNameRequest.AccountName))
		authTokenLen   = uint32(len(mountByAccountNameRequest.AuthToken))
		bytesBuffer    *bytes.Buffer
		err            error
		n              int
	)

	bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+accountNameLen+4+authTokenLen))

	err = binary.Write(bytesBuffer, binary.LittleEndian, accountNameLen)
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, accountNameLen) failed: %v", err))
	}

	n, err = bytesBuffer.WriteString(mountByAccountNameRequest.AccountName)
	if nil != err {
		panic(fmt.Errorf("bytesBuffer.WriteString(mountByAccountNameRequest.AccountName) failed: %v", err))
	}
	if uint32(n) != accountNameLen {
		panic(fmt.Errorf("bytesBuffer.WriteString(mountByAccountNameRequest.AccountName) returned n==%v (expected %v)", n, accountNameLen))
	}

	err = binary.Write(bytesBuffer, binary.LittleEndian, authTokenLen)
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, authTokenLen) failed: %v", err))
	}

	n, err = bytesBuffer.WriteString(mountByAccountNameRequest.AuthToken)
	if nil != err {
		panic(fmt.Errorf("bytesBuffer.WriteString(mountByAccountNameRequest.AuthToken) failed: %v", err))
	}
	if uint32(n) != authTokenLen {
		panic(fmt.Errorf("bytesBuffer.WriteString(mountByAccountNameRequest.AuthToken) returned n==%v (expected %v)", n, authTokenLen))
	}

	mountByAccountNameRequestBuf = bytesBuffer.Bytes()

	return
}

func benchmarkRpcLeaseCustomMountByAccountNameRequestUnmarshal(mountByAccountNameRequestBuf []byte) (mountByAccountNameRequest *MountByAccountNameRequest, unmarshalErr error) {
	var (
		accountNameLen                  uint32
		authTokenLen                    uint32
		mountByAccountNameRequestBufLen        = uint32(len(mountByAccountNameRequestBuf))
		mountByAccountNameRequestBufPos uint32 = 0
	)

	mountByAccountNameRequest = &MountByAccountNameRequest{}

	if mountByAccountNameRequestBufPos+4 > mountByAccountNameRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read accountNameLen")
		return
	}

	accountNameLen = binary.LittleEndian.Uint32(mountByAccountNameRequestBuf[mountByAccountNameRequestBufPos : mountByAccountNameRequestBufPos+4])
	mountByAccountNameRequestBufPos += 4

	if mountByAccountNameRequestBufPos+accountNameLen > mountByAccountNameRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read AccountName")
		return
	}

	mountByAccountNameRequest.AccountName = string(mountByAccountNameRequestBuf[mountByAccountNameRequestBufPos : mountByAccountNameRequestBufPos+accountNameLen])
	mountByAccountNameRequestBufPos += accountNameLen

	if mountByAccountNameRequestBufPos+4 > mountByAccountNameRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read authTokenLen")
		return
	}

	authTokenLen = binary.LittleEndian.Uint32(mountByAccountNameRequestBuf[mountByAccountNameRequestBufPos : mountByAccountNameRequestBufPos+4])
	mountByAccountNameRequestBufPos += 4

	if mountByAccountNameRequestBufPos+authTokenLen > mountByAccountNameRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read AuthToken")
		return
	}

	mountByAccountNameRequest.AuthToken = string(mountByAccountNameRequestBuf[mountByAccountNameRequestBufPos : mountByAccountNameRequestBufPos+authTokenLen])
	mountByAccountNameRequestBufPos += authTokenLen

	unmarshalErr = nil
	return
}

func benchmarkRpcLeaseCustomMountByAccountNameReplyMarshal(rpcErr error, mountByAccountNameReply *MountByAccountNameReply) (mountByAccountNameReplyBuf []byte) {
	var (
		bytesBuffer     *bytes.Buffer
		err             error
		mountIDLen      = uint32(len(mountByAccountNameReply.MountID))
		n               int
		rpcErrString    string
		rpcErrStringLen uint32
	)

	if nil == rpcErr {
		bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+4+mountIDLen))

		err = binary.Write(bytesBuffer, binary.LittleEndian, uint32(0))
		if nil != err {
			panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, uint32(0)) failed: %v", err))
		}
	} else {
		rpcErrString = rpcErr.Error()
		rpcErrStringLen = uint32(len(rpcErrString))

		bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+rpcErrStringLen+4+mountIDLen))

		err = binary.Write(bytesBuffer, binary.LittleEndian, rpcErrStringLen)
		if nil != err {
			panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, rpcErrStringLen) failed: %v", err))
		}

		n, err = bytesBuffer.WriteString(string(rpcErrString))
		if nil != err {
			panic(fmt.Errorf("bytesBuffer.WriteString(rpcErrString) failed: %v", err))
		}
		if uint32(n) != rpcErrStringLen {
			panic(fmt.Errorf("bytesBuffer.WriteString(rpcErrString) returned n==%v (expected %v)", n, rpcErrStringLen))
		}
	}

	err = binary.Write(bytesBuffer, binary.LittleEndian, mountIDLen)
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, mountIDLen) failed: %v", err))
	}

	n, err = bytesBuffer.WriteString(string(mountByAccountNameReply.MountID))
	if nil != err {
		panic(fmt.Errorf("bytesBuffer.WriteString(mountByAccountNameReply.MountID) failed: %v", err))
	}
	if uint32(n) != mountIDLen {
		panic(fmt.Errorf("bytesBuffer.WriteString(mountByAccountNameReply.MountID) returned n==%v (expected %v)", n, mountIDLen))
	}

	mountByAccountNameReplyBuf = bytesBuffer.Bytes()

	return
}

func benchmarkRpcLeaseCustomMountByAccountNameReplyUnmarshal(mountByAccountNameReplyBuf []byte) (rpcErr error, mountByAccountNameReply *MountByAccountNameReply, unmarshalErr error) {
	var (
		mountByAccountNameReplyBufLen        = uint32(len(mountByAccountNameReplyBuf))
		mountByAccountNameReplyBufPos uint32 = 0
		mountIDLen                    uint32
		rpcErrAsString                string
		rpcErrLen                     uint32
	)

	if mountByAccountNameReplyBufPos+4 > mountByAccountNameReplyBufLen {
		unmarshalErr = fmt.Errorf("couldn't read rpcErrLen")
		return
	}

	rpcErrLen = binary.LittleEndian.Uint32(mountByAccountNameReplyBuf[mountByAccountNameReplyBufPos : mountByAccountNameReplyBufPos+4])
	mountByAccountNameReplyBufPos += 4

	if 0 == rpcErrLen {
		rpcErr = nil
	} else {
		if mountByAccountNameReplyBufPos+rpcErrLen > mountByAccountNameReplyBufLen {
			unmarshalErr = fmt.Errorf("couldn't read rpcErr")
			return
		}

		rpcErrAsString = string(mountByAccountNameReplyBuf[mountByAccountNameReplyBufPos : mountByAccountNameReplyBufPos+rpcErrLen])
		mountByAccountNameReplyBufPos += rpcErrLen

		rpcErr = fmt.Errorf("%s", rpcErrAsString)
	}

	mountByAccountNameReply = &MountByAccountNameReply{}

	if mountByAccountNameReplyBufPos+4 > mountByAccountNameReplyBufLen {
		unmarshalErr = fmt.Errorf("couldn't read mountIDLen")
		return
	}

	mountIDLen = binary.LittleEndian.Uint32(mountByAccountNameReplyBuf[mountByAccountNameReplyBufPos : mountByAccountNameReplyBufPos+4])
	mountByAccountNameReplyBufPos += 4

	if mountByAccountNameReplyBufPos+mountIDLen > mountByAccountNameReplyBufLen {
		unmarshalErr = fmt.Errorf("couldn't read MountID")
		return
	}

	mountByAccountNameReply.MountID = MountIDAsString(string(mountByAccountNameReplyBuf[mountByAccountNameReplyBufPos : mountByAccountNameReplyBufPos+mountIDLen]))
	mountByAccountNameReplyBufPos += mountIDLen

	unmarshalErr = nil
	return
}

func benchmarkRpcLeaseCustomLeaseRequestMarshal(leaseRequest *LeaseRequest) (leaseRequestBuf []byte) {
	var (
		bytesBuffer *bytes.Buffer
		err         error
		mountIDLen  = uint32(len(leaseRequest.InodeHandle.MountID))
		n           int
	)

	bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+mountIDLen+8+4))

	err = binary.Write(bytesBuffer, binary.LittleEndian, mountIDLen)
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, mountIDLen) failed: %v", err))
	}

	n, err = bytesBuffer.WriteString(string(leaseRequest.InodeHandle.MountID))
	if nil != err {
		panic(fmt.Errorf("bytesBuffer.WriteString(leaseRequest.InodeHandle.MountID) failed: %v", err))
	}
	if uint32(n) != mountIDLen {
		panic(fmt.Errorf("bytesBuffer.WriteString(leaseRequest.InodeHandle.MountID) returned n==%v (expected %v)", n, mountIDLen))
	}

	err = binary.Write(bytesBuffer, binary.LittleEndian, uint64(leaseRequest.InodeHandle.InodeNumber))
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, uint64(leaseRequest.InodeHandle.InodeNumber)) failed: %v", err))
	}

	err = binary.Write(bytesBuffer, binary.LittleEndian, leaseRequest.LeaseRequestType)
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, leaseRequest.LeaseRequestType) failed: %v", err))
	}

	leaseRequestBuf = bytesBuffer.Bytes()

	return
}

func benchmarkRpcLeaseCustomLeaseRequestUnmarshal(leaseRequestBuf []byte) (leaseRequest *LeaseRequest, unmarshalErr error) {
	var (
		leaseRequestBufLen        = uint32(len(leaseRequestBuf))
		leaseRequestBufPos uint32 = 0
		mountIDLen         uint32
	)

	leaseRequest = &LeaseRequest{}

	if leaseRequestBufPos+4 > leaseRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read mountIDLen")
		return
	}

	mountIDLen = binary.LittleEndian.Uint32(leaseRequestBuf[leaseRequestBufPos : leaseRequestBufPos+4])
	leaseRequestBufPos += 4

	if leaseRequestBufPos+mountIDLen > leaseRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read MountID")
		return
	}

	leaseRequest.InodeHandle.MountID = MountIDAsString(string(leaseRequestBuf[leaseRequestBufPos : leaseRequestBufPos+mountIDLen]))
	leaseRequestBufPos += mountIDLen

	if leaseRequestBufPos+8 > leaseRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read InodeNumber")
		return
	}

	leaseRequest.InodeHandle.InodeNumber = int64(binary.LittleEndian.Uint64(leaseRequestBuf[leaseRequestBufPos : leaseRequestBufPos+8]))
	leaseRequestBufPos += 8

	if leaseRequestBufPos+4 > leaseRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read LeaseRequestType")
		return
	}

	leaseRequest.LeaseRequestType = LeaseRequestType(binary.LittleEndian.Uint32(leaseRequestBuf[leaseRequestBufPos : leaseRequestBufPos+4]))
	leaseRequestBufPos += 4

	unmarshalErr = nil
	return
}

func benchmarkRpcLeaseCustomLeaseReplyMarshal(rpcErr error, leaseReply *LeaseReply) (leaseReplyBuf []byte) {
	var (
		bytesBuffer     *bytes.Buffer
		err             error
		n               int
		rpcErrString    string
		rpcErrStringLen uint32
	)

	if nil == rpcErr {
		bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+4))

		err = binary.Write(bytesBuffer, binary.LittleEndian, uint32(0))
		if nil != err {
			panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, uint32(0)) failed: %v", err))
		}
	} else {
		rpcErrString = rpcErr.Error()
		rpcErrStringLen = uint32(len(rpcErrString))

		bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+rpcErrStringLen+4))

		err = binary.Write(bytesBuffer, binary.LittleEndian, rpcErrStringLen)
		if nil != err {
			panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, rpcErrStringLen) failed: %v", err))
		}

		n, err = bytesBuffer.WriteString(string(rpcErrString))
		if nil != err {
			panic(fmt.Errorf("bytesBuffer.WriteString(rpcErrString) failed: %v", err))
		}
		if uint32(n) != rpcErrStringLen {
			panic(fmt.Errorf("bytesBuffer.WriteString(rpcErrString) returned n==%v (expected %v)", n, rpcErrStringLen))
		}
	}

	err = binary.Write(bytesBuffer, binary.LittleEndian, leaseReply.LeaseReplyType)
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, leaseReply.LeaseReplyType) failed: %v", err))
	}

	leaseReplyBuf = bytesBuffer.Bytes()

	return
}

func benchmarkRpcLeaseCustomLeaseReplyUnmarshal(leaseReplyBuf []byte) (rpcErr error, leaseReply *LeaseReply, unmarshalErr error) {
	var (
		leaseReplyBufLen        = uint32(len(leaseReplyBuf))
		leaseReplyBufPos uint32 = 0
		rpcErrAsString   string
		rpcErrLen        uint32
	)

	if leaseReplyBufPos+4 > leaseReplyBufLen {
		unmarshalErr = fmt.Errorf("couldn't read rpcErrLen")
		return
	}

	rpcErrLen = binary.LittleEndian.Uint32(leaseReplyBuf[leaseReplyBufPos : leaseReplyBufPos+4])
	leaseReplyBufPos += 4

	if 0 == rpcErrLen {
		rpcErr = nil
	} else {
		if leaseReplyBufPos+rpcErrLen > leaseReplyBufLen {
			unmarshalErr = fmt.Errorf("couldn't read rpcErr")
			return
		}

		rpcErrAsString = string(leaseReplyBuf[leaseReplyBufPos : leaseReplyBufPos+rpcErrLen])
		leaseReplyBufPos += rpcErrLen

		rpcErr = fmt.Errorf("%s", rpcErrAsString)
	}

	leaseReply = &LeaseReply{}

	if leaseReplyBufPos+4 > leaseReplyBufLen {
		unmarshalErr = fmt.Errorf("couldn't read LeaseReplyType")
		return
	}

	leaseReply.LeaseReplyType = LeaseReplyType(binary.LittleEndian.Uint32(leaseReplyBuf[leaseReplyBufPos : leaseReplyBufPos+4]))
	leaseReplyBufPos += 4

	unmarshalErr = nil
	return
}

func benchmarkRpcLeaseCustomUnmountRequestMarshal(unmountRequest *UnmountRequest) (unmountRequestBuf []byte) {
	var (
		bytesBuffer *bytes.Buffer
		err         error
		mountIDLen  = uint32(len(unmountRequest.MountID))
		n           int
	)

	bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+mountIDLen))

	err = binary.Write(bytesBuffer, binary.LittleEndian, mountIDLen)
	if nil != err {
		panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, mountIDLen) failed: %v", err))
	}

	n, err = bytesBuffer.WriteString(string(unmountRequest.MountID))
	if nil != err {
		panic(fmt.Errorf("bytesBuffer.WriteString(unmountRequest.MountID) failed: %v", err))
	}
	if uint32(n) != mountIDLen {
		panic(fmt.Errorf("bytesBuffer.WriteString(unmountRequest.MountID) returned n==%v (expected %v)", n, mountIDLen))
	}

	unmountRequestBuf = bytesBuffer.Bytes()

	return
}

func benchmarkRpcLeaseCustomUnmountRequestUnmarshal(unmountRequestBuf []byte) (unmountRequest *UnmountRequest, unmarshalErr error) {
	var (
		unmountRequestBufLen        = uint32(len(unmountRequestBuf))
		unmountRequestBufPos uint32 = 0
		mountIDLen           uint32
	)

	unmountRequest = &UnmountRequest{}

	if unmountRequestBufPos+4 > unmountRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read mountIDLen")
		return
	}

	mountIDLen = binary.LittleEndian.Uint32(unmountRequestBuf[unmountRequestBufPos : unmountRequestBufPos+4])
	unmountRequestBufPos += 4

	if unmountRequestBufPos+mountIDLen > unmountRequestBufLen {
		unmarshalErr = fmt.Errorf("couldn't read MountID")
		return
	}

	unmountRequest.MountID = MountIDAsString(string(unmountRequestBuf[unmountRequestBufPos : unmountRequestBufPos+mountIDLen]))
	unmountRequestBufPos += mountIDLen

	unmarshalErr = nil
	return
}

func benchmarkRpcLeaseCustomUnmountReplyMarshal(rpcErr error, unmountReply *Reply) (unmountReplyBuf []byte) {
	var (
		bytesBuffer     *bytes.Buffer
		err             error
		n               int
		rpcErrString    string
		rpcErrStringLen uint32
	)

	if nil == rpcErr {
		bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4))

		err = binary.Write(bytesBuffer, binary.LittleEndian, uint32(0))
		if nil != err {
			panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, uint32(0)) failed: %v", err))
		}
	} else {
		rpcErrString = rpcErr.Error()
		rpcErrStringLen = uint32(len(rpcErrString))

		bytesBuffer = bytes.NewBuffer(make([]byte, 0, 4+rpcErrStringLen))

		err = binary.Write(bytesBuffer, binary.LittleEndian, rpcErrStringLen)
		if nil != err {
			panic(fmt.Errorf("binary.Write(bytesBuffer, binary.LittleEndian, rpcErrStringLen) failed: %v", err))
		}

		n, err = bytesBuffer.WriteString(string(rpcErrString))
		if nil != err {
			panic(fmt.Errorf("bytesBuffer.WriteString(rpcErrString) failed: %v", err))
		}
		if uint32(n) != rpcErrStringLen {
			panic(fmt.Errorf("bytesBuffer.WriteString(rpcErrString) returned n==%v (expected %v)", n, rpcErrStringLen))
		}
	}

	unmountReplyBuf = bytesBuffer.Bytes()

	return
}

func benchmarkRpcLeaseCustomUnmountReplyUnmarshal(unmountReplyBuf []byte) (rpcErr error, unmountReply *Reply, unmarshalErr error) {
	var (
		unmountReplyBufLen        = uint32(len(unmountReplyBuf))
		unmountReplyBufPos uint32 = 0
		rpcErrAsString     string
		rpcErrLen          uint32
	)

	if unmountReplyBufPos+4 > unmountReplyBufLen {
		unmarshalErr = fmt.Errorf("couldn't read rpcErrLen")
		return
	}

	rpcErrLen = binary.LittleEndian.Uint32(unmountReplyBuf[unmountReplyBufPos : unmountReplyBufPos+4])
	unmountReplyBufPos += 4

	if 0 == rpcErrLen {
		rpcErr = nil
	} else {
		if unmountReplyBufPos+rpcErrLen > unmountReplyBufLen {
			unmarshalErr = fmt.Errorf("couldn't read rpcErr")
			return
		}

		rpcErrAsString = string(unmountReplyBuf[unmountReplyBufPos : unmountReplyBufPos+rpcErrLen])
		unmountReplyBufPos += rpcErrLen

		rpcErr = fmt.Errorf("%s", rpcErrAsString)
	}

	unmountReply = &Reply{}

	unmarshalErr = nil
	return
}

func BenchmarkRpcLeaseCustomTCP(b *testing.B) {
	benchmarkRpcLeaseCustom(b, false)
}

func BenchmarkRpcLeaseCustomTLS(b *testing.B) {
	benchmarkRpcLeaseCustom(b, true)
}

func benchmarkRpcLeaseCustom(b *testing.B, useTLS bool) {
	var (
		benchmarkIteration           int
		doneWG                       sync.WaitGroup
		err                          error
		leaseReply                   *LeaseReply
		leaseReplyBuf                []byte
		leaseRequest                 *LeaseRequest
		leaseRequestBuf              []byte
		mountByAccountNameReply      *MountByAccountNameReply
		mountByAccountNameReplyBuf   []byte
		mountByAccountNameRequest    *MountByAccountNameRequest
		mountByAccountNameRequestBuf []byte
		netConn                      net.Conn
		netConnRetries               int
		netConnRetryDelay            time.Duration
		ok                           bool
		rootCACertPool               *x509.CertPool
		rpcErr                       error
		tlsConfig                    *tls.Config
		tlsConn                      *tls.Conn
		unmarshalErr                 error
		unmountReplyBuf              []byte
		unmountRequest               *UnmountRequest
		unmountRequestBuf            []byte
	)

	doneWG.Add(1)
	go benchmarkRpcLeaseCustomServer(useTLS, &doneWG)

	netConnRetries = 0

	netConnRetryDelay, err = time.ParseDuration(testRpcLeaseShortcutClientConnRetryDelay)
	if nil != err {
		b.Fatalf("time.ParseDuration(testRpcLeaseShortcutClientConnRetryDelay=\"%s\") failed: %v", testRpcLeaseShortcutClientConnRetryDelay, err)
	}

	if useTLS {
		rootCACertPool = x509.NewCertPool()
		ok = rootCACertPool.AppendCertsFromPEM(testTLSCerts.caCertPEMBlock)
		if !ok {
			b.Fatalf("rootCACertPool.AppendCertsFromPEM(testTLSCerts.caCertPEMBlock) returned !ok")
		}
		tlsConfig = &tls.Config{
			RootCAs: rootCACertPool,
		}
	}

	for {
		if useTLS {
			tlsConn, err = tls.Dial("tcp", testRpcLeaseShortcutIPAddrPort, tlsConfig)
			if nil == err {
				netConn = tlsConn
				break
			}
		} else {
			netConn, err = net.Dial("tcp", testRpcLeaseShortcutIPAddrPort)
			if nil == err {
				break
			}
		}

		netConnRetries++

		if netConnRetries > testRpcLeaseShortcutClientConnMaxRetries {
			b.Fatalf("netConnRetries exceeded testRpcLeaseShortcutClientConnMaxRetries (%v)", testRpcLeaseShortcutClientConnMaxRetries)
		}

		time.Sleep(netConnRetryDelay)
	}

	mountByAccountNameRequest = &MountByAccountNameRequest{
		AccountName: testAccountName,
		AuthToken:   "",
	}

	mountByAccountNameRequestBuf = benchmarkRpcLeaseCustomMountByAccountNameRequestMarshal(mountByAccountNameRequest)

	mountByAccountNameReplyBuf = benchmarkRpcLeaseCustomDoRequest(b, netConn, benchmarkRpcLeaseCustomRequestTypeMountByAccountName, mountByAccountNameRequestBuf)

	rpcErr, mountByAccountNameReply, unmarshalErr = benchmarkRpcLeaseCustomMountByAccountNameReplyUnmarshal(mountByAccountNameReplyBuf)
	if nil != unmarshalErr {
		b.Fatalf("benchmarkRpcLeaseCustomMountByAccountNameReplyUnmarshal(mountByAccountNameReplyBuf) returned unmarshalErr: %v", unmarshalErr)
	}
	if nil != rpcErr {
		b.Fatalf("benchmarkRpcLeaseCustomMountByAccountNameReplyUnmarshal(mountByAccountNameReplyBuf) returned rpcErr: %v", rpcErr)
	}

	b.ResetTimer()

	for benchmarkIteration = 0; benchmarkIteration < b.N; benchmarkIteration++ {
		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeExclusive,
		}

		leaseRequestBuf = benchmarkRpcLeaseCustomLeaseRequestMarshal(leaseRequest)

		leaseReplyBuf = benchmarkRpcLeaseCustomDoRequest(b, netConn, benchmarkRpcLeaseCustomRequestTypeLease, leaseRequestBuf)

		rpcErr, leaseReply, unmarshalErr = benchmarkRpcLeaseCustomLeaseReplyUnmarshal(leaseReplyBuf)
		if nil != unmarshalErr {
			b.Fatalf("benchmarkRpcLeaseCustomLeaseReplyUnmarshal(leaseReplyBuf) returned unmarshalErr: %v", unmarshalErr)
		}
		if nil != rpcErr {
			b.Fatalf("benchmarkRpcLeaseCustomLeaseReplyUnmarshal(leaseReplyBuf) returned rpcErr: %v", rpcErr)
		}

		if LeaseReplyTypeExclusive != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseRequestTypeExclusive", leaseReply.LeaseReplyType)
		}

		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeRelease,
		}

		leaseRequestBuf = benchmarkRpcLeaseCustomLeaseRequestMarshal(leaseRequest)

		leaseReplyBuf = benchmarkRpcLeaseCustomDoRequest(b, netConn, benchmarkRpcLeaseCustomRequestTypeLease, leaseRequestBuf)

		rpcErr, leaseReply, unmarshalErr = benchmarkRpcLeaseCustomLeaseReplyUnmarshal(leaseReplyBuf)
		if nil != unmarshalErr {
			b.Fatalf("benchmarkRpcLeaseCustomLeaseReplyUnmarshal(leaseReplyBuf) returned unmarshalErr: %v", unmarshalErr)
		}
		if nil != rpcErr {
			b.Fatalf("benchmarkRpcLeaseCustomLeaseReplyUnmarshal(leaseReplyBuf) returned rpcErr: %v", rpcErr)
		}

		if LeaseReplyTypeReleased != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseReplyTypeReleased", leaseReply.LeaseReplyType)
		}
	}

	b.StopTimer()

	unmountRequest = &UnmountRequest{
		MountID: mountByAccountNameReply.MountID,
	}

	unmountRequestBuf = benchmarkRpcLeaseCustomUnmountRequestMarshal(unmountRequest)

	unmountReplyBuf = benchmarkRpcLeaseCustomDoRequest(b, netConn, benchmarkRpcLeaseCustomRequestTypeUnmount, unmountRequestBuf)

	rpcErr, _, unmarshalErr = benchmarkRpcLeaseCustomUnmountReplyUnmarshal(unmountReplyBuf)
	if nil != unmarshalErr {
		b.Fatalf("benchmarkRpcLeaseCustomUnmountReplyUnmarshal(unmountReplyBuf) returned unmarshalErr: %v", unmarshalErr)
	}
	if nil != rpcErr {
		b.Fatalf("benchmarkRpcLeaseCustomUnmountReplyUnmarshal(unmountReplyBuf) returned rpcErr: %v", rpcErr)
	}

	err = netConn.Close()
	if nil != err {
		b.Fatalf("netConn.Close() failed: %v", err)
	}

	doneWG.Wait()
}

func benchmarkRpcLeaseCustomServer(useTLS bool, doneWG *sync.WaitGroup) {
	var (
		err                         error
		jserver                     *Server
		leaseReply                  *LeaseReply
		leaseRequest                *LeaseRequest
		mountByAccountNameReply     *MountByAccountNameReply
		mountByAccountNameRequest   *MountByAccountNameRequest
		n                           int
		netConn                     net.Conn
		netListener                 net.Listener
		replyBuf                    []byte
		replyLen                    uint32
		replyLenBuf                 []byte
		requestBuf                  []byte
		requestLen                  uint32
		requestLenAndRequestTypeBuf []byte
		requestType                 uint32
		tlsConfig                   *tls.Config
		unmountReply                *Reply
		unmountRequest              *UnmountRequest
	)

	jserver = NewServer()

	netListener, err = net.Listen("tcp", testRpcLeaseShortcutIPAddrPort)
	if nil != err {
		panic(fmt.Errorf("net.Listen(\"tcp\", testRpcLeaseShortcutIPAddrPort) failed: %v", err))
	}

	if useTLS {
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{testTLSCerts.endpointTLSCert},
		}

		netListener = tls.NewListener(netListener, tlsConfig)
	}

	netConn, err = netListener.Accept()
	if nil != err {
		panic(fmt.Errorf("netListener.Accept() failed: %v", err))
	}

	for {
		requestLenAndRequestTypeBuf = make([]byte, 8)

		n, err = netConn.Read(requestLenAndRequestTypeBuf)
		if nil != err {
			if io.EOF != err {
				panic(fmt.Errorf("netConn.Read(requestLenAndRequestTypeBuf) failed: %v", err))
			}

			err = netConn.Close()
			if nil != err {
				panic(fmt.Errorf("netConn.Close() failed: %v", err))
			}

			err = netListener.Close()
			if nil != err {
				panic(fmt.Errorf("netListener.Close() failed: %v", err))
			}

			doneWG.Done()

			return
		}
		if n != 8 {
			panic(fmt.Errorf("netConn.Read(requestLenAndRequestTypeBuf) returned n == %v (expected 8)", n))
		}

		requestLen = binary.LittleEndian.Uint32(requestLenAndRequestTypeBuf[:4])
		requestType = binary.LittleEndian.Uint32(requestLenAndRequestTypeBuf[4:])

		requestBuf = make([]byte, requestLen)

		n, err = netConn.Read(requestBuf)
		if nil != err {
			panic(fmt.Errorf("netConn.Read(requestBuf) failed: %v", err))
		}
		if n != int(requestLen) {
			panic(fmt.Errorf("netConn.Read(requestBuf) returned n == %v (expected %v)", n, requestLen))
		}

		switch requestType {
		case benchmarkRpcLeaseCustomRequestTypeMountByAccountName:
			mountByAccountNameRequest, err = benchmarkRpcLeaseCustomMountByAccountNameRequestUnmarshal(requestBuf)
			if nil != err {
				panic(fmt.Errorf("benchmarkRpcLeaseCustomMountByAccountNameRequestUnmarshal(requestBuf) failed: %v", err))
			}

			mountByAccountNameReply = &MountByAccountNameReply{}

			err = jserver.RpcMountByAccountName(testRpcLeaseLocalClientID, mountByAccountNameRequest, mountByAccountNameReply)

			replyBuf = benchmarkRpcLeaseCustomMountByAccountNameReplyMarshal(err, mountByAccountNameReply)
		case benchmarkRpcLeaseCustomRequestTypeLease:
			leaseRequest, err = benchmarkRpcLeaseCustomLeaseRequestUnmarshal(requestBuf)
			if nil != err {
				panic(fmt.Errorf("benchmarkRpcLeaseCustomLeaseRequestUnmarshal(requestBuf) failed: %v", err))
			}

			leaseReply = &LeaseReply{}

			err = jserver.RpcLease(leaseRequest, leaseReply)

			replyBuf = benchmarkRpcLeaseCustomLeaseReplyMarshal(err, leaseReply)
		case benchmarkRpcLeaseCustomRequestTypeUnmount:
			unmountRequest, err = benchmarkRpcLeaseCustomUnmountRequestUnmarshal(requestBuf)
			if nil != err {
				panic(fmt.Errorf("benchmarkRpcLeaseCustomUnmountRequestUnmarshal(requestBuf) failed: %v", err))
			}

			unmountReply = &Reply{}

			err = jserver.RpcUnmount(unmountRequest, unmountReply)

			replyBuf = benchmarkRpcLeaseCustomUnmountReplyMarshal(err, unmountReply)
		default:
			panic(fmt.Errorf("requestType (%v) not recognized", requestType))
		}

		replyLen = uint32(len(replyBuf))
		replyLenBuf = make([]byte, 4)
		binary.LittleEndian.PutUint32(replyLenBuf, replyLen)

		n, err = netConn.Write(replyLenBuf)
		if nil != err {
			panic(fmt.Errorf("netConn.Write(replyLenBuf) failed: %v", err))
		}
		if n != 4 {
			panic(fmt.Errorf("netConn.Write(replyLenBuf) returned n == %v (expected 4)", n))
		}

		n, err = netConn.Write(replyBuf)
		if nil != err {
			panic(fmt.Errorf("netConn.Write(replyBuf) failed: %v", err))
		}
		if n != int(replyLen) {
			panic(fmt.Errorf("netConn.Write(replyBuf) returned n == %v (expected %v)", n, replyLen))
		}
	}
}

func benchmarkRpcLeaseCustomDoRequest(b *testing.B, netConn net.Conn, requestType uint32, requestBuf []byte) (replyBuf []byte) {
	var (
		err                         error
		n                           int
		replyLen                    uint32
		replyLenBuf                 []byte
		requestLen                  uint32 = uint32(len(requestBuf))
		requestLenAndRequestTypeBuf []byte
	)

	requestLenAndRequestTypeBuf = make([]byte, 8)

	binary.LittleEndian.PutUint32(requestLenAndRequestTypeBuf[:4], requestLen)
	binary.LittleEndian.PutUint32(requestLenAndRequestTypeBuf[4:], requestType)

	n, err = netConn.Write(requestLenAndRequestTypeBuf)
	if nil != err {
		b.Fatalf("netConn.Write(requestLenAndRequestTypeBuf) failed: %v", err)
	}
	if n != 8 {
		b.Fatalf("netConn.Write(requestLenAndRequestTypeBuf) returned n == %v (expected 8)", n)
	}

	n, err = netConn.Write(requestBuf)
	if nil != err {
		b.Fatalf("netConn.Write(requestBuf) failed: %v", err)
	}
	if n != int(requestLen) {
		b.Fatalf("netConn.Write(requestBuf) returned n == %v (expected %v)", n, requestLen)
	}

	replyLenBuf = make([]byte, 4)

	n, err = netConn.Read(replyLenBuf)
	if nil != err {
		b.Fatalf("netConn.Read(replyLenBuf) failed: %v", err)
	}
	if n != 4 {
		b.Fatalf("netConn.Read(replyLenBuf) returned n == %v (expected 4)", n)
	}

	replyLen = binary.LittleEndian.Uint32(replyLenBuf)
	replyBuf = make([]byte, replyLen)

	n, err = netConn.Read(replyBuf)
	if nil != err {
		b.Fatalf("netConn.Read(replyBuf) failed: %v", err)
	}
	if n != int(replyLen) {
		b.Fatalf("netConn.Read(replyBuf) returned n == %v (expected %v)", n, replyLen)
	}

	return
}

func BenchmarkRpcLeaseShortcutTCP(b *testing.B) {
	benchmarkRpcLeaseShortcut(b, false)
}

func BenchmarkRpcLeaseShortcutTLS(b *testing.B) {
	benchmarkRpcLeaseShortcut(b, true)
}

func benchmarkRpcLeaseShortcut(b *testing.B, useTLS bool) {
	var (
		benchmarkIteration               int
		doneWG                           sync.WaitGroup
		err                              error
		leaseReply                       *LeaseReply
		leaseReplyWrapped                *benchmarkRpcShortcutLeaseReplyStruct
		leaseRequest                     *LeaseRequest
		leaseRequestWrapped              *benchmarkRpcShortcutLeaseRequestStruct
		mountByAccountNameReply          *MountByAccountNameReply
		mountByAccountNameReplyWrapped   *benchmarkRpcShortcutMountByAccountNameReplyStruct
		mountByAccountNameRequest        *MountByAccountNameRequest
		mountByAccountNameRequestWrapped *benchmarkRpcShortcutMountByAccountNameRequestStruct
		netConn                          net.Conn
		netConnRetries                   int
		netConnRetryDelay                time.Duration
		ok                               bool
		rootCACertPool                   *x509.CertPool
		tlsConfig                        *tls.Config
		tlsConn                          *tls.Conn
		unmountReply                     *Reply
		unmountReplyWrapped              *benchmarkRpcShortcutUnmountReplyStruct
		unmountRequest                   *UnmountRequest
		unmountRequestWrapped            *benchmarkRpcShortcutUnmountRequestStruct
	)

	doneWG.Add(1)
	go benchmarkRpcLeaseShortcutServer(useTLS, &doneWG)

	netConnRetries = 0

	netConnRetryDelay, err = time.ParseDuration(testRpcLeaseShortcutClientConnRetryDelay)
	if nil != err {
		b.Fatalf("time.ParseDuration(testRpcLeaseShortcutClientConnRetryDelay=\"%s\") failed: %v", testRpcLeaseShortcutClientConnRetryDelay, err)
	}

	if useTLS {
		rootCACertPool = x509.NewCertPool()
		ok = rootCACertPool.AppendCertsFromPEM(testTLSCerts.caCertPEMBlock)
		if !ok {
			b.Fatalf("rootCACertPool.AppendCertsFromPEM(testTLSCerts.caCertPEMBlock) returned !ok")
		}
		tlsConfig = &tls.Config{
			RootCAs: rootCACertPool,
		}
	}

	for {
		if useTLS {
			tlsConn, err = tls.Dial("tcp", testRpcLeaseShortcutIPAddrPort, tlsConfig)
			if nil == err {
				netConn = tlsConn
				break
			}
		} else {
			netConn, err = net.Dial("tcp", testRpcLeaseShortcutIPAddrPort)
			if nil == err {
				break
			}
		}

		netConnRetries++

		if netConnRetries > testRpcLeaseShortcutClientConnMaxRetries {
			b.Fatalf("netConnRetries exceeded testRpcLeaseShortcutClientConnMaxRetries (%v)", testRpcLeaseShortcutClientConnMaxRetries)
		}

		time.Sleep(netConnRetryDelay)
	}

	mountByAccountNameRequest = &MountByAccountNameRequest{
		AccountName: testAccountName,
		AuthToken:   "",
	}
	mountByAccountNameReply = &MountByAccountNameReply{}

	mountByAccountNameRequestWrapped = &benchmarkRpcShortcutMountByAccountNameRequestStruct{Method: "RpcMountByAccountName", Request: mountByAccountNameRequest}
	mountByAccountNameReplyWrapped = &benchmarkRpcShortcutMountByAccountNameReplyStruct{Reply: mountByAccountNameReply}

	benchmarkRpcLeaseShortcutDoRequest(b, netConn, mountByAccountNameRequestWrapped, mountByAccountNameReplyWrapped)
	if nil != mountByAccountNameReplyWrapped.Err {
		b.Fatalf("benchmarkRpcLeaseShortcutDoRequest(mountByAccountNameRequestWrapped, mountByAccountNameReplyWrapped) failed: %v", mountByAccountNameReplyWrapped.Err)
	}

	b.ResetTimer()

	for benchmarkIteration = 0; benchmarkIteration < b.N; benchmarkIteration++ {
		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeExclusive,
		}
		leaseReply = &LeaseReply{}

		leaseRequestWrapped = &benchmarkRpcShortcutLeaseRequestStruct{Method: "RpcLease", Request: leaseRequest}
		leaseReplyWrapped = &benchmarkRpcShortcutLeaseReplyStruct{Reply: leaseReply}

		benchmarkRpcLeaseShortcutDoRequest(b, netConn, leaseRequestWrapped, leaseReplyWrapped)
		if nil != leaseReplyWrapped.Err {
			b.Fatalf("benchmarkRpcLeaseShortcutDoRequest(leaseRequestWrapped, leaseReplyWrapped) failed: %v", leaseReplyWrapped.Err)
		}

		if LeaseReplyTypeExclusive != leaseReplyWrapped.Reply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseRequestTypeExclusive", leaseReplyWrapped.Reply.LeaseReplyType)
		}

		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeRelease,
		}
		leaseReply = &LeaseReply{}

		leaseRequestWrapped = &benchmarkRpcShortcutLeaseRequestStruct{Method: "RpcLease", Request: leaseRequest}
		leaseReplyWrapped = &benchmarkRpcShortcutLeaseReplyStruct{Reply: leaseReply}

		benchmarkRpcLeaseShortcutDoRequest(b, netConn, leaseRequestWrapped, leaseReplyWrapped)
		if nil != leaseReplyWrapped.Err {
			b.Fatalf("benchmarkRpcLeaseShortcutDoRequest(leaseRequestWrapped, leaseReplyWrapped) failed: %v", leaseReplyWrapped.Err)
		}

		if LeaseReplyTypeReleased != leaseReplyWrapped.Reply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseReplyTypeReleased", leaseReplyWrapped.Reply.LeaseReplyType)
		}
	}

	b.StopTimer()

	unmountRequest = &UnmountRequest{
		MountID: mountByAccountNameReply.MountID,
	}
	unmountReply = &Reply{}

	unmountRequestWrapped = &benchmarkRpcShortcutUnmountRequestStruct{Method: "RpcUnmount", Request: unmountRequest}
	unmountReplyWrapped = &benchmarkRpcShortcutUnmountReplyStruct{Reply: unmountReply}

	benchmarkRpcLeaseShortcutDoRequest(b, netConn, unmountRequestWrapped, unmountReplyWrapped)
	if nil != unmountReplyWrapped.Err {
		b.Fatalf("benchmarkRpcLeaseShortcutDoRequest(unmountRequestWrapped, unmountReplyWrapped) failed: %v", unmountReplyWrapped.Err)
	}

	err = netConn.Close()
	if nil != err {
		b.Fatalf("netConn.Close() failed: %v", err)
	}

	doneWG.Wait()
}

func benchmarkRpcLeaseShortcutServer(useTLS bool, doneWG *sync.WaitGroup) {
	var (
		benchmarkRpcShortcutRequestMethodOnly *benchmarkRpcShortcutRequestMethodOnlyStruct
		err                                   error
		jserver                               *Server
		leaseReply                            *LeaseReply
		leaseReplyWrapped                     *benchmarkRpcShortcutLeaseReplyStruct
		leaseRequest                          *LeaseRequest
		leaseRequestWrapped                   *benchmarkRpcShortcutLeaseRequestStruct
		mountByAccountNameReply               *MountByAccountNameReply
		mountByAccountNameReplyWrapped        *benchmarkRpcShortcutMountByAccountNameReplyStruct
		mountByAccountNameRequest             *MountByAccountNameRequest
		mountByAccountNameRequestWrapped      *benchmarkRpcShortcutMountByAccountNameRequestStruct
		n                                     int
		netConn                               net.Conn
		netListener                           net.Listener
		replyBuf                              []byte
		replyLen                              uint32
		replyLenBuf                           []byte
		requestBuf                            []byte
		requestLen                            uint32
		requestLenBuf                         []byte
		tlsConfig                             *tls.Config
		unmountReply                          *Reply
		unmountReplyWrapped                   *benchmarkRpcShortcutUnmountReplyStruct
		unmountRequest                        *UnmountRequest
		unmountRequestWrapped                 *benchmarkRpcShortcutUnmountRequestStruct
	)

	jserver = NewServer()

	netListener, err = net.Listen("tcp", testRpcLeaseShortcutIPAddrPort)
	if nil != err {
		panic(fmt.Errorf("net.Listen(\"tcp\", testRpcLeaseShortcutIPAddrPort) failed: %v", err))
	}

	if useTLS {
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{testTLSCerts.endpointTLSCert},
		}

		netListener = tls.NewListener(netListener, tlsConfig)
	}

	netConn, err = netListener.Accept()
	if nil != err {
		panic(fmt.Errorf("netListener.Accept() failed: %v", err))
	}

	for {
		requestLenBuf = make([]byte, 4)

		n, err = netConn.Read(requestLenBuf)
		if nil != err {
			if io.EOF != err {
				panic(fmt.Errorf("netConn.Read(requestLenBuf) failed: %v", err))
			}

			err = netConn.Close()
			if nil != err {
				panic(fmt.Errorf("netConn.Close() failed: %v", err))
			}

			err = netListener.Close()
			if nil != err {
				panic(fmt.Errorf("netListener.Close() failed: %v", err))
			}

			doneWG.Done()

			return
		}
		if n != 4 {
			panic(fmt.Errorf("netConn.Read(requestLenBuf) returned n == %v (expected 4)", n))
		}

		requestLen = binary.LittleEndian.Uint32(requestLenBuf)
		requestBuf = make([]byte, requestLen)

		n, err = netConn.Read(requestBuf)
		if nil != err {
			panic(fmt.Errorf("netConn.Read(requestBuf) failed: %v", err))
		}
		if n != int(requestLen) {
			panic(fmt.Errorf("netConn.Read(requestBuf) returned n == %v (expected %v)", n, requestLen))
		}

		benchmarkRpcShortcutRequestMethodOnly = &benchmarkRpcShortcutRequestMethodOnlyStruct{}

		err = json.Unmarshal(requestBuf, benchmarkRpcShortcutRequestMethodOnly)
		if nil != err {
			panic(fmt.Errorf("json.Unmarshal(requestBuf, benchmarkRpcShortcutRequestMethodOnly) failed: %v", err))
		}

		switch benchmarkRpcShortcutRequestMethodOnly.Method {
		case "RpcMountByAccountName":
			mountByAccountNameRequestWrapped = &benchmarkRpcShortcutMountByAccountNameRequestStruct{}

			err = json.Unmarshal(requestBuf, mountByAccountNameRequestWrapped)
			if nil != err {
				panic(fmt.Errorf("json.Unmarshal(requestBuf, mountByAccountNameRequestWrapped) failed: %v", err))
			}

			mountByAccountNameRequest = mountByAccountNameRequestWrapped.Request
			mountByAccountNameReply = &MountByAccountNameReply{}

			err = jserver.RpcMountByAccountName(testRpcLeaseLocalClientID, mountByAccountNameRequest, mountByAccountNameReply)

			mountByAccountNameReplyWrapped = &benchmarkRpcShortcutMountByAccountNameReplyStruct{
				Err:   err,
				Reply: mountByAccountNameReply,
			}

			replyBuf, err = json.Marshal(mountByAccountNameReplyWrapped)
			if nil != err {
				panic(fmt.Errorf("json.Marshal(mountByAccountNameReplyWrapped) failed"))
			}
		case "RpcLease":
			leaseRequestWrapped = &benchmarkRpcShortcutLeaseRequestStruct{}

			err = json.Unmarshal(requestBuf, leaseRequestWrapped)
			if nil != err {
				panic(fmt.Errorf("json.Unmarshal(requestBuf, leaseRequestWrapped) failed: %v", err))
			}

			leaseRequest = leaseRequestWrapped.Request
			leaseReply = &LeaseReply{}

			err = jserver.RpcLease(leaseRequest, leaseReply)

			leaseReplyWrapped = &benchmarkRpcShortcutLeaseReplyStruct{
				Err:   err,
				Reply: leaseReply,
			}

			replyBuf, err = json.Marshal(leaseReplyWrapped)
			if nil != err {
				panic(fmt.Errorf("json.Marshal(leaseReplyWrapped) failed"))
			}
		case "RpcUnmount":
			unmountRequestWrapped = &benchmarkRpcShortcutUnmountRequestStruct{}

			err = json.Unmarshal(requestBuf, unmountRequestWrapped)
			if nil != err {
				panic(fmt.Errorf("json.Unmarshal(requestBuf, unmountRequestWrapped) failed: %v", err))
			}

			unmountRequest = unmountRequestWrapped.Request
			unmountReply = &Reply{}

			err = jserver.RpcUnmount(unmountRequest, unmountReply)

			unmountReplyWrapped = &benchmarkRpcShortcutUnmountReplyStruct{
				Err:   err,
				Reply: unmountReply,
			}

			replyBuf, err = json.Marshal(unmountReplyWrapped)
			if nil != err {
				panic(fmt.Errorf("json.Marshal(unmountReplyWrapped) failed"))
			}
		default:
			panic(fmt.Errorf("benchmarkRpcShortcutRequestMethodOnly.Method (\"%s\") not recognized", benchmarkRpcShortcutRequestMethodOnly.Method))
		}

		replyLen = uint32(len(replyBuf))
		replyLenBuf = make([]byte, 4)
		binary.LittleEndian.PutUint32(replyLenBuf, replyLen)

		n, err = netConn.Write(replyLenBuf)
		if nil != err {
			panic(fmt.Errorf("netConn.Write(replyLenBuf) failed: %v", err))
		}
		if n != 4 {
			panic(fmt.Errorf("netConn.Write(replyLenBuf) returned n == %v (expected 4)", n))
		}

		n, err = netConn.Write(replyBuf)
		if nil != err {
			panic(fmt.Errorf("netConn.Write(replyBuf) failed: %v", err))
		}
		if n != int(replyLen) {
			panic(fmt.Errorf("netConn.Write(replyBuf) returned n == %v (expected %v)", n, replyLen))
		}
	}
}

func benchmarkRpcLeaseShortcutDoRequest(b *testing.B, netConn net.Conn, request interface{}, reply interface{}) {
	var (
		err           error
		n             int
		replyBuf      []byte
		replyLen      uint32
		replyLenBuf   []byte
		requestBuf    []byte
		requestLen    uint32
		requestLenBuf []byte
	)

	requestBuf, err = json.Marshal(request)
	if nil != err {
		b.Fatalf("json.Marshal(request) failed: %v", err)
	}

	requestLen = uint32(len(requestBuf))
	requestLenBuf = make([]byte, 4)
	binary.LittleEndian.PutUint32(requestLenBuf, requestLen)

	n, err = netConn.Write(requestLenBuf)
	if nil != err {
		b.Fatalf("netConn.Write(requestLenBuf) failed: %v", err)
	}
	if n != 4 {
		b.Fatalf("netConn.Write(requestLenBuf) returned n == %v (expected 4)", n)
	}

	n, err = netConn.Write(requestBuf)
	if nil != err {
		b.Fatalf("netConn.Write(requestBuf) failed: %v", err)
	}
	if n != int(requestLen) {
		b.Fatalf("netConn.Write(requestBuf) returned n == %v (expected %v)", n, requestLen)
	}

	replyLenBuf = make([]byte, 4)

	n, err = netConn.Read(replyLenBuf)
	if nil != err {
		b.Fatalf("netConn.Read(replyLenBuf) failed: %v", err)
	}
	if n != 4 {
		b.Fatalf("netConn.Read(replyLenBuf) returned n == %v (expected 4)", n)
	}

	replyLen = binary.LittleEndian.Uint32(replyLenBuf)
	replyBuf = make([]byte, replyLen)

	n, err = netConn.Read(replyBuf)
	if nil != err {
		b.Fatalf("netConn.Read(replyBuf) failed: %v", err)
	}
	if n != int(replyLen) {
		b.Fatalf("netConn.Read(replyBuf) returned n == %v (expected %v)", n, replyLen)
	}

	err = json.Unmarshal(replyBuf, reply)
	if nil != err {
		b.Fatalf("json.Unmarshal(replyBuf, reply) failed: %v", err)
	}
}

func BenchmarkRpcLeaseLocal(b *testing.B) {
	var (
		benchmarkIteration        int
		err                       error
		jserver                   *Server
		leaseReply                *LeaseReply
		leaseRequest              *LeaseRequest
		mountByAccountNameReply   *MountByAccountNameReply
		mountByAccountNameRequest *MountByAccountNameRequest
		unmountReply              *Reply
		unmountRequest            *UnmountRequest
	)

	jserver = NewServer()

	mountByAccountNameRequest = &MountByAccountNameRequest{
		AccountName: testAccountName,
		AuthToken:   "",
	}
	mountByAccountNameReply = &MountByAccountNameReply{}

	err = jserver.RpcMountByAccountName(testRpcLeaseLocalClientID, mountByAccountNameRequest, mountByAccountNameReply)
	if nil != err {
		b.Fatalf("jserver.RpcMountByAccountName(testRpcLeaseLocalClientID, mountByAccountNameRequest, mountByAccountNameReply) failed: %v", err)
	}

	b.ResetTimer()

	for benchmarkIteration = 0; benchmarkIteration < b.N; benchmarkIteration++ {
		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeExclusive,
		}
		leaseReply = &LeaseReply{}

		err = jserver.RpcLease(leaseRequest, leaseReply)
		if nil != err {
			b.Fatalf("jserver.RpcLease(leaseRequest, leaseReply) failed: %v", err)
		}

		if LeaseReplyTypeExclusive != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseRequestTypeExclusive", leaseReply.LeaseReplyType)
		}

		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeRelease,
		}
		leaseReply = &LeaseReply{}

		err = jserver.RpcLease(leaseRequest, leaseReply)
		if nil != err {
			b.Fatalf("jserver.RpcLease(leaseRequest, leaseReply) failed: %v", err)
		}

		if LeaseReplyTypeReleased != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseReplyTypeReleased", leaseReply.LeaseReplyType)
		}
	}

	b.StopTimer()

	unmountRequest = &UnmountRequest{
		MountID: mountByAccountNameReply.MountID,
	}
	unmountReply = &Reply{}

	err = jserver.RpcUnmount(unmountRequest, unmountReply)
	if nil != err {
		b.Fatalf("jserver.RpcUnmount(unmountRequest, unmountReply) failed: %v", err)
	}
}

func BenchmarkRpcLeaseRemote(b *testing.B) {
	var (
		benchmarkIteration        int
		deadlineIO                time.Duration
		err                       error
		keepAlivePeriod           time.Duration
		leaseReply                *LeaseReply
		leaseRequest              *LeaseRequest
		mountByAccountNameReply   *MountByAccountNameReply
		mountByAccountNameRequest *MountByAccountNameRequest
		retryRPCClient            *retryrpc.Client
		retryrpcClientConfig      *retryrpc.ClientConfig
		testRpcLeaseClient        *testRpcLeaseClientStruct
		unmountReply              *Reply
		unmountRequest            *UnmountRequest
	)

	deadlineIO, err = time.ParseDuration(testRpcLeaseRetryRPCDeadlineIO)
	if nil != err {
		b.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCDeadlineIO, err)
	}
	keepAlivePeriod, err = time.ParseDuration(testRpcLeaseRetryRPCKeepAlivePeriod)
	if nil != err {
		b.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCKeepAlivePeriod, err)
	}

	retryrpcClientConfig = &retryrpc.ClientConfig{
		IPAddr:                   globals.publicIPAddr,
		Port:                     int(globals.retryRPCPort),
		RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
		Callbacks:                testRpcLeaseClient,
		DeadlineIO:               deadlineIO,
		KeepAlivePeriod:          keepAlivePeriod,
	}

	retryRPCClient, err = retryrpc.NewClient(retryrpcClientConfig)
	if nil != err {
		b.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	mountByAccountNameRequest = &MountByAccountNameRequest{
		AccountName: testAccountName,
		AuthToken:   "",
	}
	mountByAccountNameReply = &MountByAccountNameReply{}

	err = retryRPCClient.Send("RpcMountByAccountName", mountByAccountNameRequest, mountByAccountNameReply)
	if nil != err {
		b.Fatalf("retryRPCClient.Send(\"RpcMountByAccountName\",,) failed: %v", err)
	}

	b.ResetTimer()

	for benchmarkIteration = 0; benchmarkIteration < b.N; benchmarkIteration++ {
		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeExclusive,
		}
		leaseReply = &LeaseReply{}

		err = retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
		if nil != err {
			b.Fatalf("retryRPCClient.Send(\"RpcLease\",LeaseRequestTypeExclusive) failed: %v", err)
		}

		if LeaseReplyTypeExclusive != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseRequestTypeExclusive", leaseReply.LeaseReplyType)
		}

		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeRelease,
		}
		leaseReply = &LeaseReply{}

		err = retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
		if nil != err {
			b.Fatalf("retryRPCClient.Send(\"RpcLease\",LeaseRequestTypeRelease) failed: %v", err)
		}

		if LeaseReplyTypeReleased != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseReplyTypeReleased", leaseReply.LeaseReplyType)
		}
	}

	b.StopTimer()

	unmountRequest = &UnmountRequest{
		MountID: mountByAccountNameReply.MountID,
	}
	unmountReply = &Reply{}

	err = retryRPCClient.Send("RpcUnmount", unmountRequest, unmountReply)
	if nil != err {
		b.Fatalf("retryRPCClient.Send(\"RpcUnmount\",,) failed: %v", err)
	}

	retryRPCClient.Close()
}

func TestRpcLease(t *testing.T) {
	var (
		instance           int
		testRpcLeaseClient []*testRpcLeaseClientStruct
		wg                 sync.WaitGroup
	)

	// Setup Single Lease instances

	wg.Add(testRpcLeaseSingleNumInstances)

	testRpcLeaseClient = make([]*testRpcLeaseClientStruct, testRpcLeaseSingleNumInstances)

	for instance = 0; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance] = &testRpcLeaseClientStruct{
			instance:         instance,
			inodeNumber:      testRpcLeaseSingleInodeNumber,
			chIn:             make(chan LeaseRequestType),
			chOut:            make(chan interface{}),
			alreadyUnmounted: false,
			wg:               &wg,
			t:                t,
		}

		go testRpcLeaseClient[instance].instanceGoroutine()
	}

	// Perform Single Lease test cases

	testRpcLeaseLogTestCase("1 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("3 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then Demote", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeDemoted)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 1 Shared leading to Demotion", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeDemote)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeDemoted, RPCInterruptTypeDemote)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Shared then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Shared then Promotion leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypePromoted)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 2 Shared leading to Demotion", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeDemote)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeDemoted, RPCInterruptTypeDemote)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Shared then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Exclusives leading to Release that Expires", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseClient[0].alreadyUnmounted = true

	testRpcLeaseLogTestCase("2 Shared then 2 Promotions leading to Release", true)

	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[2].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeDenied, RPCInterruptTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypePromoted)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Shared", testRpcLeaseSingleNumInstances-1), false)

	for instance = 1; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeShared)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	}
	for instance = 1; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeRelease)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	}

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Exclusives", testRpcLeaseSingleNumInstances-1), false)

	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	for instance = 2; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeExclusive)
		testRpcLeaseClient[(instance - 1)].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
		testRpcLeaseClient[(instance - 1)].sendLeaseRequest(LeaseRequestTypeRelease)
		testRpcLeaseClient[(instance-1)].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	}
	testRpcLeaseClient[(testRpcLeaseSingleNumInstances - 1)].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[(testRpcLeaseSingleNumInstances - 1)].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	// Shutdown Single Lease instances

	for instance = 0; instance < testRpcLeaseSingleNumInstances; instance++ {
		close(testRpcLeaseClient[instance].chIn)
	}

	wg.Wait()

	// Setup Multi Lease instances

	wg.Add(testRpcLeaseMultiNumInstances)

	testRpcLeaseClient = make([]*testRpcLeaseClientStruct, testRpcLeaseMultiNumInstances)

	for instance = 0; instance < testRpcLeaseMultiNumInstances; instance++ {
		testRpcLeaseClient[instance] = &testRpcLeaseClientStruct{
			instance:         instance,
			inodeNumber:      (testRpcLeaseMultiFirstInodeNumber + int64(instance)),
			chIn:             make(chan LeaseRequestType),
			chOut:            make(chan interface{}),
			alreadyUnmounted: false,
			wg:               &wg,
			t:                t,
		}

		go testRpcLeaseClient[instance].instanceGoroutine()
	}

	// Perform Multi Lease test case

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Unique InodeNumber Exclusives", testRpcLeaseMultiNumInstances), true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeExclusive)

	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)

	testRpcLeaseClient[3].sendLeaseRequest(LeaseRequestTypeExclusive)

	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)

	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)

	testRpcLeaseClient[3].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)

	testRpcLeaseClient[4].sendLeaseRequest(LeaseRequestTypeExclusive)

	testRpcLeaseClient[4].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)

	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[3].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[4].sendLeaseRequest(LeaseRequestTypeRelease)

	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[3].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[4].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	// Shutdown Multi Lease instances

	for instance = 0; instance < testRpcLeaseMultiNumInstances; instance++ {
		close(testRpcLeaseClient[instance].chIn)
	}

	wg.Wait()
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) instanceGoroutine() {
	var (
		deadlineIO                time.Duration
		err                       error
		keepAlivePeriod           time.Duration
		leaseReply                *LeaseReply
		leaseRequest              *LeaseRequest
		leaseRequestType          LeaseRequestType
		mountByAccountNameRequest *MountByAccountNameRequest
		mountByAccountNameReply   *MountByAccountNameReply
		ok                        bool
		retryRPCClient            *retryrpc.Client
		retryrpcClientConfig      *retryrpc.ClientConfig
		unmountReply              *Reply
		unmountRequest            *UnmountRequest
	)

	deadlineIO, err = time.ParseDuration(testRpcLeaseRetryRPCDeadlineIO)
	if nil != err {
		testRpcLeaseClient.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCDeadlineIO, err)
	}
	keepAlivePeriod, err = time.ParseDuration(testRpcLeaseRetryRPCKeepAlivePeriod)
	if nil != err {
		testRpcLeaseClient.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCKeepAlivePeriod, err)
	}

	retryrpcClientConfig = &retryrpc.ClientConfig{
		IPAddr:                   globals.publicIPAddr,
		Port:                     int(globals.retryRPCPort),
		RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
		Callbacks:                testRpcLeaseClient,
		DeadlineIO:               deadlineIO,
		KeepAlivePeriod:          keepAlivePeriod,
	}

	retryRPCClient, err = retryrpc.NewClient(retryrpcClientConfig)
	if nil != err {
		testRpcLeaseClient.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	mountByAccountNameRequest = &MountByAccountNameRequest{
		AccountName: testAccountName,
		AuthToken:   "",
	}
	mountByAccountNameReply = &MountByAccountNameReply{}

	err = retryRPCClient.Send("RpcMountByAccountName", mountByAccountNameRequest, mountByAccountNameReply)
	if nil != err {
		testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcMountByAccountName\",,) failed: %v", err)
	}

	for {
		leaseRequestType, ok = <-testRpcLeaseClient.chIn

		if ok {
			leaseRequest = &LeaseRequest{
				InodeHandle: InodeHandle{
					MountID:     mountByAccountNameReply.MountID,
					InodeNumber: testRpcLeaseClient.inodeNumber,
				},
				LeaseRequestType: leaseRequestType,
			}
			leaseReply = &LeaseReply{}

			testRpcLeaseClient.logEvent(leaseRequest.LeaseRequestType)

			err = retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
			if nil != err {
				testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcLease\",LeaseRequestType=%d) failed: %v", leaseRequestType, err)
			}

			testRpcLeaseClient.logEvent(leaseReply.LeaseReplyType)

			testRpcLeaseClient.chOut <- leaseReply.LeaseReplyType
		} else {
			unmountRequest = &UnmountRequest{
				MountID: mountByAccountNameReply.MountID,
			}
			unmountReply = &Reply{}

			err = retryRPCClient.Send("RpcUnmount", unmountRequest, unmountReply)
			if testRpcLeaseClient.alreadyUnmounted {
				if nil == err {
					testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcUnmount\",,) should have failed")
				}
			} else {
				if nil != err {
					testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcUnmount\",,) failed: %v", err)
				}
			}

			retryRPCClient.Close()

			testRpcLeaseClient.wg.Done()

			runtime.Goexit()
		}
	}
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) Interrupt(rpcInterruptBuf []byte) {
	var (
		err          error
		rpcInterrupt *RPCInterrupt
	)

	rpcInterrupt = &RPCInterrupt{}

	err = json.Unmarshal(rpcInterruptBuf, rpcInterrupt)
	if nil != err {
		testRpcLeaseClient.Fatalf("json.Unmarshal() failed: %v", err)
	}
	if rpcInterrupt.InodeNumber != testRpcLeaseClient.inodeNumber {
		testRpcLeaseClient.Fatalf("Interrupt() called for InodeNumber %v... expected to be for %v", rpcInterrupt.InodeNumber, testRpcLeaseClient.inodeNumber)
	}

	testRpcLeaseClient.logEvent(rpcInterrupt.RPCInterruptType)

	testRpcLeaseClient.chOut <- rpcInterrupt.RPCInterruptType
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) Fatalf(format string, args ...interface{}) {
	var (
		argsForPrintf   []interface{}
		argsIndex       int
		argsValue       interface{}
		formatForPrintf string
	)

	formatForPrintf = "Failing testRpcLeaseClient %v: " + format + "\n"

	argsForPrintf = make([]interface{}, len(args)+1)
	argsForPrintf[0] = testRpcLeaseClient.instance
	for argsIndex, argsValue = range args {
		argsForPrintf[argsIndex+1] = argsValue
	}

	fmt.Printf(formatForPrintf, argsForPrintf...)

	os.Exit(-1)
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) sendLeaseRequest(leaseRequestType LeaseRequestType) {
	time.Sleep(testRpcLeaseDelayBeforeSendingRequest)
	testRpcLeaseClient.chIn <- leaseRequestType
	time.Sleep(testRpcLeaseDelayAfterSendingRequest)
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) sendLeaseRequestPromptly(leaseRequestType LeaseRequestType) {
	testRpcLeaseClient.chIn <- leaseRequestType
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsLeaseReplyType(expectedLeaseReplyType LeaseReplyType) {
	var (
		chOutValueAsInterface      interface{}
		chOutValueAsLeaseReplyType LeaseReplyType
		ok                         bool
	)

	chOutValueAsInterface = <-testRpcLeaseClient.chOut

	chOutValueAsLeaseReplyType, ok = chOutValueAsInterface.(LeaseReplyType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a LeaseReplyType")
	}
	if chOutValueAsLeaseReplyType != expectedLeaseReplyType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned LeaseReplyType %v... expected %v", chOutValueAsLeaseReplyType, expectedLeaseReplyType)
	}
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(expectedLeaseReplyType LeaseReplyType, ignoredRPCInterruptType RPCInterruptType) {
	var (
		chOutValueAsInterface        interface{}
		chOutValueAsRPCInterruptType RPCInterruptType
		chOutValueAsLeaseReplyType   LeaseReplyType
		ok                           bool
	)

	for {
		chOutValueAsInterface = <-testRpcLeaseClient.chOut

		chOutValueAsRPCInterruptType, ok = chOutValueAsInterface.(RPCInterruptType)
		if ok {
			if chOutValueAsRPCInterruptType != ignoredRPCInterruptType {
				testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return an ignored RPCInterruptType")
			}
		} else {
			break
		}
	}

	chOutValueAsLeaseReplyType, ok = chOutValueAsInterface.(LeaseReplyType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a LeaseReplyType or ignored RPCInterruptType")
	}
	if chOutValueAsLeaseReplyType != expectedLeaseReplyType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned LeaseReplyType %v... expected %v", chOutValueAsLeaseReplyType, expectedLeaseReplyType)
	}
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsRPCInterruptType(expectedRPCInterruptType RPCInterruptType) {
	var (
		chOutValueAsInterface        interface{}
		chOutValueAsRPCInterruptType RPCInterruptType
		ok                           bool
	)

	chOutValueAsInterface = <-testRpcLeaseClient.chOut

	chOutValueAsRPCInterruptType, ok = chOutValueAsInterface.(RPCInterruptType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a RPCInterruptType")
	}
	if chOutValueAsRPCInterruptType != expectedRPCInterruptType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned RPCInterruptType %v... expected %v", chOutValueAsRPCInterruptType, expectedRPCInterruptType)
	}
}

func testRpcLeaseLogTestCase(testCase string, verbose bool) {
	fmt.Printf("%v %s\n", time.Now().Format(testRpcLeaseTimeFormat), testCase)
	testRpcLeaseLogVerbosely = verbose
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) logEvent(ev interface{}) {
	if testRpcLeaseLogVerbosely {
		switch ev.(type) {
		case LeaseRequestType:
			fmt.Printf("%v      %s%s-> \n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", testRpcLeaseClient.instance), testRpcLeaseRequestLetters[ev.(LeaseRequestType)])
		case LeaseReplyType:
			fmt.Printf("%v      %s <-%s\n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", testRpcLeaseClient.instance), testRpcLeaseReplyLetters[ev.(LeaseReplyType)])
		case RPCInterruptType:
			fmt.Printf("%v      %s ^^%s\n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", testRpcLeaseClient.instance), testRpcLeaseInterruptLetters[ev.(RPCInterruptType)])
		}
	}
}
