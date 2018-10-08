package stats

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/transitions"
)

type testGlobalsStruct struct {
	t           *testing.T
	useUDP      bool //          Logically useTCP == !useUDP
	udpLAddr    *net.UDPAddr
	tcpLAddr    *net.TCPAddr
	udpConn     *net.UDPConn
	tcpListener *net.TCPListener
	statsLog    []string
	donePending bool      //     true if parent has called Close() to terminate testStatsd
	doneChan    chan bool //     sufficient to buffer the lone true
	doneErr     error
	stopPending bool
}

var testGlobals testGlobalsStruct

/*
	useUDP           bool   //              Logically useTCP == !useUDP
	udpLAddr         *net.UDPAddr
	tcpLAddr         *net.TCPAddr
	globals.ipAddr = "localhost" // Hard-coded since we only want to talk to the local StatsD
		globals.udpLAddr, err = net.ResolveUDPAddr("udp", globals.ipAddr+":0")
		globals.tcpLAddr, err = net.ResolveTCPAddr("tcp", globals.ipAddr+":0")
*/

func TestStatsAPIviaUDP(t *testing.T) {
	var (
		confMap     conf.ConfMap
		confStrings []string
		err         error
		portString  string
	)

	testGlobals.t = t

	testGlobals.useUDP = true

	testGlobals.udpLAddr, err = net.ResolveUDPAddr("udp", "localhost:0")
	if nil != err {
		t.Fatalf("net.RessolveUDPAddr(\"udp\", \"localhost:0\") returned error: %v", err)
	}

	testGlobals.udpConn, err = net.ListenUDP("udp", testGlobals.udpLAddr)
	if nil != err {
		t.Fatalf("net.ListenUDP(\"udp\", testGlobals.udpLAddr) returned error: %v", err)
	}

	_, portString, err = net.SplitHostPort(testGlobals.udpConn.LocalAddr().String())
	if nil != err {
		t.Fatalf("net.SplitHostPort(testGlobals.udpConn.LocalAddr().String()) returned error: %v", err)
	}

	testGlobals.statsLog = make([]string, 0, 100)

	testGlobals.doneChan = make(chan bool, 1)

	testGlobals.doneErr = nil
	testGlobals.stopPending = false

	go testStatsd()

	confStrings = []string{
		"Logging.LogFilePath=/dev/null",
		"Cluster.WhoAmI=nobody",
		"FSGlobals.VolumeGroupList=",
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=" + portString,
		"Stats.BufferLength=1000",
		"Stats.MaxLatency=100ms",
	}

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned error: %v", err)
	}

	err = transitions.Up(confMap)
	if nil != err {
		t.Fatalf("transitions.Up(confMap) returned error: %v", err)
	}

	testSendStats()
	testVerifyStats()

	err = transitions.Down(confMap)
	if nil != err {
		t.Fatalf("transitions.Down() returned error: %v", err)
	}

	testGlobals.stopPending = true

	err = testGlobals.udpConn.Close()
	if nil != err {
		t.Fatalf("testGlobals.udpConn.Close() returned error: %v", err)
	}

	_ = <-testGlobals.doneChan

	if nil != testGlobals.doneErr {
		t.Fatalf("testStatsd() returned error: %v", testGlobals.doneErr)
	}
}

func TestStatsAPIviaTCP(t *testing.T) {
	var (
		confMap     conf.ConfMap
		confStrings []string
		err         error
		portString  string
	)

	testGlobals.t = t

	testGlobals.useUDP = false

	testGlobals.tcpLAddr, err = net.ResolveTCPAddr("tcp", "localhost:0")
	if nil != err {
		t.Fatalf("net.RessolveTCPAddr(\"tcp\", \"localhost:0\") returned error: %v", err)
	}

	testGlobals.tcpListener, err = net.ListenTCP("tcp", testGlobals.tcpLAddr)
	if nil != err {
		t.Fatalf("net.ListenTCP(\"tcp\", testGlobals.tcpLAddr) returned error: %v", err)
	}

	_, portString, err = net.SplitHostPort(testGlobals.tcpListener.Addr().String())
	if nil != err {
		t.Fatalf("net.SplitHostPort(testGlobals.tcpListener.Addr().String()) returned error: %v", err)
	}

	testGlobals.statsLog = make([]string, 0, 100)

	testGlobals.doneChan = make(chan bool, 1)

	testGlobals.doneErr = nil
	testGlobals.stopPending = false

	go testStatsd()

	confStrings = []string{
		"Logging.LogFilePath=/dev/null",
		"Cluster.WhoAmI=nobody",
		"FSGlobals.VolumeGroupList=",
		"Stats.IPAddr=localhost",
		"Stats.TCPPort=" + portString,
		"Stats.BufferLength=1000",
		"Stats.MaxLatency=100ms",
	}

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned error: %v", err)
	}

	err = transitions.Up(confMap)
	if nil != err {
		t.Fatalf("transitions.Up() returned error: %v", err)
	}

	testSendStats()
	testVerifyStats()

	err = transitions.Down(confMap)
	if nil != err {
		t.Fatalf("transitions.Down() returned error: %v", err)
	}

	testGlobals.stopPending = true

	err = testGlobals.tcpListener.Close()
	if nil != err {
		t.Fatalf("testGlobals.tcpListener.Close() returned error: %v", err)
	}

	_ = <-testGlobals.doneChan

	if nil != testGlobals.doneErr {
		t.Fatalf("testStatsd() returned error: %v", testGlobals.doneErr)
	}
}

func testStatsd() {
	var (
		buf         []byte
		bufConsumed int
		err         error
		testTCPConn *net.TCPConn
	)

	buf = make([]byte, 2048)

	for {
		if testGlobals.useUDP {
			bufConsumed, _, err = testGlobals.udpConn.ReadFromUDP(buf)
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
				}
				testGlobals.doneChan <- true
				return
			}

			if 0 == bufConsumed {
				if !testGlobals.stopPending {
					err = fmt.Errorf("0 == bufConsumed")
					testGlobals.doneErr = err
				}
				testGlobals.doneChan <- true
				return
			}

			testGlobals.statsLog = append(testGlobals.statsLog, string(buf[:bufConsumed]))
		} else { // testGlobals.useTCP
			testTCPConn, err = testGlobals.tcpListener.AcceptTCP()
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
				}
				testGlobals.doneChan <- true
				return
			}
			if nil == testTCPConn {
				if !testGlobals.stopPending {
					err = fmt.Errorf("nil == testTCPConn")
					testGlobals.doneErr = err
				}
				testGlobals.doneChan <- true
				return
			}

			bufConsumed, err = testTCPConn.Read(buf)
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
				}
				testGlobals.doneChan <- true
				return
			}
			if 0 == bufConsumed {
				if !testGlobals.stopPending {
					err = fmt.Errorf("0 == bufConsumed")
					testGlobals.doneErr = err
				}
				testGlobals.doneChan <- true
				return
			}

			testGlobals.statsLog = append(testGlobals.statsLog, string(buf[:bufConsumed]))

			err = testTCPConn.Close()
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
				}
				testGlobals.doneChan <- true
				return
			}
		}
	}
}

func testSendStats() {
	var (
		sleepDuration time.Duration
	)

	sleepDuration = 4 * globals.maxLatency

	IncrementOperations(&LogSegCreateOps)
	IncrementOperationsBy(&FileFlushOps, 3)
	IncrementOperationsAndBytes(SwiftObjTail, 1024)
	IncrementOperationsEntriesAndBytes(DirRead, 48, 2048)
	IncrementOperationsAndBucketedBytes(FileRead, 4096)
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 8192, 0, 0)
	time.Sleep(sleepDuration) // ensures above doesn't get merged with below
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 16384, 400, 0)
	time.Sleep(sleepDuration) // ensures above doesn't get merged with below
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 32768, 0, 500)
	time.Sleep(sleepDuration) // ensures above doesn't get merged with below
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 65536, 600, 700)
	IncrementOperationsBucketedEntriesAndBucketedBytes(FileReadplan, 7, 131072)
	time.Sleep(sleepDuration) // ensures we get all of the stats entries
}

func testVerifyStats() {
	var (
		expectedStatValue        uint64
		expectedStats            []string
		expectedStatsValueMap    map[string]uint64
		ok                       bool
		statCount                uint16
		statName                 string
		statLine                 string
		statLineSplitOnColon     []string
		statLineSuffixSplitOnBar []string
		statValue                uint64
		statValueIncrement       uint64
		statsCountMap            map[string]uint16
		statsDumpMap             map[string]uint64
	)

	// Build a slice of the stats we expect

	expectedStats = []string{
		LogSegCreateOps + ":1|c",

		FileFlushOps + ":3|c",

		SwiftObjTailOps + ":1|c",
		SwiftObjTailBytes + ":1024|c",

		DirReadOps + ":1|c",
		DirReadEntries + ":48|c",
		DirReadBytes + ":2048|c",

		FileReadOps + ":1|c",
		FileReadOps4K + ":1|c",
		FileReadBytes + ":4096|c",

		FileWriteOps + ":1|c",
		FileWriteOps8K + ":1|c",
		FileWriteBytes + ":8192|c",

		FileWriteOps + ":1|c",
		FileWriteOps16K + ":1|c",
		FileWriteBytes + ":16384|c",
		FileWriteAppended + ":400|c",

		FileWriteOps + ":1|c",
		FileWriteOps32K + ":1|c",
		FileWriteBytes + ":32768|c",
		FileWriteOverwritten + ":500|c",

		FileWriteOps + ":1|c",
		FileWriteOps64K + ":1|c",
		FileWriteBytes + ":65536|c",
		FileWriteAppended + ":600|c",
		FileWriteOverwritten + ":700|c",

		FileReadplanOps + ":1|c",
		FileReadplanOpsOver64K + ":1|c",
		FileReadplanBytes + ":131072|c",
		FileReadplanOpsEntriesTo16 + ":1|c",
	}

	// Check that the stats sent to the TCP/UDP port are what we expect
	//
	// Note that this test has been written so that it does not depend on stats
	// appearing in the same order they are sent in testSendStats().

	if len(testGlobals.statsLog) != len(expectedStats) {
		testGlobals.t.Fatalf("verifyStats() failed... wrong number of statsLog elements")
	}

	statsCountMap = make(map[string]uint16)

	for _, statLine = range testGlobals.statsLog {
		statCount, ok = statsCountMap[statLine]
		if ok {
			statCount++
		} else {
			statCount = 1
		}
		statsCountMap[statLine] = statCount
	}

	for _, statLine = range expectedStats {
		statCount, ok = statsCountMap[statLine]
		if ok {
			statCount--
			if 0 == statCount {
				delete(statsCountMap, statLine)
			} else {
				statsCountMap[statLine] = statCount
			}
		} else {
			testGlobals.t.Fatalf("verifyStats() failed... missing stat: %v", statLine)
		}
	}

	if 0 < len(statsCountMap) {
		for statLine = range statsCountMap {
			testGlobals.t.Logf("verifyStats() failed... extra stat: %v", statLine)
		}
		testGlobals.t.FailNow()
	}

	// Compress expectedStats to be comparable to Dump() return

	expectedStatsValueMap = make(map[string]uint64)

	for _, statLine = range expectedStats {
		statLineSplitOnColon = strings.Split(statLine, ":")

		statName = statLineSplitOnColon[0]

		statLineSuffixSplitOnBar = strings.Split(statLineSplitOnColon[1], "|")

		statValueIncrement, _ = strconv.ParseUint(statLineSuffixSplitOnBar[0], 10, 64)

		statValue, ok = expectedStatsValueMap[statName]
		if ok {
			statValue += statValueIncrement
		} else {
			statValue = statValueIncrement
		}
		expectedStatsValueMap[statName] = statValue
	}

	// Check that the stats held in memory are what we expect

	statsDumpMap = Dump()

	if len(statsDumpMap) != len(expectedStatsValueMap) {
		testGlobals.t.Fatalf("verifyStats() failed... wrong number of statsDumpMap elements")
	}

	for statName, statValue = range statsDumpMap {
		expectedStatValue, ok = expectedStatsValueMap[statName]
		if !ok {
			testGlobals.t.Fatalf("verifyStats() failed... received unpected statName: %v", statName)
		}
		if statValue != expectedStatValue {
			testGlobals.t.Fatalf("verifyStats() failed... received unexpected statValue (%v) for statName %v (should have been %v)", statValue, statName, expectedStatValue)
		}
	}
}
