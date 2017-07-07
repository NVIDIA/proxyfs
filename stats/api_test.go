package stats

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/swiftstack/conf"
)

type testGlobalsStruct struct {
	t           *testing.T
	udpConn     *net.UDPConn
	tcpListener *net.TCPListener
	statChan    chan string //   sufficient to buffer the expected 28 stats
	donePending bool        //   true if parent has called Close() to terminate testStatsd
	doneChan    chan bool   //   sufficient to buffer the lone true
	doneErr     error
	stopPending bool
}

var testGlobals testGlobalsStruct

func TestStatsAPIviaUDP(t *testing.T) {
	testGlobals.t = t

	confStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=41582",
		"Stats.BufferLength=1000",
		"Stats.MaxLatency=100ms",
	}

	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned error: %v", err)
	}

	err = Up(confMap)
	if nil != err {
		t.Fatalf("stats.Up(confMap) returned error: %v", err)
	}

	testGlobals.udpConn, err = net.ListenUDP("udp", globals.udpRAddr)
	if nil != err {
		t.Fatalf("net.ListenUDP(\"udp\", globals.udpRAddr) returned error: %v", err)
	}

	testGlobals.statChan = make(chan string, 100)
	testGlobals.doneChan = make(chan bool, 1)

	testGlobals.doneErr = nil
	testGlobals.stopPending = false

	go testStatsd()

	testSendStats()
	testVerifyStats()

	err = Down()
	if nil != err {
		t.Fatalf("stats.Down() returned error: %v", err)
	}

	testGlobals.stopPending = true

	err = testGlobals.udpConn.Close()
	if nil != err {
		t.Fatalf("testGlobals.udpConn.Close() returned error: %v", err)
	}

	_ = <-testGlobals.doneChan
}

func TestStatsAPIviaTCP(t *testing.T) {
	testGlobals.t = t

	confStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.TCPPort=52184",
		"Stats.BufferLength=1000",
		"Stats.MaxLatency=100ms",
	}

	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned error: %v", err)
	}

	err = Up(confMap)
	if nil != err {
		t.Fatalf("stats.Up(confMap) returned error: %v", err)
	}

	testGlobals.tcpListener, err = net.ListenTCP("tcp", globals.tcpRAddr)
	if nil != err {
		t.Fatalf("net.ListenTCP(\"tcp\", globals.tcpRAddr) returned error: %v", err)
	}

	testGlobals.statChan = make(chan string, 28)
	testGlobals.doneChan = make(chan bool, 1)

	testGlobals.doneErr = nil
	testGlobals.stopPending = false

	go testStatsd()

	testSendStats()
	testVerifyStats()

	err = Down()
	if nil != err {
		t.Fatalf("stats.Down() returned error: %v", err)
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
	buf := make([]byte, 2048)

	for {
		if globals.useUDP {
			bufConsumed, _, err := testGlobals.udpConn.ReadFromUDP(buf)
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
					fmt.Println(err)
				}
				testGlobals.doneChan <- true
				return
			}

			if 0 == bufConsumed {
				if !testGlobals.stopPending {
					err = fmt.Errorf("0 == bufConsumed")
					testGlobals.doneErr = err
					fmt.Println(err)
				}
				testGlobals.doneChan <- true
				return
			}

			testGlobals.statChan <- string(buf[:bufConsumed])
		} else { // globals.useTCP
			testTCPConn, err := testGlobals.tcpListener.AcceptTCP()
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
					fmt.Println(err)
				}
				testGlobals.doneChan <- true
				return
			}
			if nil == testTCPConn {
				if !testGlobals.stopPending {
					err = fmt.Errorf("nil == testTCPConn")
					testGlobals.doneErr = err
					fmt.Println(err)
				}
				testGlobals.doneChan <- true
				return
			}

			bufConsumed, err := testTCPConn.Read(buf)
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
					fmt.Println(err)
				}
				testGlobals.doneChan <- true
				return
			}
			if 0 == bufConsumed {
				if !testGlobals.stopPending {
					err = fmt.Errorf("0 == bufConsumed")
					testGlobals.doneErr = err
					fmt.Println(err)
				}
				testGlobals.doneChan <- true
				return
			}

			testGlobals.statChan <- string(buf[:bufConsumed])

			err = testTCPConn.Close()
			if nil != err {
				if !testGlobals.stopPending {
					testGlobals.doneErr = err
					fmt.Println(err)
				}
				testGlobals.doneChan <- true
				return
			}
		}
	}
}

func testSendStats() {
	sleepDuration := 100 * time.Millisecond

	IncrementOperations(&LogSegCreateOps)
	IncrementOperationsAndBytes(SwiftObjTail, 1024)
	IncrementOperationsEntriesAndBytes(DirRead, 48, 2048)
	IncrementOperationsAndBucketedBytes(FileRead, 4096)
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 8192, 0, 0)
	time.Sleep(sleepDuration)
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 16384, 400, 0)
	time.Sleep(sleepDuration)
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 32768, 0, 500)
	time.Sleep(sleepDuration)
	IncrementOperationsBucketedBytesAndAppendedOverwritten(FileWrite, 65536, 600, 700)
	IncrementOperationsAndBucketedBytes(FileReadplan, 131072)
}

func testVerifyStats() {
	var (
		ok        bool
		stat      string
		statMap   map[string]uint64
		statValue uint64
	)

	// Build a slice of the stats we expect
	expectedStats := []string{
		LogSegCreateOps + ":1|c",
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
	}

	// Check that the stats sent to the TCP/UDP port are what we expect
	//
	// Note that this test has been written so that it does not depend on stats
	// appearing in the same order they are sent in the function above.
	// Since the APIs send to the channel in a goroutine, there is no guarantee
	// that ordering will be preserved.

	// Pull the stats off the channel. There should be as many as items in expectedStats.
	statsFromChannel := make(map[string]int, len(expectedStats))
	for _ = range expectedStats {
		stat = <-testGlobals.statChan
		statsFromChannel[stat]++
	}

	// Check that the map contains all the stats we expected
	for _, expectedStat := range expectedStats {
		stat, ok := statsFromChannel[expectedStat]
		if !ok || stat <= 0 {
			testGlobals.t.Fatalf("verifyStats() failed, could not find %v (ok=%v count=%v); stats are %v", expectedStat, ok, stat, statsFromChannel)
		}
		statsFromChannel[expectedStat]--
	}

	// Check that the stats held in memory are what we expect
	statMap = Dump()

	// Build a slice of the stats we expect to find in memory, by adding up any repeated stats
	// in the original raw expectedStats map
	expectedInmemoryStats := make(map[string]uint64, len(statMap))

	for _, statStr := range expectedStats {
		// Split expected stat at : into stat name and count info
		colonSplit := strings.Split(statStr, ":")
		statName := colonSplit[0]

		// Split count info at | to get the count
		sepSplit := strings.Split(colonSplit[1], "|")
		statCount, _ := strconv.ParseUint(sepSplit[0], 10, 64)

		// Add the stat to the map
		expectedInmemoryStats[statName] += statCount
	}

	// Make sure the in-memory stats we got is the same size as what we expect
	if len(statMap) != len(expectedInmemoryStats) {
		testGlobals.t.Fatalf("verifyStats() failed; expected %v stats, got %v", len(expectedInmemoryStats), len(statMap))
	}

	// Now check our stats
	for statName, expectedCount := range expectedInmemoryStats {
		fmt.Printf("checking stat %v == %v\n", statName, expectedCount)

		if statValue, ok = statMap[statName]; !ok || (statValue != expectedCount) {
			testGlobals.t.Fatalf("verifyStats() failed; stat %v found=%v, got count %v expected %v", statName, ok, statValue, expectedCount)
		}
	}
}
