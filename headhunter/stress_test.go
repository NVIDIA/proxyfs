package headhunter

import (
	"bytes"
	cryptoRand "crypto/rand"
	"math/big"
	mathRand "math/rand"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

const (
	testNumFetchNonceCalls = 1000

	testNumInodeRecs                                = 1000
	testMaxInodeRecSize                             = 100
	testMaxNewInodesBetweenRePutInodes              = 5
	testMaxPutInodeRecCallsBetweenPutInodeRecsCalls = 5
	testMaxInodesPerPutInodeRecsCall                = 5

	testNumLogSegmentRecs    = 1000
	testMaxLogSegmentRecSize = 20

	testNumBPlusTreeObjects    = 1000
	testMaxBPlusTreeObjectSize = 1000

	testMaxPutDeleteCallsPerGet = 5

	testMaxPutDeleteCallsPerCheckpoint = 10

	testMaxCheckpointsPerDownUp = 5

	testPseudoRandom     = false
	testPseudoRandomSeed = int64(0)
)

var testMathRandSource *mathRand.Rand // A source for pseudo-random numbers (if selected)

func testRandU64FLessThanN(t *testing.T, n uint64) (r uint64) {
	var (
		bigN *big.Int
		bigR *big.Int
		err  error
	)

	if testPseudoRandom {
		if nil == testMathRandSource {
			testMathRandSource = mathRand.New(mathRand.NewSource(testPseudoRandomSeed))
		}
		r = uint64(testMathRandSource.Int63n(int64(n)))
	} else {
		bigN = big.NewInt(int64(n))
		bigR, err = cryptoRand.Int(cryptoRand.Reader, bigN)
		if nil != err {
			t.Fatalf("cryptoRand.Int(cryptoRand.Reader, bigN) returned error: %v", err)
		}
		r = bigR.Uint64()
	}

	return
}

func testRandByteSlice(t *testing.T, maxLen uint64) (slice []byte) {
	var (
		i        uint64
		sliceLen uint64
	)

	sliceLen = testRandU64FLessThanN(t, maxLen) + 1

	slice = make([]byte, sliceLen)

	for i = uint64(0); i < sliceLen; i++ {
		slice[i] = byte(testRandU64FLessThanN(t, uint64(0x100)))
	}

	return
}

func testKnuthShuffledU64Slice(t *testing.T, n uint64) (u64Slice []uint64) {
	var (
		i        uint64
		swapFrom uint64
		swapTo   uint64
	)

	u64Slice = make([]uint64, n)
	for i = uint64(0); i < n; i++ {
		u64Slice[i] = i
	}

	for swapFrom = uint64(n - 1); swapFrom > uint64(0); swapFrom-- {
		swapTo = testRandU64FLessThanN(t, swapFrom)
		u64Slice[swapFrom], u64Slice[swapTo] = u64Slice[swapTo], u64Slice[swapFrom]
	}

	return
}

func TestHeadHunterStress(t *testing.T) {
	var (
		bPlusTreeObjectMap                       map[uint64][]byte
		bPlusTreeObjectNumbers                   []uint64
		checkpointsSinceDownUp                   uint64
		checkpointsSinceDownUpCap                uint64
		confMap                                  conf.ConfMap
		confStrings                              []string
		deleteIndex                              uint64
		deleteKey                                uint64
		doneChan                                 chan bool
		err                                      error
		fetchNonceIndex                          uint64
		getIndex                                 uint64
		getKey                                   uint64
		inodeMap                                 map[uint64][]byte
		inodeNumbers                             []uint64
		logSegmentMap                            map[uint64][]byte
		logSegmentNumbers                        []uint64
		mapKey                                   uint64
		newInodesSinceRePutInode                 uint64
		newInodesSinceRePutInodeCap              uint64
		numInodeRecs                             uint64
		ok                                       bool
		putDeleteCallsSinceCheckpoint            uint64
		putDeleteCallsSinceCheckpointCap         uint64
		putDeleteCallsSinceGet                   uint64
		putDeleteCallsSinceGetCap                uint64
		putIndex                                 uint64
		putIndexPrev                             uint64
		putInodeRecCallsSincePutInodeRecsCall    uint64
		putInodeRecCallsSincePutInodeRecsCallCap uint64
		putInodeRecsIndex                        uint64
		putKey                                   uint64
		putKeys                                  []uint64
		putValues                                [][]byte
		signalHandlerIsArmed                     bool
		slice                                    []byte
		volumeHandle                             VolumeHandle
	)

	confStrings = []string{
		"Logging.LogFilePath=/dev/null",
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"SwiftClient.NoAuthIPAddr=127.0.0.1",
		"SwiftClient.NoAuthTCPPort=9999",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=0",
		"SwiftClient.RetryLimitObject=0",
		"SwiftClient.RetryDelay=1s",
		"SwiftClient.RetryDelayObject=1s",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"SwiftClient.StarvationCallbackFrequency=100ms",
		"Cluster.WhoAmI=Peer0",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"FlowControl:TestFlowControl.MaxFlushSize=10000000",
		"Volume:TestVolume.PrimaryPeer=Peer0",
		"Volume:TestVolume.AccountName=TestAccount",
		"Volume:TestVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:TestVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:TestVolume.CheckpointInterval=10h", // We never want a time-based checkpoint
		"Volume:TestVolume.FlowControl=TestFlowControl",
		"Volume:TestVolume.NonceValuesToReserve=1", // We want to force worst-case nonce fetching
		"Volume:TestVolume.MaxInodesPerMetadataNode=32",
		"Volume:TestVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:TestVolume.MaxDirFileNodesPerMetadataNode=16",
		"FSGlobals.VolumeList=TestVolume",
		"FSGlobals.InodeRecCacheEvictLowLimit=10000",
		"FSGlobals.InodeRecCacheEvictHighLimit=10010",
		"FSGlobals.LogSegmentRecCacheEvictLowLimit=10000",
		"FSGlobals.LogSegmentRecCacheEvictHighLimit=10010",
		"FSGlobals.BPlusTreeObjectCacheEvictLowLimit=10000",
		"FSGlobals.BPlusTreeObjectCacheEvictHighLimit=10010",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
	}

	// Launch a ramswift instance

	signalHandlerIsArmed = false
	doneChan = make(chan bool, 1) // Must be buffered to avoid race

	go ramswift.Daemon("/dev/null", confStrings, &signalHandlerIsArmed, doneChan, unix.SIGTERM)

	for !signalHandlerIsArmed {
		time.Sleep(100 * time.Millisecond)
	}

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned error: %v", err)
	}

	// Up packages up to headhunter

	err = logger.Up(confMap)
	if nil != err {
		t.Fatalf("logger.Up() returned error: %v", err)
	}

	err = evtlog.Up(confMap)
	if nil != err {
		t.Fatalf("evtlog.Up() returned error: %v", err)
	}

	err = stats.Up(confMap)
	if nil != err {
		t.Fatalf("stats.Up() returned error: %v", err)
	}

	err = dlm.Up(confMap)
	if nil != err {
		t.Fatalf("dlm.Up() returned error: %v", err)
	}

	err = swiftclient.Up(confMap)
	if nil != err {
		t.Fatalf("swiftclient.Up() returned error: %v", err)
	}

	// Format and launch headhunter package

	err = Format(confMap, "TestVolume")
	if nil != err {
		t.Fatalf("headhunter.Format() returned error: %v", err)
	}

	err = Up(confMap)
	if nil != err {
		t.Fatalf("headhunter.Up() returned error: %v", err)
	}

	volumeHandle, err = FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("headhunter.FetchVolumeHandle(\"TestVolume\") returned error: %v", err)
	}

	// Stress FetchNonce()

	for fetchNonceIndex = uint64(0); fetchNonceIndex < testNumFetchNonceCalls; fetchNonceIndex++ {
		_, err = volumeHandle.FetchNonce()
		if nil != err {
			t.Fatalf("headhunter.FetchNonce() returned error: %v", err)
		}
	}

	// Stress *InodeRec[|s]()

	inodeMap = make(map[uint64][]byte)

	for mapKey = uint64(0); mapKey < testNumInodeRecs; mapKey++ {
		inodeMap[mapKey] = testRandByteSlice(t, testMaxInodeRecSize)
	}

	checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
	newInodesSinceRePutInodeCap = testRandU64FLessThanN(t, testMaxNewInodesBetweenRePutInodes) + 1
	putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
	putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1
	putInodeRecCallsSincePutInodeRecsCallCap = testRandU64FLessThanN(t, testMaxPutInodeRecCallsBetweenPutInodeRecsCalls) + 1

	checkpointsSinceDownUp = 0
	newInodesSinceRePutInode = 0
	putDeleteCallsSinceCheckpoint = 0
	putDeleteCallsSinceGet = 0
	putInodeRecCallsSincePutInodeRecsCall = 0

	inodeNumbers = testKnuthShuffledU64Slice(t, testNumInodeRecs)

	putIndex = 0

	for putIndex < testNumInodeRecs {
		putIndexPrev = putIndex

		if putInodeRecCallsSincePutInodeRecsCall == putInodeRecCallsSincePutInodeRecsCallCap {
			numInodeRecs = testRandU64FLessThanN(t, testMaxInodesPerPutInodeRecsCall) + 1
			if (putIndex + numInodeRecs) > testNumInodeRecs {
				numInodeRecs = testNumInodeRecs - putIndex
			}
			putKeys = make([]uint64, numInodeRecs)
			putValues = make([][]byte, numInodeRecs)
			for putInodeRecsIndex = 0; putInodeRecsIndex < numInodeRecs; putInodeRecsIndex++ {
				putKey = inodeNumbers[putIndex]
				putKeys[putInodeRecsIndex] = putKey
				putValues[putInodeRecsIndex] = inodeMap[putKey]
				putIndex++
			}
			err = volumeHandle.PutInodeRecs(putKeys, putValues)
			if nil != err {
				t.Fatalf("headhunter.PutInodeRecs() returned error: %v", err)
			}
			putInodeRecCallsSincePutInodeRecsCallCap = testRandU64FLessThanN(t, testMaxPutInodeRecCallsBetweenPutInodeRecsCalls) + 1
			putInodeRecCallsSincePutInodeRecsCall = 0
		} else {
			putKey = inodeNumbers[putIndex]
			err = volumeHandle.PutInodeRec(putKey, inodeMap[putKey])
			if nil != err {
				t.Fatalf("headhunter.PutInodeRec() returned error: %v", err)
			}
			putInodeRecCallsSincePutInodeRecsCall++
			putIndex++
		}

		putDeleteCallsSinceGet++
		if putDeleteCallsSinceGet == putDeleteCallsSinceGetCap {
			getIndex = testRandU64FLessThanN(t, putIndex)
			getKey = inodeNumbers[getIndex]
			slice, ok, err = volumeHandle.GetInodeRec(getKey)
			if nil != err {
				t.Fatalf("headhunter.GetInodeRec() returned error: %v", err)
			}
			if !ok {
				t.Fatalf("headhunter.GetInodeRec() returned !ok")
			}
			if 0 != bytes.Compare(slice, inodeMap[getKey]) {
				t.Fatalf("headhunter.GetInodeRec() returned unexpected slice")
			}
			putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1
			putDeleteCallsSinceGet = 0
		}

		putDeleteCallsSinceCheckpoint++
		if putDeleteCallsSinceCheckpoint == putDeleteCallsSinceCheckpointCap {
			err = volumeHandle.DoCheckpoint()
			if nil != err {
				t.Fatalf("headhunter.DoCheckpoint() returned error: %v", err)
			}
			putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
			putDeleteCallsSinceCheckpoint = 0

			checkpointsSinceDownUp++
			if checkpointsSinceDownUp == checkpointsSinceDownUpCap {
				err = Down()
				if nil != err {
					t.Fatalf("headhunter.Down() returned error: %v", err)
				}
				err = swiftclient.Down()
				if nil != err {
					t.Fatalf("swiftclient.Down() returned error: %v", err)
				}
				err = swiftclient.Up(confMap)
				if nil != err {
					t.Fatalf("swiftclient.Up() returned error: %v", err)
				}
				err = Up(confMap)
				if nil != err {
					t.Fatalf("headhunter.Up() returned error: %v", err)
				}
				volumeHandle, err = FetchVolumeHandle("TestVolume")
				if nil != err {
					t.Fatalf("headhunter.FetchVolumeHandle(\"TestVolume\") returned error: %v", err)
				}
				checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
				checkpointsSinceDownUp = 0
			}
		}

		newInodesSinceRePutInode++
		if newInodesSinceRePutInode == newInodesSinceRePutInodeCap {
			for putIndex > putIndexPrev {
				putIndex--
				inodeMap[mapKey] = testRandByteSlice(t, testMaxInodeRecSize)
			}
			newInodesSinceRePutInodeCap = testRandU64FLessThanN(t, testMaxNewInodesBetweenRePutInodes) + 1
			newInodesSinceRePutInode = 0
		}
	}

	checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
	putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
	putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1

	checkpointsSinceDownUp = 0
	putDeleteCallsSinceCheckpoint = 0
	putDeleteCallsSinceGet = 0

	inodeNumbers = testKnuthShuffledU64Slice(t, testNumInodeRecs)

	deleteIndex = 0

	for deleteIndex < testNumInodeRecs {
		deleteKey = inodeNumbers[deleteIndex]
		err = volumeHandle.DeleteInodeRec(deleteKey)
		if nil != err {
			t.Fatalf("headhunter.DeleteInodeRec() returned error: %v", err)
		}

		deleteIndex++

		if deleteIndex < testNumInodeRecs {
			putDeleteCallsSinceGet++
			if putDeleteCallsSinceGet == putDeleteCallsSinceGetCap {
				getIndex = testRandU64FLessThanN(t, testNumInodeRecs-deleteIndex) + deleteIndex
				getKey = inodeNumbers[getIndex]
				slice, ok, err = volumeHandle.GetInodeRec(getKey)
				if nil != err {
					t.Fatalf("headhunter.GetInodeRec() returned error: %v", err)
				}
				if !ok {
					t.Fatalf("headhunter.GetInodeRec() returned !ok")
				}
				if 0 != bytes.Compare(slice, inodeMap[getKey]) {
					t.Fatalf("headhunter.GetInodeRec() returned unexpected slice")
				}
				putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1
				putDeleteCallsSinceGet = 0
			}
		}

		putDeleteCallsSinceCheckpoint++
		if putDeleteCallsSinceCheckpoint == putDeleteCallsSinceCheckpointCap {
			err = volumeHandle.DoCheckpoint()
			if nil != err {
				t.Fatalf("headhunter.DoCheckpoint() returned error: %v", err)
			}
			putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
			putDeleteCallsSinceCheckpoint = 0

			checkpointsSinceDownUp++
			if checkpointsSinceDownUp == checkpointsSinceDownUpCap {
				err = Down()
				if nil != err {
					t.Fatalf("headhunter.Down() returned error: %v", err)
				}
				err = swiftclient.Down()
				if nil != err {
					t.Fatalf("swiftclient.Down() returned error: %v", err)
				}
				err = swiftclient.Up(confMap)
				if nil != err {
					t.Fatalf("swiftclient.Up() returned error: %v", err)
				}
				err = Up(confMap)
				if nil != err {
					t.Fatalf("headhunter.Up() returned error: %v", err)
				}
				volumeHandle, err = FetchVolumeHandle("TestVolume")
				if nil != err {
					t.Fatalf("headhunter.FetchVolumeHandle(\"TestVolume\") returned error: %v", err)
				}
				checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
				checkpointsSinceDownUp = 0
			}
		}
	}

	// Stress *LogSegmentRec()

	logSegmentMap = make(map[uint64][]byte)

	for mapKey = uint64(0); mapKey < testNumLogSegmentRecs; mapKey++ {
		logSegmentMap[mapKey] = testRandByteSlice(t, testMaxLogSegmentRecSize)
	}

	checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
	putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
	putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1

	checkpointsSinceDownUp = 0
	putDeleteCallsSinceCheckpoint = 0
	putDeleteCallsSinceGet = 0

	logSegmentNumbers = testKnuthShuffledU64Slice(t, testNumLogSegmentRecs)

	putIndex = 0

	for putIndex < testNumLogSegmentRecs {
		putKey = logSegmentNumbers[putIndex]
		err = volumeHandle.PutLogSegmentRec(putKey, logSegmentMap[putKey])
		if nil != err {
			t.Fatalf("headhunter.PutLogSegmentRec() returned error: %v", err)
		}

		putIndex++

		putDeleteCallsSinceGet++
		if putDeleteCallsSinceGet == putDeleteCallsSinceGetCap {
			getIndex = testRandU64FLessThanN(t, putIndex)
			getKey = logSegmentNumbers[getIndex]
			slice, err = volumeHandle.GetLogSegmentRec(getKey)
			if nil != err {
				t.Fatalf("headhunter.GetLogSegmentRec() returned error: %v", err)
			}
			if 0 != bytes.Compare(slice, logSegmentMap[getKey]) {
				t.Fatalf("headhunter.GetLogSegmentRec() returned unexpected slice")
			}
			putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1
			putDeleteCallsSinceGet = 0
		}

		putDeleteCallsSinceCheckpoint++
		if putDeleteCallsSinceCheckpoint == putDeleteCallsSinceCheckpointCap {
			err = volumeHandle.DoCheckpoint()
			if nil != err {
				t.Fatalf("headhunter.DoCheckpoint() returned error: %v", err)
			}
			putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
			putDeleteCallsSinceCheckpoint = 0

			checkpointsSinceDownUp++
			if checkpointsSinceDownUp == checkpointsSinceDownUpCap {
				err = Down()
				if nil != err {
					t.Fatalf("headhunter.Down() returned error: %v", err)
				}
				err = swiftclient.Down()
				if nil != err {
					t.Fatalf("swiftclient.Down() returned error: %v", err)
				}
				err = swiftclient.Up(confMap)
				if nil != err {
					t.Fatalf("swiftclient.Up() returned error: %v", err)
				}
				err = Up(confMap)
				if nil != err {
					t.Fatalf("headhunter.Up() returned error: %v", err)
				}
				volumeHandle, err = FetchVolumeHandle("TestVolume")
				if nil != err {
					t.Fatalf("headhunter.FetchVolumeHandle(\"TestVolume\") returned error: %v", err)
				}
				checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
				checkpointsSinceDownUp = 0
			}
		}
	}

	checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
	putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
	putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1

	checkpointsSinceDownUp = 0
	putDeleteCallsSinceCheckpoint = 0
	putDeleteCallsSinceGet = 0

	logSegmentNumbers = testKnuthShuffledU64Slice(t, testNumLogSegmentRecs)

	deleteIndex = 0

	for deleteIndex < testNumLogSegmentRecs {
		deleteKey = logSegmentNumbers[deleteIndex]
		err = volumeHandle.DeleteLogSegmentRec(deleteKey)
		if nil != err {
			t.Fatalf("headhunter.DeleteLogSegmentRec() returned error: %v", err)
		}

		deleteIndex++

		if deleteIndex < testNumLogSegmentRecs {
			putDeleteCallsSinceGet++
			if putDeleteCallsSinceGet == putDeleteCallsSinceGetCap {
				getIndex = testRandU64FLessThanN(t, testNumLogSegmentRecs-deleteIndex) + deleteIndex
				getKey = logSegmentNumbers[getIndex]
				slice, err = volumeHandle.GetLogSegmentRec(getKey)
				if nil != err {
					t.Fatalf("headhunter.GetLogSegmentRec() returned error: %v", err)
				}
				if 0 != bytes.Compare(slice, logSegmentMap[getKey]) {
					t.Fatalf("headhunter.GetLogSegmentRec() returned unexpected slice")
				}
				putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1
				putDeleteCallsSinceGet = 0
			}
		}

		putDeleteCallsSinceCheckpoint++
		if putDeleteCallsSinceCheckpoint == putDeleteCallsSinceCheckpointCap {
			err = volumeHandle.DoCheckpoint()
			if nil != err {
				t.Fatalf("headhunter.DoCheckpoint() returned error: %v", err)
			}
			putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
			putDeleteCallsSinceCheckpoint = 0

			checkpointsSinceDownUp++
			if checkpointsSinceDownUp == checkpointsSinceDownUpCap {
				err = Down()
				if nil != err {
					t.Fatalf("headhunter.Down() returned error: %v", err)
				}
				err = swiftclient.Down()
				if nil != err {
					t.Fatalf("swiftclient.Down() returned error: %v", err)
				}
				err = swiftclient.Up(confMap)
				if nil != err {
					t.Fatalf("swiftclient.Up() returned error: %v", err)
				}
				err = Up(confMap)
				if nil != err {
					t.Fatalf("headhunter.Up() returned error: %v", err)
				}
				volumeHandle, err = FetchVolumeHandle("TestVolume")
				if nil != err {
					t.Fatalf("headhunter.FetchVolumeHandle(\"TestVolume\") returned error: %v", err)
				}
				checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
				checkpointsSinceDownUp = 0
			}
		}
	}

	// Stress *BPlusTreeObject()

	bPlusTreeObjectMap = make(map[uint64][]byte)

	for mapKey = uint64(0); mapKey < testNumBPlusTreeObjects; mapKey++ {
		bPlusTreeObjectMap[mapKey] = testRandByteSlice(t, testMaxBPlusTreeObjectSize)
	}

	checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
	putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
	putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1

	checkpointsSinceDownUp = 0
	putDeleteCallsSinceCheckpoint = 0
	putDeleteCallsSinceGet = 0

	bPlusTreeObjectNumbers = testKnuthShuffledU64Slice(t, testNumBPlusTreeObjects)

	putIndex = 0

	for putIndex < testNumBPlusTreeObjects {
		putKey = bPlusTreeObjectNumbers[putIndex]
		err = volumeHandle.PutBPlusTreeObject(putKey, bPlusTreeObjectMap[putKey])
		if nil != err {
			t.Fatalf("headhunter.PutBPlusTreeObject() returned error: %v", err)
		}

		putIndex++

		putDeleteCallsSinceGet++
		if putDeleteCallsSinceGet == putDeleteCallsSinceGetCap {
			getIndex = testRandU64FLessThanN(t, putIndex)
			getKey = bPlusTreeObjectNumbers[getIndex]
			slice, err = volumeHandle.GetBPlusTreeObject(getKey)
			if nil != err {
				t.Fatalf("headhunter.GetBPlusTreeObject() returned error: %v", err)
			}
			if 0 != bytes.Compare(slice, bPlusTreeObjectMap[getKey]) {
				t.Fatalf("headhunter.GetBPlusTreeObject() returned unexpected slice")
			}
			putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1
			putDeleteCallsSinceGet = 0
		}

		putDeleteCallsSinceCheckpoint++
		if putDeleteCallsSinceCheckpoint == putDeleteCallsSinceCheckpointCap {
			err = volumeHandle.DoCheckpoint()
			if nil != err {
				t.Fatalf("headhunter.DoCheckpoint() returned error: %v", err)
			}
			putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
			putDeleteCallsSinceCheckpoint = 0

			checkpointsSinceDownUp++
			if checkpointsSinceDownUp == checkpointsSinceDownUpCap {
				err = Down()
				if nil != err {
					t.Fatalf("headhunter.Down() returned error: %v", err)
				}
				err = swiftclient.Down()
				if nil != err {
					t.Fatalf("swiftclient.Down() returned error: %v", err)
				}
				err = swiftclient.Up(confMap)
				if nil != err {
					t.Fatalf("swiftclient.Up() returned error: %v", err)
				}
				err = Up(confMap)
				if nil != err {
					t.Fatalf("headhunter.Up() returned error: %v", err)
				}
				volumeHandle, err = FetchVolumeHandle("TestVolume")
				if nil != err {
					t.Fatalf("headhunter.FetchVolumeHandle(\"TestVolume\") returned error: %v", err)
				}
				checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
				checkpointsSinceDownUp = 0
			}
		}
	}

	checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
	putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
	putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1

	checkpointsSinceDownUp = 0
	putDeleteCallsSinceCheckpoint = 0
	putDeleteCallsSinceGet = 0

	bPlusTreeObjectNumbers = testKnuthShuffledU64Slice(t, testNumBPlusTreeObjects)

	deleteIndex = 0

	for deleteIndex < testNumBPlusTreeObjects {
		deleteKey = bPlusTreeObjectNumbers[deleteIndex]
		err = volumeHandle.DeleteBPlusTreeObject(deleteKey)
		if nil != err {
			t.Fatalf("headhunter.DeleteBPlusTreeObject() returned error: %v", err)
		}

		deleteIndex++

		if deleteIndex < testNumBPlusTreeObjects {
			putDeleteCallsSinceGet++
			if putDeleteCallsSinceGet == putDeleteCallsSinceGetCap {
				getIndex = testRandU64FLessThanN(t, testNumBPlusTreeObjects-deleteIndex) + deleteIndex
				getKey = bPlusTreeObjectNumbers[getIndex]
				slice, err = volumeHandle.GetBPlusTreeObject(getKey)
				if nil != err {
					t.Fatalf("headhunter.GetBPlusTreeObject() returned error: %v", err)
				}
				if 0 != bytes.Compare(slice, bPlusTreeObjectMap[getKey]) {
					t.Fatalf("headhunter.GetBPlusTreeObject() returned unexpected slice")
				}
				putDeleteCallsSinceGetCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerGet) + 1
				putDeleteCallsSinceGet = 0
			}
		}

		putDeleteCallsSinceCheckpoint++
		if putDeleteCallsSinceCheckpoint == putDeleteCallsSinceCheckpointCap {
			err = volumeHandle.DoCheckpoint()
			if nil != err {
				t.Fatalf("headhunter.DoCheckpoint() returned error: %v", err)
			}
			putDeleteCallsSinceCheckpointCap = testRandU64FLessThanN(t, testMaxPutDeleteCallsPerCheckpoint) + 1
			putDeleteCallsSinceCheckpoint = 0

			checkpointsSinceDownUp++
			if checkpointsSinceDownUp == checkpointsSinceDownUpCap {
				err = Down()
				if nil != err {
					t.Fatalf("headhunter.Down() returned error: %v", err)
				}
				err = swiftclient.Down()
				if nil != err {
					t.Fatalf("swiftclient.Down() returned error: %v", err)
				}
				err = swiftclient.Up(confMap)
				if nil != err {
					t.Fatalf("swiftclient.Up() returned error: %v", err)
				}
				err = Up(confMap)
				if nil != err {
					t.Fatalf("headhunter.Up() returned error: %v", err)
				}
				volumeHandle, err = FetchVolumeHandle("TestVolume")
				if nil != err {
					t.Fatalf("headhunter.FetchVolumeHandle(\"TestVolume\") returned error: %v", err)
				}
				checkpointsSinceDownUpCap = testRandU64FLessThanN(t, testMaxCheckpointsPerDownUp) + 1
				checkpointsSinceDownUp = 0
			}
		}
	}

	// Cleanly shutdown and restart

	err = Down()
	if nil != err {
		t.Fatalf("headhunter.Down() returned error: %v", err)
	}

	err = swiftclient.Down()
	if nil != err {
		t.Fatalf("swiftclient.Down() returned error: %v", err)
	}

	err = dlm.Down()
	if nil != err {
		t.Fatalf("dlm.Down() returned error: %v", err)
	}

	err = stats.Down()
	if nil != err {
		t.Fatalf("stats.Down() returned error: %v", err)
	}

	err = evtlog.Down()
	if nil != err {
		t.Fatalf("evtlog.Down() returned error: %v", err)
	}

	err = logger.Down()
	if nil != err {
		t.Fatalf("logger.Down() returned error: %v", err)
	}

	err = logger.Up(confMap)
	if nil != err {
		t.Fatalf("logger.Up() returned error: %v", err)
	}

	err = evtlog.Up(confMap)
	if nil != err {
		t.Fatalf("evtlog.Up() returned error: %v", err)
	}

	err = stats.Up(confMap)
	if nil != err {
		t.Fatalf("stats.Up() returned error: %v", err)
	}

	err = dlm.Up(confMap)
	if nil != err {
		t.Fatalf("dlm.Up() returned error: %v", err)
	}

	err = swiftclient.Up(confMap)
	if nil != err {
		t.Fatalf("swiftclient.Up() returned error: %v", err)
	}

	err = Up(confMap)
	if nil != err {
		t.Fatalf("headhunter.Up() returned error: %v", err)
	}

	// TODO: Please stress reading test steps here...
	//   GetInodeRec(inodeNumber uint64) (value []byte, ok bool, err error)
	//   GetLogSegmentRec(logSegmentNumber uint64) (value []byte, err error)
	//   GetBPlusTreeObject(objectNumber uint64) (value []byte, err error)

	// Cleanly shutdown

	err = Down()
	if nil != err {
		t.Fatalf("headhunter.Down() returned error: %v", err)
	}

	err = swiftclient.Down()
	if nil != err {
		t.Fatalf("swiftclient.Down() returned error: %v", err)
	}

	err = dlm.Down()
	if nil != err {
		t.Fatalf("dlm.Down() returned error: %v", err)
	}

	err = stats.Down()
	if nil != err {
		t.Fatalf("stats.Down() returned error: %v", err)
	}

	err = evtlog.Down()
	if nil != err {
		t.Fatalf("evtlog.Down() returned error: %v", err)
	}

	err = logger.Down()
	if nil != err {
		t.Fatalf("logger.Down() returned error: %v", err)
	}

	// Send ourself a SIGTERM to terminate ramswift.Daemon()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	_ = <-doneChan
}
