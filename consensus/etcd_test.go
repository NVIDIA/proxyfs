package consensus

import (
	"context"
	"github.com/stretchr/testify/assert"
	tu "github.com/swiftstack/ProxyFS/consensus/testutils"
	clientv3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/namespace"
	"sync"
	"testing"
	"time"
)

// Test basic etcd primitives
func TestEtcdAPI(t *testing.T) {

	testBasicPutGet(t)
	testBasicTxn(t)
	testTxnWatcher(t)
	testTxnHb(t)
	testGetMultipleKeys(t)
}

// TODO - don't start clients since want Register() to create it....
// how create with http interface, etc?
//
// test:
// 1. start 3 node cluster
// 2. Register() with endpoints
// 3. Do some tests with API
// 4. Shutdown more than half of cluster
// 6. attempt transaction and get back no leader error....

// Test basic etcd API based on this link:
// https://godoc.org/github.com/coreos/etcd/clientv3/namespace
//
// This test uses Put() and Get()
func testBasicPutGet(t *testing.T) {

	// Start a 3 node etcd cluster
	clus := tu.CreateCluster(t, 3)
	defer clus.Terminate(t)

	// Create a client to send/receive requests from cluster
	cli := clus.Client(0)
	defer tu.CleanupClient(cli, 0, clus)

	assert := assert.New(t)

	// Override the client interfaces using a namespace.  This
	// allows volumes to only get relevant messages.
	cli.KV = namespace.NewKV(cli.KV, "TESTmyVolume/")
	cli.Watcher = namespace.NewWatcher(cli.Watcher, "TESTmyVolume/")
	cli.Lease = namespace.NewLease(cli.Lease, "TESTmyVolume/")

	// All Put() and Get() requests on this cli will automatically
	// include the prefix (namespace).
	cli.Put(context.TODO(), "TESTabc", "123")

	resp, err := cli.Get(context.TODO(), "TESTabc")
	assert.Equal("123", string(resp.Kvs[0].Value), "Get() returned different key than Put()")
	assert.Nil(err, "Get() err != nil")
}

// Test transactions
func testBasicTxn(t *testing.T) {

	// Start a 3 node etcd cluster
	clus := tu.CreateCluster(t, 3)
	defer clus.Terminate(t)

	// Create a client to send/receive requests from cluster
	cli := clus.Client(0)
	defer tu.CleanupClient(cli, 0, clus)

	kvc := clientv3.NewKV(cli)

	assert := assert.New(t)
	_, err := kvc.Put(context.TODO(), "TESTkey", "xyz")
	if err != nil {
		assert.Nil(err, "kvc.Put() returned err")
	}

	// NOTE: cancel() function and call after transaction!
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(clientv3.Compare(clientv3.Value("TESTkey"), ">", "abc")).

		// the "Then" runs, since "xyz" > "abc"
		Then(clientv3.OpPut("TESTkey", "XYZ")).

		// the "Else" does not run
		Else(clientv3.OpPut("TESTkey", "ABC")).
		Commit()
	cancel()
	if err != nil {
		assert.Nil(err, "kvc.Txn() returned err")
	}

	gresp, err := kvc.Get(context.TODO(), "TESTkey")
	cancel()
	if err != nil {
		assert.Nil(err, "kvc.Get() returned err")
	}
	for _, ev := range gresp.Kvs {
		assert.Equal("XYZ", string(ev.Value), "Get() returned different key than Put()")
	}
}

// Test multiple updates of two keys in same transaction and a watcher
// per key updated.
//
// TODO - simulate online, offline, failover/fencing, all nodes start from scratch....
func testTxnWatcher(t *testing.T) {
	var (
		testKey1  = "TESTKEY1"
		testData1 = "testdata1"
		testKey2  = "TESTKEY2"
		testData2 = "testdata2"
		startWG   sync.WaitGroup // Used to sync start of watcher
		finishWG  sync.WaitGroup // Used to block until watcher sees event
	)

	// Start a 3 node etcd cluster
	clus := tu.CreateCluster(t, 3)
	defer clus.Terminate(t)

	// Create a client to send/receive requests from cluster
	cli := clus.Client(0)
	defer tu.CleanupClient(cli, 0, clus)

	assert := assert.New(t)

	kvc := clientv3.NewKV(cli)

	// Reset the test keys to be used
	testData := make(map[string]string)
	testData[testKey1] = testData1
	testData[testKey2] = testData2
	tu.ResetKeys(t, cli, testData)

	// Create watchers for each key
	tu.StartWatchers(t, cli, testData, &startWG, &finishWG)

	// NOTE: "etcd does not ensure linearizability for watch operations. Users are
	// expected to verify the revision of watch responses to ensure correct ordering."
	// TODO - figure out what this means
	// TODO - review use of WithRequireLeader.  Seems important for a subcluster to
	//     recognize partitioned out of cluster.

	// Update multiple keys at once and guarantee get watch event for
	// all key updates.
	finishWG.Add(len(testData))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(testKey1), "=", ""),
			clientv3.Compare(clientv3.Value(testKey2), "=", ""),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(testKey1, testData1),
		clientv3.OpPut(testKey2, testData2),

	// the "Else" does not run
	).Else(
		clientv3.OpPut(testKey1, "FailedTestKey1"),
		clientv3.OpPut(testKey2, "FailedTestKey2"),
	).Commit()
	cancel()
	if err != nil {
		assert.Nil(err, "kvc.Txn() returned err")
	}

	// Wait for watchers to get events before exiting test
	finishWG.Wait()
}

// Test sending heartbeat in txn() and watching for heartbeat to arrive.
// per key updated.
func testTxnHb(t *testing.T) {
	var (
		testHb   = "TESTHB"
		startWG  sync.WaitGroup // Used to sync start of watcher
		finishWG sync.WaitGroup // Used to block until watcher sees event
	)

	// Start a 3 node etcd cluster
	clus := tu.CreateCluster(t, 3)
	defer clus.Terminate(t)

	// Create a client to send/receive requests from cluster
	cli := clus.Client(0)
	defer tu.CleanupClient(cli, 0, clus)

	assert := assert.New(t)

	testHbData, errTime := time.Now().UTC().MarshalText()
	time.Sleep(1 * time.Second)
	assert.Nil(errTime, "time.Now.UTC().MarshalText() returned err")

	kvc := clientv3.NewKV(cli)

	// Reset the test keys to be used
	testData := make(map[string]string)
	testData[testHb] = string(testHbData)
	tu.ResetKeys(t, cli, testData)

	// Create watchers for each key
	tu.StartWatchers(t, cli, testData, &startWG, &finishWG)

	// NOTE: "etcd does not ensure linearizability for watch operations. Users are
	// expected to verify the revision of watch responses to ensure correct ordering."
	// TODO - figure out what this means
	// TODO - review use of WithRequireLeader.  Seems important for a subcluster to
	//     recognize partitioned out of cluster.

	// Send HB and guarantee get watch event for HB
	finishWG.Add(len(testData))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(testHb), "=", ""),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(testHb, string(testHbData)),

	// the "Else" does not run
	).Else(
		clientv3.OpPut(testHb, "FailedTestHb"),
	).Commit()
	cancel()
	if err != nil {
		assert.Nil(err, "kvc.Txn() returned err")
	}

	// Wait for watchers to get events before exiting test
	finishWG.Wait()
}

// Test getting multiple different keys in one txn()
func testGetMultipleKeys(t *testing.T) {
	var (
		testStr   = "TEST"
		testKey   = "KEY"
		testKey1  = testStr + testKey + "1"
		testData1 = testStr + "DATA1"
		testKey2  = testStr + testKey + "2"
		testData2 = testStr + "DATA2"
		startWG   sync.WaitGroup // Used to sync start of watcher
		finishWG  sync.WaitGroup // Used to block until watcher sees event
	)

	// Start a 3 node etcd cluster
	clus := tu.CreateCluster(t, 3)
	defer clus.Terminate(t)

	// Create a client to send/receive requests from cluster
	cli := clus.Client(0)
	defer tu.CleanupClient(cli, 0, clus)

	assert := assert.New(t)

	kvc := clientv3.NewKV(cli)

	// Reset the test keys to be used
	testData := make(map[string]string)
	testData[testKey1] = string(testData1)
	testData[testKey2] = string(testData2)
	tu.ResetKeys(t, cli, testData)

	// Create watchers for each key
	tu.StartWatchers(t, cli, testData, &startWG, &finishWG)

	// Create the keys with our data so that we can retrieve them
	finishWG.Add(len(testData))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(testKey1), "=", ""),
			clientv3.Compare(clientv3.Value(testKey2), "=", ""),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(testKey1, string(testData1)),
		clientv3.OpPut(testKey2, string(testData2)),

	// the "Else" does not run
	).Else(
		clientv3.OpPut(testKey1, "FailedPutMultipleKeys"),
		clientv3.OpPut(testKey2, "FailedPutMultipleKeys"),
	).Commit()
	cancel()
	if err != nil {
		assert.Nil(err, "kvc.Txn() returned err")
	}

	// Wait for watchers to see keys
	finishWG.Wait()

	// Now get multiple keys
	resp, err := cli.Get(context.TODO(), testStr+testKey, clientv3.WithPrefix())
	assert.Equal(int64(2), resp.Count, "Get() returned wrong length for number of keys")
}
