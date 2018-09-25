package consensus

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/namespace"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test basic etcd primitives
func TestEtcdAPI(t *testing.T) {

	testBasicPutGet(t)
	testBasicTxn(t)
	testTxnWatcher(t)
	testTxnHb(t)
	testGetMultipleKeys(t)

	testCleanupTestKeys(t) // NOTE: Leave this test last!
}

// Delete test keys and recreate them using the etcd API
func resetKeys(t *testing.T, cli *clientv3.Client, km map[string]string) {
	assert := assert.New(t)
	kvc := clientv3.NewKV(cli)
	for k := range km {
		_, _ = cli.Delete(context.TODO(), k)
		_, err := kvc.Put(context.TODO(), k, "")
		assert.Nil(err, "kvc.Put() returned err")
	}
}

// Test basic etcd API based on this link:
// https://godoc.org/github.com/coreos/etcd/clientv3/namespace
//
// This test uses Put() and Get()
func testBasicPutGet(t *testing.T) {
	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	//
	// TODO - store cli in inode volume struct.  Any issue with storing
	// context() - comments on webpage seems to discourage it?
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

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

	// To retrieve the key from the command line you must do:
	// # ETCDCTL_API=3 etcdctl --endpoints=http://192.168.60.11:2379 get "myVolume/abc"
}

// Test transactions
// TODO - change to use namespace
func testBasicTxn(t *testing.T) {
	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

	kvc := clientv3.NewKV(cli)

	_, err = kvc.Put(context.TODO(), "TESTkey", "xyz")
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

// TODO - must wrap with WithRequiredLeader
func watcher(t *testing.T, cli *clientv3.Client, key string, expectedValue string,
	swg *sync.WaitGroup, fwg *sync.WaitGroup) {

	swg.Done() // The watcher is running!

	assert := assert.New(t)
	wch1 := cli.Watch(context.Background(), key)
	for wresp1 := range wch1 {
		for _, e := range wresp1.Events {
			assert.Equal(expectedValue, string(e.Kv.Value),
				"watcher saw different value than expected")

			// Handle heartbeat test case
			if string(e.Kv.Key) == "TESTHB" {

				// Get current time
				currentTime := time.Now().UTC()

				// Unmarshal time HB sent
				var sentTime time.Time
				err := sentTime.UnmarshalText(e.Kv.Value)
				assert.Nil(err, "UnmarshalText() failed with HB")

				// Current time must be at least 500 milliseconds later
				testTime := sentTime
				testTime.Add(500 * time.Millisecond)
				assert.True(currentTime.After(testTime), "testTime !> 500 milliseconds")
			}
		}

		// The watcher has received it's event and will now return
		fwg.Done()
		return
	}
}

// Start a different watcher for each key
func startWatchers(t *testing.T, cli *clientv3.Client, km map[string]string, swg *sync.WaitGroup,
	fwg *sync.WaitGroup) {
	for k, v := range km {
		swg.Add(1)
		go watcher(t, cli, k, v, swg, fwg)
	}

	// Wait for watchers to start
	swg.Wait()
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
	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

	kvc := clientv3.NewKV(cli)

	// Reset the test keys to be used
	m := make(map[string]string)
	m[testKey1] = testData1
	m[testKey2] = testData2
	resetKeys(t, cli, m)

	// Create watchers for each key
	startWatchers(t, cli, m, &startWG, &finishWG)

	// NOTE: "etcd does not ensure linearizability for watch operations. Users are
	// expected to verify the revision of watch responses to ensure correct ordering."
	// TODO - figure out what this means
	// TODO - review use of WithRequireLeader.  Seems important for a subcluster to
	//     recognize partitioned out of cluster.

	// Update multiple keys at once and guarantee get watch event for
	// all key updates.
	finishWG.Add(len(m))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = kvc.Txn(ctx).

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
	assert := assert.New(t)

	testHbData, errTime := time.Now().UTC().MarshalText()
	time.Sleep(1 * time.Second)
	assert.Nil(errTime, "time.Now.UTC().MarshalText() returned err")

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

	kvc := clientv3.NewKV(cli)

	// Reset the test keys to be used
	m := make(map[string]string)
	m[testHb] = string(testHbData)
	resetKeys(t, cli, m)

	// Create watchers for each key
	startWatchers(t, cli, m, &startWG, &finishWG)

	// NOTE: "etcd does not ensure linearizability for watch operations. Users are
	// expected to verify the revision of watch responses to ensure correct ordering."
	// TODO - figure out what this means
	// TODO - review use of WithRequireLeader.  Seems important for a subcluster to
	//     recognize partitioned out of cluster.

	// Send HB and guarantee get watch event for HB
	finishWG.Add(len(m))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = kvc.Txn(ctx).

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
	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

	kvc := clientv3.NewKV(cli)

	// Reset the test keys to be used
	m := make(map[string]string)
	m[testKey1] = string(testData1)
	m[testKey2] = string(testData2)
	resetKeys(t, cli, m)

	// Create watchers for each key
	startWatchers(t, cli, m, &startWG, &finishWG)

	// Create the keys with our data so that we can retrieve them
	finishWG.Add(len(m))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = kvc.Txn(ctx).

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

// Cleanup all the test keys created by these tests
func testCleanupTestKeys(t *testing.T) {
	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

	_, _ = cli.Delete(context.TODO(), "TEST", clientv3.WithPrefix())
}
