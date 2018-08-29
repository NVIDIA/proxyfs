package etcd

import (
	"context"
	"flag"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/swiftstack/ProxyFS/logger"
)

// Largely stolen from fs/api_test.go
func testSetup() (err error) {
	return
}

// Largely stolen from fs/api_test.go
func testTeardown() (err error) {
	return
}

// Largely stolen from fs/api_test.go
func TestMain(m *testing.M) {
	flag.Parse()

	err := testSetup()
	if nil != err {
		logger.ErrorWithError(err)
	}

	testResults := m.Run()

	err = testTeardown()
	if nil != err {
		logger.ErrorWithError(err)
	}

	os.Exit(testResults)
}

// Test basic etcd primitives
func TestEtcdAPI(t *testing.T) {

	testBasicPutGet(t)
	testBasicTxn(t)
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
	// context() - comments on webpage seem to discourage it
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

	// Override the client interfaces using a namespace.  This
	// allows volumes to only get relevant messages.
	cli.KV = namespace.NewKV(cli.KV, "myVolume/")
	cli.Watcher = namespace.NewWatcher(cli.Watcher, "myVolume/")
	cli.Lease = namespace.NewLease(cli.Lease, "myVolume/")

	// All Put() and Get() requests on this cli will automatically
	// include the prefix (namespace).
	cli.Put(context.TODO(), "abc", "123")

	resp, err := cli.Get(context.TODO(), "abc")
	assert.Equal("123", string(resp.Kvs[0].Value), "Get() returned different key than Put()")

	// To retrieve the key from the command line you must do:
	// # ETCDCTL_API=3 etcdctl --endpoints=http://192.168.60.11:2379 get "myVolume/abc"
}

// Test transactions
//
// TODO - watcher, transaction... simulate 2 different volumes
// each with our conf settings for VIP, etc...
//
// simulate online, offline, failover....
func testBasicTxn(t *testing.T) {
	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	//
	// TODO - store cli in inode volume struct.  Any issue with storing
	// context() - comments on webpage seem to discourage it
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.60.10:2379",
		"192.168.60.11:2379", "192.168.60.12:2379"}, DialTimeout: 2 * time.Second})
	if err != nil {
		assert.Nil(err, "clientv3.New() returned err")
		// handle error!
	}
	defer cli.Close()

	kvc := clientv3.NewKV(cli)

	_, err = kvc.Put(context.TODO(), "key", "xyz")
	if err != nil {
		assert.Nil(err, "kvc.Put() returned err")
	}

	// TODO - TODO - change to use namespace....
	// also change multiple keys at once and prove that we get multiple
	// watch events as expected...

	// NOTE: cancel() function and call after transaction!
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(clientv3.Compare(clientv3.Value("key"), ">", "abc")).

		// the "Then" runs, since "xyz" > "abc"
		Then(clientv3.OpPut("key", "XYZ")).

		// the "Else" does not run
		Else(clientv3.OpPut("key", "ABC")).
		Commit()
	cancel()
	if err != nil {
		assert.Nil(err, "kvc.Txn() returned err")
	}

	gresp, err := kvc.Get(context.TODO(), "key")
	cancel()
	if err != nil {
		assert.Nil(err, "kvc.Get() returned err")
	}
	for _, ev := range gresp.Kvs {
		assert.Equal("XYZ", string(ev.Value), "Get() returned different key than Put()")
	}
}
