package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/clientv3"
	"testing"
	"time"
)

// Test etcd wrapper routines and demonstrate how to use them.
func TestEtcdNoWrapperAPI(t *testing.T) {

	testAddWWidget(t)
	//testRmWidget(t)
	//testUpdateWidget(t)
	// TODO - add test for mv from workQ to finishQ case...
}

// WidInfoValue is the "value" of the key/value store.  It is the information
// associated with the widget in the etcd database (except its JSON encoded
// in the database)
type WWidInfoValue struct {
	State   string
	AttrStr string
	AttrInt int
}

// WidInfo is information associated with a widget at a given revision number.
// It includes when the info was fetched and the last time the value was modified.
type WWidInfo struct {
	EtcdKeyHeader
	WWidInfoValue
}

func getWWidKeyPrefix() string {
	return "WonkaWidgetKey:"
}

func makeWWidKey(name string) string {
	return getWWidKeyPrefix() + name
}

// getWWidInfo is the function used to GET the key from etcd and unpack the
// JSON encoded "value" into a WWidInfo structure.
func getWWidInfo(cs *EtcdConn, name string, revNum RevisionNumber) (wwidInfo *WWidInfo,
	err error) {

	wwidKey := makeWWidKey(name)

	// Do a GET on the key and use extract value struct from JSON
	getResp, err := cs.cli.Get(context.TODO(), wwidKey)
	if getResp.Count == 0 {
		fmt.Printf("Key: %v does not exist\n", wwidKey)
		return nil, err
	}

	var wwidInfoValue WWidInfoValue
	err = json.Unmarshal(getResp.Kvs[0].Value, &wwidInfoValue)
	if err != nil {
		fmt.Printf("getWWidInfo(): Unmarshal of key '%s' "+
			"value '%v' failed: %s\n",
			wwidKey, string(getResp.Kvs[0].Value), err)
		return nil, err
	}

	wwidInfo.CreateRevNum = RevisionNumber(getResp.Kvs[0].CreateRevision)
	wwidInfo.ModRevNum = RevisionNumber(getResp.Kvs[0].ModRevision)
	wwidInfo.Version = getResp.Kvs[0].Version
	wwidInfo.WWidInfoValue = wwidInfoValue
	return
}

// Pack the value into JSON and do a Put via a txn()
func putWWidInfo(cs *EtcdConn, name string, wwidInfo *WWidInfo, assert *assert.Assertions,
	shouldSucceed bool) (err error) {
	wwJSON, err := json.MarshalIndent(wwidInfo, "", "  ")
	if err != nil {
		fmt.Printf("putWWidInfo() key: %v wwidInfo: %v - json.MarshalIndent() returned err: %v\n",
			name, wwidInfo, err)
		return
	}

	var (
		txnResp *clientv3.TxnResponse
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(name, string(wwJSON)),
	).Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	if shouldSucceed {
		assert.True(txnResp.Succeeded, "Expected txn to succeed")
	} else {
		assert.False(txnResp.Succeeded, "Expected txn to fail")
	}
	return
}

// Use the wrappers to create a key for the first time
func testAddWWidget(t *testing.T) {
	var (
		name = "testWidget"
	)
	assert := assert.New(t)

	cs, tc := newHA(t)
	defer closeHA(t, cs, tc)
	assert.NotNil(cs, "cs should not be nil")

	// Check if the widget already exists by doing a GET with
	// revision number of 0
	wwidInfo, err := getWWidInfo(cs, name, RevisionNumber(0))
	assert.Nil(wwidInfo, "wwidInfo should be nil")
	assert.Nil(err, "err should be nil")

	// The widget does not exist (yet), so initialize the WidInfoValue
	// information it will have when created.
	wwidInfo = &WWidInfo{
		WWidInfoValue: WWidInfoValue{
			State:   "SOMESTATE",
			AttrInt: 5,
			AttrStr: "someString",
		},
	}

	// Create the operation to add this Widget
	err = putWWidInfo(cs, name, wwidInfo, assert, true)
}
