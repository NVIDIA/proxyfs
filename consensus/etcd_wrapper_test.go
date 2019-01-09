package consensus

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/clientv3"
	"strings"
	"testing"
)

// Test etcd wrapper routines and demonstrate how to use them.TestEtcdWrapperAPI
func TestEtcdWrapperAPI(t *testing.T) {

	testAddWidget(t)
	//testRmWidget(t)
	//testUpdateWidget(t)
	// TODO - add test for mv from workQ to finishQ case...
}

// WidInfoValue is the "value" of the key/value store.  It is the information
// associated with the widget in the etcd database (except its JSON encoded
// in the database)
type WidInfoValue struct {
	State   string
	AttrStr string
	AttrInt int
}

// WidInfo is information associated with a widget at a given revision number.
// It includes when the info was fetched and the last time the value was modified.
type WidInfo struct {
	EtcdKeyHeader
	WidInfoValue
}

func getWidKeyPrefix() string {
	return "WidgetKey:"
}

func makeWidKey(name string) string {
	return getWidKeyPrefix() + name
}

// Given a key/value pair from etcd and revision information for the key, unpack
// the key (strip the prefix), unpack the value into a WidInfo and return both.
// If there's a problem, return an err indication.
func unpackKeyValueToWidInfo(key string, header *EtcdKeyHeader, value []byte) (
	unpackedKey string, unpackedValue interface{}, err error) {

	unpackedKey = strings.TrimPrefix(key, getWidKeyPrefix())

	var widInfoValue WidInfoValue
	if header.CreateRevNum != 0 {
		err = json.Unmarshal(value, &widInfoValue)
		if err != nil {
			fmt.Printf("unpackKeyValueToWidInfo(): Unmarshal of key '%s' header '%v' "+
				"value '%v' failed: %s\n",
				key, *header, string(value), err)
			return
		}
	}

	unpackedValue = &WidInfo{
		EtcdKeyHeader: *header,
		WidInfoValue:  widInfoValue,
	}
	return
}

// getWidInfo is the function used to GET the key from etcd and unpack the
// JSON encoded "value" into a WidInfo structure.
//
// It also returns a slice to be used for comparisons in future transactions.
func getWidInfo(cs *EtcdConn, name string, revNum RevisionNumber) (widInfo *WidInfo,
	widInfoCmp []clientv3.Cmp, err error) {

	widKey := makeWidKey(name)

	// Do a GET on the key and use unpackKeyValueToWidInfo() to decode the
	// JSON.
	values, widInfoCmps, err := cs.getUnpackedKeyValues(revNum, widKey, unpackKeyValueToWidInfo)

	// debug: there should be at most one key in this result
	if len(values) > 1 || len(widInfoCmps) != 1 {
		err = fmt.Errorf("getVgInfo(): found too many keys for node name '%s': values '%v' Cmps '%v'",
			name, values, widInfoCmps)
		fmt.Printf("%s\n", err)
		return
	}

	// NOTE: getUnpackedKeyValues() could return multiple keys.
	// That is why values is a map.  In our case, it only returns one key.
	widInfoCmp = widInfoCmps[name]
	if len(values) > 0 {
		widInfo = values[name].(*WidInfo)
	}
	return
}

// Given a name (unpacked key) and WidInfo (unpacked value) as an
// interface, pack them for storage in the etcd database.  This is the reverse
// operation of unpackKeyValueToWidInfo().
//
func packWidInfoToKeyValue(name string, unpackedValue interface{}) (
	packedKey string, packedValue string, err error) {

	// Only the WidInfoValue part is stored in etcd
	widInfoValue := unpackedValue.(*WidInfo).WidInfoValue

	widInfoValueAsString, err := json.Marshal(widInfoValue)
	if err != nil {
		err = fmt.Errorf("packWidInfoToKeyValue: node '%s': json.MarshalIndent complained: %s",
			name, err)
		fmt.Printf("%s\n", err)
		return
	}

	packedKey = makeWidKey(name)
	packedValue = string(widInfoValueAsString)
	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to change the WidgetInfo for a Widget to the passed Value.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getWidInfo() for this same Widget to insure that
// the changes should be applied.
//
func putWidInfo(cs *EtcdConn, name string, widInfo *WidInfo) (operations []clientv3.Op, err error) {

	packedKey, packedValue, err := packWidInfoToKeyValue(name, widInfo)
	if err != nil {
		fmt.Printf("putWidInfo(): name '%s': json.MarshalIndent complained: %s\n",
			name, err)
		return
	}

	op := clientv3.OpPut(packedKey, packedValue)
	operations = []clientv3.Op{op}
	return
}

// Use the wrappers to create a key for the first time
func testAddWidget(t *testing.T) {
	var (
		name         = "testWidget"
		conditionals = make([]clientv3.Cmp, 0, 1)
		operations   = make([]clientv3.Op, 0, 1)
	)
	assert := assert.New(t)

	cs, tc := newHA(t)
	defer closeHA(t, cs, tc)
	assert.NotNil(cs, "cs should not be nil")

	// Check if the widget already exists by doing a GET with
	// revision number of 0
	widInfo, cmpWidInfoName, err := getWidInfo(cs, name, RevisionNumber(0))
	if err != nil {
		return
	}
	fmt.Printf("WidInfo: %+v cmpWidInfoName: %+v\n", widInfo, cmpWidInfoName)
	assert.Nil(widInfo, "widInfo should be nil")
	conditionals = append(conditionals, cmpWidInfoName...)

	// The widget does not exist (yet), so initialize the WidInfoValue
	// information it will have when created.
	widInfo = &WidInfo{
		WidInfoValue: WidInfoValue{
			State:   "SOMESTATE",
			AttrInt: 5,
			AttrStr: "someString",
		},
	}

	// Create the operation to add this Widget
	putOps, err := putWidInfo(cs, name, widInfo)
	operations = append(operations, putOps...)

	// Update the shared state (or fail)
	txnResp, err := cs.updateEtcd(conditionals, operations)
	assert.True(txnResp.Succeeded, "Expected txn to succeed")

	fmt.Printf("testAddWidget(): txnResp: %v err %v\n", txnResp, err)
}
