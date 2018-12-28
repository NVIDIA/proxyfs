// Interface routines between etcd and etcd clients to cut down boilerplate code

package consensus

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	etcdserverpb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"reflect"
	"strings"
	"sync"
	"time"
)

// RevisionNumber refers to a particular transaction in the raft stream that
// updated the database (or failed).
type RevisionNumber int64

// Revision numbers associated with a key.  If two keys in the database have the
// same revision number for CreateRevNum, ModRevNum, or RevNum then they were
// created in the database, modified in the datbase, or looked up in the
// database at that revision number, respectively.
//
type EtcdKeyHeader struct {
	CreateRevNum RevisionNumber
	ModRevNum    RevisionNumber
	RevNum       RevisionNumber
	// Number of times the value for the key has been modified, starting
	// with 1.
	Version int64
}

// Given a slice of etcd KeyValue ([]*mvccpb.KeyValue) build and return several maps:
//  - a map of keys to the corresponding key header information;
//  - a map of keys to the corresponding Values; and
//  - a map of keys to a comparison function (clientv3.Cmp) that can be used as
//    a conditional in an etcd transaction to determine if the key or its value
//    has changed.
//
// These maps are empty if there are no keys in the response.
//
// revNum should be the revision number of the transaction or query that
// returned the keys.
//
func mapKeyValues(revNum RevisionNumber, etcdKV []*mvccpb.KeyValue) (headers map[string]*EtcdKeyHeader,
	values map[string][]byte, cmps map[string]clientv3.Cmp, err error) {

	headers = make(map[string]*EtcdKeyHeader)
	values = make(map[string][]byte)
	cmps = make(map[string]clientv3.Cmp)

	for _, keyValue := range etcdKV {
		key := string(keyValue.Key)
		header := EtcdKeyHeader{
			CreateRevNum: RevisionNumber(keyValue.CreateRevision),
			ModRevNum:    RevisionNumber(keyValue.ModRevision),
			RevNum:       revNum,
			Version:      keyValue.Version,
		}

		// Create a comparison function that will return false if the
		// key or corresponding value has changed since the key was
		// fetched.  If CreateRevNum is 0 then the key was deleted by
		// the event so the compare function will check against that.
		var cmp clientv3.Cmp
		if keyValue.CreateRevision == 0 {
			cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		} else {
			cmp = clientv3.Compare(clientv3.ModRevision(key), "=", keyValue.ModRevision)
		}

		headers[key] = &header
		values[key] = keyValue.Value
		cmps[key] = cmp

		// sanity check
		if header.ModRevNum > revNum || header.CreateRevNum > revNum {
			err = fmt.Errorf("mapKeyValues(): key '%s' request revNum %d has too recent revNum '%v'\n",
				key, revNum, keyValue)
			fmt.Printf("PANIC: %s\n", err)
			panic(err)
		}
	}
	return
}

// Unpack a slice of KeyValue pairs from an event, a Get request, or a range
// request and return a map from keys to unpacked values and a map from keys to
// slices of compare conditionals.
//
// For each KeyValue passed in, the etcd value is unpacked using the passed
// unpack function and the unpacked value is returned in the unpackedValues map.
// In addition a slice of Compare conditionals (clientv3.Cmp) is returned for
// each key in a separate map, where the Compare can be used as a conditional in
// a transaction (the comparison will evaluate to false if the key has changed
// since the query).
//
// If the key has been deleted or does not exist then the compare function
// (conditional) checks that CreateRevNum == 0 for the key; such a compare
// function does *not* guarantee that the key has not been modified since the
// query, it simply guarantees the key doesn't exist.  If the key is created
// again and then deleted before the compare function is used it will still
// evaluate to true even though the key has changed since the query.
//
func (cs *EtcdConn) unpackKeyValues(revNum RevisionNumber, etcdKVs []*mvccpb.KeyValue,
	unpackKeyValueFunc func(key string, header *EtcdKeyHeader, value []byte) (
		unpackedKey string, unpackedValue interface{}, err error)) (
	unpackedValues map[string]interface{}, nodeInfoCmps map[string][]clientv3.Cmp, err error) {

	unpackedValues = make(map[string]interface{})
	nodeInfoCmps = make(map[string][]clientv3.Cmp)

	keyHeaders, values, modCmps, err := mapKeyValues(revNum, etcdKVs)
	if err != nil {
		fmt.Printf("unpackNodeInfo(): unpackKeyValues for '%v' returned err: %s\n", etcdKVs, err)
		return
	}

	for key, header := range keyHeaders {

		unpackedKey, unpackedValue, err2 := unpackKeyValueFunc(key, header, values[key])
		if err2 != nil {
			fmt.Printf("unpackKeyValues(): unpack key '%s' header '%v' "+
				"value '%v' failed: %s\n",
				key, header, string(values[key]), err2)
			err = err2
			return
		}
		unpackedValues[unpackedKey] = unpackedValue
		nodeInfoCmps[unpackedKey] = []clientv3.Cmp{modCmps[key]}
	}

	return
}

// Fetch the values(s) for the specified key or key prefix as of the specified
// revNum and unpack them into a map of keys to unpacked values and keys to
// comparison functions.
//
// If revNum == 0 then return the information for the current revision number.
//
// The comparision function (conditional) can be used as a conditional in a
// transaction to insure the value for the associated key has not changed.
//
// Note that it is not an error if the key does not exist.  Instead, the
// returned unpackedValues map is empty and a comparison function that can be
// used as a conditional to check that the key doesn't exist is returned.  (This
// is probably not what you want if the value passed is a prefix).
//
// Only one comparison function per key is returned, but we return it in a slice
// for convenience of the caller.
//
func (cs *EtcdConn) getUnpackedKeyValues(revNum RevisionNumber, key string,
	unpackKeyValueFunc func(key string, header *EtcdKeyHeader, value []byte) (
		unpackedKey string, unpackedValue interface{}, err error)) (
	unpackedValues map[string]interface{},
	nodeInfoCmps map[string][]clientv3.Cmp, err error) {

	// create a context for the request
	ctx, cancel := context.WithTimeout(context.Background(), etcdQueryTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	if revNum != 0 {
		resp, err = cs.cli.Get(ctx, key, clientv3.WithRev(int64(revNum)))
	} else {
		resp, err = cs.cli.Get(ctx, key)
	}
	if err != nil {
		return
	}

	// if revNum not specified then return the version that we got
	if revNum == 0 {
		revNum = RevisionNumber(resp.Header.Revision)
	}
	if resp.OpResponse().Get().Count == int64(0) {
		cmps := []clientv3.Cmp{
			clientv3.Compare(clientv3.CreateRevision(key), "=", 0),
		}
		nodeInfoCmps = map[string][]clientv3.Cmp{key: cmps}
		return
	}

	unpackedValues, nodeInfoCmps, err = cs.unpackKeyValues(revNum, resp.OpResponse().Get().Kvs, unpackKeyValueFunc)
	if err != nil {
		fmt.Printf("getUnpackedKeyValues(): unpackKeyValues of %v returned err: %s\n",
			resp.OpResponse().Get().Kvs, err)
		return
	}

	return
}

// Perform a transaction to update the etcd database (shared state).  The
// transaction consits of operations (changes to keys in the database) and
// conditionals which must be satisfied for the operations to take effect.  If
// the conditionals are not satisfied then none of the operations are performed,
// txnResponse.Succeeded is false, and err is nil.
//
// If err != nil then it probably indicates a failure to communicate with etcd.
//
// This routine should probably retry the operation until the transaction
// succeeds or fails.
//
func (cs *EtcdConn) updateEtcd(conditionals []clientv3.Cmp, operations []clientv3.Op) (txnResp *clientv3.TxnResponse, err error) {
	if len(operations) == 0 {
		panic("no operations passed to updateEtcd()")
	}

	// create a context and then a transaction to update the database;
	// cancel() must be called or we will leak memory
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn := cs.kvc.Txn(ctx)

	// if the conditionals are not satisfied then none of the operations are performed
	txn = txn.If(conditionals...).Then(operations...)

	// this should probably be logged
	//fmt.Printf("updateEtcd(): called on node '%s' Transaction:\n%s", cs.hostName, cs.formatTxn(txn))

	// let's do this thing ...
	txnResp, err = txn.Commit()

	// we should probably log the pass/fail results ...
	if err != nil {
		fmt.Printf("updateEtcd(): transaction failed: %s:\n%s", err, cs.formatTxn(txn))
	} else {
		//fmt.Printf("updateEtcd(): Transaction response: %s\n", cs.formatTxnResp(txnResp))
	}

	return
}

// Watch for events on the passed watchChan channel and invoke the watchFunc()
// function with each event.  Continue looping until an item is received on
// stopChan or an error occurs at which point doneWg is signalled, the error (or
// nil) is pushed to errChan and this function returns.
//
// If an item is received on stopChan it is pushed back on stopChan (so multiple
// watchers can share the same stopChan without the creator having to count.  If
// stopChan is not required it can be nil, otherwise it must have a capacity of
// at least 1.
//
// If an error occurs, such as waitChan being closed or watchFunc() returning an
// error then signal finishedWg, push the error down errChan and return.
//
// Any of errChan, stopChan, and doneWg can be nil if they are not needed.
//
// Typically StartWatcher() will be called in a new goroutine, but it does not
// have to be, i.e. if a goroutine wants pause until an event is received.
//
func (cs *EtcdConn) StartWatcher(watchChan clientv3.WatchChan,
	watchFunc func(cs *EtcdConn, response *clientv3.WatchResponse) (err error),
	stopChan chan struct{}, errChan chan<- error, doneWg *sync.WaitGroup) {

	var err error
	keepLooping := true
	for keepLooping {

		// wait for something to arrive on watchChan or stopChan
		select {

		case response, ok := <-watchChan:
			if !ok {
				err = fmt.Errorf("StartWatcher: watchChan was closed")
				keepLooping = false
				break
			}

			err = watchFunc(cs, &response)
			if err != nil {
				keepLooping = false
			}

		case item := <-stopChan:
			// forward item down the channel to the next watcher waiting
			// for it (the capacity of stopChan must be at least 1 so
			// this does not block if its the last watcher)
			stopChan <- item
			err = nil
			keepLooping = false
		}
	}

	fmt.Printf("StartWatcher(): loop broken with err %v\n", err)

	// if errChan exist, push err or nil down it
	if errChan != nil {
		errChan <- err
	}
	if doneWg != nil {
		doneWg.Done()
	}

	return
}

// dump a transaction (conditionals, operations, failures and all).
//
// because none of these fields are exported, we can only get at them via
// reflection, so let the navel gazing commence!
//
func (cs *EtcdConn) formatTxn(txnPtr clientv3.Txn) (dump string) {

	// txnPtr should be '*txn' from go.etcd.io/etcd/clientv3/txn.go; start by
	// drilling down to the struct
	if reflect.TypeOf(txnPtr).Kind() != reflect.Ptr {
		dump += fmt.Sprintf("expected a pointer, got %v", reflect.TypeOf(txnPtr).Kind())
		panic(dump)
		// return
	}
	if reflect.ValueOf(txnPtr).Elem().Type().Kind() != reflect.Struct {
		dump += fmt.Sprintf("expected a pointer to a struct, got %v", reflect.TypeOf(txnPtr).Kind())
		panic(dump)
		// return
	}
	// txnAsValue "points to" a clientv3.txn; uncomment this line to use it
	// to jump to the definition
	// _ = clientv3.txn
	txnAsValue := reflect.ValueOf(txnPtr).Elem()

	// this code is brittle; if clientv3.txn is changed it will probably panic.
	//
	// format the interesting fields of txn, which are:
	// "cmps"	-- conditionals
	// "sus"	-- actions if conditionals true (success)
	// "fas"	-- actions if conditionatls false (fail)
	// we don't care much about:
	// "kv"		-- key value connection(???)
	// "ctx"	-- request context (deadlines and cancellation)
	// "mu"         -- a mutex which should be locked
	// "cif"        -- debug field to insure If() called only once
	// "cten"       -- debug field to insure Then() called only once
	// "celse"      -- debug field to insure Else() called only once
	// "isWrite"    -- true if transaction might modify database
	// "callOpts"   -- call options(???)
	cmpsAsValue := txnAsValue.FieldByName("cmps")
	susAsValue := txnAsValue.FieldByName("sus")
	fasAsValue := txnAsValue.FieldByName("fas")

	// dump the comparisons (conditionals)
	//
	// cmpsAsValue "points to" a slice of *etcdserverpb.Compare;
	// use the next line to jump to that definition
	_ = etcdserverpb.Compare{}
	dump += fmt.Sprintf("    If: [")
	for i := 0; i < cmpsAsValue.Len(); i++ {
		cmp := cmpsAsValue.Index(i)
		dump += cs.formatCompare(cmp)
		if i+1 < cmpsAsValue.Len() {
			dump += " && "
		}
	}
	dump += fmt.Sprintf("]\n")

	// dump the ops (actions) performed on success
	//
	// susAsValue "points to" a slice of *etcdserverpb.RequestOp;
	// use the next line to jump to that definition
	_ = etcdserverpb.RequestOp{}
	dump += fmt.Sprintf("    Then: [")
	for i := 0; i < susAsValue.Len(); i++ {
		cmp := susAsValue.Index(i)
		dump += cs.formatOp(cmp)
		if i+1 < susAsValue.Len() {
			dump += ", "
		}
	}
	dump += fmt.Sprintf("]\n")

	// dump the ops (actions) performed on !success
	//
	// fasAsValue "points to" a slice of *etcdserverpb.RequestOp;
	// use the next line to jump to that definition
	_ = etcdserverpb.RequestOp{}
	dump += fmt.Sprintf("    Else: [")
	for i := 0; i < fasAsValue.Len(); i++ {
		cmp := fasAsValue.Index(i)
		dump += cs.formatOp(cmp)
		if i+1 < fasAsValue.Len() {
			dump += ", "
		}
	}
	dump += fmt.Sprintf("]\n")

	return
}

// pretty print a single etcdserverpb.Compare struct
//
func (cs *EtcdConn) formatCompare(cmpPtrAsValue reflect.Value) (dump string) {

	// drill down through the pointer to the Compare struct
	cmpPtrAsType := cmpPtrAsValue.Type()
	if cmpPtrAsType.Kind() != reflect.Ptr {
		dump += fmt.Sprintf("expected a pointer, got %v", cmpPtrAsType.Kind())
		panic(dump)
		// return
	}
	if cmpPtrAsValue.Elem().Type().Kind() != reflect.Struct {
		dump += fmt.Sprintf("expected a pointer to a struct, got %v", cmpPtrAsValue)
		panic(dump)
		// return
	}
	// cmpsAsValue "points to" an etcdserverpb.Compare; use the next line to jump to
	// that definition
	_ = etcdserverpb.Compare{}
	cmpAsValue := cmpPtrAsValue.Elem()

	result := cmpAsValue.FieldByName("Result").Int()
	target := cmpAsValue.FieldByName("Target").Int()
	key := cmpAsValue.FieldByName("Key").Bytes()
	rangeEnd := cmpAsValue.FieldByName("RangeEnd").Bytes()
	targetUnionAsValue := cmpAsValue.FieldByName("TargetUnion")

	var targetUnion string = "<nil>"
	if !targetUnionAsValue.IsNil() {

		switch targetUnionAsValue.Elem().Type().String() {

		case "*etcdserverpb.Compare_Version":
			targetUnion = fmt.Sprintf("Revision:%d",
				targetUnionAsValue.Elem().Elem().FieldByName("Version").Int())

		case "*etcdserverpb.Compare_CreateRevision":
			targetUnion = fmt.Sprintf("CreateRevision:%d",
				targetUnionAsValue.Elem().Elem().FieldByName("CreateRevision").Int())

		case "*etcdserverpb.Compare_ModRevision":
			targetUnion = fmt.Sprintf("ModRevision:%d",
				targetUnionAsValue.Elem().Elem().FieldByName("ModRevision").Int())

		case "*etcdserverpb.Compare_Value":
			targetUnion = fmt.Sprintf("Value:%s",
				string(targetUnionAsValue.Elem().Elem().FieldByName("Value").Bytes()))

		case "*etcdserverpb.Compare_Lease":
			targetUnion = fmt.Sprintf("Lease:%d",
				targetUnionAsValue.Elem().Elem().FieldByName("Lease").Int())

		default:
			targetUnion = "unknown"
			panic("unknown type!")

		}
	}

	if len(rangeEnd) == 0 {
		dump = fmt.Sprintf("(key:'%s' %s %s %s)", key,
			etcdserverpb.Compare_CompareResult_name[int32(result)],
			etcdserverpb.Compare_CompareTarget_name[int32(target)], targetUnion)
	} else {
		dump = fmt.Sprintf("(key:'%s'-'%s' %s %s %s)", key, rangeEnd,
			etcdserverpb.Compare_CompareResult_name[int32(result)],
			etcdserverpb.Compare_CompareTarget_name[int32(target)], targetUnion)
	}

	return
}

// pretty print a single etcdserverpb.RequestOp struct
//
func (cs *EtcdConn) formatOp(opPtrAsValue reflect.Value) (dump string) {

	// drill down through the pointer to the RequestOp struct
	cmpPtrAsType := opPtrAsValue.Type()
	if cmpPtrAsType.Kind() != reflect.Ptr {
		dump += fmt.Sprintf("expected a pointer, got %v", cmpPtrAsType.Kind())
		panic(dump)
		// return
	}
	if opPtrAsValue.Elem().Type().Kind() != reflect.Struct {
		dump += fmt.Sprintf("expected a pointer to a struct, got %v", opPtrAsValue)
		panic(dump)
		// return
	}
	// opAsValue "points to" a etcdserverpb.RequestOp;
	// use the next line to jump to that definition
	_ = etcdserverpb.RequestOp{}
	opAsValue := opPtrAsValue.Elem()

	requestAsValue := opAsValue.FieldByName("Request")

	var opAsString string
	switch requestAsValue.Elem().Type().String() {

	case "*etcdserverpb.RequestOp_RequestRange":
		// RangeRequestAsValue "points to" an etcdserverpb.RangeRequest;
		// use the next line to jump to that definition
		_ = etcdserverpb.RangeRequest{}
		RangeRequestAsValue := requestAsValue.Elem().Elem().FieldByName("RequestRange").Elem()

		if len(RangeRequestAsValue.FieldByName("RangeEnd").Bytes()) == 0 {
			opAsString = fmt.Sprintf("(DELETERANGE key:'%s')",
				RangeRequestAsValue.FieldByName("Key").Bytes())
		} else {
			opAsString = fmt.Sprintf("(DELETERANGE keys:'%s'-'%s')",
				RangeRequestAsValue.FieldByName("Key").Bytes(),
				RangeRequestAsValue.FieldByName("RangeEnd").Bytes())
		}

	case "*etcdserverpb.RequestOp_RequestPut":
		// PutRequestAsValue "points to" an etcdserverpb.PutRequest;
		// use the next line to jump to that definition
		_ = etcdserverpb.PutRequest{}
		putRequestAsValue := requestAsValue.Elem().Elem().FieldByName("RequestPut").Elem()

		opAsString = fmt.Sprintf("(PUT key:'%s' Value:'%s')",
			putRequestAsValue.FieldByName("Key").Bytes(),
			putRequestAsValue.FieldByName("Value").Bytes())

	case "*etcdserverpb.RequestOp_RequestDeleteRange":
		// deleteRangeRequestAsValue "points to" an etcdserverpb.DeleteRangeRequest;
		// use the next line to jump to that definition
		_ = etcdserverpb.DeleteRangeRequest{}
		deleteRangeRequestAsValue := requestAsValue.Elem().Elem().FieldByName("RequestDeleteRange").Elem()

		if len(deleteRangeRequestAsValue.FieldByName("RangeEnd").Bytes()) == 0 {
			opAsString = fmt.Sprintf("(DELETERANGE key:'%s')",
				deleteRangeRequestAsValue.FieldByName("Key").Bytes())
		} else {
			opAsString = fmt.Sprintf("(DELETERANGE keys:'%s'-'%s')",
				deleteRangeRequestAsValue.FieldByName("Key").Bytes(),
				deleteRangeRequestAsValue.FieldByName("RangeEnd").Bytes())
		}

	case "*etcdserverpb.RequestOp_RequestTxn":
		// TxnRequestAsValue "points to" an etcdserverpb.TxnRequest (apparently
		// operations in a transaction can contain transactions -- this code
		// doesn't go there); use the next line to jump to that definition
		_ = etcdserverpb.TxnRequest{}
		txnRequestAsValue := requestAsValue.Elem().Elem().FieldByName("RequestTxn").Elem()
		opAsString = fmt.Sprintf("(TRANSACTION:'%v')", txnRequestAsValue.String())

	default:
		opAsString = fmt.Sprintf("Unknown request op: %s", requestAsValue.Elem().Type().String())
	}

	dump = opAsString
	return
}

func (cs *EtcdConn) formatTxnResp(txnResp *clientv3.TxnResponse) (dump string) {

	opList := make([]string, 0, 1)
	for _, responseOp := range txnResp.Responses {

		switch op := responseOp.Response.(type) {

		case *etcdserverpb.ResponseOp_ResponseRange:
			opList = append(opList, "range")

		case *etcdserverpb.ResponseOp_ResponsePut:
			opList = append(opList, fmt.Sprintf("put %v", op))

		case *etcdserverpb.ResponseOp_ResponseDeleteRange:
			opList = append(opList, "deleteRange")

		case *etcdserverpb.ResponseOp_ResponseTxn:
			opList = append(opList, "Transaction")

		default:
			opList = append(opList, fmt.Sprintf("UNKNOWN RESPONSE type %T", op))
		}
	}

	dump = fmt.Sprintf("Revision %d  Succeeded: %v  Ops[%s]",
		txnResp.Header.Revision, txnResp.Succeeded,
		strings.Join(opList, ", "))
	return
}
