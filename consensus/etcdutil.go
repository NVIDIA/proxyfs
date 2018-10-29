// Interface routines between etcd and etcd clients to cut down boilerplate code

package consensus

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
	"reflect"
	"time"
)

// An etcd revision number.  A revision number refers to a particular
// transaction in the raft stream that updated the database (or failed).
//
type RevisionNumber int64

// An etcd revision number.  All values in the database with the same revision
// number for CreateRevNum, ModRevNum, or RevNum were created in the database,
// modified in the datbase, or looked up in the database at the same revision
// number, respectively.
//
type EtcdKeyHeader struct {
	CreateRevNum RevisionNumber
	ModRevNum    RevisionNumber
	RevNum       RevisionNumber
}

// Unpack a slice of etcd KeyValue into a map from the keys to the corresponding
// key header information, a map from the keys to the corresponding Values, and
// a map from the keys to a comparison function (clientv3.Cmp) that can be used
// as a conditional in an etcd transaction to determine if the key or its value
// has changed.
//
// These maps are empty if there are no keys in the response.
//
func unpackKeyValues(revNum RevisionNumber, etcdKV []*mvccpb.KeyValue) (keyHeaders map[string]EtcdKeyHeader,
	values map[string][]byte, keyCmps map[string]clientv3.Cmp, err error) {

	keyHeaders = make(map[string]EtcdKeyHeader)
	values = make(map[string][]byte)
	keyCmps = make(map[string]clientv3.Cmp)

	for _, keyValue := range etcdKV {
		key := string(keyValue.Key)
		header := EtcdKeyHeader{
			CreateRevNum: RevisionNumber(keyValue.CreateRevision),
			ModRevNum:    RevisionNumber(keyValue.ModRevision),
			RevNum:       revNum,
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

		keyHeaders[key] = header
		values[key] = keyValue.Value
		keyCmps[key] = cmp
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
// succeedds or fails.
//
func (cs *EtcdConn) updateEtcd(conditionals []clientv3.Cmp, operations []clientv3.Op) (txnResp *clientv3.TxnResponse, err error) {

	fmt.Printf("updateEtcd() called on node '%s' for something\n", cs.hostName)

	// create a context and then a transaction to update the database;
	// cancel() must be called or we will leak memory
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn := cs.kvc.Txn(ctx)

	// if the conditionals are not satisfied then none of the operations are performed
	txn = txn.If(conditionals...).Then(operations...)

	// let's do this thing ...
	txnResp, err = txn.Commit()

	fmt.Printf("updateEtcd(): Transaction: %s\n", cs.dumpTxn(txn))
	if err != nil {
		fmt.Printf("updateEtcd(): transaction failed: %s\n", err)
	} else {
		fmt.Printf("updateEtcd(): Response: %s\n", cs.dumpTxnResp(txnResp))
	}

	return
}

// dump a transaction (conditionals, operations, failures and all).
//
// because none of these fields are exported, we can only get at them via
// reflection, so let the navel gazing commence!
//
func (cs *EtcdConn) dumpTxn(txn clientv3.Txn) (dump string) {

	// this is ~/code/ProxyFS/src/go.etcd.io/etcd/clientv3/txn.go 'type txn'
	dump = fmt.Sprintf("txn type '%T' and value '%v'\n", txn, txn)

	if reflect.TypeOf(txn).Kind() != reflect.Ptr {
		dump += fmt.Sprintf("expected a pointer, got %v", reflect.TypeOf(txn).Kind())

		panic(fmt.Sprintf("dumpTxn() Type(%T) should be a pointer to _something_", txn))
		return
	}
	if reflect.ValueOf(txn).Elem().Type().Kind() != reflect.Struct {
		dump += fmt.Sprintf("expected a pointer to a struct, got %v", reflect.TypeOf(txn).Kind())

		panic(fmt.Sprintf("dumpTxn() Type(%T) should be a pointer to struct!", txn))
		return
	}
	fmt.Printf("dumpTxn(): %s\n", dump)

	structAsValue := reflect.ValueOf(txn).Elem()
	structAsType := structAsValue.Type()

	for i := 0; i < structAsType.NumField(); i++ {
		fieldName := structAsType.Field(i).Name
		fieldAsType := structAsType.Field(i).Type
		fieldAsValue := structAsValue.Field(i)

		switch fieldName {

		case "kv":
			// pb.KVClient is not (very) interesting

		case "ctx":
			// context.Context is not interesting

		case "mu":
			// sync.Mutex not interestiong -- but it should be locked!

		case "cif":
			// cif should be false!

		case "cthen":
			// cthen should be false!

		case "celse":
			// celse should be false!

		case "isWrite":
			// isWrite -- not sure what this is

		case "cmps":
			// dump the comparison operations

		case "sus":
			// dump the success operations

		case "fas":
			// dump the fail operations

		case "callOpts":
			// grpc call options -- not interesting?

		default:
			fmt.Printf("dumpTxn(): tx has an unknown field named '%s' type '%v' value '%v'\n",
				fieldName, fieldAsType, fieldAsValue)
		}
	}

	return
}

func (cs *EtcdConn) dumpTxnResp(txnResp *clientv3.TxnResponse) (dump string) {

	dump = fmt.Sprintf("txnResp type `%T' and value '%v'\n", *txnResp, *txnResp)
	return
}
