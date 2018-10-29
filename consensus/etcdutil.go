// Interface routines between etcd and etcd clients to cut down boilerplate code

package consensus

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
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
// as a conditional in an etcd transaction to determine if the key's value has
// changed.
//
// These maps are empty if there are no keys in the response.
//
func unpackKeyValues(revNum RevisionNumber, etcdKV []*mvccpb.KeyValue) (keyHeaders map[string]EtcdKeyHeader,
	values map[string][]byte, modCmps map[string]clientv3.Cmp, err error) {

	keyHeaders = make(map[string]EtcdKeyHeader)
	values = make(map[string][]byte)
	modCmps = make(map[string]clientv3.Cmp)

	for _, keyValue := range etcdKV {
		key := string(keyValue.Key)
		header := EtcdKeyHeader{
			CreateRevNum: RevisionNumber(keyValue.CreateRevision),
			ModRevNum:    RevisionNumber(keyValue.ModRevision),
			RevNum:       revNum,
		}

		// a comparison function that will return false if the key or
		// value has changed since the key was fetched
		cmp := clientv3.Compare(clientv3.ModRevision(key), "=", keyValue.ModRevision)

		keyHeaders[key] = header
		values[key] = keyValue.Value
		modCmps[key] = cmp
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

	// create a context and then a transaction to update the database;
	// cancel() must be called or we will leak memory
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn := cs.kvc.Txn(ctx)

	// if the conditionals are not satisfied then none of the operations are performed
	txn = txn.If(conditionals...).Then(operations...)

	// let's do this thing ...
	txnResp, err = txn.Commit()

	if err != nil {
		fmt.Printf("updateEtcd(): transaction failed: %s\n", err)
	}
	fmt.Printf("updateEtcd(): Transaction: %s\n", cs.dumpTxn(txn))
	fmt.Printf("updateEtcd(): Response: %s\n", cs.dumpTxnResp(txnResp))

	return
}

func (cs *EtcdConn) dumpTxn(txn clientv3.Txn) (dump string) {

	return
}

func (cs *EtcdConn) dumpTxnResp(txnResp *clientv3.TxnResponse) (dump string) {

	return
}
