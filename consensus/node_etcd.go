package consensus

import (
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"os"
	"strings"
)

// Given a key/value pair from etcd and revision information for the key, unpack
// the key (strip the prefix), unpack the value into a NodeInfo and return both.
// If there's a problem, return an err indication.
//
func unpackKeyValueToNodeInfo(key string, header *EtcdKeyHeader, value []byte) (
	unpackedKey string, unpackedValue interface{}, err error) {

	unpackedKey = strings.TrimPrefix(key, getNodeStateKeyPrefix())

	var nodeInfoValue NodeInfoValue
	if header.CreateRevNum != 0 {
		err = json.Unmarshal(value, &nodeInfoValue)
		if err != nil {
			fmt.Printf("unpackKeyValueToNodeInfo(): Unmarshall of key '%s' header '%v' "+
				"value '%v' failed: %s\n",
				key, *header, string(value), err)
			return
		}
	}

	unpackedValue = &NodeInfo{
		EtcdKeyHeader: *header,
		NodeInfoValue: nodeInfoValue,
	}
	return
}

// Given a node name (unpacked key) and NodeInfo (unpacked value) as an
// interface, pack them for storage in the etcd database.  This is the reverse
// operation of unpackKeyValueToNodeInfo().
//
func packNodeInfoToKeyValue(nodeName string, unpackedValue interface{}) (
	packedKey string, packedValue string, err error) {

	// only the NodeInfoValue part is stored in etcd
	nodeInfoValue := unpackedValue.(*NodeInfo).NodeInfoValue

	// nodeInfoValueAsString, err := json.MarshalIndent(nodeInfoValue, "", "  ")
	nodeInfoValueAsString, err := json.Marshal(nodeInfoValue)
	if err != nil {
		err = fmt.Errorf("packNodeInfoToKeyValue(): node '%s': json.MarshalIndent complained: %s\n",
			nodeName, err)
		fmt.Printf("%s\n", err)
		return
	}

	packedKey = makeNodeStateKey(nodeName)
	packedValue = string(nodeInfoValueAsString)
	return
}

// Unpack a slice of NodeInfo's from the slice of KeyValue's received from an event,
// a Get request or a Range request.
//
// For each incoming key a NodeInfo is returned and a slice of clientv3.Cmp that
// can be used as a conditional in a transaction.  If used in a transaction the
// comparison will evaluate to false if the VG has changed from this
// information.
//
// If the key was deleted then the corresponding value will be an empty slice of
// bytes.
//
// Note: if NodeInfo.CreateRevNum == 0 then the VG has been deleted and the
// returned NodeValue part of NodeInfo contains zero values.
//
func (cs *EtcdConn) unpackNodeInfo(revNum RevisionNumber, etcdKVs []*mvccpb.KeyValue) (
	nodeInfos map[string]*NodeInfo, nodeInfoCmps map[string][]clientv3.Cmp, err error) {

	var values map[string]interface{}
	values, nodeInfoCmps, err = cs.unpackKeyValues(revNum, etcdKVs, unpackKeyValueToNodeInfo)

	nodeInfos = make(map[string]*NodeInfo)
	for key, value := range values {
		nodeInfos[key] = value.(*NodeInfo)
	}
	return
}

// Fetch the node information for node "name" as of revNum, or as of the
// "current" revision number if revNum is 0.
//
// If there's no error, return the NodeInfo as well as a comparision operation
// that can be used as a conditional in a transaction to insure the info hasn't
// changed.  Note that its not an error if the VG doesn't exist -- instead nil
// is returned for nodeInfo and the comparison function can still be used as a
// conditional that it doesn't exist.
//
// Only one comparison function is returned, but we return it in a slice for
// convenience of the caller.
//
func (cs *EtcdConn) getNodeInfo(nodeName string, revNum RevisionNumber) (
	nodeInfo *NodeInfo, nodeInfoCmp []clientv3.Cmp, err error) {

	nodeKey := makeNodeStateKey(nodeName)
	values, nodeInfoCmps, err := cs.getUnpackedKeyValues(revNum, nodeKey, unpackKeyValueToNodeInfo)

	// debug: there should be at most one key in this result
	if len(values) > 1 || len(nodeInfoCmps) != 1 {
		err = fmt.Errorf("getNodeInfo(): found too many keys for node name '%s': values '%v' Cmps '%v'",
			nodeName, values, nodeInfoCmps)
		fmt.Printf("%s\n", err)
		return
	}

	nodeInfoCmp = nodeInfoCmps[nodeName]
	if len(values) > 0 {
		nodeInfo = values[nodeName].(*NodeInfo)
	}
	return
}

// getAllNodeInfo() gathers information about all the nodes and returns it as a
// map from node name to NodeInfo
//
// All data is taken from the same etcd global revision number.
//
func (cs *EtcdConn) getAllNodeInfo(revNum RevisionNumber) (
	allNodeInfo map[string]*NodeInfo, allNodeInfoCmp map[string][]clientv3.Cmp, err error) {

	nodeKey := getNodeStateKeyPrefix()
	allNodeInfoInterface, allNodeInfoCmp, err :=
		cs.getUnpackedKeyValues(revNum, nodeKey, unpackKeyValueToNodeInfo)
	if err != nil {
		err = fmt.Errorf("getAllNodeInfo(): Get Node state failed with: %v\n", err)
		fmt.Printf("%s\n", err)
		os.Exit(-1)
	}

	allNodeInfo = make(map[string]*NodeInfo)
	for unpackedKey, unpackedValue := range allNodeInfoInterface {
		allNodeInfo[unpackedKey] = unpackedValue.(*NodeInfo)
	}
	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to change the NodeInfo for a VolumeGroup to the passed Value.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getNodeInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) putNodeInfo(nodeName string, nodeInfo *NodeInfo) (operations []clientv3.Op, err error) {

	packedKey, packedValue, err := packNodeInfoToKeyValue(nodeName, nodeInfo)
	if err != nil {
		fmt.Printf("putNodeInfo(): name '%s': json.MarshalIndent complained: %s\n",
			nodeName, err)
		return
	}

	op := clientv3.OpPut(packedKey, packedValue)
	operations = []clientv3.Op{op}
	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to delete the Volume Group for the specified name.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getNodeInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) deleteNodeInfo(nodeName string) (op clientv3.Op, err error) {

	nodeKey := makeNodeStateKey(nodeName)
	op = clientv3.OpDelete(nodeKey)
	return
}
