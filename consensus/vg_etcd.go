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
// the key (strip the prefix), unpack the value into a VgInfo and return both.
// If there's a problem, return an err indication.
//
func unpackKeyValueToVgInfo(key string, header *EtcdKeyHeader, value []byte) (
	unpackedKey string, unpackedValue interface{}, err error) {

	unpackedKey = strings.TrimPrefix(key, getVgKeyPrefix())

	var vgInfoValue VgInfoValue
	if header.CreateRevNum != 0 {
		err = json.Unmarshal(value, &vgInfoValue)
		if err != nil {
			fmt.Printf("unpackKeyValueToVgInfo(): Unmarshall of key '%s' header '%v' "+
				"value '%v' failed: %s\n",
				key, *header, string(value), err)
			return
		}
	}

	unpackedValue = &VgInfo{
		EtcdKeyHeader: *header,
		VgInfoValue:   vgInfoValue,
	}
	return
}

// Given a node name (unpacked key) and VgInfo (unpacked value) as an
// interface, pack them for storage in the etcd database.  This is the reverse
// operation of unpackKeyValueToVgInfo().
//
func packVgInfoToKeyValue(vgName string, unpackedValue interface{}) (
	packedKey string, packedValue string, err error) {

	// only the VgInfoValue part is stored in etcd
	vgInfoValue := unpackedValue.(*VgInfo).VgInfoValue

	// vgInfoValueAsString, err := json.MarshalIndent(vgInfoValue, "", "  ")
	vgInfoValueAsString, err := json.Marshal(vgInfoValue)
	if err != nil {
		err = fmt.Errorf("packVgInfoToKeyValue(): node '%s': json.MarshalIndent complained: %s\n",
			vgName, err)
		fmt.Printf("%s\n", err)
		return
	}

	packedKey = makeVgKey(vgName)
	packedValue = string(vgInfoValueAsString)
	return
}

// Unpack a slice of VgInfo's from the slice of KeyValue's received from an event,
// a Get request or a Range request.
//
// For each incoming key a VgInfo is returned and a slice of clientv3.Cmp that
// can be used as a conditional in a transaction.  If used in a transaction the
// comparison will evaluate to false if the VG has changed from this
// information.
//
// If the key was deleted then the corresponding value will be an empty slice of
// bytes.
//
// Note: if VgInfo.CreateRevNum == 0 then the VG has been deleted and the
// returned NodeValue part of VgInfo contains zero values.
//
func (cs *EtcdConn) unpackVgInfo(revNum RevisionNumber, etcdKVs []*mvccpb.KeyValue) (
	vgInfos map[string]*VgInfo, vgInfoCmps map[string][]clientv3.Cmp, err error) {

	var values map[string]interface{}
	values, vgInfoCmps, err = cs.unpackKeyValues(revNum, etcdKVs, unpackKeyValueToVgInfo)

	vgInfos = make(map[string]*VgInfo)
	for key, value := range values {
		vgInfos[key] = value.(*VgInfo)
	}
	return
}

// Fetch the node information for node "name" as of revNum, or as of the
// "current" revision number if revNum is 0.
//
// If there's no error, return the VgInfo as well as a comparision operation
// that can be used as a conditional in a transaction to insure the info hasn't
// changed.  Note that its not an error if the VG doesn't exist -- instead nil
// is returned for vgInfo and the comparison function can still be used as a
// conditional that it doesn't exist.
//
// Only one comparison function is returned, but we return it in a slice for
// convenience of the caller.
//
func (cs *EtcdConn) getVgInfo(vgName string, revNum RevisionNumber) (
	vgInfo *VgInfo, vgInfoCmp []clientv3.Cmp, err error) {

	nodeKey := makeVgKey(vgName)
	values, vgInfoCmps, err := cs.getUnpackedKeyValues(revNum, nodeKey, unpackKeyValueToVgInfo)

	// debug: there should be at most one key in this result
	if len(values) > 1 || len(vgInfoCmps) != 1 {
		err = fmt.Errorf("getVgInfo(): found too many keys for node name '%s': values '%v' Cmps '%v'",
			vgName, values, vgInfoCmps)
		fmt.Printf("%s\n", err)
		return
	}

	vgInfoCmp = vgInfoCmps[vgName]
	if len(values) > 0 {
		vgInfo = values[vgName].(*VgInfo)
	}
	return
}

// getAllVgInfo() gathers information about all the nodes and returns it as a
// map from node name to VgInfo
//
// All data is taken from the same etcd global revision number.
//
func (cs *EtcdConn) getAllVgInfo(revNum RevisionNumber) (
	allVgInfo map[string]*VgInfo, allVgInfoCmp map[string][]clientv3.Cmp, err error) {

	nodeKey := getNodeStateKeyPrefix()
	allVgInfoInterface, allVgInfoCmp, err :=
		cs.getUnpackedKeyValues(revNum, nodeKey, unpackKeyValueToVgInfo)
	if err != nil {
		err = fmt.Errorf("getAllVgInfo(): Get Node state failed with: %v\n", err)
		fmt.Printf("%s\n", err)
		os.Exit(-1)
	}

	allVgInfo = make(map[string]*VgInfo)
	for unpackedKey, unpackedValue := range allVgInfoInterface {
		allVgInfo[unpackedKey] = unpackedValue.(*VgInfo)
	}
	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to change the VgInfo for a VolumeGroup to the passed Value.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getVgInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) putVgInfo(vgName string, vgInfo *VgInfo) (operations []clientv3.Op, err error) {

	packedKey, packedValue, err := packVgInfoToKeyValue(vgName, vgInfo)
	if err != nil {
		fmt.Printf("putVgInfo(): name '%s': json.MarshalIndent complained: %s\n",
			vgName, err)
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
// "function") returned by getVgInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) deleteVgInfo(vgName string) (op clientv3.Op, err error) {

	nodeKey := makeVgKey(vgName)
	op = clientv3.OpDelete(nodeKey)
	return
}
