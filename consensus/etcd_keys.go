package consensus

import ()

// Key prefixes used by Proxyfs in the etcd database.  Changing these names will
// break existing installations.
//
const (
	nodeKeyStatePrefixName = "ProxyFS_NodeState:"
	vgKeyPrefixName        = "ProxyFS_VgName:"
)

// vgNamePrefix returns a string containing the prefix
// for volume group name keys
func getVgKeyPrefix() string {
	return vgKeyPrefixName
}

// makeVgKey() uses the VG name as the key for the database
// (we will need to change this to a UUID)
//
func makeVgKey(vgName string) string {
	return getVgKeyPrefix() + vgName
}

// nodeNamePrefix returns a string containing the prefix
// for volume group name keys
func getNodeStateKeyPrefix() string {
	return nodeKeyStatePrefixName
}

// makeNodeKey() uses the node name as the key for the database
// (we will need to change this to a UUID)
//
func makeNodeStateKey(nodeName string) string {
	return getNodeStateKeyPrefix() + nodeName
}
