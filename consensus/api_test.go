package consensus

import (
	"context"
	"flag"
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

// Test basic API
func TestAPI(t *testing.T) {

	testBasicAPI(t)
	testAddRmVolumeGroup(t)
	testStartVolumeGroup(t)

	// To add:
	// - online, failover, verify bad input such as bad IP address?
}

// registerEtcd sets up a connection to etcd
func registerEtcd(t *testing.T) (cs *EtcdConn) {
	assert := assert.New(t)

	endpoints := []string{"192.168.60.10:2379", "192.168.60.11:2379", "192.168.60.12:2379"}

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs, err := Register(endpoints, 2*time.Second)
	assert.NotNil(cs, "Register() failed")
	assert.Nil(err, "Register() returned err")

	return cs
}

// unregisterEtcd unregisters from etcd
func unregisterEtcd(t *testing.T, cs *EtcdConn) {

	// Unregister from the etcd cluster
	cs.Unregister()
}

func vgKeysToReset(vgTestName string) (keys map[string]string) {
	keys = make(map[string]string)
	keys[makeVgNameKey(vgTestName)] = ""
	keys[makeVgStateKey(vgTestName)] = ""
	keys[makeVgNodeKey(vgTestName)] = ""
	keys[makeVgIpAddrKey(vgTestName)] = ""
	keys[makeVgNetmaskKey(vgTestName)] = ""
	keys[makeVgNicKey(vgTestName)] = ""
	keys[makeVgAutoFailoverKey(vgTestName)] = ""
	keys[makeVgEnabledKey(vgTestName)] = ""
	keys[makeVgVolumeListKey(vgTestName)] = ""
	return
}

func testBasicAPI(t *testing.T) {
	cs := registerEtcd(t)
	unregisterEtcd(t, cs)
}

// Delete test keys
func resetVgKeys(t *testing.T, cs *EtcdConn, km map[string]string) {
	for k := range km {
		_, _ = cs.cli.Delete(context.TODO(), k)
	}
}

func testAddRmVolumeGroup(t *testing.T) {
	var (
		vgTestName   = "myTestVg"
		ipAddr       = "192.168.20.20"
		netMask      = "1.1.1.1"
		nic          = "eth0"
		autoFailover = true
		enabled      = true
	)
	assert := assert.New(t)

	cs := registerEtcd(t)

	keys := vgKeysToReset(vgTestName)
	resetVgKeys(t, cs, keys)

	// TODO - how add volume list to a volume group?
	// assume volumes are unique across VGs???
	err := cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)
	assert.Nil(err, "AddVolumeGroup() returned err")

	// If recreate the VG again it should fail
	err = cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)
	assert.NotNil(err, "AddVolumeGroup() twice should err")

	// Now remove the volume group
	err = cs.RmVolumeGroup(vgTestName)
	assert.Nil(err, "RmVolumeGroup() returned err")

	// Trying to removing a volume group a second time should fail
	err = cs.RmVolumeGroup(vgTestName)
	assert.NotNil(err, "RmVolumeGroup() twice should err")

	unregisterEtcd(t, cs)
}

func testStartVolumeGroup(t *testing.T) {
	var (
		vgTestName   = "myTestVg"
		ipAddr       = "192.168.20.20"
		netMask      = "1.1.1.1"
		nic          = "eth0"
		autoFailover = true
		enabled      = true
	)
	assert := assert.New(t)

	cs := registerEtcd(t)

	keys := vgKeysToReset(vgTestName)
	resetVgKeys(t, cs, keys)

	// TODO - how add volume list to a volume group?
	// assume volumes are unique across VGs???
	err := cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)
	assert.Nil(err, "AddVolumeGroup() returned err")

	// Start the VG -
	cs.startVgs(RevisionNumber(0))

	// Now remove the volume group - should fail since VG is in ONLINE
	// or ONLINING state.  Only VGs which are OFFLINE can be removed.
	err = cs.RmVolumeGroup(vgTestName)
	assert.NotNil(err, "RmVolumeGroup() returned err")

	unregisterEtcd(t, cs)
}
