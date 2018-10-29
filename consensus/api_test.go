package consensus

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	tu "github.com/swiftstack/ProxyFS/consensus/testutils"

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

// newHA sets up 3 node test cluster and opens connection to HA
func newHA(t *testing.T) (cs *EtcdConn, tc *tu.TestCluster) {

	// Start a 3 node etcd cluster
	tc = tu.NewTC(t, 3)

	// Grab endpoint used by client 0 and pass to New()
	endpoints := tc.Endpoints(0)

	assert := assert.New(t)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs, err := New(endpoints, tc.HostName(), 2*time.Second)
	assert.NotNil(cs, "Register() failed")
	assert.Nil(err, "Register() returned err")

	// Setup test script, etc
	cs.SetTest(true)
	cs.SetTestSWD(tc.SWD)

	return cs, tc
}

// closeHA unregisters from etcd
func closeHA(t *testing.T, cs *EtcdConn, tc *tu.TestCluster) {

	// Unregister from the etcd cluster
	cs.Close()

	// Remove cluster
	tc.Destroy(t)
}

func vgKeysToDelete(vgTestName string) (keys map[string]struct{}) {
	keys = make(map[string]struct{})
	keys[makeVgKey(vgTestName)] = struct{}{}
	return
}

func testBasicAPI(t *testing.T) {
	cs, tc := newHA(t)
	closeHA(t, cs, tc)
}

// Delete test keys
func deleteVgKeys(t *testing.T, cs *EtcdConn, km map[string]struct{}) {
	tu.DeleteKeys(t, cs.cli, km)
}

func testAddRmVolumeGroup(t *testing.T) {
	var (
		vgTestName   = "myTestVg"
		ipAddr       = "192.168.20.20"
		netMask      = "255.255.255.0"
		nic          = "eth0"
		autoFailover = true
		enabled      = true
	)
	assert := assert.New(t)

	cs, tc := newHA(t)
	defer closeHA(t, cs, tc)

	keys := vgKeysToDelete(vgTestName)
	deleteVgKeys(t, cs, keys)

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
}

func testStartVolumeGroup(t *testing.T) {
	var (
		vgTestName   = "myTestVg"
		ipAddr       = "10.10.10.10"
		netMask      = "255.255.255.0"
		nic          = "eth0"
		autoFailover = true
		enabled      = true
	)
	assert := assert.New(t)

	cs, tc := newHA(t)
	defer closeHA(t, cs, tc)

	keys := vgKeysToDelete(vgTestName)
	deleteVgKeys(t, cs, keys)

	// TODO - how add volume list to a volume group?
	// assume volumes are unique across VGs???
	err := cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)
	assert.Nil(err, "AddVolumeGroup() returned err")

	// Setup as a server so that startVgs() will start the
	// VG.
	err = cs.Server()

	// TODO - block until server is ONLINE

	// Now remove the volume group - should fail since VG is in ONLINE
	// or ONLINING state.  Only VGs which are OFFLINE can be removed.
	err = cs.RmVolumeGroup(vgTestName)
	assert.NotNil(err, "RmVolumeGroup() returned err")
}
