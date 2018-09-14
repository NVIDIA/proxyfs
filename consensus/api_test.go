package consensus

import (
	"flag"
	"fmt"
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
	testAddVolumeGroup(t)

	// To add:
	// - online, failover, add, rm, verify bad input
}

// registerEtcd sets up a connection to etcd
func registerEtcd(t *testing.T) (cs *Struct) {
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
func unregisterEtcd(t *testing.T, cs *Struct) {

	// Unregister from the etcd cluster
	cs.Unregister()
}

func vgKeysToReset(name string) (keys map[string]string) {
	keys = make(map[string]string)
	keys[makeVgNameKey(name)] = ""
	keys[makeVgStateKey(name)] = ""
	keys[makeVgNodeKey(name)] = ""
	keys[makeVgIpaddrKey(name)] = ""
	keys[makeVgNetmaskKey(name)] = ""
	keys[makeVgNicKey(name)] = ""
	keys[makeVgAutoFailoverKey(name)] = ""
	keys[makeVgEnabledKey(name)] = ""
	keys[makeVgVolumeListKey(name)] = ""
	return
}

func testBasicAPI(t *testing.T) {
	fmt.Printf("testBasicAPI - START\n")
	cs := registerEtcd(t)
	fmt.Printf("testBasicAPI - 2\n")
	unregisterEtcd(t, cs)
	fmt.Printf("testBasicAPI - 3\n")
}

func testAddVolumeGroup(t *testing.T) {
	var (
		name         = "myTestVg"
		ipAddr       = "192.168.20.20"
		netMask      = "1.1.1.1"
		nic          = "eth0"
		autoFailover = true
		enabled      = true
	)
	assert := assert.New(t)

	cs := registerEtcd(t)

	// TODO - // add VG routines to grab these...
	// should we just delete all keys of VG* ???
	keys := vgKeysToReset(name)
	resetKeys(t, cs.cli, keys)

	// TODO - how add volume list to a volume group?
	// assume volumes are unique across VGs???
	err := cs.AddVolumeGroup(name, ipAddr, netMask, nic, autoFailover, enabled)
	assert.Nil(err, "AddVolumeGroup() returned err")

	// TODO - add a volume to a volume group!!!!

	// TODO - remove volume group
	err = cs.RmVolumeGroup(name)
	assert.Nil(cs, "RmVolumeGroup() returned err")

	resetKeys(t, cs.cli, keys)
	unregisterEtcd(t, cs)
}
