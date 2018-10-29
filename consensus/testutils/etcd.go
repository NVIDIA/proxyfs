package testutils

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/clientv3"
	ei "go.etcd.io/etcd/integration"
	"os"
	"sync"
	"testing"
	"time"
)

// TODO - don't start clients since want Register() to create it....
// how create with http interface, etc?
//
// Add example of how to use the test infrastructure...
//
// test:
// 1. start 3 node cluster
// 2. Register() with endpoints
// 3. Do some tests with API
// 4. Shutdown more than half of cluster
// 6. attempt transaction and get back no leader error....

// TestCluster wraps etcd's notation of a cluster being tested.
type TestCluster struct {
	Clus *ei.ClusterV3
	SWD  string // Starting working directory
}

// NewTC creates and launches a test cluster
func NewTC(t *testing.T, size int) (tc *TestCluster) {

	// When running the unit tests in a container, we are unable
	// to bind to the port for the Unix domain socket due to the mounted
	// file system.  Therefore, change directory to /tmp before starting
	// the cluster.
	swd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Unable to chdir to /tmp")
		os.Exit(-1)
	}

	err = os.Chdir("/tmp")
	if err != nil {
		fmt.Printf("Unable to chdir to /tmp")
		os.Exit(-1)
	}

	tc = &TestCluster{Clus: ei.NewClusterV3(t, &ei.ClusterConfig{Size: size}), SWD: swd}

	return
}

// Endpoints returns the endpoints used by the client
func (tc *TestCluster) Endpoints(id int) []string {
	return tc.Clus.Client(id).Endpoints()
}

// Destroy stops and destroys the test cluster
func (tc *TestCluster) Destroy(t *testing.T) {
	tc.Clus.Terminate(t)

	err := os.Chdir(tc.SWD)
	if err != nil {
		fmt.Printf("Unable to chdir to: %v", tc.SWD)
		os.Exit(-1)
	}
}

// Client returns a client pointer from the test cluster
func (tc *TestCluster) Client(id int) (cli *clientv3.Client) {
	return tc.Clus.Client(id)
}

// HostName returns the "hostname" where this client is running
func (tc *TestCluster) HostName() (hostName string) {
	return tc.Clus.Members[0].Name
}

// DestroyClient closes the client and nils the entry in the clus object
func (tc *TestCluster) DestroyClient(cli *clientv3.Client, id int) {
	cli.Close()
	tc.Clus.TakeClient(id)
}

// watcher watches for a key change and asserts that it has the expected value.
// There is a separate watcher routine per key.
func watcher(t *testing.T, cli *clientv3.Client, key string, expectedValue string,
	swg *sync.WaitGroup, fwg *sync.WaitGroup) {

	swg.Done() // The watcher is running!

	assert := assert.New(t)
	wch1 := cli.Watch(context.Background(), key)
	for wresp1 := range wch1 {
		for _, e := range wresp1.Events {
			assert.Equal(expectedValue, string(e.Kv.Value),
				"watcher saw different value than expected")

			// Handle heartbeat test case
			if string(e.Kv.Key) == "TESTHB" {

				// Get current time
				currentTime := time.Now().UTC()

				// Unmarshal time HB sent
				var sentTime time.Time
				err := sentTime.UnmarshalText(e.Kv.Value)
				assert.Nil(err, "UnmarshalText() failed with HB")

				// Current time must be at least 500 milliseconds later
				testTime := sentTime
				testTime.Add(500 * time.Millisecond)
				assert.True(currentTime.After(testTime), "testTime !> 500 milliseconds")
			}
		}

		// The watcher has received it's event and will now return
		fwg.Done()
		return
	}
}

// StartWatchers starts a go routine for each key in km
func StartWatchers(t *testing.T, cli *clientv3.Client, km map[string]string, swg *sync.WaitGroup,
	fwg *sync.WaitGroup) {
	for k, v := range km {
		swg.Add(1)
		go watcher(t, cli, k, v, swg, fwg)
	}

	// Wait for watchers to start
	swg.Wait()
}

// ResetKeys resets the value of the key to ""
func ResetKeys(t *testing.T, cli *clientv3.Client, km map[string]string) {
	assert := assert.New(t)
	kvc := clientv3.NewKV(cli)
	for k := range km {
		_, err := kvc.Put(context.TODO(), k, "")
		assert.Nil(err, "kvc.Put() returned err")
	}
}

// DeleteKeys deletes test keys in km
func DeleteKeys(t *testing.T, cli *clientv3.Client, km map[string]struct{}) {
	for k := range km {
		_, _ = cli.Delete(context.TODO(), k)
	}
}
