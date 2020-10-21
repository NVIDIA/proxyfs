package inode

import (
	"testing"

	"github.com/swiftstack/ProxyFS/transitions"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	var err error

	assert := assert.New(t)

	testSetup(t, false)

	// verify that proxyfs doesn't panic for a bad config value
	// (remove the volume group we're going to update)
	testConfUpdateStrings := []string{
		"FSGlobals.VolumeGroupList=",
	}

	err = testConfMap.UpdateFromStrings(testConfUpdateStrings)
	assert.Nil(err, "testConfMap.UpdateFromStrings(testConfUpdateStrings) failed")

	err = transitions.Signaled(testConfMap)
	assert.Nil(err, "transitions.Signaled failed")

	// now try with bogus ReadCacheWeight
	testConfUpdateStrings = []string{
		"FSGlobals.VolumeGroupList=TestVolumeGroup",
		"VolumeGroup:TestVolumeGroup.ReadCacheWeight=0",
	}

	err = testConfMap.UpdateFromStrings(testConfUpdateStrings)
	assert.Nil(err, "testConfMap.UpdateFromStrings(testConfUpdateStrings) failed")

	err = transitions.Signaled(testConfMap)
	assert.Nil(err, "transitions.Signaled failed")

	testTeardown(t)
}
