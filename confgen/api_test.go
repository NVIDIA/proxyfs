// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package confgen

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/stretchr/testify/assert"
)

func getConfMap(t *testing.T, filename string) (confMap conf.ConfMap, err error) {
	assert := assert.New(t)
	confMap, err = conf.MakeConfMapFromFile(filename)
	assert.Nil(err, "MakeConfMapFromFile errored out")
	assert.NotNil(confMap, "confMap should not be nil")
	return
}

// This test tests that the correct smb.conf files are created
// for each VG.
//
// 1. Read sample config using ConfigMap
// 2. Pass ConfigMap to both templates to generate the smb.conf files and prove
//    correct
func TestConfigPath(t *testing.T) {
	assert := assert.New(t)

	// Get the configuration from the config file
	confMap, err := getConfMap(t, "sample-proxyfs-configuration/proxyfs.conf")

	// Grab volumes and volume group information
	_, localVolumeGroupMap, _, _, _, err := fetchVolumeInfo(confMap)
	assert.Nil(err, "fetchVolumeInfo should succeed")

	// Create temp directory for SMB VG configuration files
	var tmpDir string
	tmpDir, err = ioutil.TempDir(".", "tst-gen-files")
	assert.Nil(err, "ioutil.TempDir returned error")

	err = os.Mkdir(tmpDir+"/vips", confDirPerm)
	assert.Nil(err, "os.Mkdir returned error")

	err = createSMBConf(tmpDir, localVolumeGroupMap)
	assert.Nil(err, "createSMBConf returned error")

	// TODO - verify new contents

	err = os.RemoveAll(tmpDir)
	assert.Nil(err, "Remove of generated directory returned err error")
}

// Test the IsVolumeShared*() and IsVolumeGroupShared* functions.
func TestIsSharing(t *testing.T) {
	assert := assert.New(t)

	// Get the configuration from the config file
	confMap, err := getConfMap(t, "sample-proxyfs-configuration/proxyfs.conf")
	assert.Nil(err, "getConMap(sample-proxyfs-configuration/proxyfs.conf) should not fail")

	var shared bool
	shared, err = IsVolumeSharedViaSMB(confMap, "volume3")
	assert.Nil(err, "IsVolumeSharedViaSMB(volume3) should not fail")
	assert.False(shared, "volume3 is not shared via SMB")

	shared, err = IsVolumeSharedViaNFS(confMap, "volume3")
	assert.Nil(err, "IsVolumeSharedViaNFS(volume3) should not fail")
	assert.True(shared, "volume3 is shared via NFS")

	shared, err = IsVolumeSharedViaSMB(confMap, "vol-vg32-2")
	assert.Nil(err, "IsVolumeSharedViaSMB(vol-vg32-2) should not fail")
	assert.True(shared, "vol-vg32-2 is shared via SMB")

	shared, err = IsVolumeGroupSharedViaSMB(confMap, "vg32-2")
	assert.Nil(err, "IsVolumeSharedViaSMB(vg32-2) should not fail")
	assert.True(shared, "vg32-2 is shared via SMB")

	shared, err = IsVolumeGroupSharedViaNFS(confMap, "vg32-2")
	assert.Nil(err, "IsVolumeSharedViaNFS(vg32-2) should not fail")
	assert.False(shared, "vg32-2 is not shared via NFS")

	shared, err = IsVolumeGroupSharedViaNFS(confMap, "VG1")
	assert.Nil(err, "IsVolumeSharedViaNFS(VG1) should not fail")
	assert.True(shared, "VG1 is shared via NFS")

	shared, err = IsVolumeGroupSharedViaNFS(confMap, "bazbaz")
	assert.NotNil(err, "volume group 'bazbaz' does not exist")

	shared, err = IsVolumeSharedViaNFS(confMap, "bazbaz")
	assert.NotNil(err, "volume 'bazbaz' does not exist")

	shared, err = IsVolumeSharedViaSMB(confMap, "bambam")
	assert.NotNil(err, "volume 'bambam' does not exist")

	shared, err = IsVolumeGroupSharedViaSMB(confMap, "bambam")
	assert.NotNil(err, "volume group 'bambam' does not exist")
}
