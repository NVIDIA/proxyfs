package confgen

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/conf"
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
	assert.Nil(err, "fetchVolumeInfo returned an error")

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
