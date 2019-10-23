package confgen

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"text/template"

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

// This test tests the path where we have updated the confMap
// with the location where a VG shall be started and now want to
// put the new configuration into place.
//
// It calls confgen to generate all of the config files
// for SMB, NFS, etc.

// 1. Read sample config using ConfigMap
// 2. Pass ConfigMap to both templates to generate the smb.conf files and prove
//    correct
func TestConfigPath(t *testing.T) {
	assert := assert.New(t)

	// Get the configuration from the config file
	confMap, err := getConfMap(t, "sample-proxyfs-configuration/proxyfs.conf")
	fmt.Printf("ConfMap: %v err: %v\n", confMap, err)

	// Grab volumes and volume group information
	_, localVolumeGroupMap, _, _, _, err := fetchVolumeInfo(confMap)

	/*
		// TODO
		// 1. call createSMBConf() on a per VG basis
		// 2. verify that smb.conf, exports, etc are correct
	*/

	// Load the template for the global section of smb.conf
	globalTplate, err := template.ParseFiles("templates/smb_globals.tmpl")
	if err != nil {
		fmt.Printf("Parse of template file returned err: %v\n", err)
		os.Exit(-1)
	}

	// Load the template for the share section of smb.conf
	sharesTplate, err := template.ParseFiles("templates/smb_shares.tmpl")
	if err != nil {
		fmt.Printf("Parse of template file returned err: %v\n", err)
		os.Exit(-1)
	}

	// Create temp directory for SMB VG configuration files
	var tmpDir string
	tmpDir, err = ioutil.TempDir(".", "tst-gen-files")
	assert.Nil(err, "ioutil.TempDir returned error")

	// Execute the templates
	for _, vg := range localVolumeGroupMap {
		fileName := tmpDir + "/smb-VG-" + vg.VolumeGroupName + ".conf"
		f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, smbConfPerm)
		if err != nil {
			return
		}

		err = globalTplate.Execute(f, vg)
		if err != nil {
			log.Println("executing globalTplate:", err)
		}

		err = sharesTplate.Execute(f, vg)
		if err != nil {
			log.Println("executing sharesTplate:", err)
		}
	}

	// TODO - verify new contents
	// remove tmpDir directory
}
