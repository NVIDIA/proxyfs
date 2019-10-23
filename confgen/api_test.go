package confgen

import (
	"fmt"
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

// TODO:
// 1. Read sample config using ConfigMap
// 2. Pass ConfigMap to both templates to generate the smb.conf files and prove
//    correct.
// TODO - later run through confgen to create all files...
func TestConfigPath(t *testing.T) {
	// Get the configuration from the config file
	confMap, err := getConfMap(t, "sample-proxyfs-configuration/proxyfs.conf")
	fmt.Printf("ConfMap: %v err: %v\n", confMap, err)

	// Grab volumes and volume group information
	_, localVolumeGroupMap, _, _, _, err := fetchVolumeInfo(confMap)

	/*
		// TODO - End goal
		// 1. call confgen with info pulled from fetchVolumeInfo()
		// 2. verify that smb.conf, exports, etc are correct

		// TODO - current goal
		// Build my SMB related information and call templates
		numVGs := len(localVolumeGroupMap)
		myVGinfo := make([]SMBVg, numVGs)

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

	// Execute the templates
	for _, vg := range localVolumeGroupMap {
		err := globalTplate.Execute(os.Stdout, vg)
		if err != nil {
			log.Println("executing globalTplate:", err)
		}

		err = sharesTplate.Execute(os.Stdout, vg)
		if err != nil {
			log.Println("executing sharesTplate:", err)
		}
	}
}
