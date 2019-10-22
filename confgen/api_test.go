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

type SMBVGConfig struct {
	WorkGroup         string
	MapToGuest        string
	HostName          string
	ServerMinProtocol string
	ADDisabled        bool
	AuditLogging      bool
	Security          string
	RPCServerLSARPC   string
	Realm             string
	IDSchema          string
	IDRangeMin1       uint64
	IDRangeMax1       uint64
	IDRangeMin2       uint64
	IDRangeMax2       uint64
	BackEnd           string
	BrowserAnnounce   string
	PrivateProxyFSIP  string
	TCPPort           int
	FastTCPPort       int
}

type SMBGlobalConfig struct {
	TCPPort     string
	FastTCPPort string
}

// TODO - show basic code for printing smb.conf for my 3 VGs...
// Take proxyfs*.conf configuration, read it with ConfigMap and then
// create files in a directory
func TestParser(t *testing.T) {
	// TODO - handle case where no VGs and therefore do not generate any smb.conf files...

	// One entry per VG - each share represents a different volume
	var mySMB = []SMBVGConfig{
		{"workgroup1", "map1", "host1", "proto1", true, false, "some security", "lsa1", "realm1",
			"ID1", 1, 1, 11, 11, "back1", "", "", 11, 111},
		{"workgroup2", "map2", "host2", "proto2", false, false, "more security", "lsa2", "realm2",
			"ID2", 2, 2, 22, 22, "back2", "", "", 22, 222},
		{"workgroup3", "map3", "host3", "proto3", true, true, "even more security", "lsa3", "realm3",
			"ID3", 3, 3, 33, 33, "back3", "", "", 33, 333},
	}

	// Load the template for the global section of smb.conf
	tplate, err := template.ParseFiles("templates/smb_globals.tmpl")
	if err != nil {
		fmt.Printf("Parse of template file returned err: %v\n", err)
		os.Exit(-1)
	}

	// Execute the globals template
	for _, c := range mySMB {
		err := tplate.Execute(os.Stdout, c)
		if err != nil {
			log.Println("executing template:", err)
		}
	}

	// TODO TODO - add creation of shares/volumes section in output file using
	// template smb_shares.tmpl

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
	whoAmI, localVolumeGroupMap, localVolumeMap, globalVolumeGroupMap, globalVolumeMap,
		tcpPort, fastTCPPort, err := fetchVolumeInfo(confMap)

	fmt.Printf("------whoAmI: %v localVolumeGroupMap: %v localVolumeMap: %v globalVolumeGroupMap: %v globalVolumeMap: %v tcpPort: %v fastTCPPort: %v err: %v\n",
		whoAmI, localVolumeGroupMap, localVolumeMap, globalVolumeGroupMap,
		globalVolumeMap, tcpPort, fastTCPPort, err)

	fmt.Printf("\n\n ------ VG named - VG1: %+v\n", localVolumeGroupMap["VG1"])

	/*
		// TODO - End goal
		// 1. call confgen with info pulled from fetchVolumeInfo()
		// 2. verify that smb.conf, exports, etc are correct

		// TODO - current goal
		// Build my SMB related information and call templates
		numVGs := len(localVolumeGroupMap)
		myVGinfo := make([]SMBVg, numVGs)

	*/
}
