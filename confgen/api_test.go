package confgen

import (
	"fmt"
	"log"
	"os"
	"testing"

	"text/template"
)

func TestDummy(t *testing.T) {
}

type SMBConfig struct {
	WorkGroup         string
	MapToGuest        string
	HostName          string
	ServerMinProtocol string
	AdDisabled        bool
	AuditLogging      bool
	Security          string
	RPCServerLSARPC   string
	Realm             string
	IDSchema          string
	IDRangeMin1       int
	IDRangeMax1       int
	IDRangeMin2       int
	IDRangeMax2       int
	BackEnd           string
	BrowserAnnounce   string
	PrivateProxyFSIP  string
	TCPPort           string
	FastTCPPort       string
}

// TODO - show basic code for printing smb.conf for my 3 VGs...
func TestParser(t *testing.T) {
	// TODO - handle case where no VGs and therefore do not generate any smb.conf files...

	// One entry per VG - each share represents a different volume
	var mySMB = []SMBConfig{
		{"workgroup1", "map1", "host1", "proto1", true, false, "some security", "lsa1", "realm1",
			"ID1", 1, 1, 11, 11, "back1", "", "", "", ""},
		{"workgroup2", "map2", "host2", "proto2", false, false, "more security", "lsa2", "realm2",
			"ID2", 2, 2, 22, 22, "back2", "", "", "", ""},
		{"workgroup3", "map3", "host3", "proto3", true, true, "even more security", "lsa3", "realm3",
			"ID3", 3, 3, 33, 33, "back3", "", "", "", ""},
	}

	// Load the template for the global section of smb.conf
	tplate, err := template.ParseFiles("testData/smb_globals.tmpl")
	if err != nil {
		fmt.Printf("Parse of template file returned err: %v\n", err)
		os.Exit(-1)
	}
	fmt.Printf("parsed file - tplate: %v\n", tplate)

	// Execute the global template
	for _, c := range mySMB {
		err := tplate.Execute(os.Stdout, c)
		if err != nil {
			log.Println("executing template:", err)
		}
	}

}
