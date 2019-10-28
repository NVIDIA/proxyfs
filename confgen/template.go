package confgen

import (
	"fmt"
	"os"
	"text/template"
)

// createSMBConf writes the per VG smb.conf file
func createSMBConf(initialDirPath string, localVolumeGroupMap volumeGroupMap) (err error) {
	var (
		vipDirPath  string
		volumeGroup *VolumeGroup
	)

	// Locate the proxyfs config directory with the templates
	var proxyfsConfDir string
	confDirs := []string{proxyfsConfDir0, proxyfsConfDir1, proxyfsConfDir2}
	for _, proxyfsConfDir = range confDirs {

		// if the directory has a parsable "templates/smb_globals.tmpl"
		// then its the right one
		_, err = template.ParseFiles(proxyfsConfDir + "/" + "templates/smb_globals.tmpl")
		if err == nil {
			break
		}
	}
	if err != nil {
		fmt.Printf("Could not find '%s' in any of %v\n", "templates/smb_globals.tmpl", confDirs)
		return
	}

	// Load the template for the global section of smb.conf
	// from one of several possible locations
	globalTplate, err := template.ParseFiles(proxyfsConfDir + "/" + "templates/smb_globals.tmpl")
	if nil != err {

		// TODO - log this appropriately
		fmt.Printf("Parse of template file returned err: %v\n", err)
		return
	}

	// Load the template for the share section of smb.conf
	sharesTplate, err := template.ParseFiles("templates/smb_shares.tmpl")
	if nil != err {
		// TODO - log this appropriately
		fmt.Printf("Parse of template file returned err: %v\n", err)
		return
	}

	for _, volumeGroup = range localVolumeGroupMap {
		vipDirPath = initialDirPath + "/" + vipsDirName + "/" + volumeGroup.VirtualIPAddr

		err = os.Mkdir(vipDirPath, confDirPerm)
		if nil != err {
			return
		}

		fileName := vipDirPath + "/smb-VG-" + volumeGroup.VolumeGroupName + ".conf"
		f, openErr := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, smbConfPerm)
		if nil != openErr {
			err = openErr
			return
		}

		err = globalTplate.Execute(f, volumeGroup)
		if nil != err {
			return
		}

		err = sharesTplate.Execute(f, volumeGroup)
		if nil != err {
			return
		}
	}

	return
}
