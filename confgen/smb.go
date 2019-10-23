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

	// Load the template for the global section of smb.conf
	globalTplate, err := template.ParseFiles("templates/smb_globals.tmpl")
	if err != nil {
		// TODO - log this appropriately
		fmt.Printf("Parse of template file returned err: %v\n", err)
		return
	}

	// Load the template for the share section of smb.conf
	sharesTplate, err := template.ParseFiles("templates/smb_shares.tmpl")
	if err != nil {
		// TODO - log this appropriately
		fmt.Printf("Parse of template file returned err: %v\n", err)
		return
	}

	for _, volumeGroup = range localVolumeGroupMap {
		vipDirPath = initialDirPath + "/" + vipsDirName + "/" + volumeGroup.VirtualIPAddr

		err = os.Mkdir(vipDirPath, confDirPerm)
		if nil != err {
			// TODO - log it
			return
		}

		fileName := vipDirPath + "/smb-VG-" + volumeGroup.VolumeGroupName + ".conf"
		f, openErr := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, smbConfPerm)
		if openErr != nil {
			// TODO - log it
			err = openErr
			return
		}

		err = globalTplate.Execute(f, volumeGroup)
		if err != nil {
			// TODO - log it
			fmt.Printf("executing globalTplate: %v\n", err)
			return
		}

		err = sharesTplate.Execute(f, volumeGroup)
		if err != nil {
			// TODO - log it
			fmt.Printf("executing sharesTplate: %v\n", err)
			return
		}
	}

	return
}