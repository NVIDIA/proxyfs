package confgen

import (
	"io/ioutil"
	"os"

	"github.com/swiftstack/ProxyFS/conf"
)

const (
	confDirPerm         = os.FileMode(0777) // Let umask "restrict" this as desired
	confFilePerm        = os.FileMode(0666) // Let umask "restrict" this as desired
	proxyFSConfFileName = "proxyfs.conf"
)

func computeInitial(confFilePath string, confOverrides []string, initialDirPath string) (err error) {
	var (
		initialConfMap conf.ConfMap
	)

	// Load supplied config with overrides

	initialConfMap, err = conf.MakeConfMapFromFile(confFilePath)
	if nil != err {
		return
	}

	err = initialConfMap.UpdateFromStrings(confOverrides)
	if nil != err {
		return
	}

	// Store Initial Config

	err = os.Mkdir(initialDirPath, confDirPerm)
	if nil != err {
		return
	}

	err = ioutil.WriteFile(initialDirPath+"/"+proxyFSConfFileName, []byte(initialConfMap.Dump()), confFilePerm)
	if nil != err {
		return
	}

	return nil // TODO
}

func computePhases(initialDirPath string, confFilePath string, confOverrides []string, phaseOneDirPath string, phaseTwoDirPath string) (err error) {
	var (
		initialConfMap  conf.ConfMap
		phaseOneConfMap conf.ConfMap
		phaseTwoConfMap conf.ConfMap
	)

	// Load config from initialDirPath

	initialConfMap, err = conf.MakeConfMapFromFile(initialDirPath + "/" + proxyFSConfFileName)
	if nil != err {
		return
	}

	// Load supplied config with overrides that will be used for Phase Two

	phaseTwoConfMap, err = conf.MakeConfMapFromFile(confFilePath)
	if nil != err {
		return
	}

	err = phaseTwoConfMap.UpdateFromStrings(confOverrides)
	if nil != err {
		return
	}

	// Compute config that will be used for Phase One

	phaseOneConfMap = initialConfMap // TODO: for now, just use this one

	// Store Phase One Config

	err = os.Mkdir(phaseOneDirPath, confDirPerm)
	if nil != err {
		return
	}

	err = ioutil.WriteFile(phaseOneDirPath+"/"+proxyFSConfFileName, []byte(phaseOneConfMap.Dump()), confFilePerm)
	if nil != err {
		return
	}

	// Store Phase Two Config

	err = os.Mkdir(phaseTwoDirPath, confDirPerm)
	if nil != err {
		return
	}

	err = ioutil.WriteFile(phaseTwoDirPath+"/"+proxyFSConfFileName, []byte(phaseTwoConfMap.Dump()), confFilePerm)
	if nil != err {
		return
	}

	return nil // TODO
}
