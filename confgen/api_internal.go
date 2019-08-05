package confgen

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/swiftstack/ProxyFS/conf"
)

const (
	linuxUserCommentDefault = "user-created-for-samba"
	netDirDefault           = "/usr/bin"
	pdbdditDirDefault       = "/usr/bin"
	smbdDirDefault          = "/usr/sbin"
	smbpasswdDirDefault     = "/usr/bin"
)

type envSettingsStruct struct {
	linuxUserComment string
	netPath          string
	pdbeditPath      string
	smbdPath         string
	smbpasswdPath    string
}

const (
	useraddPath = "/usr/sbin/useradd"
	userdelPath = "/usr/sbin/userdel"
)

const (
	smbConfCommonFileName = "smb_common.conf"      // Used for pdbedit & smbpasswd operations
	smbUsersSetupFileName = "smb_users_setup.bash" // Used to {create|update|destroy} SMB & Linux users
	proxyFSConfFileName   = "proxyfs.conf"         // Used to pass to mkproxyfs & proxyfsd
)

const (
	confDirPerm  = os.FileMode(0777) // Let umask "restrict" this as desired
	confFilePerm = os.FileMode(0666) // Let umask "restrict" this as desired
	scriptPerm   = os.FileMode(0777) // Let umask "restrict" this as desired
)

type smbUserMap map[string]string // Key=SMBUserName; Value=SMBUserPassword decoded via base64.StdEncoding.DecodeString()

func computeInitial(envMap EnvMap, confFilePath string, confOverrides []string, initialDirPath string) (err error) {
	var (
		envSettings             *envSettingsStruct
		initialConfMap          conf.ConfMap
		smbUsersSetupFile       *os.File
		toCreateSMBUserMap      smbUserMap
		toCreateSMBUserName     string
		toCreateSMBUserPassword string
	)

	// Fetch environ settings

	envSettings = fetchEnvironSettings(envMap)

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

	// Compute SMB Users script

	_, toCreateSMBUserMap, _, _, err = computeSMBUserListChange(make(smbUserMap), initialConfMap)

	smbUsersSetupFile, err = os.OpenFile(initialDirPath+"/"+smbUsersSetupFileName, os.O_CREATE|os.O_WRONLY, scriptPerm)
	if nil != err {
		return
	}

	_, err = smbUsersSetupFile.WriteString("#!/bin/bash\n")
	if nil != err {
		return
	}
	_, err = smbUsersSetupFile.WriteString("set -e\n")
	if nil != err {
		return
	}

	for toCreateSMBUserName, toCreateSMBUserPassword = range toCreateSMBUserMap {
		_, err = smbUsersSetupFile.WriteString(fmt.Sprintf("%s --comment %s --no-create-home %s\n", useraddPath, envSettings.linuxUserComment, toCreateSMBUserName))
		if nil != err {
			return
		}
		_, err = smbUsersSetupFile.WriteString(fmt.Sprintf("echo \"%s\\n%s\" | %s -c %s -a %s\n", toCreateSMBUserPassword, toCreateSMBUserPassword, envSettings.smbpasswdPath, smbConfCommonFileName, toCreateSMBUserName))
		if nil != err {
			return
		}
	}

	err = smbUsersSetupFile.Close()
	if nil != err {
		return
	}

	return nil // TODO
}

func computePhases(envMap EnvMap, initialDirPath string, confFilePath string, confOverrides []string, phaseOneDirPath string, phaseTwoDirPath string) (err error) {
	var (
		envSettings     *envSettingsStruct
		initialConfMap  conf.ConfMap
		phaseOneConfMap conf.ConfMap
		phaseTwoConfMap conf.ConfMap
	)

	// Fetch environ settings

	envSettings = fetchEnvironSettings(envMap)
	fmt.Println("UNDO:", envSettings)

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

	// Compute SMB Users
	// computeSMBUserListChange(oldSMBUserMap smbUserMap, newConfMap conf.ConfMap)
	//   (newSMBUserMap smbUserMap, toCreateSMBUserMap smbUserMap, toUpdateSMBUserMap smbUserMap, toDeleteSMBUserMap smbUserMap, err error)

	return nil // TODO
}

func fetchEnvironSettings(envMap EnvMap) (envSettings *envSettingsStruct) {
	var (
		inEnv bool
	)

	envSettings = &envSettingsStruct{}

	envSettings.linuxUserComment, inEnv = envMap[LinuxUserCommentEnv]
	if !inEnv {
		envSettings.linuxUserComment = linuxUserCommentDefault
	}

	envSettings.netPath, inEnv = envMap[NetDirEnv]
	if !inEnv {
		envSettings.netPath = netDirDefault
	}
	envSettings.netPath += "/net"

	envSettings.pdbeditPath, inEnv = envMap[PdbeditDirEnv]
	if !inEnv {
		envSettings.pdbeditPath = pdbdditDirDefault
	}
	envSettings.pdbeditPath += "/pdbedit"

	envSettings.smbdPath, inEnv = envMap[SmbdDirEnv]
	if !inEnv {
		envSettings.smbdPath = smbdDirDefault
	}
	envSettings.smbdPath += "/smbd"

	envSettings.smbpasswdPath, inEnv = envMap[SmbpasswdDirEnv]
	if !inEnv {
		envSettings.smbpasswdPath = smbpasswdDirDefault
	}
	envSettings.smbpasswdPath += "/smbpasswd"

	return
}

func computeSMBUserListChange(oldSMBUserMap smbUserMap, newConfMap conf.ConfMap) (newSMBUserMap smbUserMap, toCreateSMBUserMap smbUserMap, toUpdateSMBUserMap smbUserMap, toDeleteSMBUserMap smbUserMap, err error) {
	var (
		inNewSMBUserMap    bool
		inOldSMBUserMap    bool
		newSMBUserName     string
		newSMBUserNameList []string
		newSMBUserPassword string
		oldSMBUserName     string
	)

	newSMBUserMap = make(smbUserMap)
	toCreateSMBUserMap = make(smbUserMap)
	toUpdateSMBUserMap = make(smbUserMap)
	toDeleteSMBUserMap = make(smbUserMap)

	newSMBUserNameList, err = newConfMap.FetchOptionValueStringSlice("FSGlobals", "SMBUserList")
	if nil != err {
		newSMBUserNameList = make([]string, 0) // TODO: Eventually just return
	}

	for _, newSMBUserName = range newSMBUserNameList {
		newSMBUserPassword, err = newConfMap.FetchOptionValueBase64String("SMBUsers", newSMBUserName)
		if nil != err {
			return
		}
		newSMBUserMap[newSMBUserName] = newSMBUserPassword
		_, inOldSMBUserMap = oldSMBUserMap[newSMBUserName]
		if inOldSMBUserMap {
			toUpdateSMBUserMap[newSMBUserName] = newSMBUserPassword
		} else {
			toCreateSMBUserMap[newSMBUserName] = newSMBUserPassword
		}
	}

	for oldSMBUserName = range oldSMBUserMap {
		_, inNewSMBUserMap = newSMBUserMap[oldSMBUserName]
		if !inNewSMBUserMap {
			toDeleteSMBUserMap[oldSMBUserName] = ""
		}
	}

	return
}
