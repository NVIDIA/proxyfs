package confgen

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

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
	exportsFileName       = "exports"              // Used to identify NFS exports
	fuseSetupFileName     = "fuse_setup.bash"      // Used to create FUSE export directories
	proxyFSConfFileName   = "proxyfs.conf"         // Used to pass to mkproxyfs & proxyfsd
	smbConfCommonFileName = "smb_common.conf"      // Used for pdbedit & smbpasswd operations
	smbUsersSetupFileName = "smb_users_setup.bash" // Used to {create|update|destroy} SMB & Linux users
)

const (
	confDirPerm     = os.FileMode(0777) // Let umask "restrict" this as desired
	confFilePerm    = os.FileMode(0666) // Let umask "restrict" this as desired
	exportsFilePerm = os.FileMode(0666) // Let umask "restrict" this as desired
	fuseDirPerm     = os.FileMode(0000) // Fail all non-root access to missing FUSE exports
	scriptPerm      = os.FileMode(0777) // Let umask "restrict" this as desired
)

type smbUserMap map[string]string // Key=SMBUserName; Value=SMBUserPassword decoded via base64.StdEncoding.DecodeString()

type uint64Set map[uint64]struct{}
type stringSet map[string]struct{}

const (
	nfsSubtreeCheck = "no_subtree_check" // Force this for every NFS Export
	nfsSync         = "sync"             // Force this for every NFS Export
)

type NFSClient struct {
	clientName    string
	ClientPattern string
	AccessMode    string
	RootSquash    string
	Secure        string
}

type NFSClientList []*NFSClient
type NFSClientMap map[string]*NFSClient // Key=NFSClient.clientName

type Volume struct {
	volumeName         string        // Must be unique
	volumeGroup        *VolumeGroup  //
	FSID               uint64        // Must be unique
	FUSEMountPointName string        // Must be unique unless == "" (no FUSE mount point...and cannot be NFS exported)
	nfsClientList      NFSClientList // Must be empty (no NFS Export) if FUSEMountPointName == ""
	nfsClientMap       NFSClientMap  // Must be empty (no NFS Export) if FUSEMountPointName == ""
	SMBShareName       string        // Must be unique unless == "" (no SMB Share)
	AccountName        string        // Must be unique
}

type volumeMap map[string]*Volume // Key=volume.volumeName

type VolumeGroup struct {
	volumeGroupName string    // Must be unique
	volumeMap       volumeMap //
	VirtualIPAddr   string    // Must be unique unless == ""
	PrimaryPeer     string    //
}

type volumeGroupMap map[string]*VolumeGroup // Key=VolumeGroup.volumeGroupName

func computeInitial(envMap EnvMap, confFilePath string, confOverrides []string, initialDirPath string) (err error) {
	var (
		envSettings             *envSettingsStruct
		exportsFile             *os.File
		fuseSetupFile           *os.File
		initialConfMap          conf.ConfMap
		localVolumeMap          volumeMap
		nfsClient               *NFSClient
		smbUsersSetupFile       *os.File
		toCreateSMBUserMap      smbUserMap
		toCreateSMBUserName     string
		toCreateSMBUserPassword string
		volume                  *Volume
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
		_, err = smbUsersSetupFile.WriteString(fmt.Sprintf("echo -e \"%s\\n%s\" | %s -c %s -a %s\n", toCreateSMBUserPassword, strings.ReplaceAll(toCreateSMBUserPassword, "\\", "\\\\"), envSettings.smbpasswdPath, smbConfCommonFileName, toCreateSMBUserName))
		if nil != err {
			return
		}
	}

	err = smbUsersSetupFile.Close()
	if nil != err {
		return
	}

	// Compute FUSE MountPoint Directory script

	_, _, localVolumeMap, _, _, err = fetchVolumeInfo(initialConfMap)
	if nil != err {
		err = fmt.Errorf("In initialConfMap: %v", err)
		return
	}

	fuseSetupFile, err = os.OpenFile(initialDirPath+"/"+fuseSetupFileName, os.O_CREATE|os.O_WRONLY, scriptPerm)
	if nil != err {
		return
	}

	_, err = fuseSetupFile.WriteString("#!/bin/bash\n")
	if nil != err {
		return
	}
	_, err = fuseSetupFile.WriteString("set -e\n")
	if nil != err {
		return
	}

	for _, volume = range localVolumeMap {
		if "" != volume.FUSEMountPointName {
			_, err = fuseSetupFile.WriteString(fmt.Sprintf("mkdir -p -m 0%03o %s\n", fuseDirPerm, volume.FUSEMountPointName))
			if nil != err {
				return
			}
		}
	}

	err = fuseSetupFile.Close()
	if nil != err {
		return
	}

	// Compute exports file

	exportsFile, err = os.OpenFile(initialDirPath+"/"+exportsFileName, os.O_CREATE|os.O_WRONLY, exportsFilePerm)
	if nil != err {
		return
	}

	for _, volume = range localVolumeMap {
		if 0 < len(volume.nfsClientList) {
			_, err = exportsFile.WriteString(fmt.Sprintf("\"%s\"", volume.FUSEMountPointName))
			if nil != err {
				return
			}
			for _, nfsClient = range volume.nfsClientList {
				_, err = exportsFile.WriteString(fmt.Sprintf(" %s(%s,%s,fsid=%d,%s,%s,%s)", nfsClient.ClientPattern, nfsClient.AccessMode, nfsSync, volume.FSID, nfsClient.RootSquash, nfsClient.Secure, nfsSubtreeCheck))
				if nil != err {
					return
				}
			}
			_, err = exportsFile.WriteString("\n")
			if nil != err {
				return
			}
		}
	}

	err = exportsFile.Close()
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
		newSMBUserNameList = make([]string, 0)
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

func computeVolumeSetChange(oldConfMap conf.ConfMap, newConfMap conf.ConfMap) (toDeleteVolumeMap volumeMap, toCreateVolumeMap volumeMap, err error) {
	var (
		newLocalVolumeMap volumeMap
		newVolume         *Volume
		ok                bool
		oldLocalVolumeMap volumeMap
		oldVolume         *Volume
		volumeName        string
	)

	_, _, oldLocalVolumeMap, _, _, err = fetchVolumeInfo(oldConfMap)
	if nil != err {
		err = fmt.Errorf("In oldConfMap: %v", err)
		return
	}
	_, _, newLocalVolumeMap, _, _, err = fetchVolumeInfo(newConfMap)
	if nil != err {
		err = fmt.Errorf("In newConfMap: %v", err)
	}

	toDeleteVolumeMap = make(volumeMap)

	for volumeName, oldVolume = range oldLocalVolumeMap {
		newVolume, ok = newLocalVolumeMap[volumeName]
		if !ok || (oldVolume.volumeGroup.volumeGroupName != newVolume.volumeGroup.volumeGroupName) {
			toDeleteVolumeMap[volumeName] = oldVolume
		}
	}

	toCreateVolumeMap = make(volumeMap)

	for volumeName, newVolume = range newLocalVolumeMap {
		oldVolume, ok = oldLocalVolumeMap[volumeName]
		if !ok || (oldVolume.volumeGroup.volumeGroupName != newVolume.volumeGroup.volumeGroupName) {
			toCreateVolumeMap[volumeName] = newVolume
		}
	}

	return
}

func fetchVolumeInfo(confMap conf.ConfMap) (whoAmI string, localVolumeGroupMap volumeGroupMap, localVolumeMap volumeMap, globalVolumeGroupMap volumeGroupMap, globalVolumeMap volumeMap, err error) {
	var (
		accountNameSet                stringSet
		fsidSet                       uint64Set
		fuseMountPointNameSet         stringSet
		fuseMountPointNameSlice       []string
		nfsClient                     *NFSClient
		nfsClientSection              string
		nfsExportClientMapList        []string
		nfsExportClientMapListElement string
		nfsExportClientMapSet         stringSet
		ok                            bool
		primaryPeerSlice              []string
		smbShareNameSet               stringSet
		smbShareNameSlice             []string
		virtualIPAddrSet              stringSet
		virtualIPAddrSlice            []string
		volume                        *Volume
		volumeGroup                   *VolumeGroup
		volumeGroupList               []string
		volumeGroupName               string
		volumeGroupSection            string
		volumeList                    []string
		volumeName                    string
		volumeNameSet                 stringSet
		volumeSection                 string
	)

	accountNameSet = make(stringSet)
	fsidSet = make(uint64Set)
	fuseMountPointNameSet = make(stringSet)
	smbShareNameSet = make(stringSet)
	virtualIPAddrSet = make(stringSet)
	volumeNameSet = make(stringSet)

	globalVolumeGroupMap = make(volumeGroupMap)

	volumeGroupList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != err {
		return
	}

	for _, volumeGroupName = range volumeGroupList {
		_, ok = globalVolumeGroupMap[volumeGroupName]
		if ok {
			err = fmt.Errorf("Found duplicate volumeGroupName (\"%s\") in [FSGlobals]VolumeGroupList", volumeGroupName)
			return
		}

		volumeGroup = &VolumeGroup{volumeGroupName: volumeGroupName, volumeMap: make(volumeMap)}

		volumeGroupSection = "VolumeGroup:" + volumeGroupName

		volumeList, err = confMap.FetchOptionValueStringSlice(volumeGroupSection, "VolumeList")
		if nil != err {
			return
		}

		virtualIPAddrSlice, err = confMap.FetchOptionValueStringSlice(volumeGroupSection, "VirtualIPAddr")
		if (nil != err) || (0 == len(virtualIPAddrSlice)) {
			volumeGroup.VirtualIPAddr = ""
		} else if 1 == len(virtualIPAddrSlice) {
			volumeGroup.VirtualIPAddr = virtualIPAddrSlice[0]
			_, ok = virtualIPAddrSet[volumeGroup.VirtualIPAddr]
			if ok {
				err = fmt.Errorf("Found duplicate [%s]VirtualIPAddr (\"%s\")", volumeGroupSection, volumeGroup.VirtualIPAddr)
				return
			}
			virtualIPAddrSet[volumeGroup.VirtualIPAddr] = struct{}{}
		} else {
			err = fmt.Errorf("Found multiple values for [%s]VirtualIPAddr", volumeGroupSection)
			return
		}

		primaryPeerSlice, err = confMap.FetchOptionValueStringSlice(volumeGroupSection, "PrimaryPeer")
		if (nil != err) || (0 == len(primaryPeerSlice)) {
			volumeGroup.PrimaryPeer = ""
		} else if 1 == len(primaryPeerSlice) {
			volumeGroup.PrimaryPeer = primaryPeerSlice[0]
		} else {
			err = fmt.Errorf("Found multiple values for [%s]PrimaryPeer", volumeGroupSection)
		}

		for _, volumeName = range volumeList {
			_, ok = volumeNameSet[volumeName]
			if ok {
				err = fmt.Errorf("Found duplicate volumeName (\"%s\") in [%s]VolumeList", volumeName, volumeGroupSection)
				return
			}

			volume = &Volume{volumeName: volumeName, volumeGroup: volumeGroup}

			volumeSection = "Volume:" + volumeName

			volume.FSID, err = confMap.FetchOptionValueUint64(volumeSection, "FSID")
			if nil != err {
				return
			}
			_, ok = fsidSet[volume.FSID]
			if ok {
				err = fmt.Errorf("Found duplicate [%s]FSID (%d)", volumeSection, volume.FSID)
				return
			}
			fsidSet[volume.FSID] = struct{}{}

			fuseMountPointNameSlice, err = confMap.FetchOptionValueStringSlice(volumeSection, "FUSEMountPointName")
			if (nil != err) || (0 == len(fuseMountPointNameSlice)) {
				volume.FUSEMountPointName = ""
			} else if 1 == len(fuseMountPointNameSlice) {
				volume.FUSEMountPointName = fuseMountPointNameSlice[0]
				_, ok = fuseMountPointNameSet[volume.FUSEMountPointName]
				if ok {
					err = fmt.Errorf("Found duplicate [%s]FUSEMountPointName (\"%s\")", volumeSection, volume.FUSEMountPointName)
					return
				}
				fuseMountPointNameSet[volume.FUSEMountPointName] = struct{}{}
			} else {
				err = fmt.Errorf("Found multiple values for [%s]FUSEMountPointName", volumeSection)
				return
			}

			nfsExportClientMapList, err = confMap.FetchOptionValueStringSlice(volumeSection, "NFSExportClientMapList")
			if nil == err {
				if (0 < len(nfsExportClientMapList)) && ("" == volume.FUSEMountPointName) {
					err = fmt.Errorf("Found empty [%s]FUSEMountPointName but [%s]NFSExportClientMapList is non-empty", volumeSection, volumeSection)
					return
				}

				volume.nfsClientList = make(NFSClientList, 0, len(nfsExportClientMapList))
				volume.nfsClientMap = make(NFSClientMap)

				nfsExportClientMapSet = make(stringSet)

				for _, nfsExportClientMapListElement = range nfsExportClientMapList {
					_, ok = nfsExportClientMapSet[nfsExportClientMapListElement]
					if ok {
						err = fmt.Errorf("Found duplicate nfsExportClientMapListElement (\"%s\") in [%s]NFSExportClientMapList", nfsExportClientMapListElement, volumeSection)
						return
					}

					nfsClient = &NFSClient{clientName: nfsExportClientMapListElement}

					nfsClientSection = "NFSClientMap:" + nfsExportClientMapListElement

					nfsClient.ClientPattern, err = confMap.FetchOptionValueString(nfsClientSection, "ClientPattern")
					if nil != err {
						return
					}
					nfsClient.AccessMode, err = confMap.FetchOptionValueString(nfsClientSection, "AccessMode")
					if nil != err {
						return
					}
					nfsClient.RootSquash, err = confMap.FetchOptionValueString(nfsClientSection, "RootSquash")
					if nil != err {
						return
					}
					nfsClient.Secure, err = confMap.FetchOptionValueString(nfsClientSection, "Secure")
					if nil != err {
						return
					}

					volume.nfsClientList = append(volume.nfsClientList, nfsClient)
					volume.nfsClientMap[nfsExportClientMapListElement] = nfsClient

					nfsExportClientMapSet[nfsExportClientMapListElement] = struct{}{}
				}
			} else {
				volume.nfsClientList = make(NFSClientList, 0)
				volume.nfsClientMap = make(NFSClientMap)
			}

			smbShareNameSlice, err = confMap.FetchOptionValueStringSlice(volumeSection, "SMBShareName")
			if (nil != err) || (0 == len(smbShareNameSlice)) {
				volume.SMBShareName = ""
			} else if 1 == len(smbShareNameSlice) {
				volume.SMBShareName = smbShareNameSlice[0]
				_, ok = smbShareNameSet[volume.SMBShareName]
				if ok {
					err = fmt.Errorf("Found duplicate [%s]SMBShareName (\"%s\")", volumeSection, volume.SMBShareName)
					return
				}
				smbShareNameSet[volume.SMBShareName] = struct{}{}
			} else {
				err = fmt.Errorf("Found multiple values for [%s]SMBShareName", volumeSection)
				return
			}

			volume.AccountName, err = confMap.FetchOptionValueString(volumeSection, "AccountName")
			if nil != err {
				return
			}
			_, ok = accountNameSet[volume.AccountName]
			if ok {
				err = fmt.Errorf("Found duplicate AccountName (\"%s\") in [%s]FSID", volume.AccountName, volumeSection)
				return
			}
			accountNameSet[volume.AccountName] = struct{}{}

			volumeGroup.volumeMap[volumeName] = volume
		}

		globalVolumeGroupMap[volumeGroupName] = volumeGroup
	}

	localVolumeGroupMap = make(volumeGroupMap)
	localVolumeMap = make(volumeMap)
	globalVolumeMap = make(volumeMap)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	for volumeGroupName, volumeGroup = range globalVolumeGroupMap {
		if whoAmI == volumeGroup.PrimaryPeer {
			localVolumeGroupMap[volumeGroupName] = volumeGroup
			for volumeName, volume = range volumeGroup.volumeMap {
				localVolumeMap[volumeName] = volume
			}
		}
		for volumeName, volume = range volumeGroup.volumeMap {
			globalVolumeMap[volumeName] = volume
		}
	}

	return
}
