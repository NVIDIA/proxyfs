package confgen

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/swiftstack/ProxyFS/conf"
)

type envSettingsStruct struct {
	linuxUserComment          string
	pathToNet                 string
	pathToKRB5ConfDir         string
	pathToPDBEdit             string
	pathToPerVirtualIPAddrDir string
	pathToSMBD                string
	pathToSMBPasswd           string
}

const (
	useraddPath = "/usr/sbin/useradd"
	userdelPath = "/usr/sbin/userdel"
)

const (
	exportsFileName       = "exports"              // Used to identify NFS exports
	fuseSetupFileName     = "fuse_setup.bash"      // Used to create FUSE export directories
	proxyFSConfFileName   = "proxyfs.conf"         // Used to pass to mkproxyfs & proxyfsd
	realmsSourceDirName   = "realms"               // Used to hold files destined for realmsDestinationDirName
	smbConfFileName       = "smb.conf"             // Used for passing to various SAMBA(7) components
	smbCommonConfFileName = "smb_common.conf"      // Used for generating a smbPassdbFileName cloned for each vips/{VirtualIPAddr}
	smbPassdbFileName     = "passdb.tdb"           // Used in vips/{VirtualIPAddr} to hold "local" SMB Passwords
	smbUsersSetupFileName = "smb_users_setup.bash" // Used to {create|update|destroy} SMB & Linux users
	vipsDirName           = "vips"                 // Used to hold a set of VirtualIPAddr-named subdirectories
	//                                                  where each holds files specific to that VirtualIPAddr
)

const (
	confDirPerm     = os.FileMode(0777) // Let umask "restrict" this as desired
	confFilePerm    = os.FileMode(0666) // Let umask "restrict" this as desired
	exportsFilePerm = os.FileMode(0666) // Let umask "restrict" this as desired
	fuseDirPerm     = os.FileMode(0000) // Fail all non-root access to missing FUSE exports
	scriptPerm      = os.FileMode(0777) // Let umask "restrict" this as desired
	smbConfPerm     = os.FileMode(0644) // Let umask "restrict" this as desired
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
	VolumeName         string        // Must be unique
	VolumeGroup        *VolumeGroup  //
	FSID               uint64        // Must be unique
	FUSEMountPointName string        // Must be unique unless == "" (no FUSE mount point...and cannot be NFS exported)
	nfsClientList      NFSClientList // Must be empty (no NFS Export) if FUSEMountPointName == ""
	nfsClientMap       NFSClientMap  // Must be empty (no NFS Export) if FUSEMountPointName == ""
	AccountName        string        // Must be unique
	SMB                SMBVolume
}

type volumeMap map[string]*Volume // Key=volume.volumeName

// SMBVolume contains per volume SMB settings
type SMBVolume struct {
	AuditLogging       bool
	Browseable         bool
	EncryptionRequired bool
	Path               string // TODO - Not implemented
	ShareName          string // Must be unique unless == "" (no SMB Share)
	StrictSync         bool
	ValidUsers         []string // TODO - Not implemented
}

// SMBVG contains per Volume Group SMB settings
type SMBVG struct {
	ADBackEnd           []string // TODO - not implemented
	ADEnabled           bool
	ADIDMapDefaultMin   int
	ADIDMapDefaultMax   int
	ADIDMapWorkgroupMin int
	ADIDMapWorkgroupMax int
	ADIDSchema          []string // TODO - not implemented
	AuditLogging        bool     // TODO - not implemented - needs testing
	BrowserAnnounce     string   // TODO - not implemented
	FastTCPPort         int
	MapToGuest          []string // TODO - not implemented
	ADRealm             []string
	RPCServerLSARPC     []string // TODO - not implemented
	Security            []string // TODO - not implemented
	ServerMinProtocol   string   // TODO - not implemented
	TCPPort             int
	WorkGroup           string
}

// VolumeGroup contains VolumeGroup conf settings
type VolumeGroup struct {
	PrimaryPeer     string    //
	SMB             SMBVG     // SMB specific settings of the VG
	VirtualHostName string    // Must be unique
	VirtualIPAddr   string    // Must be unique
	VolumeGroupName string    // Must be unique
	VolumeMap       volumeMap //
}

type volumeGroupMap map[string]*VolumeGroup // Key=VolumeGroup.volumeGroupName

func computeInitial(envMap EnvMap, confFilePath string, confOverrides []string, initialDirPath string) (err error) {
	var (
		envSettings                    *envSettingsStruct
		exportsFile                    *os.File
		fuseSetupFile                  *os.File
		initialConfMap                 conf.ConfMap
		localVolumeGroupMap            volumeGroupMap
		localVolumeMap                 volumeMap
		nfsClient                      *NFSClient
		smbUsersSetupFile              *os.File
		toCreateSMBUserMap             smbUserMap
		toCreateSMBUserName            string
		toCreateSMBUserPassword        string
		toCreateSMBUserPasswordEscaped string // == strings.ReplaceAll(toCreateSMBUserPassword, "\\", "\\\\")
		volume                         *Volume
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

	// Fetch pertinent data from Initial Config

	_, localVolumeGroupMap, localVolumeMap, _, _, err = fetchVolumeInfo(initialConfMap)
	if nil != err {
		return
	}

	_, toCreateSMBUserMap, _, _, err = computeSMBUserListChange(make(smbUserMap), initialConfMap)
	if nil != err {
		return
	}

	// Compute common exports file - NFSd will (unfortunately) serve ALL VirtualIPAddrs

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

	// Compute SMB Users script

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

		// TODO: Replace following line once Golang 1.12 (sporting new strings.ReplaceAll() func) is required with:
		//
		//   toCreateSMBUserPasswordEscaped = strings.ReplaceAll(toCreateSMBUserPassword, "\\", "\\\\\\")
		//
		toCreateSMBUserPasswordEscaped = strings.Replace(toCreateSMBUserPassword, "\\", "\\\\\\", -1)

		_, err = smbUsersSetupFile.WriteString(fmt.Sprintf("echo -e \"%s\\n%s\" | %s -c %s -a %s\n", toCreateSMBUserPasswordEscaped, toCreateSMBUserPasswordEscaped, envSettings.pathToSMBPasswd, smbConfFileName, toCreateSMBUserName))
		if nil != err {
			return
		}
	}

	err = smbUsersSetupFile.Close()
	if nil != err {
		return
	}

	// Compute per-VitualIPAddr (Samba)

	err = os.Mkdir(initialDirPath+"/"+vipsDirName, confDirPerm)
	if nil != err {
		return
	}

	// Create per VG smb.conf files
	err = createSMBConf(initialDirPath, localVolumeGroupMap)
	if nil != err {
		// TODO - logging
		return
	}

	// Compute FUSE MountPoint Directory script

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
		envSettings.linuxUserComment = LinuxUserCommentDefault
	}

	envSettings.pathToNet, inEnv = envMap[PathToNetEnv]
	if !inEnv {
		envSettings.pathToNet = PathToNetDefault
	}

	envSettings.pathToKRB5ConfDir, inEnv = envMap[PathToKRB5ConfDirEnv]
	if !inEnv {
		envSettings.pathToKRB5ConfDir = PathToKRB5ConfDirDefault
	}

	envSettings.pathToPDBEdit, inEnv = envMap[PathToPDBEditEnv]
	if !inEnv {
		envSettings.pathToPDBEdit = PathToPDBEditDefault
	}

	envSettings.pathToPerVirtualIPAddrDir, inEnv = envMap[PathToPerVirtualIPAddrDirEnv]
	if !inEnv {
		envSettings.pathToPerVirtualIPAddrDir = PathToPerVirtualIPAddrDirDefault
	}

	envSettings.pathToSMBD, inEnv = envMap[PathToSMBDEnv]
	if !inEnv {
		envSettings.pathToSMBD = PathToSMBDDefault
	}

	envSettings.pathToSMBPasswd, inEnv = envMap[PathToSMBPasswdEnv]
	if !inEnv {
		envSettings.pathToSMBPasswd = PathToSMBPasswdDefault
	}

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
		if !ok || (oldVolume.VolumeGroup.VolumeGroupName != newVolume.VolumeGroup.VolumeGroupName) {
			toDeleteVolumeMap[volumeName] = oldVolume
		}
	}

	toCreateVolumeMap = make(volumeMap)

	for volumeName, newVolume = range newLocalVolumeMap {
		oldVolume, ok = oldLocalVolumeMap[volumeName]
		if !ok || (oldVolume.VolumeGroup.VolumeGroupName != newVolume.VolumeGroup.VolumeGroupName) {
			toCreateVolumeMap[volumeName] = newVolume
		}
	}

	return
}

// populateVolumeSMB populates the Volume with SMB related info
func populateVolumeSMB(confMap conf.ConfMap, volumeSection string, volume *Volume) (err error) {
	var (
		shareNameSet   stringSet
		shareNameSlice []string
		ok             bool
	)

	shareNameSet = make(stringSet)

	volume.SMB.AuditLogging, err = confMap.FetchOptionValueBool(volumeSection, "SMBAuditLogging")
	if nil != err {
		return
	}
	volume.SMB.Browseable, err = confMap.FetchOptionValueBool(volumeSection, "SMBBrowseable")
	if nil != err {
		return
	}

	volume.SMB.EncryptionRequired, err = confMap.FetchOptionValueBool(volumeSection, "SMBEncryptionRequired")
	if nil != err {
		return
	}

	shareNameSlice, err = confMap.FetchOptionValueStringSlice(volumeSection, "SMBShareName")
	if (nil != err) || (0 == len(shareNameSlice)) {
		volume.SMB.ShareName = ""
	} else if 1 == len(shareNameSlice) {
		volume.SMB.ShareName = shareNameSlice[0]
		_, ok = shareNameSet[volume.SMB.ShareName]
		if ok {
			err = fmt.Errorf("Found duplicate [%s]SMBShareName (\"%s\")", volumeSection, volume.SMB.ShareName)
			return
		}
		shareNameSet[volume.SMB.ShareName] = struct{}{}
	} else {
		err = fmt.Errorf("Found multiple values for [%s]SMBShareName", volumeSection)
		return
	}

	volume.SMB.StrictSync, err = confMap.FetchOptionValueBool(volumeSection, "SMBStrictSync")
	if nil != err {
		return
	}

	// TODO - figure out the ValidUserList, SMBMapToGuest, SMBNetBiosName, SMBUserList
	// Make consistent with controller, etc
	/*
		                "SMBValidUserList": [
		                        "InN3aWZ0IiwgImJvYiIsICJlZCIsICJibGFrZSI="
						],
	*/

	return
}

// populateVolumeGroupSMB populates the VolumeGroup with SMB related
// info
func populateVolumeGroupSMB(confMap conf.ConfMap, volumeGroupSection string, tcpPort int, fastTCPPort int, volumeGroup *VolumeGroup,
	workGroupSet stringSet) (err error) {
	var (
		idUint32       uint32
		ok             bool
		workGroupSlice []string
	)
	volumeGroup.SMB.TCPPort = tcpPort
	volumeGroup.SMB.FastTCPPort = fastTCPPort

	volumeGroup.SMB.ADEnabled, err = confMap.FetchOptionValueBool(volumeGroupSection, "SMBActiveDirectoryEnabled")
	if nil != err {
		return
	}
	idUint32, err = confMap.FetchOptionValueUint32(volumeGroupSection, "SMBActiveDirectoryIDMapDefaultMin")
	volumeGroup.SMB.ADIDMapDefaultMin = int(idUint32)
	if nil != err {
		return
	}
	idUint32, err = confMap.FetchOptionValueUint32(volumeGroupSection, "SMBActiveDirectoryIDMapDefaultMax")
	volumeGroup.SMB.ADIDMapDefaultMax = int(idUint32)
	if nil != err {
		return
	}

	idUint32, err = confMap.FetchOptionValueUint32(volumeGroupSection, "SMBActiveDirectoryIDMapWorkgroupMin")
	volumeGroup.SMB.ADIDMapWorkgroupMin = int(idUint32)
	if nil != err {
		return
	}
	idUint32, err = confMap.FetchOptionValueUint32(volumeGroupSection, "SMBActiveDirectoryIDMapWorkgroupMax")
	volumeGroup.SMB.ADIDMapWorkgroupMax = int(idUint32)
	if nil != err {
		return
	}

	workGroupSlice, err = confMap.FetchOptionValueStringSlice(volumeGroupSection, "SMBWorkgroup")
	if (nil != err) || (0 == len(workGroupSlice)) {
		volumeGroup.SMB.WorkGroup = ""
	} else if 1 == len(workGroupSlice) {
		volumeGroup.SMB.WorkGroup = workGroupSlice[0]

		_, ok = workGroupSet[volumeGroup.SMB.WorkGroup]
		if ok {
			err = fmt.Errorf("Found duplicate [%s]WorkGroup (\"%s\")", volumeGroupSection, volumeGroup.SMB.WorkGroup)
			return
		}

		workGroupSet[volumeGroup.SMB.WorkGroup] = struct{}{}
	} else {
		err = fmt.Errorf("Found multiple values for [%s]VirtualIPAddr", volumeGroupSection)
		return
	}

	volumeGroup.SMB.ADRealm, err = confMap.FetchOptionValueStringSlice(volumeGroupSection, "SMBActiveDirectoryRealm")
	if nil != err {
		return
	}

	return
}

// populateVolumeGroup is a helper function to populate the volume group information
func populateVolumeGroup(confMap conf.ConfMap, globalVolumeGroupMap volumeGroupMap,
	volumeGroupList []string) (err error) {
	var (
		accountNameSet                stringSet
		fastTCPPort                   int
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
		tcpPort                       int
		virtualHostNameSet            stringSet
		virtualHostNameSlice          []string
		virtualIPAddrSet              stringSet
		virtualIPAddrSlice            []string
		volume                        *Volume
		volumeGroup                   *VolumeGroup
		volumeGroupName               string
		volumeGroupSection            string
		volumeList                    []string
		volumeName                    string
		volumeNameSet                 stringSet
		volumeSection                 string
		workGroupSet                  stringSet
	)

	// Fetch tcpPort and fastTCPPort number from config file.
	//
	// We store this in each SMBVG since the per volume group template used for generating
	// smb.conf files needs it.
	portString, confErr := confMap.FetchOptionValueString("JSONRPCServer", "TCPPort")
	if nil != confErr {
		err = fmt.Errorf("failed to get JSONRPCServer.TCPPort from config file")
		return
	}
	tcpPort, err = strconv.Atoi(portString)
	if nil != err {
		return
	}
	fastPortString, confErr := confMap.FetchOptionValueString("JSONRPCServer", "FastTCPPort")
	if nil != confErr {
		err = fmt.Errorf("failed to get JSONRPCServer.TCPFastPort from config file")
		return
	}
	fastTCPPort, err = strconv.Atoi(fastPortString)
	if nil != err {
		return
	}

	accountNameSet = make(stringSet)
	fsidSet = make(uint64Set)
	fuseMountPointNameSet = make(stringSet)
	virtualHostNameSet = make(stringSet)
	virtualIPAddrSet = make(stringSet)
	volumeNameSet = make(stringSet)
	workGroupSet = make(stringSet)

	for _, volumeGroupName = range volumeGroupList {
		var (
			haveVolumeWithAuditLogging bool
		)
		_, ok = globalVolumeGroupMap[volumeGroupName]
		if ok {
			err = fmt.Errorf("Found duplicate volumeGroupName (\"%s\") in [FSGlobals]VolumeGroupList", volumeGroupName)
			return
		}

		volumeGroup = &VolumeGroup{VolumeGroupName: volumeGroupName, VolumeMap: make(volumeMap)}

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

		virtualHostNameSlice, err = confMap.FetchOptionValueStringSlice(volumeGroupSection, "VirtualHostname")
		if (nil != err) || (0 == len(virtualHostNameSlice)) {
			volumeGroup.VirtualHostName = ""
		} else if 1 == len(virtualHostNameSlice) {
			volumeGroup.VirtualHostName = virtualHostNameSlice[0]

			_, ok = virtualHostNameSet[volumeGroup.VirtualHostName]
			if ok {
				err = fmt.Errorf("Found duplicate [%s]VirtualHostName (\"%s\")", volumeGroupSection, volumeGroup.VirtualHostName)
				return
			}

			virtualHostNameSet[volumeGroup.VirtualHostName] = struct{}{}
		} else {
			err = fmt.Errorf("Found multiple values for [%s]VirtualHostName", volumeGroupSection)
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

		// Fill in VG SMB information
		err = populateVolumeGroupSMB(confMap, volumeGroupSection, tcpPort, fastTCPPort, volumeGroup,
			workGroupSet)
		if nil != err {
			return
		}

		// Grab the volumes in the VG
		for _, volumeName = range volumeList {
			_, ok = volumeNameSet[volumeName]
			if ok {
				err = fmt.Errorf("Found duplicate volumeName (\"%s\") in [%s]VolumeList", volumeName, volumeGroupSection)
				return
			}

			volume = &Volume{VolumeName: volumeName, VolumeGroup: volumeGroup}

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

			err = populateVolumeSMB(confMap, volumeSection, volume)
			if nil != err {
				return
			}

			// If any volume has audit logging then the volume group
			// has audit logging.
			if volume.SMB.AuditLogging {
				haveVolumeWithAuditLogging = volume.SMB.AuditLogging
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

			volumeGroup.VolumeMap[volumeName] = volume
		}
		volumeGroup.SMB.AuditLogging = haveVolumeWithAuditLogging

		globalVolumeGroupMap[volumeGroupName] = volumeGroup
	}

	return
}

func fetchVolumeInfo(confMap conf.ConfMap) (whoAmI string, localVolumeGroupMap volumeGroupMap,
	localVolumeMap volumeMap, globalVolumeGroupMap volumeGroupMap, globalVolumeMap volumeMap,
	err error) {
	var (
		volume          *Volume
		volumeGroup     *VolumeGroup
		volumeGroupList []string
		volumeGroupName string
		volumeName      string
	)

	globalVolumeGroupMap = make(volumeGroupMap)

	volumeGroupList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != err {
		return
	}

	err = populateVolumeGroup(confMap, globalVolumeGroupMap, volumeGroupList)
	if nil != err {
		return
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
			for volumeName, volume = range volumeGroup.VolumeMap {
				localVolumeMap[volumeName] = volume
			}
		}
		for volumeName, volume = range volumeGroup.VolumeMap {
			globalVolumeMap[volumeName] = volume
		}
	}

	return
}
