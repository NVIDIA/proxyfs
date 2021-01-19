// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package confgen provides a mechanism by which a supplied configuration is converted into a
// set of configuration files to be supplied to ProxyFS (proxyfsd), Samba (smbd et. al.), and
// NFS (nfsd). In (at least) the case of Samba, multiple configuration files will be produced
// with a per-IP path.
//
// Only the configuration files for the Cluster.WhoAmI-identified Peer will be produced though,
// in the case of the ProxyFS configuration file, all VolumeGroups will be included given that
// ProxyFS needs to know the mapping of VolumeGroup to Peer. This allows one ProxyFS instance
// to redirect a JSONRPC client to the appropriate ProxyFS instance servicing the refernected
// Volume. The Cluster.WhoAmI can either already be present in the `confFilePath` or be provided
// as an element of the `confOverrides` argument in each of the APIs below.
//
// As would be typical in a deployment where the placement of VolumeGroups on Peers in dynamic,
// the `confFilePath` would provide `VirtualIPAddr` values as opposed to fixed `PrimaryPeer`
// assignments. It is the callers responsibility to compute the assignments of VolumeGroups
// to Peers and either modify the provided `confFilePath` to include PrimaryPeer values or to
// supply those via the `confOverrides` (e.g. "VolumeGroup:CommonVolumeGroup.PrimaryPeer=Peer0").
package confgen

// EnvMap allows the caller to provide environment-specific paths for various programs invoked
// by this API as well as customize the scripts produced by this API.
type EnvMap map[string]string

const (
	// LinuxUserCommentDefault is the default value of environment variable LinuxUserCommentEnv.
	LinuxUserCommentDefault = "user-created-for-samba"
	// LinuxUserCommentEnv specifies a comment to be applied to each Linux user created
	// to be referenced by the SMB user system as provided by SAMBA(7).
	LinuxUserCommentEnv = "LINUX_USER_COMMENT"

	// PathToNetDefault is the default value of environment variable PathToNetEnv.
	PathToNetDefault = "/usr/bin/net"
	// PathToNetEnv is the name of the environment variable used to specify the path to the
	// NET(8) tool used to administer a SAMBA(7) installation.
	PathToNetEnv = "PATH_TO_NET"

	// PathToKRB5ConfDirDefault is the default value of environment variable PathToKRB5ConfDirEnv.
	PathToKRB5ConfDirDefault = "/etc/krb5.conf.d"
	// PathToKRB5ConfDirEnv is the name of the environment variable used to specify the path to
	// the KRB5 configuration directory where realm declarations are placed. This method is used
	// to avoid having to merge all realm declarations into a common KRB5 configuration file.
	PathToKRB5ConfDirEnv = "PATH_TO_KRB5_CONF_DIR"

	// PathToPDBEditDefault is the default value of environment variable PathToPDBEditEnv.
	PathToPDBEditDefault = "/usr/bin/pdbedit"
	// PathToPDBEditEnv is the name of the environment variable used to specify the path to
	// the PDBEDIT(8) tool used to administer a SAMBA(7) installation.
	PathToPDBEditEnv = "PATH_TO_PDBEDIT"

	// PathToPerVirtualIPAddrDirDefault is the default value of environment variable PathToPerVirtualIPAddrDirEnv.
	PathToPerVirtualIPAddrDirDefault = "/var/lib/vips"
	// PathToPerVirtualIPAddrDirEnv is the name of the environment variable used to specify the
	// path to a directory containing a set of subdirectories each named for the corresponding
	// VirtualIPAddr to which they pertain. In particular, each such subdirectory will contain
	// a "samba" subdirectory containing all files referenced by that VirtualIPAddr's instance
	// of SAMBA(7).
	PathToPerVirtualIPAddrDirEnv = "PATH_TO_PER_VIRTUAL_IP_ADDR_DIR"

	// PathToSMBDDefault is the default value of environment variable PathToSMBDEnv.
	PathToSMBDDefault = "/usr/bin/smbd"
	// PathToSMBDEnv is the name of the environment variable used to specify the path to the
	// SMBD(8) program in a SAMBA(7) installation used to provide SMB file serving to clients.
	PathToSMBDEnv = "PATH_TO_SMBD"

	// PathToSMBPasswdDefault is the default value of environment variable PathToSMBPasswdEnv.
	PathToSMBPasswdDefault = "/usr/bin/smbpasswd"
	// PathToSMBPasswdEnv is the name of the environment variable used to specify the path to
	// the SMBPASSWD(8) tool used to add an SMB user or update an SMB user's password in a
	// SAMBA(7) installation.
	PathToSMBPasswdEnv = "PATH_TO_SMBPASSWD"
)

// ComputeInitial takes a supplied ConfFile, overlays ConfOverrides, and computes an initial
// set of configuration files that are used by a per-IPAddr set of Samba instances as well as
// NFSd & ProxyFS.
func ComputeInitial(envMap EnvMap, confFilePath string, confOverrides []string, initialDirPath string) (err error) {
	err = computeInitial(envMap, confFilePath, confOverrides, initialDirPath)
	return
}

// ComputePhases takes a supplied initial set of conf files (such as produced by ComputeInitial()
// above) along with a new ConfFile and new ConfOverrides and produces two sets of conf files
// used in a 2-phase migration from the initial config to a new config. Presumably, the 2nd phase
// produced will be used as the initial config in the next call to ComputePhases().
func ComputePhases(envMap EnvMap, initialDirPath string, confFilePath string, confOverrides []string, phaseOneDirPath string, phaseTwoDirPath string) (err error) {
	err = computePhases(envMap, initialDirPath, confFilePath, confOverrides, phaseOneDirPath, phaseTwoDirPath)
	return
}
