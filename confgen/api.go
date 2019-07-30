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

// ComputeInitial takes a supplied ConfFile, overlays ConfOverrides, and computes an initial
// set of configuration files that are used by a per-IPAddr set of Samba instances as well as
// NFSd & ProxyFS.
func ComputeInitial(confFilePath string, confOverrides []string, initialDirPath string) (err error) {
	err = computeInitial(confFilePath, confOverrides, initialDirPath)
	return
}

// ComputePhases takes a supplied initial set of conf files (such as produced by ComputeInitial()
// above) along with a new ConfFile and new ConfOverrides and produces two sets of conf files
// used in a 2-phase migration from the initial config to a new config. Presumably, the 2nd phase
// produced will be used as the initial config in the next call to ComputePhases().
func ComputePhases(initialDirPath string, confFilePath string, confOverrides []string, phaseOneDirPath string, phaseTwoDirPath string) (err error) {
	err = computePhases(initialDirPath, confFilePath, confOverrides, phaseOneDirPath, phaseTwoDirPath)
	return
}
