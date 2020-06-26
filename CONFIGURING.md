# Configuring ProxyFS

Two programs are produced by running `make` in the top-level directory of this repository: `mkproxyfs` and `proxyfsd`. This file endeavors to describe how to specify a configuration that is consumed by these two programs. In addition, two libraries are also produced that work in concert with Samba to provide SMB support: `libproxyfs.so` and `proxyfs.so`. The two Samba libraries, like the rest of Samba, are typically configured with a `smb.conf` file. This Samba configuration specifies what SMB Shares to expose and how to route requests to ProxyFS Volumes. This file will defer explanation of the contents of `smb.conf` to other documentation but it is best practice that all of the sections needed to be added to `smb.conf` would be derivable from the configuration provided to `mkproxyfs` and `proxyfsd`. In addition, NFS is served via the normal `nfsd` service via local FUSE exports of ProxyFS Volumes. As such, their exportation is provided to `nfsd` via the normal `/etc/exports` mechanism with info that is, again by best practice, also derivable from the configuration provided to `mkproxyfs` and `proxyfsd`.

## ProxyFS Programs

* mkproxyfs

    Used to format a ProxyFS Volume

    One of the following command line options must be provided:

    * -N indicates that the underlying Swift Account must be empty
    * -I indicates that the underlying Swift Account should only be formatted if necessary
    * -F indicates that the underlying Swift Account will first be emptied before formatting

    Invocation: `mkproxyfs -N|-I|-F VolumeNameToFormat ConfFile [ConfFileOverrides]*`

* proxyfsd

    Mounts all ProxyFS Volumes to be served by this instance presented via two mechanisms:

    * FUSE (consumed by `nfsd`)
    * JSON RPC (consumed by Samba, Swift Proxy, and PFSAgent)

    Invocation: `proxyfsd ConfFile [ConfFileOverrides]*`

## ConfFile and ConfFileOverrides Format

The file supplied to both `mkproxyfs` and `proxyfsd` (they should be the same) are in .INI format. As an example:
```
# This is a comment
[SectionName] # This is a comment as well
Key0:
Key1: Value1
Key2: Value2,Value3
.include included.conf
```
There is not hard limit to the number of values specified for a given key.

Note the `.include` directive that enables assembling a set of configuration sections, keys, and values from multiple files even when only a single ConfFile is specified on the command line to either `mkproxyfs` or `proxyfsd`.

It is permissible to repeat sections. All keys supplied in the subsequent mention of a particular section are logically appended (i.e. it's as if all keys specified in both mentions of a section were together). If a key is repeated, it replaces the previous definition of that key. Zero or more values for a given key are allowed.

While the `.include` mechanism is convenient for constructing configurations from a combination of files, there is another mechanism to amend a configuration. Note the `[ConfFileOverrides]*` optional portion of either the `mkproxyfs` or `proxyfsd` command line invocations. Each space-separated `ConfFileOverride` is of one of the forms:
```
SectionName.Key0=
SectionName.Key1=Value1
SectionName.Key2: Value2,Value3
```
As before, there is no hard limit to the number of values specified for a given key. Also, repeated keys merely replace the prior set of values for that key.

## Key Requirements

The following table will list what keys are required or optional. While it is the convention of ProxyFS that keys are not defaulted when not provided, the need to support graceful upgrades necessitates some optional keys to be defaulted until such time as they are considered required.

There are three points in time when the configuration is consumed:
* At the time of `mkproyxfs` invocation to format a volume
* At the time when `proxyfsd` is started
* At the time when `proxyfsd` receives a SIGHUP

Given that a key might want to be modified between each of the above points in time, it is important to specify which ones are modifiable and under which circumstance such a modification will be honored.

The SIGHUP mechanism is provided to ask `proxyfsd` to reconsume the ConfFile without requiring a restart. Note that the ConfFileOverrides (if any) will still override any key settings when the ConfFile is re-read.

For each of the keys supported, the following table will list whether or not its required, any default if it is not, and when changes to it will be allowed, ignored, and/or honored:

| Section<br>Name                           | Key<br>Name                              | <br>Required | <br>Default        | Changeable<br>On Restart | Changeable<br>On SIGHUP      |
| ----------------------------------------- | ---------------------------------------- | ------------ | ------------------ | ------------------------ | ---------------------------- |
| Cluster                                   | WhoAmI                                   | Yes          |                    | Yes                      | No                           |
|                                           | Peers                                    | Yes          |                    | Yes                      | Yes but WhoAmI should remain |
|                                           | PrivateClusterUDPPort                    | No           | 8123               | Yes                      | No                           |
|                                           | UDPPacketSendSize                        | No           | 1400               | Yes                      | No                           |
|                                           | UDPPacketRecvSize                        | No           | 1500               | Yes                      | No                           |
|                                           | UDPPacketCapPerMessage                   | No           | 255                | Yes                      | No                           |
|                                           | HeartBeatDuration                        | No           | 1s                 | Yes                      | No                           |
|                                           | HeartBeatMissLimit                       | No           | 3                  | Yes                      | No                           |
|                                           | MessageQueueDepthPerPeer                 | No           | 4                  | Yes                      | No                           |
|                                           | MaxRequestDuration                       | No           | 1s                 | Yes                      | No                           |
|                                           | LivenessCheckRedundancy                  | No           | 2                  | Yes                      | No                           |
|                                           | LogLevel                                 | No           | <i>None</i>        | Yes                      | No                           |
| Peer:<i>PeerName</i>                      | PublicIPAddr                             | Yes          |                    | Yes                      | Yes but WhoAmI should remain |
|                                           | PrivateIPAddr                            | Yes          |                    | Yes                      | Yes but WhoAmI should remain |
|                                           | ReadCacheQuotaFraction                   | Yes          |                    | Yes                      | Yes                          |
| SwiftClient                               | NoAuthIPAddr                             | No           | 127.0.0.1          | Yes                      | No                           |
|                                           | NoAuthTCPPort                            | Yes          |                    | Yes                      | No                           |
|                                           | RetryLimit                               | Yes          |                    | Yes                      | Yes                          |
|                                           | RetryLimitObject                         | Yes          |                    | Yes                      | Yes                          |
|                                           | RetryDelay                               | Yes          |                    | Yes                      | Yes                          |
|                                           | RetryDelayObject                         | Yes          |                    | Yes                      | Yes                          |
|                                           | RetryExpBackoff                          | Yes          |                    | Yes                      | Yes                          |
|                                           | RetryExpBackoffObject                    | Yes          |                    | Yes                      | Yes                          |
|                                           | ChunkedConnectionPoolSize                | Yes          |                    | Yes                      | No                           |
|                                           | NonChunkedConnectionPoolSize             | Yes          |                    | Yes                      | No                           |
|                                           | ChecksumChunkedPutChunks                 | No           | false              | Yes                      | Yes                          |
|                                           | SwiftReconNoWriteThreshold               | No           | 80                 | Yes                      | Yes                          |
|                                           | SwiftReconNoWriteErrno                   | No           | ENOSPC             | Yes                      | Yes                          |
|                                           | SwiftReconReadOnlyThreshold              | No           | 90                 | Yes                      | Yes                          |
|                                           | SwiftReconReadOnlyErrno                  | No           | EROFS              | Yes                      | Yes                          |
|                                           | SwiftConfDir                             | No           | /etc/swift         | Yes                      | Yes                          |
|                                           | SwiftReconChecksPerConfCheck             | No           | 10                 | Yes                      | Yes                          |
| FSGlobals                                 | VolumeGroupList                          | Yes          |                    | Yes                      | Yes                          |
|                                           | CheckpointHeaderConsensusAttempts        | No           | 5                  | Yes                      | No                           |
|                                           | MountRetryLimit                          | No           | 6                  | Yes                      | No                           |
|                                           | MountRetryDelay                          | No           | 1s                 | Yes                      | No                           |
|                                           | MountRetryExpBackoff                     | No           | 2                  | Yes                      | No                           |
|                                           | LogCheckpointHeaderPosts                 | No           | true               | Yes                      | No                           |
|                                           | TryLockBackoffMin                        | No           | 10ms               | Yes                      | No                           |
|                                           | TryLockBackoffMax                        | No           | 50ms               | Yes                      | No                           |
|                                           | TryLockSerializationThreshhold           | No           | 5                  | Yes                      | No                           |
|                                           | SymlinkMax                               | No           | 32                 | Yes                      | No                           |
|                                           | InodeRecCacheEvictLowLimit               | Yes          |                    | Yes                      | No                           |
|                                           | InodeRecCacheEvictHighLimit              | Yes          |                    | Yes                      | No                           |
|                                           | LogSegmentRecCacheEvictLowLimit          | Yes          |                    | Yes                      | No                           |
|                                           | LogSegmentRecCacheEvictHighLimit         | Yes          |                    | Yes                      | No                           |
|                                           | BPlusTreeObjectCacheEvictLowLimit        | Yes          |                    | Yes                      | No                           |
|                                           | BPlusTreeObjectCacheEvictHighLimit       | Yes          |                    | Yes                      | No                           |
|                                           | CreatedDeletedObjectsCacheEvictLowLimit  | No           | as for LogSegments | Yes                      | No                           |
|                                           | CreatedDeletedObjectsCacheEvictHighLimit | No           | as for LogSegments | Yes                      | No                           |
|                                           | DirEntryCacheEvictLowLimit               | Yes          |                    | Yes                      | No                           |
|                                           | DirEntryCacheEvictHighLimit              | Yes          |                    | Yes                      | No                           |
|                                           | FileExtentMapEvictLowLimit               | Yes          |                    | Yes                      | No                           |
|                                           | FileExtentMapEvictHighLimit              | Yes          |                    | Yes                      | No                           |
|                                           | EtcdEnabled                              | No           | false              | Yes but don't re-enable  | No                           |
|                                           | EtcdEndpoints                            | If enabled   |                    | Yes                      | No                           |
|                                           | EtcdAutoSyncInterval                     | If enabled   |                    | Yes                      | No                           |
|                                           | EtcdDialTimeout                          | If enabled   |                    | Yes                      | No                           |
|                                           | EtcdOpTimeout                            | If enabled   |                    | Yes                      | No                           |
|                                           | MetadataRecycleBin                       | No           | false              | Yes                      | No                           |
|                                           | SMBUserList                              | No           | <i>None</i>        | Yes                      | Yes                          |
| VolumeGroup:<i>VolumeGroupName</i>        | VolumeList                               | Yes          |                    | Yes                      | Yes                          |
|                                           | VirtualIPAddr                            | Yes          |                    | Yes                      | Yes                          |
|                                           | PrimaryPeer                              | Yes          |                    | Yes                      | Yes but WhoAmI should remain |
|                                           | ReadCacheLineSize                        | Yes          |                    | Yes                      | No                           |
|                                           | ReadCacheWeight                          | Yes          |                    | Yes                      | No - though it should be     |
|                                           | SMBWorkgroup                             | No           | WORKGROUP          | Yes                      | Yes                          |
|                                           | SMBActiveDirectoryEnabled                | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBActiveDirectoryRealm                  | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBActiveDirectoryIDMapDefaultMin        | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBActiveDirectoryIDMapDefaultMax        | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBActiveDirectoryIDMapWorkgroupMin      | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBActiveDirectoryIDMapWorkgroupMax      | Yes          |                    | Yes                      | Yes                          |
| Volume:<i>VolumeName</i>                  | FSID                                     | Yes          |                    | Yes                      | No                           |
|                                           | FUSEMountPointName                       | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | NFSExportClientMapList                   | No           | <i>None</i>        | Yes                      | Yes                          |
|                                           | NFSExportName                            | No           | <i>None</i>        | Yes                      | Yes                          |
|                                           | SMBShareName                             | No           | <i>None</i>        | Yes                      | Yes                          |
|                                           | AccountName                              | Yes          |                    | No                       | No                           |
|                                           | AutoFormat                               | No           | false              | No                       | No                           |
|                                           | NonceValuesToReserve                     | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | MaxEntriesPerDirNode                     | Yes          |                    | Yes for new directories  | Yes for newly served volume  |
|                                           | MaxExtentsPerFileNode                    | Yes          |                    | Yes for new files        | Yes for newly served volume  |
|                                           | MaxInodesPerMetadataNode                 | Yes          |                    | No                       | No                           |
|                                           | MaxLogSegmentsPerMetadataNode            | Yes          |                    | No                       | No                           |
|                                           | MaxDirFileNodesPerMetadataNode           | Yes          |                    | No                       | No                           |
|                                           | MaxCreatedDeletedObjectsPerMetadataNode  | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | CheckpointEtcdKeyName                    | If enabled   |                    | No                       | No                           |
|                                           | CheckpointContainerName                  | Yes          |                    | No                       | No                           |
|                                           | CheckpointContainerStoragePolicy         | Yes          |                    | No                       | No                           |
|                                           | CheckpointInterval                       | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | ReplayLogFileName                        | No           | <i>None</i>        | No                       | No                           |
|                                           | DefaultPhysicalContainerLayout           | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | MaxFlushSize                             | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | MaxFlushTime                             | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | FileDefragmentChunkSize                  | No           | 10485760           | Yes                      | Yes for newly served volume  |
|                                           | FileDefragmentChunkDelay                 | No           | 10ms               | Yes                      | Yes for newly served volume  |
|                                           | ReportedBlockSize                        | No           | 64Kibi             | Yes                      | Yes for newly served volume  |
|                                           | ReportedFragmentSize                     | No           | 64Kibi             | Yes                      | Yes for newly served volume  |
|                                           | ReportedNumBlocks                        | No           | 100Tebi/64Kibi     | Yes                      | Yes for newly served volume  |
|                                           | ReportedNumInodes                        | No           | 100Gibi            | Yes                      | Yes for newly served volume  |
|                                           | SnapShotIDNumBits                        | No           | 10                 | No                       | No                           |
|                                           | MaxBytesInodeCache                       | No           | 10485760           | Yes                      | Yes for newly served volume  |
|                                           | InodeCacheEvictInterval                  | No           | 1s                 | Yes                      | Yes for newly served volume  |
|                                           | SnapShotPolicy                           | No           | <i>None</i>        | Yes                      | Yes                          |
|                                           | SMBValidUserList                         | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBBrowseable                            | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBStrictSync                            | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBAuditLogging                          | Yes          |                    | Yes                      | Yes                          |
|                                           | SMBEncryptionRequired                    | Yes          |                    | Yes                      | Yes                          |
| NFSClientMap:<i>MapName</i>               | ClientPattern                            | Yes          |                    | Yes                      | Yes                          |
|                                           | AccessMode                               | Yes          |                    | Yes                      | Yes                          |
|                                           | RootSquash                               | Yes          |                    | Yes                      | Yes                          |
|                                           | Secure                                   | Yes          |                    | Yes                      | Yes                          |
| SMBUsers                                  | <i>SMBUserName</i>                       | Yes          |                    | Yes                      | Yes                          |
| PhysicalContainerLayout:<i>LayoutName</i> | ContainerStoragePolicy                   | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | ContainerNamePrefix                      | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | ContainersPerPeer                        | Yes          |                    | Yes                      | Yes for newly served volume  |
|                                           | MaxObjectsPerContainer                   | Yes          |                    | Yes                      | Yes for newly served volume  |
| SnapShotPolicy:<i>PolicyName</i>          | ScheduleList                             | Yes          |                    | Yes                      | Yes                          |
|                                           | TimeZone                                 | Yes          |                    | Yes                      | Yes                          |
| SnapShotSchedule:<i>ScheduleName</i>      | CronTab                                  | Yes          |                    | Yes                      | Yes                          |
|                                           | Keep                                     | Yes          |                    | Yes                      | Yes                          |
| JSONRPCServer                             | TCPPort                                  | Yes          |                    | Yes                      | No                           |
|                                           | FastTCPPort                              | Yes          |                    | Yes                      | No                           |
|                                           | DataPathLogging                          | Yes          |                    | Yes                      | No                           |
|                                           | RetryRPCPort                             | No           | <i>None</i>        | Yes                      | No                           |
|                                           | RetryRPCTTLCompleted                     | No           | 10m                | Yes                      | No                           |
|                                           | RetryRPCAckTrim                          | No           | 100ms              | Yes                      | No                           |
|                                           | RetryRPCDeadlineIO                       | No           | 60s                | Yes                      | No                           |
|                                           | RetryRPCKeepAlivePeriod                  | No           | 60s                | Yes                      | No                           |
| Logging                                   | LogFilePath                              | No           | <i>None</i>        | Yes                      | No                           |
|                                           | LogToConsole                             | No           | false              | Yes                      | No                           |
|                                           | TraceLevelLogging                        | No           | <i>None</i>        | Yes                      | No                           |
|                                           | DebugLevelLogging                        | No           | <i>None</i>        | Yes                      | No                           |
| EventLog                                  | Enabled                                  | No           | false              | Yes                      | Yes                          |
|                                           | BufferKey                                | If enabled   |                    | Yes                      | Yes                          |
|                                           | BufferLength                             | If enabled   |                    | Yes                      | Yes                          |
|                                           | MinBackoff                               | If enabled   |                    | Yes                      | Yes                          |
|                                           | MaxBackoff                               | If enabled   |                    | Yes                      | Yes                          |
|                                           | DaemonPollDelay                          | No           |                    | N/A                      | N/A                          |
|                                           | DaemonOutputPath                         | No           | os.Stdout          | N/A                      | N/A                          |
| Stats                                     | UDPPort                                  | Unless TCP   |                    | Yes                      | No                           |
|                                           | TCPPort                                  | Unless UDP   |                    | Yes                      | No                           |
|                                           | BufferLength                             | Yes          |                    | Yes                      | No                           |
|                                           | MaxLatency                               | Yes          |                    | Yes                      | No                           |
| HTTPServer                                | TCPPort                                  | Yes          |                    | Yes                      | No                           |
|                                           | JobHistoryMaxSize                        | Yes          |                    | Yes                      | No                           |
| StatsLogger                               | Period                                   | Yes          |                    | Yes                      | Yes                          |
|                                           | Verbose                                  | Yes          |                    | Yes                      | Yes                          |
| ProxyfsDebug                              | ProfileType                              | Yes          |                    | Yes                      | No                           |
|                                           | DebugServerPort                          | Yes          |                    | Yes                      | No                           |
| TrackedLock                               | LockHoldTimeLimit                        | No           | 0s                 | Yes                      | Yes                          |
|                                           | LockCheckPeriod                          | No           | 0s                 | Yes                      | Yes                          |
