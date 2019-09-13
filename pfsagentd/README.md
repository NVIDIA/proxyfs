# PFSAgent

FUSE Driver for presenting ProxyFS Volumes as locally mounted file systems

## Synopsis

PFSAgent is a program that remotely accesses a ProxyFS Volume through
a new `PROXYFS` HTTP Method targetted at any Swift Proxy Server in a
Swift cluster that has contains the `pfs_middleware` filter. All metadata
operations tunnel through `pfs_middleware` on their way to the `ProxyFS`
instance currently managing the specified Volume (Swift Account). To
allow file reads and writes to scale out, however, PFSAgent employs
a `bypass_mode` instructing `pfs_middleware` to pass through an Object
GET or PUT HTTP Method rather than redirect it over to `ProxyFS`.

## Setup

Each Swift Proxy Server configuration file (`proxy-server.conf`) is assumed
to already contain a `[filter:pfs]` section pointing the `pfs_middleware` at
one or more ProxyFS instances. This is what enables Swift API and S3 API
"BiModal" access to ProxyFS Volumes. A new Key:Value that is one of:
* bypass_mode = off
* bypass_mode = read-only
* bypass_mode = read-write
The Value `off` is assumed if the `bypass_mode` Key is not specified.

The program file to execute is `pfsagentd` might typically be installed
as `/opt/ss/bin/pfsagentd`. The program is supplied a configuration file
in .INI format. Here is an example:
```
[Agent]
FUSEVolumeName:                                    CommonVolume
FUSEMountPointPath:                             AgentMountPoint # Unless starting with '/', relative to $CWD
FUSEUnMountRetryDelay:                                    100ms
FUSEUnMountRetryCap:                                        100
SwiftAuthURL:                   http://localhost:8080/auth/v1.0 # If domain name is used, round-robin among all will be used
# SwiftAuthURL:                https://localhost:8443/auth/v1.0 # If domain name is used, round-robin among all will be used
SwiftAuthUser:                                      test:tester
SwiftAuthKey:                                           testing
SwiftAccountName:                                     AUTH_test # Must be a bi-modal account
SwiftTimeout:                                               20s # Should be significantly higher than MaxFlushTime
SwiftRetryLimit:                                             10
SwiftRetryDelay:                                             1s
SwiftRetryExpBackoff:                                       1.4
SwiftConnectionPoolSize:                                    200
FetchExtentsFromFileOffset:                                  32
FetchExtentsBeforeFileOffset:                                 0
ReadCacheLineSize:                                      1048576 # Aligned chunk of a LogSegment
ReadCacheLineCount:                                        1000
SharedFileLimit:                                           1000
ExclusiveFileLimit:                                         100
DirtyFileLimit:                                              50
MaxFlushSize:                                          10485760
MaxFlushTime:                                               10s
ReadOnly:                                                 false
LogFilePath:                                                    # Unless starting with '/', relative to $CWD; Blank to disable
LogToConsole:                                              true
TraceEnabled:                                             false
HTTPServerIPAddr:                                     127.0.0.1
HTTPServerTCPPort:                                         9090
AttrDuration:                                               10s
AttrBlockSize:                                            65536
LookupEntryDuration:                                        10s
ReaddirMaxEntries:                                         1024
```

In the above example, some important fields are as follows:
* FUSEVolumeName should be set to the ProxyFS Volume being mounted
* FUSEMountPointPath should be set to where the FUSE presentation of the Volume should appear (must pre-exist)
* SwiftAuthURL should be set to the AuthURL of your Swift Cluster
* SwiftAuthUser should be set to the Swift User that "owns" the Swift Account for the ProxyFS Volume
* SwiftAccountName should be set to the corresponding Swift Account for the ProxyFS Volume
* HTTPServerIPAddr should be set to the IP Address where PFSAgent should present its embedded HTTP Server
* HTTPServerTCPPort should be set to the TCP Port upon which the PFSAgent should present its embedded HTTP Server

The balance of the settings are more related to tuning choices. Among those, the most pertinent are:
* ReadCacheLineSize specifies how much of a Swift Object is read when a read cache miss occurs
* ReadCacheLineCount specifies how many such read cache lines will be used
* MaxFlushSize specifies how frequently in terms of byte count writes are sent to new Swift Objects
* MaxFlushTime specifies how frequently in terms of time writes are sent to new Swift Objects

Each mounted ProxyFS Volume requires an instance of PFSAgent (`pfsagentd`) to run.
Hence, each ProxyFS Volume must be described by a unique configuration file as described above.

It is convenient to employ `systemd` to launch each PFSAgent instance via unique service files.
An example service file that can be used to allow launching multiple such PFSAgent instances
(by specifying the configuration file name in the `systemd` invocation) is:
```
[Unit]
Description=ProxyFS Agent %i
Wants=network-online.target
After=network-online.target
ConditionFileNotEmpty=/opt/ss/etc/pfsagent/%i.conf

[Service]
ExecStart=/opt/ss/bin/pfsagentd /opt/ss/etc/pfsagent/%i.conf

Type=simple

# Restart the ssnoded daemon after a 2 seconds delay, in case it crashes.
Restart=always
RestartSec=2
StartLimitInterval=11s
```
