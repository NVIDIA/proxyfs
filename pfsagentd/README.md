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

There are a few distinct entities to be configured.

### OpenStack Swift Proxy

Each Swift Proxy Server configuration file (`proxy-server.conf`) is assumed
to already contain a `[filter:pfs]` section pointing the `pfs_middleware` at
one or more ProxyFS instances. This is what enables Swift API and S3 API
"BiModal" access to ProxyFS Volumes. A new Key:Value that is one of:
* bypass_mode = off
* bypass_mode = read-only
* bypass_mode = read-write
The Value `off` is assumed if the `bypass_mode` Key is not specified.

### PFSAgent Daemon (pfsagentd)

The program is supplied a configuration file in .INI format. Here is an example:
```
[Agent]
FUSEVolumeName:                                    CommonVolume
FUSEMountPointPath:                             AgentMountPoint
FUSEUnMountRetryDelay:                                    100ms
FUSEUnMountRetryCap:                                        100
PlugInPath:                         pfsagentd-swift-auth-plugin
PlugInEnvName:                                    SwiftAuthBlob
PlugInEnvValue: {"AuthURL":"http://localhost:8080/auth/v1.0"\u002C"AuthUser":"test:tester"\u002C"AuthKey":"testing"\u002C"Account":"AUTH_test"}
SwiftTimeout:                                               10m
SwiftRetryLimit:                                             10
SwiftRetryDelay:                                             1s
SwiftRetryDelayVariance:                                     25
SwiftRetryExpBackoff:                                       1.4
SwiftConnectionPoolSize:                                    200
FetchExtentsFromFileOffset:                                  32
FetchExtentsBeforeFileOffset:                                 0
ReadCacheLineSize:                                      1048576
ReadCacheLineCount:                                        1000
SharedFileLimit:                                           1000
ExclusiveFileLimit:                                         100
DirtyFileLimit:                                              50
MaxFlushSize:                                          10485760
MaxFlushTime:                                             200ms
LogFilePath:                             /var/log/pfsagentd.log
LogToConsole:                                              true
TraceEnabled:                                             false
HTTPServerIPAddr:                                       0.0.0.0
HTTPServerTCPPort:                                         9090
ReadDirPlusEnabled:                                       false
XAttrEnabled:                                             false
EntryDuration:                                              10s
AttrDuration:                                               10s
AttrBlockSize:                                            65536
ReaddirMaxEntries:                                         1024
FUSEMaxBackground:                                          100
FUSECongestionThreshhold:                                     0
FUSEMaxWrite:                                            131072
RetryRPCDeadlineIO:                                         60s
RetryRPCKEEPALIVEPeriod:                                    60s
```

In the above example, some important fields are as follows:
* FUSEVolumeName should be set to the ProxyFS Volume being mounted
* FUSEMountPointPath should be set to where the FUSE presentation of the Volume should appear (must pre-exist)
* PlugInPath should be set to point to the desired Swift Authorization PlugIn
* PlugInEnvName should be set to the name of the ENV variable used to pass Swift Auth secrets blob to the plug-in
* PlugInEnvValue should be set to the value of the Swift Auth secrets blob if PFSAgent should set it
* HTTPServerIPAddr should be set to the IP Address where PFSAgent should present its embedded HTTP Server
* HTTPServerTCPPort should be set to the TCP Port upon which the PFSAgent should present its embedded HTTP Server

The balance of the settings are more related to tuning choices. Among those, the most pertinent are:
* ReadCacheLineSize specifies how much of a Swift Object is read when a read cache miss occurs
* ReadCacheLineCount specifies how many such read cache lines will be used
* MaxFlushSize specifies how frequently in terms of byte count writes are sent to new Swift Objects
* MaxFlushTime specifies how frequently in terms of time writes are sent to new Swift Objects

Note the use of `\u002C` in the example PlugInEnvValue line above. This avoids the .INI
parser from interpreting the commas as value separators and incorrectly assuming there
are multiple values being specified. Similarly, space would be interpreted as a list
element separator so `\u0020` should be used instead.

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

[Install]
WantedBy=network-online.target
```

### Authentication Plug-In

While PFSAgent targets OpenStack Swift clusters that have provisioned
ProxyFS and placed the pfs_middleware in the Swift Proxy pipeline, it
must use the normal Swift Proxy to gain access to ProxyFS LogSegments
that host FileInode data. Further, via the pfs_middleware in the Swift
Proxy pipeline, at least the Mount JSON RPC will travel through the
normal Swift Proxy to get to ProxyFS. To perform either these GETs,
PUTs, or Mount JSON RPC, the request must contain a valid X-Auth-Token.
Rather than assume normal Swift Authentifiction is used to compute
this X-Auth-Token, PFSAgent implements a plug-in mechanism whereby an
external process, the plug-in, performs the actual authentication step
and returns the X-Auth-Token to be provided as proof in each subsequent
Swift API request. Such tokens may expire, resulting in a `401 Unauthorized`
response code. Thus, this plug-in must also support token renewal or
replacement. In cases where a full re-authorization is required, the
plug-in will already have the long-lived credentials with which to repeat
the authentication as performed originally.

#### Authentication-specific Setup

The configuration supplied to PFSAgent (`pfsagentd`) includes, among
other keys, the following:

```
[Agent]
...
PlugInPath:   # The path to the plug-in
PlugInEnvName: # Specifies the name of an ENV variable the plug-in should load
PlugInEnvValue: # If supplied, PFSAgent will set the ENV variable to this value
...
```

#### Plug-In Operation

PFSAgent will, at start-up, launch the program indicated by [Agent]PlugInPath,
sending the value of [Agent]PlugInEnvName as Arg1. If [Agent]PlugInEnvValue is
specified, it will also ensure the ENV variable named will be set to this
value in the plug-in's process.

At that point, and at any time subsequently receiving a `SIGHUP`, the plug-in
will perform the authentication (or renewal) step. The result will be used
to generate two pieces of information:

* X-Auth-Token - to be used by PFSAgent in all subsequent Swift API calls (GETs, PUTs, and Mouunt JSON RPC requests)
* StorageURL - to form the first portion of any such Swift API path

The response (either at start-up or following reception of a
`SIGHUP`) is sent to os.Stdout as a UTF-8 encoded JSON object:

```
{
    "AuthToken"  : "<valid Swift Auth Token (i.e. to be passed via `X-Auth-Token`)>"
    "StorageURL" : "<path including correct transport ("http{|s}"), version ("proxyfs"), and SwiftAccount>",
}
```

At PFSAgent termination, the plug-in should be sent either a `SIGINT` or a `SIGTERM`.
This will trigger the plug-in to also exit (perhaps after cleaning up any
not-to-be-persisted details of its execution).
