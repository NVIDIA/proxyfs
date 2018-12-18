# ProxyFS Release Notes

## 1.8.0.5 (November 27, 2018)

### Bug Fixes:

Fix a bug that caused proxyfsd to leak "connection slots" from
the ChunkedConnectionPool or the NonChunkedConnectionPool if
the noauth proxy was not running when proxyfs tried to open a
new connection (in swiftclient.acquireChunkedConnection() and
swiftclient.acquireNonChunkedConnection()).  This could happen during
a reload of the noauth proxy when a new configuraiton is pushed.

## 1.8.0.4 (November 12, 2018)

### Bug Fixes:

Fix a bug that caused proxyfsd to exit if the SwiftStack controller
deleted a file system using its two step process of first inactivating
it (one reconfig event) and then deleting it (second reconfig event).
In that case, the UnregisterForEvents() would be called twice and it
would call logger.Fatalf() to complain, causing Samba (smbd) to exit
and making the SMB client unhappy.

## 1.8.0.3 (November 9, 2018)

### Bug Fixes:

Fix a bug that caused proxyfsd to exit if the noauth proxy server
restarted while file system i/o to Swift was in progress.  In particular
this could happen during a reconfig triggered by a controller config push.

## 1.8.0.2 (October 19, 2018)

### Bug Fixes:

Fix a bug in the snapshot code that generated snapshot names that
contained a colon which Windows SMB clients find hard to cope with.

## 1.8.0.1 (October 6, 2018)

### Bug Fixes:

Fix a bug introduced in 1.8.0 that triggered a NULL pointer dereference
if an HTTP GET request specified a byte range without an ending offset.


## 1.8.0 (September 30, 2018)

### Features:

Add filesystem snapshots which are created and destroyed based on policies
that are applied to each file system.  Snapshots are accessible via the
newly created "/.snapshot/<snapshot_name>" directory in the root of each
filesystem.

Add bucketized statistics for package swiftclient, fs, and headhunter.
The statistics can be queried via the built-in web server, using
the URL "//localhost:15346:/stats" in runway environments and
"//<private_IPaddr>:1534:/stats" in stand alone environments, where
<private_IPaddr> is the private IP address used for the backend network.

Note: the bucketized statistics API, output format, and URL is unstable
and changing quickly.  It will be different in the next release.

Change config file format (for the *.conf files) so that user defined
names for:
    Volume
    PhysicalContainerLayout
    FlowControl
    Peer
are now preceeded by one of the strings "Volume:",
"PhysicalContainerLayout:", "FlowControl:", or "Peer:", as appropriate,
to insure that names are unique.

Initial work on a "liveness detector" to determine whether
ProxyFS/Samba/NFS are currently up and serving file requests.  This is
part of an HA solution that we are developing.

### Bug Fixes:

Fix a bug that could cause data corruption if a file write sent via
the FUSE or NFS interface failed on the first PUT attempt and had to be
retried, in which case the PUT could be retried with incorrect data for
the file.  This bug was exacerbated by the next bug, which could cause
PUT requests to exceed the 60 sec server timeout deadline.

Fix a bug where "log segments" containing data written to files
were not closed and flushed after the 10 sec deadline (a bug in
inFlightFileInodeDataFlusher() that could extend the PUT request beyond
the Swift server's timeout deadline of 60 sec).

Fix a bug where ProxyFS would either move to a new container for file
log segments too quickly ("MaxObjectsPerContainer" was not being checked
against correctly).

Insure that ProxyFS will not accept requests via the FUSE interface
while its re-reading its configuration (could lead to corruption).

Add additional units tests for sequential writes.

Improvements to the mock Swift testing environment, ramswift, to free
memory when ramswift is restarted within a test.

Update generatedfiles Makefile target to make files that are now
necessary.

Reworked proxyfsd daemon startup logic to avoid possible race conditions
during startup.

Reworked ramswift daemon startup logic to avoid race conditions sometimes
hit when running tests.


### Notes

* With the advent of Golang v1.11, new support for WebAssembly has arrived. To build for a
WebAssembly target, setting GOOS=js and GOARCH=wasm is required. Unfortunately, the arrival
of these two new values ("js" and "wasm"), Go source files ending with _js.go, in particular,
will only be compiled if GOOS=js has been set. Previously (Golang v1.10 and prior), a file
ending in _js.go would always be included. This release captures a name change to various
static files generated in package httpserver by adding an underscore ("_") just before ".go"
to avoid this new Golang behavior.

* Enhance the swiftclient chunked put unit tests to try and cover
concurrency and many more failure/retry scenarios.  Add a new config file
variable in the `SwiftClient` section, `ChecksumChunkedPutChunks`` which
defaults to 'false'.  If set to `true` then data cached in a chunked put
connection has a checksum computed when it is Sent and checked frequently
on subsequent operations.

## 1.7 (September 6, 2018)

### Bug Fixes:

* Fix panic in inode cache discard thread.
* Rework flush logic to fix deadlocks when connections are exhausted.

## 1.6.4 (July 24, 2018)

### Features:

* Added support for configuring the NoAuth Swift Proxy to an IP Address other than a default of (IPv4) localhost (127.0.0.1). Note that in the future, the defaulting to localhost may be removed, so users should take care to specify the NoAuth Swift Proxy IP Address in their configurations in the future.
* Added support for the "delimiter=" option for Swift API GET requests. This, along with the "prefix=" option, enables viewing of the directory hierarchy inside a Container in the same way one would view a file system.
* Added support for SnapShots. These are invoked via the RESTful API exposed by the embedded HTTP Server inside each proxyfsd instance. Note that this API is only reachable via the PrivateIPAddr and performs not authentication/authorization. You can find the new RESTful methods underneath the /Volume/<volumeName> resource (along with FSCK, Scrub, and LayoutMap). Both JSON (textual) and formatted HTML is available.
* Added support for viewing a FileInode's ExtentMap via this same RESTful API underneath the /Volume/<volumeName> resource (adjacent to the above-mentioned SnapShot resource).
* RPC Timeouts from pfs_middleware to proxyfsd have new defaults and are controllable via two distinct parameters optionally specified in the [filter:pfs] section of the proxy-server.conf:

>        rpc_finder_timeout
>            specified in (floating point) seconds
>            defaults to 3.0
>            applies when searching for a proxyfsd instance to ask where a particular Swift Account is being served

>        rpc_timeout
>            specified in (floating point) seconds
>            defaults to 30.0
>            applies to requests to the specific proxyfsd instance serving a particular BiModal Swift Account

### Bug Fixes:

* SMB clients making multiple mounts to the same Samba/ProxyFS instance could encounter all sessions/mounts being closed when requesting any one of them to terminate. This has now been corrected.
* Previously, a cache of Inode structures did not support eviction. As a result, a very large number of Inodes accessed since a ProxyFS instance was started could exhaust memory. To address this, a new background thread discards non-dirty Inodes from the Inode Cache. The behavior of the Inode Cache eviction thread is tuned by:
>          MaxBytesInodeCache - defaults to 10485760 (10MB)
>          InodeCacheEvictInterval - defaults to 1s (disabled if 0s)

### Known Issues:

* As of this version, the metadata format has been updated from V2 to V3 in support of the SnapShot functionality. Unfortunately there is no going back. Once a V2 volume is mounted, it is immediately upgraded to V3 despite not (yet) having any SnapShots declared.

## 1.5.3 (April 3, 2018)

Ignore SIGPIPE, SIGCHLD, and some other signals that were causing
proxyfsd to exit when it shouldn't.

## 1.5.2 

there is no release 1.5.2.

## 1.5.1 (March 30, 2018)

Partially botched 1.5.0 release packaging.

## 1.5.0 (March 30, 2018)

Move to go version 1.10.

Significant improvements to fsck performance and what it validates.
Fsck now detects and cleans up unreferenced objects in the checkpoint
container.  Fix a bug in fsck that caused sparse files to be flagged
corrupt and deleted.  Complementary to fsck, add "scrub" jobs that
validate the object maps (extent maps) for files.

Fix a bug that caused B+Tree nodes to become quite large, which had
a significant performance impact on large file systems.

Prettify the http pages generated by proxyfs.

## 1.4.1 (March 6, 2018)

Fix a bug in the B+Tree code that caused old objects in the
.__checkpoint__ container to become unreferenced instead of deleted.

Support Travis continuous integration testing on github.

## 1.3.0 (February 16, 2018)

## 1.2.0 (January 30, 2018)

### Bug Fixes:

* Support for hidden SMB Shares now available

### Notes:

* Development environment now enhanced with pin'd versions of dependencies
* Preliminary work for supporting an RPO of Zero in place but inactive

## 1.1.1 (January 2, 2018)

### Bug Fixes:

* Submodule vfs now no longer depends upon pre-installed submodule jrpcclient

## 1.1.0 (January 2, 2018)

### Notes:

* ProxyFS now built with a standard Makefile (obsoleting regression_test.py)

## 1.0.3 (December 4, 2017)

### Bug Fixes:

* Fix cross-container DLO authorization

### Known Issues:

* Metadata Recovery Point Objective ("RPO") is non-zero (except for file flush operations)

## 1.0.2 (December 4, 2017)

### Bug Fixes:

* Segment fault while handling SIGHUP during log rotation and volume migration

### Known Issues:

* Metadata Recovery Point Objective ("RPO") is non-zero (except for file flush operations)

## 1.0.1 (December 1, 2017)

### Features:

* Added support for "async" flush in SMB (allows multiple simultaneous flushes)

### Bug Fixes:

* Above "async" flush resolves SMB 2 and above write issues with "strict sync = yes" setting in smb.conf

### Known Issues:

* Metadata Recovery Point Objective ("RPO") is non-zero (except for file flush operations)

## 1.0.0 (November 29, 2017)

### Features:

* Source is now available on GitHub
* Added support for S3 Multi-part Uploads
* Formatting of a Volume File System now made explicit with `mkproxyfs` tool
* Volumes may be added and removed (via SIGHUP) without restarting ProxyFS
* Configuration files now allow identical section names for different types of sections
* Support added for a distinct Storage Policy for metadata
* New RESTful API added for FSCK management via HTTP

### Bug Fixes:

* Recover trapped resources in Samba when ProxyFS halts
* Fixed memory leaks and slow performance during RoboCopy
* Resolved unbounded memory consumption as file systems grow
* Fix for ctime not being updated during various operations
* Fix for missing first file in a directory if it would sort before "."
* Specification of a Storage Policy for file data now honored
* Corruption following metadata checkpoint failures now halted

### Known Issues:

* Metadata Recovery Point Objective ("RPO") is non-zero (except for file flush operations)

## 0.55.0 (October 30, 2017)

### Features:

* Caching of metadata in RAM now configurable
* Samba parameters now specified via the standard /etc/samba/smb.conf mechanism

### Bug Fixes:

* Fixed memory leaks in readdir() APIs issued via SMB
* Fixed metadata on objects set via Swift/S3 API

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.54.1 (October 10, 2017)

### Features:

* Updates to HTTP COALESCE Method
* Improved flushing of affected Swift connections during SIGHUP (reload)
* Improved dataflow during high number of unflushed open file traffic

### Bug Fixes:

* Resolved memory leaks in Samba processes during heavy Robocopy activity
* Resolved potential deadlock for unflushed files that are removed
* Hardened error handling between Samba & ProxyFS processes

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.54.0 (October 3, 2017)

### Features:

* Improved Object ETag MD5 handling
* Object SLO uploads converted to COALESCE'd Objects/Files

### Bug Fixes:

* Non BiModal Accounts remain accessible even when no ProxyFS nodes are available

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.53.0.3 (September 29, 2017)

### Features:

* Added statistics logging

### Bug Fixes:

* Fixed BiModal IP Address reporting following SIGHUP reload
* Fixed issue with large transfers causing Swift API errors

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.53.0.2 (September 19, 2017)

Note: This was just a re-tagging of 0.53.0.1

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.53.0.1 (September 15, 2017)

### Features:

* Added support for Samba version 4.6

### Bug Fixes:

* Fixed memory leak in smbd resulting from a closed TCP connection to proxyfsd

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.53.0 (September 11, 2017)

### Features:

* Added avaibility improvements for ProxyFS Swift clusters to continue when a ProxyFS node is down
* Significantly improved logging during startup and shutdown
* New `mkproxyfs` tool now available to format Volumes (Swift Accounts)

### Bug Fixes:

* Embedded HTTP Server now reports current configuration once startup/restart (SIGHUP) completes
* HTTP Head on ProxyFS-hosted Objects now returns proper HTTPStatus
* Resolved incomplete file locking semantics for SMB
* Resolved issue where a file being written is deleted before its data has been flushed
* Corrected behavior of readdir() enabling callers to bound the size of the returned list
* Corrected permissions checking & metadata updating
* Resolved NFS (FUSE) issue where the underlying file system state failed to reset during restart
* Resolved SMB (smbd) memory leak resulting from unmount/remount sequence

### Known Issues:

* SMB (smbd) memory leaks resulting from restarting the ProxyFS process (proxyfsd) underneath it
* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.52.0 (August 21, 2017)

### Features:

* Support for disabling volumes added

### Bug Fixes:

* Fixed missing flushing of modified files leading to zero-lengthed files

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.51.2 (August 15, 2017)

### Features:

* Improved metadata checkpointing mechanism (V2) performs optimized garbage collection

### Bug Fixes:

* Fixed clean-up of FUSE (and NFS) mount point upon restart after failure
* Fixed memory leaks in SMBd for readdir(), getxattr(), chdir() and list xattr
* Fixed race condition between time-based flushes and on-going write traffic
* Fixed multi-threaded socket management code in resolving DNS names
* Fixed missing support for file names containing special characters

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.51.1 (August 3, 2017)

### Features:

* Enhanced tolerance for intermittent Swift errors
* Read Cache now consumes a configurable percentage of available memory
* Flow Controls now get a weighted fraction of total Read Cache memory
* Configuration reload now supported via SIGHUP signal

### Bug Fixes:

* Fixed embedded HTTP Server handling of "empty" URLs
* Removed memory leaks in SMB handling
* Resolved potential corruption when actively written files are flushed

### Known Issues:

* Memory leak in SMB directory reading and extended attribute reading
* Process restart may leave NFS mount point in a hung state
* Named Streams are disabled in SMB (enabling this is TBD)
