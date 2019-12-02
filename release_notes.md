# ProxyFS Release Notes

## 1.14.2 (December 2, 2019)

### Bug Fixes:

Allow `chmod` to work on a directory via PFSAgent.

## 1.14.1 (November 26, 2019)

### Features:

Added an online FSCK tool (pfs-fsck). Note that the tool will not "stop the
Add bucketstat counters to measure all checkpoint operations and how
long individual parts of checkpoint processing take.  Add some bucketstat
counters to measure extent map lookups and updates for Read() and Write()
operations and their analogous object operations.  Add bucketstat counters
to measure B+Tree flush operations.

Added pfs_middleware configuration data to /info resource.

### Bug Fixes:

Significantly improve the performance of concurrent sync operations by
batching checkpoint operations.  If multiple threads request a checkpoint,
only perform one checkpoint instead of one for each request.

Significantly improve the performance of the FUSE mountpoint by treating
a Flush() operation as a no-op, which it is.  It does not imply any sort
of persistence guarantees.

Pick up sortedmap.TouchItem() fix in 1.6.1 (glide update).

Fix a few bugs in confgen.

## 1.13.4 (October 30, 2019)

### Bug Fixes:

Changes to runway environment for pfsagent mount points and allow users
to easily enable/disable core dumps.

Fixes to confgen search for template files.

## 1.13.0 (October 28, 2019)

### Features:

Add confgen tool to generate SMB, VIP, NFS and FUSE configuration files
from a proxyfs configuration

### Bug Fixes:

Fix a bug in ProxyFS in retry of chunked put operations that caused
a panic.

Sundry PFSAgent bug fixes.

## 1.12.2 (September 19, 2019)

### Bug Fixes:

Removed an unnecessary checkpoint performed before each PFSAgent
RpcWrote operation that is generated as each LogSegment is PUT
to Swift. The prior behavior put a strain on the checkpointing
system when a large set of small files are uploaded via PFSAgent
exposed FUSE mount points. Note that explicit flushes (fsync()
or fdatasync() calls) will still trigger a checkpoint so that
ProxyFS/PFSAgent can honor the request faithfully.

## 1.12.1 (September 12, 2019)

### Bug Fixes:

The "mount retry" fix was actually misnamed. What it actually does is
attempt to issue multiple Checkpoint HEAD requests and achieve agreement
on what is returned by a majority quorum. In each such HEAD request,
a retry logic pre-existed where anything other than a "200 OK" would
be retried... hence the overloading of the term "retry". Anyway, in the
case where `mkproxyfs` is being run to format a previously empty Swift
Account, the retry logic could take a very long time (e.g. *minutes*) to
give up. Thus, by performing each of those Checkpoint HEAD requests *in
series*, the formatting process could take an excessive amount of time
just coming to the decision that the Swift Account needs to be formatted.
This fix simply performs those quorum Checkpoint HEAD requests (that each
will be retried a number of times) *in parallel* thus returning the total
time for the format back to what it was prior to the 1.12.0 "mount retry"
fix.

## 1.12.0 (September 11, 2019)

### Features:

Added an online FSCK tool (pfs-fsck). Note that the tool will not "stop the
world". As such, it is possible for it to report false positives for missing
objects (both metadata and file data). Re-running the tool will typically
note those false positives were false and move on... but perhaps find more.
As such, the tool is really only reliably able to avoid announcing false
positives on an otherwise idle volume.

Implemented support for a "Recycle Bin" mechanism where objects are simply
marked as being in the Recycle Bin rather than aggressively deleted. If an
attempt is made to later access the object, a log message will report this
and statistics bumped to indicate the condition that would have been a file
system corruption had the object actually been deleted. Note that this feature
is normally disabled.

Layout Report has been streamlined to, by default, report the "running count"
of the metrics rather than actually counting them (which would involve paging
in all the metadata...a potentially very time consuming activity that must
proceed while the "world is stopped"). It is still possible to do the brute
force "count"... and note any discrepencies with the "running count" previously
reported.

The pfs-swift-load tool has been enhanced to support arbitrarily deep paths.
This is provided to demonstrate specifically the impact of file path length
on SMB performance.

PFSAgent, like ProxyFS itself, now supports (at least) an HTTP query to report
the running version. This may also be used to confirm that PFSAgent is up and
operating on a particular Volume.

### Bug Fixes:

Addressed stale/cached stat results for (FUSE and) NFS mount points by shortening
the timeout...with the tradeoff that this could increase overhead of certain
metadata querying operations somewhat.

Added support for HTTPS in ProxyFS Agent. Previously, Swift Proxies behind a
TLS-terminating Load Balancer would result in an attempt to use non-HTTPS
connections following authentication via the Swift API.

Added volume mount retry logic to handle the theoretical case where a stale
Checkpoint Header is returned during the Volume Mount process.

### Notes:

Added logging of each Checkpoint Header operation. This amounts to a log message
generated once every ten seconds per Volume typically...though clients issuing
FLUSH/SYNC operations may accellerate this.

In addition, periodic stats are logged as well (in addition to the limited set
regularly reported earlier). Default interval is 10 minutes per Volume.

## 1.11.2 (July 28, 2019)

### Notes:

This is a small delta from 1.11.1 to temporarily disable deletion of thought-to-be
unreferenced objects in the checkpoint container. A working theory of one such
issue is that a object holding metadata for a volume was inadvertantly thought to
no longer be referenced. As such, the object was scheduled for deletion. Upon a
subsequent re-mount, the object could not be found and the re-mount failed.

A new (temporary) boolean setting in the FSGlobals section titled MetadataRecycleBin
will, for now, default to TRUE and, instead of issuing DELETEs on objects in the
checkpoint container thought to now be unreferenced, a new header will be applied
to them titled RecycleBin (with a value of true).

## 1.11.1 (June 28, 2019)

### Features:

Support for storing Volume Checkpoints in an ETCD instance are now supported.
This feature is enabled by setting an optional key to true along with other
keys specifying the endpoints of the ETCD instance and the name of the key to
use for each Volume's Checkpoint.

### Bug Fixes:

Object PUTs over existing file system directories behavior has been corrected.
For instance, now a PUT over an empty directory will replace it.

## 1.11.0 (June 13, 2019)

### Features:

Adapted to latest (2.21.0.4) version of Swift.

Alpha version of PFSAgent is now available. This tool presents a FUSE mount point
of a Volume served by a ProxyFS instance by using the new PROXYFS HTTP Method of
(pfs_middleware in) Swift.

### Bug Fixes:

The COALESCE HTTP method used by S3 Multi-Part Upload had a number of deficiencies.
Most visibly, when COALESCE was issued to overwrite an existing object, metadata
was not applied correctly.

Objects formed via COALESCE now have a correctly set non-MD5 ETAG.

GET issued to a Container specifying a marker now returns the objects in the
Container following the marker. Previously, if the marker indicated an Object
in the Container's directory (i.e. rather than a subdirectory of the Container),
the list of following Objects would be empty.

SnapShotPolicy changes now picked up during SIGHUP.

### Notes:

ProxyFS typically should not expect errors coming from Swift. Indeed, the only
expected errors are during Volume/Account formatting as the "mkproxyfs" tool
attempts to verify that the underlying Account is, indeed, pristine. But other
unexpected errors were logged along with being retried. This release ensures
that any payload returned by Swift with bad HTTP Status is also logged.

## 1.10.0 (March 12, 2019)

### Bug Fixes:

A number of Swift and S3 operations are necessarily path-based. Hence, although
the RESTful Object-based APIs purport to be atomic, this is actually impossible
to honor in the presence of File-based access. In attempts to retain the atomicity
of the RESTful Object-based APIs, several deadlock conditions were unaddressed.
One such deadlock addressed in both 1.8.0.6 and 1.9.2/1.9.5 involved the contention
between the COALESCE method that references an unbounded number of file paths with
other path and Inode-based APIs. This release addresses in a much more encompassing
way all the contentions that could arise within and amongst all of the path and
Inode-based APIs.

The SnapShotPolicy feature introduced in 1.8.0 was inadvertantly disabled in 1.9.5
due to the conversion to the new transitions package mechanism introduction. As such,
only explicitly created SnapShots would be created. This release restores the ability
to schedule SnapShots via a per-Volume SnapShotPolicy.

### Notes:

Lock tracking capabilities have been significantly enhanced that will provide
several mechanisms with which both deadlocks and unusually high latency conditions
can be examined with a performant mechanism that, when enabled, clearly reports
what contentions are at the root of the observed condition. This instrumentation
includes instrumenting both the various Mutexes that serialize data structure
access as well as per-Inode locks that serialize client operations.

A modern file system such as ProxyFS is obliged to support nearly unbounded
parameters governing nearly every enumerated aspect. To clients, this includes
support for things like extremely large files as well as a huge number of such
files. Internally, management of extremely fragmented files is also a demanding
requirement. Despite this, constrained compute environments should also be supported.
This release introduces support for particularly the 32-bit architecture of Arm7L
based systems.

### Issues:

While not a new issue, the release focused on exposing the inherent incompatibility
between path-based RESTful Object APIs and File Access. Indeed, such issues are
impossible to fully accomodate. In addition, a key feature of the S3 API is support
for so-called multi-part uploads. This is accomplished by clients uploading each
part - perhaps simultaneously - to unique Objects. Once all parts have been uploaded,
a Multi-Part Put Complete operation is performed that requests that, logically, all
of the parts are combined to form the resultant single Object. Support for this fianl
step is implemented by means of a new COALESCE HTTP Method effectively added to the
OpenStack Swift API. Unfortunately, this is where the "impedance mismatch" between
the hierarchical nature of the File System clashes with the "flat" nature of an
Object API such as S3 (and, for that matter, OpenStack Swift).

The key issue is how to represent a Directory in the File System hierarchy. At this
point, a Container (or Bucket) listing (via GET) will report both Objects (Files)
and Directories. This is in conflict with an Object-only system that lacks any sort
of Directory Inode concept. Indeed, several typical client operations are confused
by the presence of Directories in the Container/Bucket listing (not the least of
which is the widely used BOTO Python library used in S3 access). This conflict
remains in the current release.

## 1.9.5 (February 13, 2019)

### Features:

Made StatVfs() responses configurable. Note that values
for space (total, free, available) continue to be set
to artificial values... but at least they are now settable.

### Bug Fixes:

Re-worked logic for various path-based operations (i.e.
operations invoked via Swift or S3 APIs) could result in
a deadlock when combined with file-based operations. These
have now largely been resolved (see Issues section for
details on what is not).

Modified pfsconfjson{|packed} to auto-upgrade supplied
.conf files to report the VolumeGroup-translated form
(if up-conversion would be done by ProxyFS itself).

COALESCE method, when targeted at an existing Object,
would previously fail to recover the overwritten Object's
space.

Various pre-VolumeGroup->VolumeGroup auto-upgrade patterns
could result in false reporting of a Volume being moved to
a VolumeGroup where it already exists in response to SIGHUP.
Just restarting ProxyFS would not see this issue.

### Issues:

While much work was completed towards avoiding deadlock
situations resulting from path-based (i.e. Swift/S3 API)
operations, the work is not yet complete. A GET on a
Container/Bucket that recurses could still result in a
deadlock but the bug fix for this case largely closes
that window. A PUT also has the potential for another
deadlock situation that is equally very unlikely. No test
case has been able to expose these remaining deadlocks
so they remain theoretical.

## 1.9.2 (January 18, 2019)

### Bug Fixes:

Resolved race condition when simultaneous first references to
an `Inode` are executed resulting in a lock blocking any further
access to the `Inode`. This condition was frequently seen when
attempting a multi-part upload via the S3 or Swift HTTP APIs
as it would be typical/expected that the uploading of all the
parts of a new `file` would begin roughly at the same time.

It is now possible to perform builds and unit tests on the
same node where an active ProxyFS session is in operation.
Previously, identical TCP and UDP Ports were being used by
default leading to bind() failures.

### Features:

Updated to leverage Golang 1.11.4 features.

Added support for `X-Object-Sysmeta-Container-Update-Override-Etag`.

Added support for `end-marker` query params.

Added support for fetching a `ReadPlan` for a given file
via the HTTP interface. The `ReadPlan` may then be used to
HEAD or GET the individual `LogSegments` that, when stitched
together, represent the contents of the file.

Liveness monitoring now active among all ProxyFS instances
visable via JSON response to an HTTP Query on the embedded
HTTP Server's Port for `/liveness`.

Added support for VolumeGroups where the set of Volumes in a
VolumeGroup are what is assigned to a Peer/Node rather than
each individual Volume.

## 1.8.0.7 (January 28, 2019)

### Features:

The response to fsstat(1), statfs(2), and statvfs(3) returns
capacities that were hardcoded (e.g. Total Space of 1TiB). While
it is currently not possible for such reporting to represent
actual capacities, new config values are available to adjust the
reported values. In addition, the defaults have been increased
(e.g. Total Space is now reported as 100 TiB).

## 1.8.0.6 (January 23, 2019)

### Bug Fixes:

A race condition triggered by e.g. multi-part uploads could
render the targeted portion of a file system indefinitely
locked requiring a restart of the proxyfsd daemon. This release
prevents this race condition.

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
