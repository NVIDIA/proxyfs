ProxyFS Data Flows
==================

As an example of how the system works, this will be a walk through of
basic filesystem operators so that we can illuminate how the system
works and everything is put together. This is meant to be an overview of
the basic operations and how they flow through ProxyFS.

Filesystem Writes
-----------------

When a filesystem client goes to do a write two things happen. First the
data bits need to be written in the storage. Second, the filesystem tree
needs to be updated so that it knows of the existence of that file.
Let’s dive into a walkthrough of how those two operations happen.

Client initiates a write
~~~~~~~~~~~~~~~~~~~~~~~~

After a client has mounted a filesystem volume, the client initiates a
read request which is received by the ProxyFS process.

Pick a unique object for “strong read-your writes” consistency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To store data in the back-end object storage cluster, a unique filename
is chosen with the help of the *nonce* configuration so that each
“block” of storage have unique URLs and inherit the ”strong read-your
writes” property of the object storage backend.

Pool connections and writes to object storage connection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

How the writes are managed is with a pool of maintained connections to
an object API endpoint. One of these connections is chosen and the data
for this write is streamed to the backend storage.

This allows ProxyFS to mediate the comparatively small write sizes to
the larger object sizes by streaming multiple filesystem writes into a
single back-end object. This improves write performance as objects are
optimized for streaming, sequential writes.

More data is accumulated from the write into this open connection of
this file. Until one of two tings happen. First, is if the “max flush
size” has been triggered. Alternatively a timeout has been reached (“max
flush time”). Either one triggers that connection to close, and the data
is sorted in the back-end object storage. This file that is written is
called a log-segment.

Data is persisted in back-end object storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the back-end object storage, data must be stored in
containers/buckets. How many files that are stored in a given
container/bucket and the number of containers/buckets utilized is also
configurable.

What storage location is used is also configurable, which enable various
back-end storage policies to be used, or to enable swift-s3-sync to
archive data to other s3 storage targets.

Update filesystem metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~

The next thing that has to happen is that the filesystem metadata needs
to be updated. There is a new log-segment that has been stored that
represents new data in this volume. The filesystem needs to be updated
to reflect this new data. Any new inodes or extents need to be captured
in the filesystem metadata.

There is a data structure that represents the filesystem. It’s called
“headhunter”. Because each modification of a B+Tree affect all nodes in
the filesystem from the updated leaf node all the way up to the root
node. This means that the log-structured filesystems are updating the
“head” of the filesystem tree.

Persisting filesystem metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The filesystem metadata is persisted inthe back-end object storage.
ProxyFS utilizes the same tricks of creating unique objects using a
*noonce* so that any persistence of the filesystem metadata also can
take advantage of “strong read your writes for new data”

Checkpoints of the filesystem tree are created and “snapshotted” into
the back-end object storage. This ensures that any filesystem metadata
that is stored in the back-end storage represents a consistent view of
the filesystem.

A few methods are used to determine when a checkpoint of the filesystem
metadata should be stored in the back-end object storage.

One method is time-based. A checkpoint can be initiated and stored in
the back-end object storage at a configured interval. The default
conjuration is 10 seconds.

Additionally a checkpoint can be triggered when the filesystem client
asks for one. For example, if the client unmounts the filesystem the
client can ask for an explicit flush. Or another example is when after a
file write, the client asks for a special close/flush. This will also
trigger a checkpoint to be made of the filesystem tree and be stored in
the back-end object storage.

Checkpoints may also be a useful tool for management software to perform
various volume management functions such as moving volumes, shutting
down services, etc.

Replay log for Zero RPO
~~~~~~~~~~~~~~~~~~~~~~~

ProxyFS additionally utilizes replay logs as a method of keeping track
of changes to the filesystem. In addition to updating the B+Tree that
represents the filesystem, a small log is kept that contains only the
instructions on how to apply the filesystem metadata updates.

This small replay log does not store file data, just filesystem
metadata. This replay log is kept to ensure that no filesystem metadata
updates are lost if the system is rebooted or there is power loss.

Additionally, this replay log can be utilized by management software to
manage volume migrations, or assist in failover.

Object API Writes
-----------------

For object API writes with the AWS S3 or OpenStack Swift API, the Swift
Proxy provides access to the object storage back end. Object storage
manages its namespace with accounts and buckets/containers as its
namespace constructs. ProxyFS creates a volume for each account in the
system and the top-level directories map to buckets/containers.

Middleware powers many of the functions provided by the Swift Proxy
node. ProxyFS provides an additional middleware that enables any Swift
Proxy to read and write data for a ProxyFS-enabled account.

When a request is made to write data via the S3 or Swift API, the
ProxyFS middleware writes data using the log-structured data format
utilizing non-overlapping *nonce* to create uniquely-named segments.

In Swift, there is a Container (Bucket) service that needs to be
informed that there is a new object in its namespace. For ProxyFS
enabled accounts, rather than contacting the Container service, the
volume’s ProxyFS service is contacted to inform which new segments need
to be added to the filesystem namespace.

Multi-part upload APIs are accommodated by “coalescing” multiple parts
into a single file.

Filesystem Reads
----------------

How filesystem reads differ from object API reads is that object reads
are optimized for larger, sequential reads. Whereas filesystem reads may
read in smaller segments.

While object APIs do support range-read requests, it’s not necessarily
efficient to do very small frequent reads. ProxyFS will do a range read
request to “read ahead” of the filesystem client and cache data from the
log segment. The size of the “read ahead” is configureable. The total
size of the read cache is configurable and each volume can be configured
with a relative weight of that cache that each volume will utilize.

There is a separate pool of connections to the object storage backend
(the size of which is configureable to support various read patterns).

When a write is requested, the filesystem metadata translates the range
of data that maps back to the referenced inode to specific log-segments
in the back-end object storage. Data is cached and appropriate byte
ranges are served back to the client.

Object Reads
------------

When a read request is made, the object server doesn’t itself know how
to map the URI (/account/bucket/file) to log-segments in the backend
storage. The Proxy Server configured with the ProxyFS middleware will
query the volume’s ProxyFS server with the URI and in response be
provided with a “read plan” that contains the appropriate log segments
and byte ranges to respond to the read request.
