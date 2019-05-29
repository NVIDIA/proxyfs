ProxyFS: A write-journaled, extent-based filesystem
===================================================

ProxyFS is a write-journaled, extent-based filesystem that utilizes
properties of an object storage backend. In this section we will cover
the motivation and rationale behind the ProxyFS architecture.

Background
----------

Traditional filesystems utilize mutable blocks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filesystems are typically architected to make use of a set of one or
more block storage devices each presenting the property that a write of
a block followed by a read of that same block will return the written
contents. A subsequent overwrite of that block followed by a second read
will return the contents of that second write. Such file systems make
use of this property as they modify in place the contents of the
underlying block storage to maintain application data and file system
metadata.

Scale-out architectures leverage eventually consistent properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Efforts to satisfy the ever growing storage capacity needs have led to
the development of what are termed “scale-out” architectures. Such
systems are characterized by independent nodes cooperating to present a
single system view albeit with several challenges. The biggest challenge
is keeping all the nodes in sync with that single system view. So called
“eventually consistent” solutions are designed to push the limits of
scale by relaxing this consistency goal in various ways.

Object APIs leverage scale-out architectures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A third storage trend has been the advent of object storage. In this
model, arbitrarily sized objects support a PUT, GET, and DELETE model
where the atomicity of block storage is retained but at the object
level. Various eventually consistent object storage systems are now
available (e.g. Amazon S3, OpenStack Swift, etc…). In such systems,
objects are identified by a Universal Resource Locator, or URL.

Shifting applications from the use of file systems to object storage is
complicated when various features innate to file systems but not present
in the chosen object storage system are a requirement. While the concept
of an object shares a lot with that of a file, there are important
differences. For one, operations on objects typically preclude
modification of their contents short of rewriting the entire object.
File systems also typically support reorganization by means of some sort
of rename or move operation. Often, the means by which scale-out storage
systems are able to scale so well has been at the cost of dropping
support for this rename capability.

Enabling a transition for simultaneous filesystem and object access
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

And yet, the advantages of moving from a traditional file system to a
scale-out eventually consistent object storage solution are compelling.
A transition strategy enabling applications currently accessing their
data via file system semantics to move to object storage is needed. Of
all the challenges in providing this transition (there are many), one of
the biggest is the need of the file system to deal with the eventual
consistency property of an underlying object storage solution.

PUT-ting a strategy together
----------------------------

Consider two successive writes (PUTs) of the same URL. In an eventually
consistent object storage solutions, a subsequent read (GET) may return
either version of the object. This could be due to a preferred location
to store the object being temporarily unavailable during some part of
the write sequence.

Using unique URLs
-----------------

Next consider that the first write was to a new URL. No second write has
ever been performed to that URL, hence a subsequent read (GET) could
only return the first write’s data (or an error due to unavailability of
course). Thus there is no confusion as to which version of an object’s
data will be returned.

A filesystem would typically map itself logically onto a set of blocks
of storage. As blocks become unreferenced (e.g. due to a file’s
deletion), those blocks would be returned to a pool and re-used later.
If a file is modified, it is also possible to modify the underlying
blocks storing the data for that file directly. In either case,
confusion as to which version of the contents of a block would be fatal
to a file system.

The solution is to take advantage of the arbitrarily large namespace
offered by the URL model of the object store system by only ever writing
to a given URL once. In this way, a file system will not be confused by
the underlying storage system returning stale versions of file system
data.

A filesystem are represented by inodes and extents
--------------------------------------------------

A filesystem must represent a number of data structures that are
persisted in the underlying storage system. The basic element of a file
system is termed an inode. An inode is a representation of familiar file
system concepts such as files and directories (though there are
typically other types of inodes supported). Each inode has associated
metadata typically of a fixed size. For file inodes, there is also a
mapping from logical extents (starting offset and length) to locations
in the storage system. For directory inodes, there is also a list of
mappings from names to inodes representing the directory’s contents. The
set of inodes as well as the contents of each file and directory inode
can be of arbitrary size and must support modification.

ProxyFS utilizes a B+Tree for filesystem inodes and extents
-----------------------------------------------------------

Support for arbitrary size in each of the above can be accomplished by
many means. ProxyFS utilizes a B+Tree structure. Lookups, insertions,
modifications, and deletions to a B+Tree structure are able to make
efficient use of in-memory caching to achieve high performance with the
ability to scale massively.

The drawback of the B+Tree structure is in how data is typically
persisted in the underlying storage. B+Trees attempt to keep the nodes
the tree below a maximum size such that they may be modified in place.
When the underlying storage is an eventually consistent object storage
system, modifications in place will be problematic due to this lack of
strong consistency. Instead, ProxyFS uses a write-journaling approach to
select a unique object into which updates are made. Each modification of
a B+Tree must therefore affect all nodes from the updated leaf node all
the way up to the root node. When these are each written to the object
storage system, only the root node’s location need be located in order
to access a consistent view of the filesystem element (be that a file or
directory).

Like any write-journalling system, portions of the journal become
unreferenced. For ProxyFS, the unreferenced elements are portions of
objects or entire objects. Garbage collection operations include
compaction (where the still referenced portions of objects are copied to
new objects) and simple object deletion (for objects not currently
referenced). Such activity must be balanced with the workload being
applied to both the file system and the underlying storage system.
