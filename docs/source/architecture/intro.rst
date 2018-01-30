ProxyFS Architecture Introduction
=================================

ProxyFS is a hierarchical file system that provides integrated file and
object API access for data stored with an object storage back-end.

.. image:: /_static/ProxyFS-file-object.png
 :width: 200pt

Not a Gateway
-------------

Rather than using a gateway to provide file-based access. ProxyFS is
directly integrated into the core of OpenStack Swift object storage to
enable direct AWS S3 and Swift object API access. This makes it
compatible with its corresponding projects for `S3
API <https://github.com/openstack/swift3>`__ and `public cloud
synchronization <https://github.com/swiftstack/swift-s3-sync>`__.

Multiple Modes of Access
------------------------

ProxyFS supports SMB and NFS along with AWS S3 and OpenStack Swift
object protocols. This enables filesystem clients to mount and
read/write data that is simultaneously read/write (GET/PUT/etc)
accessible to object API clients.

Full Filesystem
---------------

ProxyFS is a full filesystem. It is not filesystem-like. Full filesystem
operations are supported such as rename, move, growing files, modifying
data, etc.

ProxyFS is designed to support filesystem-based applications that could
benefit from leveraging cloud storage platforms.

Modernize Workflows
-------------------

ProxyFS enables file-based applications and workflows can coexist with
new applications that leverage object APIs such as AWS S3 / Swift API
and integrate with public cloud services.

Many workflows (and users!) that have increasing storage needs that are
rooted in the expectations of current filesystem access. However, these
workflows could benefit from the architecture and capabilities of object
and cloud storage.

ProxyFS enables the integration of object storage into existing
file-based workflows / applications while at the same time supporting
object APIs for newer, “cloud native” applications.

ProxyFS allows the same data that is accessed by S3 and Swift API
accessible by a Filesystem API. This provides a bridge to allow data
services for both existing applications and new applications.
