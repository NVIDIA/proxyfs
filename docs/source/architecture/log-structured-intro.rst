Log-Structured Filesystem
=========================

The ProxyFS filesystem takes advantage of the properties of its
underlying object storage platform.

Eventual Consistency and Strong Read-your-Writes
------------------------------------------------

Object storage systems utilize eventual consistency properties that help
enable its durability and availability properties. However, any new data
that is written into the system isn’t subject to eventual consistency.
This provides a strong read-your writes guarantee for new objects.

Log-Structured Files
--------------------

This strong read-your writes property enables a log-structured object
format. With this log-structured format, any data written is a new,
uniquely-named object in the back-end storage. A 64-bit nonce number
sequence that never repeats provides uniqueness.

When a file is modified through the filesystem, a new log segment is
written into the back-end storage that contains only the changes. Then,
the log-structure is updated with the new segment.

.. image:: /_static/ProxyFS-log-segments.png
 :width: 275pt


Log-structured objects store data in such a way that is great for
filesystem access and at the same time supports object API access. This
is very similar to the strategies used for large object support where a
manifest file is created for a multi-part object.

Log-Structured Filesystem
-------------------------

The filesystem tree is stored in a similar way as a log-structured data
itself. This filesystem tree can be snapshotted and stored into the
back-end storage, just like the file data.

Where filesystems and object APIs differ is that a filesystem is a
hierarchy, whereas an object API is a flat namespace.

When looking at the data through the filesystem, you will see folders,
directories and files. When using an object API, those folders become
slashes in the URL. The top-level directories simply map to
containers/buckets in the object API.
