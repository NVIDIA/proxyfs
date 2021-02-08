Access Modules & Interfaces
===========================

In this section we will discuss the various access methods for ProxyFS.


.. image:: /_static/ProxyFS-components.png
 :width: 575pt

Local filesystem access – FUSE
------------------------------

ProxyFS includes a FUSE module to provide a local filesystem capable of
being mounted on the system upon which a ProxyFS process is running.

NFS Access – nfsd
-----------------

The local FUSE mount point can be configured in conjunction with an nfsd
to export the filesystem volume via NFS.

ProxyFS API – JSON RPC interface
--------------------------------

In addition to providing a FUSE module for local filesystem, ProxyFS
includes a direct interface for other system components to more directly
interact with a volume. This interface is utilized directly for SMB
access via ProxyFS’s Samba VFS module. The Swift Proxy server processes
also leverage this API for direct access to the filesystem metadata tree
for object API (AWS S3 and Swift) access.

SMB Access – ProxyFS Samba VFS
------------------------------

ProxyFS provides a Samba VFS module written in C that integrates
directly with the Samba server processes. This module is provided in a
separate repository called
`proxyfs-vfs <https://github.com/NVIDIA/proxyfs-vfs>`__. A ProxyFS
JSON RPC client is provided in a separate repository called
`proxyfs-jrpc-client <https://github.com/NVIDIA/proxyfs-jrpc-client>`__.

The ProxyFS Samba VFS module utilizes the ProxyFS JSON RPC interface for
direct access into ProxyFS for a given volume.

Swift ProxyFS Middleware
------------------------

The Swift Proxy server is an interface to the object back end. The Proxy
server is responsible for the public-facing API requests for AWS S3 and
OpenStack Swift APIs. Much of the API functionality is provided through
Swift extensible middleware capabilities.

ProxyFS provides object API access through a set of middleware which
interfaces with the APIs provided by the ProxyFS JSON RPC interface.

This interaction is similar the ProxyFS Samba VFS module, however only
filesystem metadata is exchanged – *not* file data.
