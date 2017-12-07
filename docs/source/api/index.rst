=============================================
ProxyFS Bimodal Support of Object Storage API
=============================================

This document covers how ProxyFS supports the Swift Object Storage API listed at
http://developer.openstack.org/api-ref-objectstorage-v1.html


Bimodal access
==============

A primary of the motivation for ProxyFS was to have the ability to read and
write the same data in the same account whether accessing it via the object
API or file system API. Such access is possible via the ``pfs`` `proxy middleware
<https://github.com/swiftstack/ProxyFS/tree/development/pfs_middleware/>`__,
but there are numerous caveats.

Writing via object API
----------------------

- Containers in Swift map to top-level directories in the volume.
  Directory names are limited to 255 bytes (to match most filesystems); as a
  result, container names are limited to the lower of 255 or
  ``max_container_name_length``.
- When creating containers, the ``X-Storage-Policy`` header is ignored.
- Object names correspond to path components in the volume.

  - When creating objects that have slashes in their name (such as
    ``foo/bar/baz``), the leading sub-directories (in this case,
    ``foo/bar``) will be automatically created if they do not already
    exist. These are *not* automatically deleted when all objects below
    them are deleted.
  - You cannot delete or overwrite a directory object when there are
    still objects "within" it. Attempting to do so will result in a
    ``409 Conflict`` error response.  This complicates deleting entire
    containers with ``swift delete <container>`` or ``swift delete --all``.
  - All "parent directories" in an object's name must either not
    already exist or already exist as directories. You cannot create an
    object if one of its parent directories is in fact an object.
    Attempting to do so results in a ``409 Conflict`` error response.
  - Again, directory names are limited to 255 bytes, so no parent
    directory component may be longer. Attempting to create paths with
    longer components results in a ``400 Bad Request`` error response.
  - Ordinarily, object names are opaque strings, and the Swift paths
    ``/v1/AUTH_test/foo/bar``, ``/v1/AUTH_test/foo/./bar``,
    ``/v1/AUTH_test/foo//bar``, and ``/v1/AUTH_test/foo/baz/../bar`` are
    all distinct. In a filesystem, ``.`` and ``..`` have special meanings,
    which would lead these to all map to the same file. To prevent
    unintentional overwrites, only the first name is valid in a ProxyFS
    account. Attempting to create an object at the other names results in
    a ``400 Bad Request`` error response.

- Object ETags (MD5 digests) are written as sysmeta, similar to what was
  done for Swift's encryption and SLO features. If the client provides an
  ETag header in a PUT request, it will be verified before finalizing the
  write.
- Object metadata must be valid UTF-8. Ordinarily, Swift only requires
  UTF-8 for account and container metadata.
- Object creation does not support the ``If-None-Match:*`` header.
- Objects do not support expiration (though the ``X-Delete-At`` and
  ``X-Delete-After`` headers will be written down).


Reading objects via the object API
----------------------------------

- Account-level container counts, object counts, and total bytes used
  reflect the storage of the underlying ProxyFS log segments and
  checkpoints. As such, the counts are not particularly useful and the
  bytes-used is only a crude approximation.
- When listing an account, ``delimiter``, ``prefix``, and ``reverse``
  query params are ignored.
- When querying containers, the ``X-Storage-Policy`` header returns the
  default storage policy for the cluster, which is not necessarily the
  storage policy being used for log segment containers or the checkpoint
  container.
- Container-level object counts and total bytes used are always 0.
- When listing a container, the ``reverse`` query parameter is ignored.
  Only ``/`` is supported for ``delimiter``.
- Auto-created directory objects are presented as zero-byte objects
  with ``Content-Type: application/directory``.


Reading objects via the file API
--------------------------------

- Account, container, and object metadata are inaccessible via the file API.
- Large objects are only assembled via the object API. DLOs or SLOs, when
  accessed through the filesystem API, will be zero-byte files or JSON
  (respectively), similar to what happens when including a
  ``?multipart-manifest=get`` query parameter via the object API.
- Swift "symlink" objects are only followed when accessed through the object
  API. When accessed through the filesystem API, these will be zero-byte
  files.

Reading files via the object API
--------------------------------

- Since containers map to directories, files written at the root of the
  volume are not accessible via the object API.
- Since files may not have been written sequentially, the ETag *will
  not* be the MD5 digest of the contents. It should be considered "an
  opaque validator for differentiating between multiple representations
  of the same resource" as `RFC 2616
  <https://tools.ietf.org/html/rfc2616.html#section-13.3.3>`__ and
  `RFC 7232 <https://tools.ietf.org/html/rfc7232#section-2.3>`__
  intended.

.. note::
  Filesystem access to SLO segment data can invalidate the
  manifest, as the segment ETag may change.


Swift functionality broken as of 1.3
====================================

- Account and container quotas are mostly worthless.
- No CORS support.
- No support for expiring objects.


Swift functionality bypassed by filesystem access
=================================================

- Account and container ACLs are not enforced. ProxyFS uses an entirely
  separate authentication and authorization system.
- Object versioning is only enforced when overwrites and deletes happen
  through the object API. ProxyFS is not `VMS
  <https://en.wikipedia.org/wiki/Files-11>`__.

Swift functionality not thoroughly tested
=========================================

- Form POST
- Bulk upload/delete
- Static web
- Versioning
- Encryption
- Symlinks
