# Swift ProxyFS RPC client

ProxyFS provides a number of APIs to POSIX clients over JSON RPC.

This document describes each of the ProxyFS RPC client APIs provided in libproxyfs.so.

## General approach for returning errno from the ProxyFS RPC client

Errors we **will** support:

* ENOMEM may be returned from the RPC client library itself, but will not be returned by ProxyFS as the response to an RPC.

* ENOTDIR will be returned when the operation is expected to act on a directory but the file was not a directory.

* EISDIR will be returned when the operation is expected to act on a non-directory but the file was a directory.

* EEXIST will be returned when an operation is expected to create a file, directory or symlink but that file already exists.

* EIO will be returned when an error occurred accessing data from Swift.

* The POSIX APIs are a little inconsistent in what errno is used to indicate that the requested file/dir/symlink was not found.

    * For many APIs, ENOENT is used. But some APIs expect EBADF, and some support both. In general, ENOENT is used for path-based APIs and EBADF is used for fd-based APIs (which correspond to inode-based APIs for ProxyFS).

    * To make error handling on the ProxyFS side simpler, ENOENT will be returned for all affected APIs. The RPC client code will translate ENOENT to EBADF for the three APIs that require it (read, write, flush).

* EINVAL

    * This error can be returned by a handful of functions, indicating that an error was detected with the parameters provided for the operation.

    * In addition, **we will be using EINVAL to indicate that an invalid mount ID was provided**. This is a little outside the normal definition of EINVAL, however EINVAL is widely accepted and we want to be able to distinguish a bad mount from an invalid file/directory/symlink for the caller.


Errors we **do not** support **currently, but may support later**:

* EPERM and EACCESS (related to file access permissions)

* EROFS (read-only filesystem)

* EBUSY (open files)


Errors we **will not** support, because they don't make sense in our context:

* ENOTBLK (We are not providing a block device)

* EWOULDBLOCK (We don't support O_NONBLOCK)

* EAGAIN (We don't support O_NONBLOCK or mount with an expire option) 

* ENXIO (We only support ProxyFS device)

* ENODEV (ProxyFS device always exists)

* EINTR (Doesn't make sense for remote filesystem)

* EMLINK (Not enforcing maximum link count)

* ELOOP (Not counting symlinks)

* EOVERFLOW (We do not enforce a maximum file/read size)

* ENAMETOOLONG (Not enforcing maximum file name length)

* EDQUOT (Swift will not support quota limitations)

* ENOSPC (Swift will not run out of space)

* EFAULT

    * This error could be generated when a fullpath refers to something outside the proxyfs/swift namespace. However since ProxyFS code assumes that all fullpaths are relative to the mount point, this is not a condition that we can really detect. As a result, we won't be supporting this errno. 



## Functionality that will be supported by ProxyFS in future

In order to support all the errors expected by POSIX clients, we would need to support the following functionality in ProxyFS. 

* Support for read-only mount option

    * This will allow us to support EROFS (read-only filesystem)

    * Swift does not support this functionality, so it would need to be implemented in ProxyFS.

    * We will support this functionality; timeframe is TBD

* Support for some kind of permissions checking

    * This will allow us to support EPERM/EACCESS

    * We will support this functionality; timeframe is TBD

* Support for open/close semantics

    * This would be needed in order to support being able to determine if files/objects are currently being used by Samba. If they are, we should not support unlink/remove operations on those inodes, and they should not be garbage collected.

    * EBUSY - In general, this error is returned when some action such as unmount, unlink or rename cannot be completed because the target is the working directory of some thread, the mount point of another device, has open files, etc. We cannot return this error unless we keep track of these things in ProxyFS.



-----

# API Descriptions
## [chmod](chmod) - change permissions of a file
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_chmod(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  mode_t          in_mode);

int proxyfs_chmod_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath,
                       mode_t          in_mode);
```

### DESCRIPTION
Change permissions of a file.

The inital permissions of the file/directory are set when it is created with [create](#create) or [mkdir](#mkdir).

The caller can only use this API to change the file permissions part of the mode, i.e. bits 0777. The mode bits that indicate file/directory/symlink are set by the filesystem.

While file mode owner/group can be stored and retrieved from the filesystem, any access checking must be done by the caller. Specifically, validity of owner and group is not checked, and file access is not checked by the filesystem.

**NOTE on symlinks -
If this API is called on a symlink, currently it will change permissions for the symlink itself, and not the symlink target.**


### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENAMETOOLONG** The basename or fullpath was too long.

**ENOENT** The named file or inode does not exist.

**EEXIST** Basename already exists.

### SEE ALSO
[mount](#mount), [create](#create), [mkdir](#mkdir), [getstat](#getstat)

-----

## [chown](chown) - change ownership of a file
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_chown(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  uid_t           in_owner,
                  gid_t           in_group);

int proxyfs_chown_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath,
                       uid_t           in_owner,
                       gid_t           in_group);
```

### DESCRIPTION
Change the ownership of a file to the specified userid and groupid.

If the owner or group is specified as -1, then that ID is not changed.

The inital ownership of the file/directory is set when it is created with [create](#create) or [mkdir](#mkdir), using the userid and groupid that is specified in [mount](#mount).

While file mode owner/group can be stored and retrieved from the filesystem, any access checking must be done by the caller. Specifically, validity of owner and group is not checked, and file access is not checked by the filesystem.


### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENAMETOOLONG** The basename or fullpath was too long.

**ENOENT** The named file or inode does not exist.

**EEXIST** Basename already exists.

### SEE ALSO
[mount](#mount), [create](#create), [mkdir](#mkdir), [getstat](#getstat)

-----
## [create](id:create) - create a file
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_create(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   char*           in_basename,
                   mode_t          in_mode,
                   uint64_t*       out_inode_number);

int proxyfs_create_path(mount_handle_t* in_mount_handle,
                        char*           in_fullpath,
                        mode_t          in_mode,
                        uint64_t*       out_inode_number);
```

### DESCRIPTION
Create a file. File permissions are specified by the **in_mode** parameter.

The caller can set the file permissions part of the mode, i.e. bits 0777. The mode bits that indicate file/directory/symlink are set by the filesystem.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENAMETOOLONG** The basename or fullpath was too long.

**ENOENT** The named file or inode does not exist.

**EEXIST** Basename already exists.

**ENOMEM** Insufficient kernel memory was available.

### SEE ALSO
[mount](#mount), [lookup](#lookup)

-----

## [flush](id:flush)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_flush(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number);

```
### DESCRIPTION
### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EBADF** Inode does not exist.

**EIO** An error occurred during synchronization.

### SEE ALSO
[mount](#mount), [write](#write), [read](#read), [lookup](#lookup)

-----

## [getstat](id:getstat)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_get_stat(mount_handle_t*  in_mount_handle,
                     uint64_t         in_inode_number,
                     proxyfs_stat_t** out_stat);

int proxyfs_get_stat_path(mount_handle_t*  in_mount_handle,
                          char*            in_fullpath,
                          proxyfs_stat_t** out_stat);

typedef struct {
    uint64_t sec;        // seconds
    uint64_t nsec;       // nanoseconds
} proxyfs_timespec_t;

typedef struct {
    uint64_t           mode;   // protection (as well as file type)
    uint64_t           ino;    // inode number
    uint64_t           dev;    // ID of device containing file
    uint64_t           nlink;  // number of hard links
    uint64_t           uid;    // user ID of owner
    uint64_t           gid;    // group ID of owner
    uint64_t           size;   // total size, in bytes
    proxyfs_timespec_t atim;   // time of last access
    proxyfs_timespec_t mtim;   // time of last modification
    proxyfs_timespec_t ctim;   // time of last attribute change
    proxyfs_timespec_t crtim;  // creation time
} proxyfs_stat_t;
```

### DESCRIPTION
The **proxyfs_get_stat()**/**proxyfs_get_stat_path()** function returns a pointer to a new stat structure **out_stat**. Memory for the structure is obtained with **malloc()**, and can be freed with **free()**.

**NOTE regarding mode:**
     We currently set the file type bitmask but not any of the permissions bits.
     File type will be one of S_IFDIR|S_IFREG|S_IFLINK.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**ENOMEM** Out of memory (i.e., kernel memory).

### SEE ALSO
[mount](#mount)

-----

## [link](id:link)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_link(mount_handle_t* in_mount_handle,
                 uint64_t        in_inode_number,
                 char*           in_basename,
                 uint64_t        in_target_inode_number);

int proxyfs_link_path(mount_handle_t* in_mount_handle,
                      char*           in_src_fullpath,
                      char*           in_tgt_fullpath);

```
### DESCRIPTION
### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**ENOTDIR** in_inode_number is not a directory.

**EEXIST** Link target already exists.

**EIO** An I/O error occurred.

**ENOMEM** Insufficient kernel memory was available.

**EPERM**  in_src_fullpath is a directory.

### SEE ALSO
[mount](#mount), [lookup](#lookup)

-----

## [lookup](id:lookup)

This API doesn't correspond directly with a POSIX filesystem API, but is called by Samba VFS for several operations. The errnos listed here are an amalgamation of the errnos for those APIs.

### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_lookup(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   char*           in_basename,
                   uint64_t*       out_inode_number);

int proxyfs_lookup_path(mount_handle_t* in_mount_handle,
                        char*           in_fullpath,
                        uint64_t*       out_inode_number);

```
### DESCRIPTION
The **proxyfs_lookup()** and **proxyfs_lookup_path()** functions return the inode number that corresponds to the provided inode and basename, or to the provided fullpath, respectively.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.
### SEE ALSO
[mount](#mount)

-----

## [mkdir](id:mkdir)
```
#include <proxyfs.h>

int proxyfs_mkdir(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  char*           in_basename,
                  mode_t          in_mode,
                  uint64_t*       out_inode_number);

int proxyfs_mkdir_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath,
                       mode_t          in_mode);
```
### SYNOPSIS
### DESCRIPTION
Create a directory. Directory permissions are specified by the **in_mode** parameter.

The caller can set the file permissions part of the mode, i.e. bits 0777. The mode bits that indicate file/directory/symlink are set by the filesystem.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENAMETOOLONG** The basename or fullpath was too long.

**ENOENT** The named file or inode does not exist.

**EEXIST** pathname already exists (not necessarily as a directory).

**ENOTDIR** Inode does not refer to a directory.

**EIO** An I/O error occurred while writing to the filesystem.

**ENOMEM** Insufficient kernel memory was available.

### SEE ALSO
[mount](#mount), [create](#create)

-----

## [mount](id:mount)
```
#include <proxyfs.h>

int proxyfs_mount(char*            in_volume_name,
                  uint64_t         in_mount_options,
                  char*            in_auth_user,
                  uid_t            in_uid,
                  gid_t            in_gid,
                  mount_handle_t** out_mount_handle);
```
### SYNOPSIS
Do a mount. If successful, returns non-null handle.

A mount must be done before calling most of the functions described here, in that the functions take a mount handle returned from this call.

### DESCRIPTION
**in_volume_name** Name of the volume to be mounted

**in_mount_options** Mount options. This parameter is currently not supported.

**in_auth_user** Username of the user doing the mount

**in_uid** Unix userid of the user doing the mount

**in_gid** Unix groupid of the user doing the mount

**out_mount_handle** Mount handle returned from this function, if successful. This handle should then be passed to other functions which require a mount handle.


The **uid**/**gid** are used as the owner if **proxyfs_create()**, **proxyfs_mkdir()**, or **proxyfs_symlink()** are called, and are what will be returned if a **proxyfs_get_stat()** is later done on that file/directory/symlink. 


### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid parameter.

**ENODEV** Unable to connect to the filesystem. This error is returned from the ProxyFS RPC client, if it is unable to connect to the ProxyFS daemon over RPC.

**ENOENT** Volume name was not found.

**ENOMEM** The kernel could not allocate a free page to copy filenames or data into.

### SEE ALSO
[unmount](#unmount)

-----

## [read, synchronous](id:read)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_read(mount_handle_t* in_mount_handle,
                 uint64_t        in_inode_number,
                 uint64_t        in_offset,
                 uint64_t        in_length,
                 uint8_t*        in_bufptr,
                 size_t          in_bufsize,
                 size_t*         out_bufsize);
```
### DESCRIPTION
Synchronous (blocking) read.


Caller allocates a buffer to be filled in and passes the buffer pointer and buffer size and in_bufptr and in_bufsize, respectively. The number of bytes written to the buffer is returned in out_bufsize.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**EBADF** Inode not found.

**EIO** I/O error. 

**EISDIR** Inode refers to a directory.

### SEE ALSO
[mount](#mount), [write](#write), [flush](#flush), [lookup](#lookup)

-----
## [read, asynchronous](id:read_async)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_read_send(mount_handle_t* in_mount_handle,
                      uint64_t        in_inode_number,
                      uint64_t        in_offset,
                      uint64_t        in_length,
                      uint8_t*        in_bufptr,
                      size_t          in_bufsize,
                      void*           in_request_id,
                      void            (*in_done_callback)(void* in_request_id);

int proxyfs_read_recv(void*   in_request_id,
                      int*    out_rsp_status,
                      size_t* out_bufsize);
```
### DESCRIPTION
Asynchronous (non-blocking) read.


***proxyfs_read_send***

Caller allocates a buffer to be filled in and passes the buffer pointer and buffer size in **in_bufptr** and **in_bufsize**, respectively. The **in_request_id** must uniquely identify the request, since it will be used to match request and response when **proxyfs_read_recv** is called.

The send of the request is done in the caller's context. If success is returned, that means the request was successfully sent. When the read response is received, the **in_done_callback** will be called. Then a call to **proxyfs_read_recv** can be done. However if a non-success value is returned, the request has failed and a call to **proxyfs_read_recv** will return ENOENT.

**NOTE**: The function passed into **in_done_callback** is expected to be nonblocking.


***proxyfs_read_recv***

Blocking call to receive a response corresponding to a **proxyfs_read_send**. Request and response are matched using **in_request_id**, which must uniquely identify the request.

If the corresponding response has already been received, its status will be returned in **out_rsp_status**. If not, an error will be returned.

This API will return the response status in **out_rsp_status**, and the number of bytes written to the buffer provided in **proxyfs_read_send** is returned in **out_bufsize**.



### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**EBADF** Inode not found.

**EIO** I/O error. 

**EISDIR** Inode refers to a directory.

**ENOENT** Read data corresponding to the in_request_id specified by proxyfs_read_recv cannot be found.

### SEE ALSO
[mount](#mount), [write](#write_async), [flush](#flush), [lookup](#lookup)

-----

## [readdir](id:readdir), [readdir_plus](id:readdir_plus)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_readdir(mount_handle_t* in_mount_handle,
                    uint64_t        in_inode_number,
					int64_t         in_prev_dir_loc,
                    struct dirent** out_dir_ent);

int proxyfs_readdir_plus(mount_handle_t*  in_mount_handle,
                         uint64_t         in_inode_number,
					     int64_t          in_prev_dir_loc,
                         struct dirent**  out_dir_ent,
                         proxyfs_stat_t** out_dir_ent_stats);

```
### DESCRIPTION
The **proxyfs_readdir()**/**proxyfs_readdir_plus** function returns a pointer to a single dirent structure **out_dir_ent**. Memory for the structure is obtained with **malloc()**, and can be freed with **free()**.

The **proxyfs_readdir_plus()** function returns both directory entries and stats. It returns a pointer to a new stat structure **out_dir_ent_stats**. Memory for the structure is obtained with **malloc()**, and can be freed with **free()**.

The **in_prev_dir_loc** parameter is used to determine which directory entry will be returned. The location which was read is returned in out_dir_ent->d_off. To read the first entry in a directory, set **in_prev_dir_loc** to -1.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**ENOTDIR** File descriptor does not refer to a directory.

### SEE ALSO
[mount](#mount), [lookup](#lookup), [getstat](#getstat)

-----

## [read_symlink](id:read_symlink)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_read_symlink(mount_handle_t* in_mount_handle,
                         uint64_t        in_inode_number,
                         const char**    out_target);

int proxyfs_read_symlink_path(mount_handle_t* in_mount_handle,
                              char*           in_fullpath,
                              const char**    out_target);

```
### DESCRIPTION
The **proxyfs_read_symlink()**/**proxyfs_read_symlink_path()** function returns a pointer to a new string **out_target**. Memory for the string is obtained with **malloc()**, and can be freed with **free()**.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**EINVAL** The named file is not a symbolic link.

**EIO** An I/O error occurred while reading from the filesystem.

**ENOMEM** Insufficient kernel memory was available.

### SEE ALSO
[mount](#mount), [symlink](#symlink), [lookup](#lookup)

-----

## [rename](id:rename)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_rename(mount_handle_t* in_mount_handle,
                   uint64_t        in_src_dir_inode_number,
                   char*           in_src_basename,
                   uint64_t        in_dst_dir_inode_number,
                   char*           in_dst_basename);

// Path-based rename
int proxyfs_rename_path(mount_handle_t* in_mount_handle,
                        char*           in_src_fullpath,
                        char*           in_dst_fullpath);

```
### DESCRIPTION
### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**EIO** A physical I/O error has occurred.

**ENOENT** The named file or inode does not exist.

**ENOTDIR** A component used as a directory in in_src_fullpath or in_dst_fullpath is not, in fact, a directory. Or, in_src_fullpath is a directory, and in_dst_fullpath exists but is not a directory.

**ENOMEM** Insufficient kernel memory was available.

### SEE ALSO
[mount](#mount), [create](#create), [lookup](#lookup)

-----

## [resize](id:resize)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_resize(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   uint64_t        in_new_size);

```
### DESCRIPTION
### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**EIO** An I/O error occurred updating the inode.

**EISDIR** The named file is a directory.

### SEE ALSO
[mount](#mount), [lookup](#lookup), [getstat](#getstat)

-----

## [rmdir](id:rmdir)
```
#include <proxyfs.h>

int proxyfs_rmdir(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  char*           in_basename);

int proxyfs_rmdir_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath);
```
### SYNOPSIS
### DESCRIPTION
### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**ENOTDIR** Inode or fullpath is not, in fact, a directory.

**ENOTEMPTY** Inode or fullpath contains entries other than *.* and *..* . 

### SEE ALSO
[mount](#mount), [mkdir](#mkdir)

-----

## [settime](id:settime) - Set atime and mtime for a file
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_settime(mount_handle_t*      in_mount_handle,
                    uint64_t             in_inode_number,
                    proxyfs_timespec_t*  in_atim,
                    proxyfs_timespec_t*  in_mtim);

int proxyfs_settime_path(mount_handle_t*      in_mount_handle,
                         char*                in_fullpath,
                         proxyfs_timespec_t*  in_atim,
                         proxyfs_timespec_t*  in_mtim);

```
### DESCRIPTION
The **proxyfs_settime()**/**proxyfs_settime_path()** functions can be used to set atime and mtime for a file. This is our version of **utimens()**.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**EIO** An error updating the inode.

**ENOENT** The named file or inode does not exist.

### SEE ALSO
[mount](#mount), [getstat](#getstat)

-----

## [statvfs](id:statvfs) - Return file system stats <not implemented yet>
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_statvfs(mount_handle_t*  in_mount_handle,
                    struct statvfs** out_statvfs);
```
### DESCRIPTION
The **proxyfs_statvfs()** function returns filesystem statistics. It returns a pointer to a new statvfs structure **out_statvfs**. Memory for the structure is obtained with **malloc()**, and can be freed with **free()**.

**NOTE: This API is not functional yet.**

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**EIO**    An I/O error occurred while reading from the filesystem.

### SEE ALSO
[mount](#mount)

-----

## [symlink](id:symlink)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_symlink(mount_handle_t* in_mount_handle,
                    uint64_t        in_inode_number,
                    char*           in_basename,
                    char*           in_target);

int proxyfs_symlink_path(mount_handle_t* in_mount_handle,
                         char*           in_fullpath,
                         char*           in_target_fullpath);

```
### DESCRIPTION
Create a symlink. File permissions for symlinks are defaulted to 0777 by the filesystem.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.

### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**EEXIST** New path already exists.

**EIO** An I/O error occurred.

**ENOTDIR** A component used as a directory in in_fullpath or in_target_fullpath is not, in fact, a directory. Or, in_fullpath is a directory, and in_target_fullpath exists but is not a directory.

**ENOMEM** Insufficient kernel memory was available.

### SEE ALSO
[mount](#mount), [read_symlink](#read_symlink), [lookup](#lookup)

-----

## [type](id:type)

### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_type(mount_handle_t* in_mount_handle,
                 uint64_t        in_inode_number,
                 uint16_t*       out_file_type);
```
### DESCRIPTION
The **proxyfs_type()** function returns the file type for the specified inode number. The type returned will be one of **DT_DIR|DT_REG|DT_LNK**, as defined in **<dirent.h>**.


**NOTE: This API is not currently called by Samba VFS, and doesn't correspond directly to a POSIX API.**


### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.
### SEE ALSO
[mount](#mount)

-----

## [unlink](id:unlink)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_unlink(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   char*           in_basename);

int proxyfs_unlink_path(mount_handle_t* in_mount_handle,
                        char*           in_fullpath);

```
### DESCRIPTION
### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**ENOENT** The named file or inode does not exist.

**EIO** An I/O error occurred.

**EISDIR** pathname refers to a directory. (This is the non-POSIX value returned by Linux since 2.1.132.)

**ENOMEM** Insufficient kernel memory was available.

### SEE ALSO
[mount](#mount), [create](#create), [link](#link), [symlink](#symlink), [rmdir](#rmdir)

-----

## [unmount](id:unmount)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_unmount(mount_handle_t* in_mount_handle);
```
### DESCRIPTION
Releases reference on mount and underlying RPC context
### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** target is not a mount point.

**EINVAL** Invalid parameter.

**ENOMEM** The kernel could not allocate a free page to copy filenames or data into.

### SEE ALSO
[mount](#mount)

-----

## [write, synchronous](id:write)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_write(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  uint64_t        in_offset,
                  uint8_t*        in_bufptr,
                  size_t          in_bufsize,
```
### DESCRIPTION
Synchronous (blocking) write.

### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**EBADF** fd is not a valid file descriptor or is not open for writing.

**EIO** A low-level I/O error occurred while modifying the inode.

### SEE ALSO
[mount](#mount), [read](#read), [flush](#flush), [lookup](#lookup)

-----
## [write, asynchronous](id:write_async)
### SYNOPSIS
```
#include <proxyfs.h>

int proxyfs_write_send(mount_handle_t*  in_mount_handle,
                       uint64_t         in_inode_number,
                       uint64_t         in_offset,
                       uint8_t*         in_bufptr,
                       size_t           in_bufsize,
                       void*            in_request_id,
                       void             (*in_done_callback)(void* in_request_id);

int proxyfs_write_recv(void*   in_request_id,
                       int*    out_rsp_status,
                       size_t* out_size);
```
### DESCRIPTION
Asynchronous (non-blocking) write.


***proxyfs_write_send***


Caller passes the write buffer pointer and buffer size in **in_bufptr** and **in_bufsize**, respectively. The **in_request_id** must uniquely identify the request, since it will be used to match request and response when **proxyfs_write_recv** is called.

The send of the request is done in the caller's context. If success is returned, that means the request was successfully sent. When the write response is received, the **in_done_callback** will be called. Then a call to **proxyfs_write_recv** can be done. However if a non-success value is returned, the request has failed and a call to **proxyfs_write_recv** will return ENOENT.


**NOTE**: in_done_callback should be nonblocking.


***proxyfs_write_recv***


Blocking call to receive a response corresponding to a **proxyfs_write_send**. Request and response are matched using **in_request_id**, which must uniquely identify the request.

If the corresponding response has already been received, its status will be returned in **out_rsp_status**, and the number of bytes written will be returned in **out_size**. If the response has not been received, an error will be returned.


### RETURN VALUE
Upon successful completion 0 is returned.  Otherwise, an errno is returned.
### ERRORS
**EINVAL** Invalid mount point.

**EINVAL** Invalid parameter.

**EBADF** fd is not a valid file descriptor or is not open for writing.

**EIO** A low-level I/O error occurred while modifying the inode.

**ENOENT** Write data corresponding to the in_request_id specified by proxyfs_write_recv cannot be found.

### SEE ALSO
[mount](#mount), [read](#read_async), [flush](#flush), [lookup](#lookup)

-----

# Installation
Linux:

<!--- shell should render as a shell command, not print "shell". Odd. --->
```shell
make all
```

```shell
make install
```


# Release History

* 0.0.1
	* Initial release
	

# Meta
Distributed under the MIT license. 	







