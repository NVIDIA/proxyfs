#ifndef __PROXYFS_H__
#define __PROXYFS_H__

/*******************************************************************
 Header file with FS RPC API C-language struct and function definitions
 *******************************************************************/

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <dirent.h>
#include <sys/statvfs.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/queue.h>

// Forward declaration so that we don't have to include the real definition
// of jsonrpc_handle_t.
struct rpc_handle_t;
typedef struct rpc_handle_t jsonrpc_handle_t;

#define MAX_VOL_NAME_LENGTH   128
#define MAX_USER_NAME_LENGTH  128

typedef struct {
    jsonrpc_handle_t* rpc_handle;
    uint64_t          mount_id;
    uint64_t          root_dir_inode_num;
    char              volume_name[MAX_VOL_NAME_LENGTH];
    uint64_t          mount_options;
    uint64_t          auth_user_id;
    uint64_t          auth_group_id;
    char              auth_user[MAX_USER_NAME_LENGTH];
} mount_handle_t;

// NOTE: Both CIFS and NFS need stats to be in sys/stat.h format, i.e. like
//       this (man lstat):
//
// Source: https://en.wikipedia.org/wiki/Stat_(system_call)
//
//  struct stat {
//      mode_t    st_mode;    /* protection */
//      ino_t     st_ino;     /* inode number */
//      dev_t     st_dev;     /* ID of device containing file */
//      dev_t     st_rdev;    /* device ID (if special file) */      // optional in POSIX.1
//      nlink_t   st_nlink;   /* number of hard links */
//      uid_t     st_uid;     /* user ID of owner */
//      gid_t     st_gid;     /* group ID of owner */
//      off_t     st_size;    /* total size, in bytes */
//      struct timespec  st_atim;   /* time of last access */
//      struct timespec  st_mtim;   /* time of last modification */
//      struct timespec  st_ctim;   /* time of last status change */
//      blksize_t st_blksize; /* blocksize for filesystem I/O */     // optional in POSIX.1
//      blkcnt_t  st_blocks;  /* number of 512B blocks allocated */  // optional in POSIX.1
//  };
//
// Where: struct timespec {
//            time_t   tv_sec;        /* seconds */
//            long     tv_nsec;       /* nanoseconds */
//        };
//
typedef struct {
    uint64_t sec;        /* seconds */
    uint64_t nsec;       /* nanoseconds */
} proxyfs_timespec_t;

// NOTE: The following fields will not be supported with this API:
//
//      dev_t     st_rdev;    /* device ID (if special file) */      // optional in POSIX.1
//      blksize_t st_blksize; /* blocksize for filesystem I/O */     // optional in POSIX.1
//      blkcnt_t  st_blocks;  /* number of 512B blocks allocated */  // optional in POSIX.1
//
typedef struct {
    uint32_t           mode;   /* protection (as well as file type) */
    uint64_t           ino;    /* inode number */
    uint64_t           dev;    /* ID of device containing file */
    uint64_t           nlink;  /* number of hard links */
    uint32_t           uid;    /* user ID of owner */
    uint32_t           gid;    /* group ID of owner */
    uint64_t           size;   /* total size, in bytes */
    proxyfs_timespec_t atim;   /* time of last access */
    proxyfs_timespec_t mtim;   /* time of last modification */
    proxyfs_timespec_t ctim;   /* time of last attribute change */
    proxyfs_timespec_t crtim;  /* creation time */
} proxyfs_stat_t;

// TBD: async IO related operations, need to move to the right location.
typedef enum io_op_e {
    IO_NONE = 0,
    IO_READ,
    IO_WRITE,
} io_op_t;

typedef struct proxyfs_io_request_s {
    io_op_t         op;
    mount_handle_t  *mount_handle;
    uint64_t        inode_number;
    uint64_t        offset;
    uint64_t        length;
    void            *data;
    int             error;
    uint64_t        out_size;

    void            (*done_cb)(struct proxyfs_io_request_s *req);
    void            *done_cb_arg;
    int             done_cb_fd;

    // TBD: Hide the list entry as opaque value (void *) since this struct is an external interface.
    TAILQ_ENTRY(proxyfs_io_request_s) request_queue_entry;
} proxyfs_io_request_t;

// API to send async read/write
int proxyfs_async_io_send(proxyfs_io_request_t *req);

// API to send sync (blocking) read/write
int proxyfs_sync_io(proxyfs_io_request_t *req);


// NOTE:
//   In order to conform to the proxyfs FS APIs, all of these functions require
//   a mount ID and many expect either an inode number or full path. The caller
//   is expected to obtain/retain this information.


// Inode-based chmod
int proxyfs_chmod(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  mode_t          in_mode);

// Path-based chmod
int proxyfs_chmod_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath,
                       mode_t          in_mode);

// Inode-based chown
int proxyfs_chown(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  uid_t           in_owner,
                  gid_t           in_group);

// Path-based chown
int proxyfs_chown_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath,
                       uid_t           in_owner,
                       gid_t           in_group);

// Inode-based create
int proxyfs_create(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   char*           in_basename,
                   uid_t           in_uid,
                   gid_t           in_gid,
                   mode_t          in_mode,
                   uint64_t*       out_inode_number);

// Path-based create
int proxyfs_create_path(mount_handle_t* in_mount_handle,
                        char*           in_fullpath,
                        mode_t          in_mode,
                        uid_t           in_uid,
                        gid_t           in_gid,
                        uint64_t*       out_inode_number);

// flock: byte range file lock, inode based only.
//
// For more information - man fcntl(2)
//
// cmd : F_GETLK, F_SETLK and F_SETLKW are used to acquire, release, and test for the existence of record locks.
// type (l_type): Type of lock: F_RDLCK, F_WRLCK, F_UNLCK
int proxyfs_flock(mount_handle_t* in_mount_handle,
                 uint64_t         in_inode_number,
                 int              in_lock_cmd,
                 struct flock*    flock);

int proxyfs_flush(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number);

// Inode-based get_stat
//
// NOTE: Caller must free the memory returned in out_stat once done with it.
//
// NOTE regarding mode:
//     We currently set the file type bitmask but not any of the permissions bits.
//     File type will be one of S_IFDIR|S_IFREG|S_IFLINK.
//
int proxyfs_get_stat(mount_handle_t*  in_mount_handle,
                     uint64_t         in_inode_number,
                     proxyfs_stat_t** out_stat);

// Path-based get_stat
//
// NOTE: Caller must free the memory returned in out_stat once done with it.
//
int proxyfs_get_stat_path(mount_handle_t*  in_mount_handle,
                          char*            in_fullpath,
                          proxyfs_stat_t** out_stat);

// Inode-based get_xattr
//
// NOTE: Caller must free the memory returned once done with it.
//
//
int proxyfs_get_xattr(mount_handle_t* in_mount_handle,
                     uint64_t         in_inode_number,
                     const char*      in_attr_name,
                     void**           out_attr_value,
                     size_t*          out_attr_value_size);

// Path-based get_xattr
//
// NOTE: Caller must free the memory returned once done with it.
//
int proxyfs_get_xattr_path(mount_handle_t* in_mount_handle,
                          char*            in_fullpath,
                          const char*      in_attr_name,
                          void**           out_attr_value,
                          size_t*          out_attr_value_size);

// Inode-based link
int proxyfs_link(mount_handle_t* in_mount_handle,
                 uint64_t        in_inode_number,
                 char*           in_basename,
                 uint64_t        in_target_inode_number);

// Path-based link
int proxyfs_link_path(mount_handle_t* in_mount_handle,
                      char*           in_src_fullpath,
                      char*           in_tgt_fullpath);

// Inode-based list_xattr
//
// NOTE: Caller must free the memory returned once done with it.
//
//
int proxyfs_list_xattr(mount_handle_t* in_mount_handle,
                       uint64_t         in_inode_number,
                       char**           out_attr_list,
                       size_t*          out_attr_list_size);

// Path-based list_xattr
//
// NOTE: Caller must free the memory returned once done with it.
//
int proxyfs_list_xattr_path(mount_handle_t*  in_mount_handle,
                            char*            in_fullpath,
                            char**           out_attr_list,
                            size_t*          out_attr_list_size);

// Debug utility to generate a ProxyFS log
int proxyfs_log(mount_handle_t* in_mount_handle,
                char*           in_message);

// Inode-based inode lookup
int proxyfs_lookup(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   char*           in_basename,
                   uint64_t*       out_inode_number);

// Path-based inode lookup
int proxyfs_lookup_path(mount_handle_t* in_mount_handle,
                        char*           in_fullpath,
                        uint64_t*       out_inode_number);

// Inode-based mkdir
int proxyfs_mkdir(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  char*           in_basename,
                  uid_t           in_uid,
                  gid_t           in_gid,
                  mode_t          in_mode,
                  uint64_t*       out_inode_number);

// Path-based mkdir
//
// NOTE: Not returning created inode here because Balaji says he doesn't need it.
//
int proxyfs_mkdir_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath,
                       uid_t           in_uid,
                       gid_t           in_gid,
                       mode_t          in_mode);

// Do a mount. If successful, returns non-null handle.
//
int proxyfs_mount(char*            in_volume_name,
                  uint64_t         in_mount_options,
                  uint64_t         in_auth_user_id,
                  uint64_t         in_auth_group_id,
                  mount_handle_t** out_mount_handle);

// Inode-based read
//
// Caller allocates a buffer to be filled in and passes the buffer pointer and buffer
// size in in_bufptr and in_bufsize, respectively. The number of bytes written to the
// buffer is returned in out_bufsize.
//
int proxyfs_read(mount_handle_t* in_mount_handle,
                 uint64_t        in_inode_number,
                 uint64_t        in_offset,
                 uint64_t        in_length,
                 uint8_t*        in_bufptr,
                 size_t          in_bufsize,
                 size_t*         out_bufsize);

// Inode-based async read
//
// Caller allocates a buffer to be filled in and passes the buffer pointer and buffer
// size in in_bufptr and in_bufsize, respectively. The in_request_id must uniquely
// identify the request, since it will be used to match request and response when
// proxyfs_read_recv is called.
//
// The send of the request is done in the caller's context. If success is returned,
// that means the request was successfully sent. When the read response is received,
// the in_done_callback will be called. Then a call to proxyfs_read_recv can
// be done. However if a non-success value is returned, the request has failed and
// a call to proxyfs_read_recv will return ENOENT.
//
// NOTE: in_done_callback should be nonblocking.
//
// NEW: Restructure to eliminate the need to call proxyfs_*_recv() APIs
typedef struct {
    mount_handle_t* in_mount_handle;
    uint64_t        in_inode_number;
    uint64_t        in_offset;
    uint64_t        in_length;
    uint8_t*        in_bufptr;
    size_t          in_bufsize;
    int             out_status;
    size_t          out_size;
} proxyfs_io_info_t;

typedef void (*proxyfs_done_callback_t)(void* in_request_id, proxyfs_io_info_t* req);

int proxyfs_read_send(void*                   in_request_id,
                      proxyfs_io_info_t*      in_request,
                      proxyfs_done_callback_t in_done_callback);

// Blocking call to receive a response corresponding to a proxyfs_read_send.
// Request and response are matched using in_request_id, which must uniquely
// identify the request.
//
// If the corresponding response has already been received, its status will
// be returned in out_rsp_status. If not, an error will be returned.
//
// This API will return the response status in out_rsp_status, and the number
// of bytes written to the buffer provided in proxyfs_read_send is returned
// in out_bufsize.
//
//int proxyfs_read_recv(void*   in_request_id,
//                      int*    out_rsp_status,
//                      size_t* out_bufsize);

// Inode-based readdir
//
// NOTE: Caller must free the memory returned in out_dir_ents once done with it.
//
// NOTE: CIFS and NFS will always read 1 entry at a
//       time, though this interface is more generic
int proxyfs_readdir(mount_handle_t* in_mount_handle,
                    uint64_t        in_inode_number,
                    int64_t         in_prev_dir_loc,
                    struct dirent** out_dir_ent);

// Inode-based readdir plus; returns stats as well as the dir info
// CIFS needs this.
//
// NOTE: Caller must free the memory returned in out_dir_ents once done with it.
//
int proxyfs_readdir_plus(mount_handle_t*  in_mount_handle,
                         uint64_t         in_inode_number,
                         int64_t          in_prev_dir_loc,
                         struct dirent**  out_dir_ent,
                         proxyfs_stat_t** out_dir_ent_stats);

// Inode-based read_symlink
//
// NOTE: Caller must free the memory returned in out_target once done with it.
//
int proxyfs_read_symlink(mount_handle_t* in_mount_handle,
                         uint64_t        in_inode_number,
                         const char**    out_target);

// Path-based read_symlink
//
// NOTE: Caller must free the memory returned in out_target once done with it.
//
int proxyfs_read_symlink_path(mount_handle_t* in_mount_handle,
                              char*           in_fullpath,
                              const char**    out_target);

// Inode-based remove_xattr
int proxyfs_remove_xattr(mount_handle_t* in_mount_handle,
                         uint64_t        in_inode_number,
                         const char*     in_attr_name);

// Path-based remove_xattr
int proxyfs_remove_xattr_path(mount_handle_t* in_mount_handle,
                              char*           in_fullpath,
                              const char*     in_attr_name);

// Inode-based rename
int proxyfs_rename(mount_handle_t* in_mount_handle,
                   uint64_t        in_src_dir_inode_number,
                   char*           in_src_basename,
                   uint64_t        in_dst_dir_inode_number,
                   char*           in_dst_basename);

// Path-based rename
int proxyfs_rename_path(mount_handle_t* in_mount_handle,
                        char*           in_src_fullpath,
                        char*           in_dst_fullpath);

// Inode-based resize
int proxyfs_resize(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   uint64_t        in_new_size);

// Inode-based rmdir
int proxyfs_rmdir(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  char*           in_basename);

// Path-based rmdir
int proxyfs_rmdir_path(mount_handle_t* in_mount_handle,
                       char*           in_fullpath);

#if 0
// NOTE: This isn't used by samba; commenting out for now.s
int proxyfs_setstat(mount_handle_t* in_mount_handle,
                    uint64_t        in_inode_number,
                    uint64_t        in_stat_ctime,
                    uint64_t        in_stat_mtime,
                    uint64_t        in_stat_atime,
                    uint64_t        in_stat_size,
                    uint64_t        in_stat_nlink);
#endif

// Set atime and mtime for a file. This is our version of utimens().
//
// XXX TODO: Does the caller require any special logic to set the timestamps
//           to the current time, like using UTIME_NOW would?
//
int proxyfs_settime(mount_handle_t*      in_mount_handle,
                    uint64_t             in_inode_number,
                    proxyfs_timespec_t*  in_atim,
                    proxyfs_timespec_t*  in_mtim);

int proxyfs_settime_path(mount_handle_t*      in_mount_handle,
                         char*                in_fullpath,
                         proxyfs_timespec_t*  in_atim,
                         proxyfs_timespec_t*  in_mtim);

// Inode-based set_xattr
int proxyfs_set_xattr(mount_handle_t* in_mount_handle,
                      uint64_t        in_inode_number,
                      const char*     in_attr_name,
                      const void*     in_attr_value,
                      size_t          in_attr_size,
                      int             in_attr_flags);

// Path-based set_xattr
int proxyfs_set_xattr_path(mount_handle_t* in_mount_handle,
                           char*           in_fullpath,
                           const char*     in_attr_name,
                           const void*     in_attr_value,
                           size_t          in_attr_size,
                           int             in_attr_flags);

// statvfs structure, for reference:
//
// struct statvfs {
//     unsigned long  f_bsize;    /* filesystem block size */
//     unsigned long  f_frsize;   /* fragment size */
//     fsblkcnt_t     f_blocks;   /* size of fs in f_frsize units */
//     fsblkcnt_t     f_bfree;    /* # free blocks */
//     fsblkcnt_t     f_bavail;   /* # free blocks for unprivileged users */
//     fsfilcnt_t     f_files;    /* # inodes */
//     fsfilcnt_t     f_ffree;    /* # free inodes */
//     fsfilcnt_t     f_favail;   /* # free inodes for unprivileged users */
//     unsigned long  f_fsid;     /* filesystem ID */
//     unsigned long  f_flag;     /* mount flags */
//     unsigned long  f_namemax;  /* maximum filename length */
// };

// Return file system stats
//
// NOTE: Caller must free the memory returned in out_statvfs once done with it.
//
int proxyfs_statvfs(mount_handle_t*  in_mount_handle,
                    struct statvfs** out_statvfs);

// Inode-based symlink
int proxyfs_symlink(mount_handle_t* in_mount_handle,
                    uint64_t        in_inode_number,
                    char*           in_basename,
                    char*           in_target,
                    uid_t           in_uid,
                    gid_t           in_gid);

// Path-based symlink
int proxyfs_symlink_path(mount_handle_t* in_mount_handle,
                         char*           in_fullpath,
                         char*           in_target_fullpath,
                         uid_t           in_uid,
                         gid_t           in_gid);

// Inode-based file type.
//
// Returned file type: DT_DIR|DT_REG|DT_LNK
//
int proxyfs_type(mount_handle_t* in_mount_handle,
                 uint64_t        in_inode_number,
                 uint16_t*       out_file_type);

// Inode-based unlink
int proxyfs_unlink(mount_handle_t* in_mount_handle,
                   uint64_t        in_inode_number,
                   char*           in_basename);

// Path-based unlink
int proxyfs_unlink_path(mount_handle_t* in_mount_handle,
                        char*           in_fullpath);

// Releases reference on mount and underlying RPC context
int proxyfs_unmount(mount_handle_t* in_mount_handle);

// Inode-based write
//
// Possibly stating the obvious here, but data written using this API cannot
// be read until proxyfs_flush is called for this inode.
int proxyfs_write(mount_handle_t* in_mount_handle,
                  uint64_t        in_inode_number,
                  uint64_t        in_offset,
                  uint8_t*        in_bufptr,
                  size_t          in_bufsize,
                  uint64_t*       out_size);

// Inode-based async write
//
// Caller passes the write buffer pointer and buffer size in in_bufptr and in_bufsize,
// respectively. The in_request_id must uniquely identify the request, since it will
// be used to match request and response when proxyfs_write_recv is called.
//
// The send of the request is done in the caller's context. If success is returned,
// that means the request was successfully sent. When the write response is received,
// the in_done_callback will be called. Then a call to proxyfs_write_recv can
// be done. However if a non-success value is returned, the request has failed and
// a call to proxyfs_write_recv will return ENOENT.
//
// NOTE: in_done_callback should be nonblocking.
//
int proxyfs_write_send(void*                   in_request_id,
                       proxyfs_io_info_t*      in_request,
                       proxyfs_done_callback_t in_done_callback);

// Blocking call to receive a response corresponding to a proxyfs_write_send.
// Request and response are matched using in_request_id, which must uniquely
// identify the request.
//
// If the corresponding response has already been received, its status will
// be returned in out_rsp_status, and the number of bytes written will be
// returned in out_size. If the response has not been received, an error will
// be returned.
//
//int proxyfs_write_recv(void*   in_request_id,
//                       int*    out_rsp_status,
//                       size_t* out_size);


#endif // __PROXYFS_H__
