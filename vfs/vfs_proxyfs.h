#ifndef __VFS_PROXYFS_H__
#define __VFS_PROXYFS_H__

#include <proxyfs.h>

typedef struct {
	mount_handle_t *mnt_handle;
	char           *volume;
	char           *cwd;
	uint64_t       cwd_inum;
	uint64_t       open_count;
	bool           readonly;
} fs_ctx_t;

typedef struct file_handle_s {
	uint64_t      inum;
	off_t         offset;
	uint64_t      flags;
	uint64_t      mode;
	struct dirent dir_ent;
} file_handle_t;

/* fs_ctx_t Operations */
#define MOUNT_HANDLE(handle) (((fs_ctx_t *)handle->data)->mnt_handle)
#define CWD(handle) (((fs_ctx_t *)handle->data)->cwd)
#define CWD_INUM(handle) (((fs_ctx_t *)handle->data)->cwd_inum)

#endif // __VFS_PROXYFS_H__
