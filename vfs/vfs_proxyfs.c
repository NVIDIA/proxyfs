/*
 * Unix SMB/CIFS implementation.
 *
 * VFS module to invoke ProxyFS built on swift object store.
 */

/**
 * @file   vfs_proxyfs.c
 * @author SwiftStack
 * @date   September 2017
 * @brief  Samba VFS module for proxyfs
 *
 * A Samba VFS module for ProxyFS.
 * This is a "bottom" vfs module (not something to be stacked on top of
 * another module), and translates (most) calls to the closest actions
 * available in rpc calls to ProxyFS.
 */

#include <stdio.h>
#include <libgen.h>
#include <fcntl.h>

// Temporarily including for getpwnam():
#include <sys/types.h>
#include <pwd.h>

#include "includes.h"
#include "smbd/smbd.h"
#include "bin/default/librpc/gen_ndr/ndr_smb_acl.h"

#include "vfs_proxyfs.h"

#ifdef HAVE_LINUX_FALLOC_H
#include <linux/falloc.h>
#endif

/*
 * Proxyfs VFS module is written for SAMBA Major version 4 and supports minor versions 2, 3, 4, 5 and 6.
 * The default is 4.3 and exceptions/deviations in the interface are handled for 2, 4, 5 and 6 with #if conditions.
 */

#if !defined(SAMBA_VERSION_MAJOR) || !defined(SAMBA_VERSION_MINOR) || SAMBA_VERSION_MAJOR != 4 || SAMBA_VERSION_MINOR < 2 || SAMBA_VERSION_MINOR > 6
#error Samba Major version should be 4 and minor should be between 2 and 6 (inclusive).
#endif

#define vfs_proxyfs_init samba_init_module

#undef DBGC_CLASS
#define DBGC_CLASS DBGC_VFS

char *resolve_path(struct vfs_handle_struct *handle,
                          const char *path)
{
	char *path_to_resolve;

	if (path[0] == '/') {
		path_to_resolve = strdup((char *)path);

		if (strcmp(path_to_resolve, "/") == 0) {
			return path_to_resolve;
		}

	} else {
		if (CWD(handle) == NULL) {
			path_to_resolve = strdup("/");
		} else {
			path_to_resolve = strdup(CWD(handle));
		}

		int buf_len = strlen(path_to_resolve) + strlen("/") + strlen((char *)path) + 1;
		char *tmp_path = malloc(buf_len);
		bzero(tmp_path, buf_len);
		snprintf(tmp_path, buf_len, "%s/%s", path_to_resolve, path);
		free(path_to_resolve);
		path_to_resolve = tmp_path;
	}

	// Convert now the path to canonical path:
	char **path_stack = (char **)malloc((strlen(path_to_resolve) / 2 + 1) * sizeof(char *));
	int i = 0;
	int path_count = 0;
	char *token, *saveptr;
	char *firstptr = path_to_resolve;

	do {
		token = strtok_r(firstptr, "/", &saveptr);
		firstptr = NULL;

		if (token == NULL) {
			break;
		}

		if (strcmp(token, ".") == 0) {
			continue;
		}

		if (strcmp(token, "..") == 0) {
			if (i == 0) {
				continue;
			}

			path_count -= strlen(path_stack[i - 1]);
			i--;
			continue;
		}

		path_stack[i] = token;
		path_count += strlen(token);
		i++;
	} while (token != NULL);

	int buf_len = path_count + i + 2;
	char *rpath = (char *)malloc(buf_len);
	bzero(rpath, buf_len);
	snprintf(rpath, buf_len, "/"); // Initialize it to /, will be over written if there are other path components.

	int j, idx;
	for (idx = 0, j = 0; j < i; j++) {
		snprintf(&rpath[idx], buf_len - idx, "/%s", path_stack[j]);
		idx += strlen(path_stack[j]) + 1;
	}

	free(path_stack);
	free(path_to_resolve);

	return rpath;
}

int get_parent_ino(struct vfs_handle_struct *handle,
                   char *base_name,
                   uint64_t *parent_inop)
{
	char *path, *parent, *name;
	uint64_t parent_ino, file_ino;

	int err = 0;

	path = resolve_path(handle, base_name);
	parent = dirname(path);

	if (strcmp(CWD(handle), parent) == 0) {
		*parent_inop = CWD_INUM(handle);
	} else {
		err = proxyfs_lookup_path(MOUNT_HANDLE(handle), parent, parent_inop);
	}

	free(path);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

/**
 * Helper to convert struct stat to struct stat_ex.
 */
static void smb_stat_ex_from_stat(struct stat_ex *dst,
                                  const proxyfs_stat_t *src)
{
	bzero(dst, sizeof(struct stat_ex));

	dst->st_ex_dev = src->dev;
	dst->st_ex_ino = src->ino;
	dst->st_ex_mode = src->mode;
	dst->st_ex_nlink = src->nlink;
	dst->st_ex_uid = src->uid;
	dst->st_ex_gid = src->gid;
	dst->st_ex_rdev = src->dev;
	dst->st_ex_size = src->size;
	dst->st_ex_atime.tv_sec = src->atim.sec;
	dst->st_ex_mtime.tv_sec = src->mtim.sec;
	dst->st_ex_ctime.tv_sec = src->ctim.sec;
	dst->st_ex_btime.tv_sec = src->mtim.sec;  // TBD: should map to cr or b time.
	dst->st_ex_blksize = 65536;
	dst->st_ex_blocks = src->size / 512;
#ifdef STAT_HAVE_NSEC
	dst->st_ex_atime.tv_nsec = src->atim.nsec;
	dst->st_ex_mtime.tv_nsec = src->mtim.nsec;
	dst->st_ex_ctime.tv_nsec = src->ctim.nsec;
	dst->st_ex_btime.tv_nsec = src->mtim.nsec; // TBD: should map to cr or b time.
#endif
}

static void free_data(void **handle_data)
{
	fs_ctx_t *ctx = (fs_ctx_t *)*handle_data;

	if (ctx == NULL) {
		return;
	}

	if (ctx->cwd) {
		free(ctx->cwd);
	}
	free(ctx);
	*handle_data = NULL;
}

static int vfs_proxyfs_connect(struct vfs_handle_struct *handle,
                               const char *service,
                               const char *user)
{
	int err;

	fs_ctx_t *ctx = (fs_ctx_t *)malloc(sizeof(fs_ctx_t));
	if (ctx == NULL) {
		errno = ENOMEM;
		return -1;
	}
	bzero(ctx, sizeof(fs_ctx_t));

	ctx->volume = (char *)lp_parm_const_string(SNUM(handle->conn), "proxyfs", "volume", service);

	ctx->readonly = handle->conn->read_only;

	uint64_t mount_option = ctx->readonly ? 1 : 0;

	DEBUG(10, ("vfs_proxyfs_connect: Volume : %s Connection_path %s Service %s user %s\n", ctx->volume, handle->conn->connectpath, service, user));

	err = proxyfs_mount((char *)ctx->volume, mount_option, geteuid(), getegid(), &ctx->mnt_handle);
	if (err != 0) {
		errno = err;
		DEBUG(1, ("proxyfs_mount_failed: Volume : %s Connection_path %s Service %s user %s errno %d\n", ctx->volume, handle->conn->connectpath, service, user, errno));
		free(ctx);
		return -1;
	}

	ctx->cwd = strdup("/");
	ctx->cwd_inum = ctx->mnt_handle->root_dir_inode_num;
	ctx->open_count = 0;

	handle->data = ctx;

	handle->free_data = free_data;

	return 0;
}

static void vfs_proxyfs_disconnect(struct vfs_handle_struct *handle)
{
	fs_ctx_t *ctx = (fs_ctx_t *)handle->data;
	if (ctx == NULL) {
		DEBUG(1, ("vfs_proxyfs_disconnect: null ctx done\n"));
		return;
	}

	DEBUG(10, ("vfs_proxyfs_disconnect: %s\n",  ctx->volume));

	proxyfs_unmount(MOUNT_HANDLE(handle));

	free_data(&handle->data);
}

static uint64_t vfs_proxyfs_disk_free(struct vfs_handle_struct *handle,
                                      const char *path,
#if SAMBA_VERSION_MINOR == 2
                                      bool       small_query,
#endif
                                      uint64_t *bsize_p,
                                      uint64_t *bavail_p,
                                      uint64_t *btotal_p)
{
    uint64_t ret = 0;
    struct statvfs* stat_vfs = NULL;
	int err = proxyfs_statvfs(MOUNT_HANDLE(handle), &stat_vfs);
	if (err != 0) {
		errno = err;
		return -1;
	}

    if (bsize_p != NULL) {
		*bsize_p = (uint64_t)stat_vfs->f_bsize; /* Block size */
	}
	if (bavail_p != NULL) {
		*bavail_p = (uint64_t)stat_vfs->f_bavail; /* Available Block units */
        ret = *bavail_p;
	}
	if (btotal_p != NULL) {
		*btotal_p = (uint64_t)stat_vfs->f_blocks; /* Total Block units */
	}
    free(stat_vfs);

	DEBUG(10, ("vfs_proxyfs_disk_free: %s, returning %ld\n", handle->conn->connectpath, ret));

	return ret;
}

static int vfs_proxyfs_get_quota(struct vfs_handle_struct *handle,
                                 enum SMB_QUOTA_TYPE qtype,
                                 unid_t id,
                                 SMB_DISK_QUOTA *qt)
{
	DEBUG(10, ("vfs_proxyfs_get_quota: %s\n", handle->conn->connectpath));
	errno = ENOSYS;
	return -1;
}

#if SAMBA_VERSION_MINOR >= 4
static int vfs_proxyfs_get_quota_4_4(struct vfs_handle_struct *handle,
								 const char *path,
                                 enum SMB_QUOTA_TYPE qtype,
                                 unid_t id,
                                 SMB_DISK_QUOTA *qt)
{
	return vfs_proxyfs_get_quota(handle, qtype, id, qt);
}
#endif

static int
vfs_proxyfs_set_quota(struct vfs_handle_struct *handle,
                      enum SMB_QUOTA_TYPE qtype,
                      unid_t id,
                      SMB_DISK_QUOTA *qt)
{
	DEBUG(10, ("vfs_proxyfs_set_quota: %s\n", handle->conn->connectpath));
	errno = ENOSYS;
	return -1;
}

static int vfs_get_shadow_copy_data(struct vfs_handle_struct *handle,
                                    struct files_struct *fsp,
                                    struct shadow_copy_data *shadow_copy_data,
                                    bool labels)
{
	DEBUG(10, ("vfs_get_shadow_copy_data: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return -1;
}

static int vfs_proxyfs_statvfs(struct vfs_handle_struct *handle,
                               const char *path,
                               struct vfs_statvfs_struct *vfs_statvfs)
{
    struct statvfs* stat_vfs = NULL;
	int err = proxyfs_statvfs(MOUNT_HANDLE(handle), &stat_vfs);
	if (err != 0) {
		errno = err;
		return -1;
	}

	bzero(vfs_statvfs, sizeof(struct vfs_statvfs_struct));

	vfs_statvfs->OptimalTransferSize = stat_vfs->f_frsize;
	vfs_statvfs->BlockSize = stat_vfs->f_bsize;
	vfs_statvfs->TotalBlocks = stat_vfs->f_blocks;
	vfs_statvfs->BlocksAvail = stat_vfs->f_bfree;
	vfs_statvfs->UserBlocksAvail = stat_vfs->f_bavail;
	vfs_statvfs->TotalFileNodes = stat_vfs->f_files;
	vfs_statvfs->FreeFileNodes = stat_vfs->f_ffree;
	vfs_statvfs->FsIdentifier = stat_vfs->f_fsid;
	vfs_statvfs->FsCapabilities = FILE_CASE_SENSITIVE_SEARCH | FILE_CASE_PRESERVED_NAMES | FILE_SUPPORTS_REPARSE_POINTS;

    free(stat_vfs);

	DEBUG(10, ("vfs_proxyfs_statvfs: returning statvfs for %s : block size %d\n", handle->conn->connectpath, vfs_statvfs->BlockSize));

	return err;
}

static uint32_t vfs_proxyfs_fs_capabilities(struct vfs_handle_struct *handle,
                                            enum timestamp_set_resolution *p_ts_res)
{
	DEBUG(10, ("vfs_proxyfs_fs_capabilities: %s\n", handle->conn->connectpath));
	uint32_t caps = FILE_CASE_SENSITIVE_SEARCH | FILE_CASE_PRESERVED_NAMES | FILE_SUPPORTS_REPARSE_POINTS;

#ifdef STAT_HAVE_NSEC
	*p_ts_res = TIMESTAMP_SET_NT_OR_BETTER;
#endif

	return caps;
}

static NTSTATUS vfs_proxyfs_get_dfs_referrals(struct vfs_handle_struct *handle,
                                              struct dfs_GetDFSReferral *r)
{
	DEBUG(10, ("vfs_proxyfs_get_dfs_referrals: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED; // TBD: need to send NTSTATUS.
}

static DIR *vfs_proxyfs_opendir(struct vfs_handle_struct *handle,
                                const char *fname,
                                const char *mask,
                                uint32_t attributes)
{
	DEBUG(10, ("vfs_proxyfs_opendir: %s\n", handle->conn->connectpath));

	file_handle_t *dir;
	int err;

	dir = (file_handle_t *)malloc(sizeof(file_handle_t));
	if (dir == NULL) {
		errno = ENOMEM;
		return NULL;
	}
	bzero(dir, sizeof(file_handle_t));

	char *path = resolve_path(handle, fname);

	err = proxyfs_lookup_path(MOUNT_HANDLE(handle), path, &dir->inum);
	if (err != 0) {
		errno = err;
		free(path);
		free(dir);
		return NULL;
	}

	dir->offset = -1;
	free(path);

	return (DIR *) dir;
}

#if SAMBA_VERSION_MINOR >= 5
static DIR *vfs_proxyfs_opendir_4_4(struct vfs_handle_struct *handle,
                                const struct smb_filename *smb_fname,
                                const char *mask,
                                uint32_t attributes) {
	return vfs_proxyfs_opendir(handle, smb_fname->base_name, mask, attributes);
}
#endif

static DIR *vfs_proxyfs_fdopendir(struct vfs_handle_struct *handle,
                                  files_struct *fsp,
                                  const char *mask,
                                  uint32_t attributes)
{
	DEBUG(10, ("vfs_proxyfs_fdopendir: %s\n", handle->conn->connectpath));
	return (DIR *) *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
}

static struct dirent *vfs_proxyfs_readdir(struct vfs_handle_struct *handle,
                                          DIR *dirp,
                                          SMB_STRUCT_STAT *sbuf)
{
	int ret;
	struct dirent *dir_ent = NULL;
	file_handle_t *dir = (file_handle_t *)dirp;

	uint64_t num_ents, size;
	bool are_more_entries;
	proxyfs_stat_t *stats;

	if (sbuf != NULL) {
		DEBUG(10, ("Invoking proxyfs_readdir_plus for inum = %ld\n", dir->inum));
		ret = proxyfs_readdir_plus(MOUNT_HANDLE(handle), dir->inum, dir->offset, &dir_ent, &stats);
	} else {
		DEBUG(10, ("Invoking proxyfs_readdir for inum = %ld\n", dir->inum));
		ret = proxyfs_readdir(MOUNT_HANDLE(handle), dir->inum, dir->offset, &dir_ent);
	}

	if ((ret != 0) || (dir_ent == NULL)) {
		errno = ret;
		return NULL;
	}

	memcpy(&dir->dir_ent, dir_ent, sizeof(struct dirent));
	free(dir_ent);

	if (sbuf != NULL) {
		smb_stat_ex_from_stat(sbuf, &stats[0]);
		free(stats);
	}

	dir->offset = dir->dir_ent.d_off;
	return &dir->dir_ent;
}

static void vfs_proxyfs_seekdir(struct vfs_handle_struct *handle,
                                DIR *dirp,
                                long offset)
{
	DEBUG(10, ("vfs_proxyfs_seekdir: %s\n", handle->conn->connectpath));
	file_handle_t *dir = (file_handle_t *)dirp;
	dir->offset = offset;
}

static long vfs_proxyfs_telldir(struct vfs_handle_struct *handle, DIR *dirp)
{
	DEBUG(10, ("vfs_proxyfs_telldir: %s\n", handle->conn->connectpath));
	file_handle_t *dir = (file_handle_t *)dirp;
	return dir->offset;
}

static void vfs_proxyfs_rewinddir(struct vfs_handle_struct *handle,
                                  DIR *dirp)
{
	DEBUG(10, ("vfs_proxyfs_rewinddir: %s\n", handle->conn->connectpath));
	file_handle_t *dir = (file_handle_t *)dirp;
	dir->offset = 0;
}

static int vfs_proxyfs_mkdir(struct vfs_handle_struct *handle,
                             const char *smb_path,
                             mode_t mode)
{
	char *path = resolve_path(handle, smb_path);

	uid_t uid = get_current_uid(handle->conn);
	gid_t gid = get_current_gid(handle->conn);

	int err = proxyfs_mkdir_path(MOUNT_HANDLE(handle), path, uid, gid, mode);

	DEBUG(10, ("vfs_proxyfs_mkdir: %s mode 0%o errno = %d\n", path, mode, err));

	free(path);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

#if SAMBA_VERSION_MINOR >= 5
static int vfs_proxyfs_mkdir_4_4(struct vfs_handle_struct *handle,
                             const struct smb_filename *smb_fname,
                             mode_t mode)
{
	return vfs_proxyfs_mkdir(handle, smb_fname->base_name, mode);
}
#endif

static int vfs_proxyfs_rmdir(struct vfs_handle_struct *handle,
                             const char *smb_fname)
{
	char *path = resolve_path(handle, smb_fname);
	int err = proxyfs_rmdir_path(MOUNT_HANDLE(handle), path);

	DEBUG(10, ("vfs_proxyfs_rmdir: %s errno: %d\n", path, err));
	free(path);
	if (err != 0) {
		errno = err;
	}

	return err;
}

#if SAMBA_VERSION_MINOR >= 5
static int vfs_proxyfs_rmdir_4_4(struct vfs_handle_struct *handle,
                             const struct smb_filename *smb_fname)
{
	return vfs_proxyfs_rmdir(handle, smb_fname->base_name);
}
#endif

static int vfs_proxyfs_closedir(struct vfs_handle_struct *handle,
                                DIR *dirp)
{
	DEBUG(10, ("vfs_proxyfs_closedir: %s\n", handle->conn->connectpath));
	file_handle_t *dir = (file_handle_t *)dirp;
	free(dir);
	return 0;
}

static void vfs_proxyfs_init_search_op(struct vfs_handle_struct *handle,
                                       DIR *dirp)
{
	DEBUG(10, ("vfs_proxyfs_init_search_op: %s\n", handle->conn->connectpath));
	return;
}

static int vfs_proxyfs_open(struct vfs_handle_struct *handle,
                            struct smb_filename *smb_fname,
                            files_struct *fsp,
                            int flags,
                            mode_t mode)
{
	DEBUG(10, ("vfs_proxyfs_open: %s %s\n", handle->conn->connectpath, smb_fname->base_name));
	file_handle_t *fd;
	int err;
	char *path;

	fd = (file_handle_t *)malloc(sizeof(file_handle_t));
	if (fd == NULL) {
		errno = ENOMEM;
		return -1;
	}
	bzero(fd, sizeof(file_handle_t));

	if (flags & O_DIRECTORY) {
		path = resolve_path(handle, smb_fname->base_name);
		err = proxyfs_lookup_path(MOUNT_HANDLE(handle), path, &fd->inum);
		free(path);

		if (err != 0) {
			free(fd);
			errno = err;
			return -1;
		}

		fd->offset = 0;
	} else if (flags & O_CREAT) {

		path = resolve_path(handle, smb_fname->base_name);
		char *tmp_path = strdup(path);
		char *parent = dirname(tmp_path);

		uid_t uid = get_current_uid(handle->conn);
		gid_t gid = get_current_gid(handle->conn);

		if (strcmp(parent, CWD(handle)) == 0) {
			char *name = basename(path);
			err = proxyfs_create(MOUNT_HANDLE(handle), CWD_INUM(handle), name, uid, gid, mode, &fd->inum);
		} else {
			err = proxyfs_create_path(MOUNT_HANDLE(handle), path, uid, gid, mode, &fd->inum);
		}

		free(tmp_path);
		free(path);

		if (err != 0) {
			free(fd);
			errno = err;
			return -1;
		}

		fd->offset = 0;
		fd->flags = flags;
		fd->mode = mode;

	} else {
		path = resolve_path(handle, smb_fname->base_name);
		err = proxyfs_lookup_path(MOUNT_HANDLE(handle), path, &fd->inum);
		free(path);

		if (err != 0) {
			free(fd);
			errno = err;
			return -1;
		}

		fd->offset = 0;
		fd->flags = flags;
		fd->mode = mode;
	}

	file_handle_t **p_tmp = (file_handle_t **)VFS_ADD_FSP_EXTENSION(handle, fsp, file_handle_t *, NULL);
	*p_tmp = fd;

	DEBUG(10, ("vfs_proxyfs_open - success: %s %s\n", handle->conn->connectpath, smb_fname->base_name));

	return 1;
}

static NTSTATUS vfs_proxyfs_create(struct vfs_handle_struct *handle, struct smb_request *req,
	                          uint16_t root_dir_fid, struct smb_filename *smb_fname,
	                          uint32_t access_mask, uint32_t share_access,
	                          uint32_t create_disposition, uint32_t create_options,
	                          uint32_t file_attributes, uint32_t oplock_request,
	                          struct smb2_lease *lease, uint64_t allocation_size,
	                          uint32_t private_flags, struct security_descriptor *sd,
	                          struct ea_list *ea_list, files_struct **result,
	                          int *pinfo, const struct smb2_create_blobs *in_context_blobs,
	                          struct smb2_create_blobs *out_context_blobs) {

	DEBUG(10,("create_file: access_mask = 0x%x "
		  "file_attributes = 0x%x, share_access = 0x%x, "
		  "create_disposition = 0x%x create_options = 0x%x "
		  "oplock_request = 0x%x "
		  "private_flags = 0x%x "
		  "root_dir_fid = 0x%x, ea_list = 0x%p, sd = 0x%p, "
		  "fname = %s\n",
		  (unsigned int)access_mask,
		  (unsigned int)file_attributes,
		  (unsigned int)share_access,
		  (unsigned int)create_disposition,
		  (unsigned int)create_options,
		  (unsigned int)oplock_request,
		  (unsigned int)private_flags,
		  (unsigned int)root_dir_fid,
		  ea_list, sd, smb_fname_str_dbg(smb_fname)));

	// Samba creates files via - open, since we have set .create_file_fn = NULL
	// This function won't be inovked. Just a place holder.

	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static bool vfs_proxyfs_lock(struct vfs_handle_struct *handle, files_struct *fsp, int op, off_t offset, off_t count, int type);

static int vfs_proxyfs_close(struct vfs_handle_struct *handle,
                             files_struct *fsp)
{
	DEBUG(10, ("vfs_proxyfs_close: %s\n", fsp->fsp_name->base_name));

	file_handle_t *fd;
	fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	//proxyfs_flush(MOUNT_HANDLE(handle), fd->inum);
	// TBD: This should be handled inside proxyfs layer - unlock all the acquired locks.
	vfs_proxyfs_lock(handle, fsp, F_SETLK, 0, 0, F_UNLCK);
	VFS_REMOVE_FSP_EXTENSION(handle, fsp);
	free(fd);
}

static ssize_t vfs_proxyfs_pread(struct vfs_handle_struct *handle,
                                 files_struct *fsp,
                                 void *data,
                                 size_t n,
                                 off_t offset)
{
	file_handle_t *fd;
	fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd ==  NULL) {
		errno = EINVAL;
		return -1;
	}

	if (n < 0) {
		errno = EINVAL;
		return -1;
	}

	proxyfs_io_request_t *req = (proxyfs_io_request_t *)malloc(sizeof(proxyfs_io_request_t));
	if (req == NULL) {
		errno = ENOMEM;
		return -1;
	}

	req->op = IO_READ;
	req->mount_handle = MOUNT_HANDLE(handle);
	req->inode_number = fd->inum;
	req->offset = offset;
	req->length = n;
	req->data = data;
	req->error = 0;
	req->out_size = 0;
	req->done_cb = NULL;
	req->done_cb_arg = NULL;
	req->done_cb_fd = -1;

	int ret = proxyfs_sync_io(req);
	if (ret != 0) {
		free(req);
		errno = EIO;
		return -1;
	}

	if (req->error != 0) {
		errno = req->error;
		free(req);
		return -1;
	}

	ssize_t size = req->out_size;
	free(req);
	DEBUG(10, ("vfs_proxyfs_pread: %s inum=%ld offset=%ld size=%ld\n", fsp->fsp_name->base_name, fd->inum, offset, size));

	return size;
}

static ssize_t vfs_proxyfs_read(struct vfs_handle_struct *handle,
                                files_struct *fsp,
                                void *data,
                                size_t n)
{
	file_handle_t *fd;
	fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);

	ssize_t size = vfs_proxyfs_pread(handle, fsp, data, n, fd->offset);
	if (size > 0) {
		fd->offset += size;
	}

	return size;
}

static int read_fd = -1;
static int write_fd = -1;
static struct tevent_fd *aio_read_event = NULL;

typedef enum vfs_proxyfs_aio_state_e {
	VFS_PROXYFS_AIO_DONE                   = 1,
	VFS_PROXYFS_AIO_SENDING                = 2,
	VFS_PROXYFS_AIO_DONE_BEFORE_SEND_REPLY = 3,
	VFS_PROXYFS_AIO_WAITING                = 4,
} vfs_proxyfs_aio_state_t;

typedef struct vfs_proxyfs_tevent_status_s {
	pthread_mutex_t         mutex;
	vfs_proxyfs_aio_state_t io_state;
	void                    *req_id;
	uint64_t                ino;
	off_t                   offset;
	size_t                  length;
} vfs_proxyfs_tevent_status_t;

static void proxyfs_io_done_callback(proxyfs_io_request_t *pfs_io)
{
	struct tevent_req *req = (struct tevent_req *)pfs_io->done_cb_arg;

	int err = sys_write(pfs_io->done_cb_fd, &req, sizeof(struct tevent_req *));
	if (err < 0) {
		DEBUG(1, ("Write to AIO pipe failed: err %s\n", strerror(errno)));
	}
}

static void vfs_proxyfs_aio_done_callback(struct tevent_context *event_ctx,
                                      struct tevent_fd *fde,
                                      uint16_t flags,
									  void *data)
{
	struct tevent_req *req = NULL;

	int err = sys_read(read_fd, &req, sizeof(struct tevent_req *));
	if (err < 0) {
		DEBUG(1, ("AIO READ from pipe failed %s\n", strerror(errno)));
		return;
	}

	tevent_req_done(req);
}

static bool init_proxyfs_aio(struct vfs_handle_struct *handle)
{
	int fds[2];
	int ret = -1;

	if (read_fd != -1) {
		return true;
	}

	DEBUG(10, ("Starting proxyfs aio callback init..\n"));
	ret = pipe(fds);
	if (ret == -1) {
		return false;
	}

	read_fd = fds[0];
	write_fd = fds[1];

	aio_read_event = tevent_add_fd(server_event_context(), NULL, read_fd, TEVENT_FD_READ, vfs_proxyfs_aio_done_callback, NULL);
	if (aio_read_event == NULL) {
		DEBUG(10, ("Failed to setup pipe for aio event handling\n"));
		TALLOC_FREE(aio_read_event);
		if (read_fd != -1) {
			close(read_fd);
			close(write_fd);
			read_fd = -1;
			write_fd = -1;
		}
		return false;
	}

	return true;
}

static struct tevent_req *vfs_proxyfs_pread_send(struct vfs_handle_struct *handle,
                                                 TALLOC_CTX *mem_ctx,
                                                 struct tevent_context *ev,
                                                 struct files_struct *fsp,
                                                 void *data,
                                                 size_t n, off_t offset)
{
	proxyfs_io_request_t *pfs_io = NULL;

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd ==  NULL) {
		errno = EINVAL;
		return NULL;
	}

	DEBUG(10, ("vfs_proxyfs_pread_send: %s inum=%ld offset=%ld size=%ld\n", fsp->fsp_name->base_name, fd->inum, offset, n));

	struct tevent_req *req = tevent_req_create(mem_ctx, &pfs_io, proxyfs_io_request_t);
	if (req == NULL) {
		return NULL;
	}

	if (!init_proxyfs_aio(handle)) {
		tevent_req_error(req, EIO);
		return tevent_req_post(req, ev);
	}

	pfs_io->op = IO_READ;
	pfs_io->mount_handle = MOUNT_HANDLE(handle);
	pfs_io->inode_number = fd->inum;
	pfs_io->offset = offset;
	pfs_io->length = n;
	pfs_io->data = data;
	pfs_io->done_cb = proxyfs_io_done_callback;
	pfs_io->done_cb_arg = (void *)req;
	pfs_io->done_cb_fd = write_fd;

	int ret = proxyfs_async_io_send(pfs_io);
	if (ret < 0) {
		talloc_free(req);
		errno = ret;
		return NULL;
	}

	return req;
}

static ssize_t vfs_proxyfs_read_recv(struct tevent_req *req,
                                      int *err)
{
	proxyfs_io_request_t *pfs_io = tevent_req_data(req, proxyfs_io_request_t);
	if (pfs_io == NULL) {
		return -1;
	}

	DEBUG(10, ("vfs_proxyfs_read_recv: inum=%lu offset=%ld size=%ld\n", pfs_io->inode_number, pfs_io->offset, pfs_io->length));

	*err = pfs_io->error;
	return pfs_io->out_size;
}

#if SAMBA_VERSION_MINOR >= 5
static ssize_t vfs_proxyfs_read_recv_4_4(struct tevent_req *req,
                                         struct vfs_aio_state *state)
{
	return vfs_proxyfs_read_recv(req, &state->error);
}
#endif

static ssize_t vfs_proxyfs_pwrite(struct vfs_handle_struct *handle,
                                  files_struct *fsp,
                                  const void *data,
                                  size_t n, off_t offset)
{
	file_handle_t *fd;
	fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd ==  NULL) {
		errno = EINVAL;
		return -1;
	}

	if (n < 0) {
		errno = EINVAL;
		return -1;
	}

	proxyfs_io_request_t *req = (proxyfs_io_request_t *)malloc(sizeof(proxyfs_io_request_t));
	if (req == NULL) {
		errno = ENOMEM;
		return -1;
	}

	req->op = IO_WRITE;
	req->mount_handle = MOUNT_HANDLE(handle);
	req->inode_number = fd->inum;
	req->offset = offset;
	req->length = n;
	req->data = (void *)data;
	req->error = 0;
	req->out_size = 0;
	req->done_cb = NULL;
	req->done_cb_arg = NULL;
	req->done_cb_fd = -1;

	int ret = proxyfs_sync_io(req);
	if (ret != 0) {
		free(req);
		errno = EIO;
		return -1;
	}

	if (req->error != 0) {
		errno = req->error;
		free(req);
		return -1;
	}

	ssize_t size = req->out_size;
	free(req);
	DEBUG(10, ("vfs_proxyfs_pwrite: %s inum=%ld offset=%ld size=%ld\n", fsp->fsp_name->base_name, fd->inum, offset, size));

	return size;
}

static ssize_t vfs_proxyfs_write(struct vfs_handle_struct *handle,
                                 files_struct *fsp,
                                 const void *data, size_t n)
{
	file_handle_t *fd;
	fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd ==  NULL) {
		errno = EINVAL;
		return -1;
	}

	if (n < 0) {
		errno = EINVAL;
		return -1;
	}

	ssize_t size = vfs_proxyfs_pwrite(handle, fsp, data, n, fd->offset);
	if (size > 0) {
		fd->offset += size;
	}

	return size;
}

static struct tevent_req *vfs_proxyfs_pwrite_send(struct vfs_handle_struct *handle,
                                                  TALLOC_CTX *mem_ctx,
                                                  struct tevent_context *ev,
                                                  struct files_struct *fsp,
                                                  const void *data,
                                                  size_t n, off_t offset)
{
	proxyfs_io_request_t *pfs_io = NULL;
	file_handle_t *fd;

	fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd ==  NULL) {
		errno = EINVAL;
		return NULL;
	}

	DEBUG(10, ("vfs_proxyfs_pwrite_send: %s inum=%ld offset=%ld size=%ld\n", fsp->fsp_name->base_name, fd->inum, offset, n));

	struct tevent_req *req = tevent_req_create(mem_ctx, &pfs_io, proxyfs_io_request_t);
	if (req == NULL) {
		return NULL;
	}

	if (!init_proxyfs_aio(handle)) {
		talloc_free(req);
		errno = EIO;
		return NULL;
	}

	pfs_io->op = IO_WRITE;
	pfs_io->mount_handle = MOUNT_HANDLE(handle);
	pfs_io->inode_number = fd->inum;
	pfs_io->offset = offset;
	pfs_io->length = n;
	pfs_io->data = (void *)data;
	pfs_io->done_cb = proxyfs_io_done_callback;
	pfs_io->done_cb_arg = (void *)req;
	pfs_io->done_cb_fd = write_fd;

	int ret = proxyfs_async_io_send(pfs_io);
	if (ret < 0) {
		talloc_free(req);
		errno = ret;
		return NULL;
	}

	return req;
}

static ssize_t vfs_proxyfs_write_recv(struct tevent_req *req,
                                      int *err)
{
	proxyfs_io_request_t *pfs_io = tevent_req_data(req, proxyfs_io_request_t);
	if (pfs_io == NULL) {
		return -1;
	}

	DEBUG(10, ("vfs_proxyfs_write_recv: inum=%lu offset=%ld size=%ld\n", pfs_io->inode_number, pfs_io->offset, pfs_io->length));

	*err = pfs_io->error;
	return pfs_io->out_size;
}

#if SAMBA_VERSION_MINOR >= 5
static ssize_t vfs_proxyfs_write_recv_4_4(struct tevent_req *req,
                                          struct vfs_aio_state *state)
{
	return vfs_proxyfs_write_recv(req, &state->error);
}
#endif

static off_t vfs_proxyfs_lseek(struct vfs_handle_struct *handle,
                               files_struct *fsp,
                               off_t offset,
                               int whence)
{
	DEBUG(10, ("vfs_proxyfs_lseek: %s offset: %ld whence: %d\n", fsp->fsp_name->base_name, offset, whence));

	file_handle_t *fd;
	proxyfs_stat_t *stat;
	int err;

	uint64_t stat_ctime, stat_stime, stat_mtime, stat_atime, stat_size, stat_nlink;

	fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		return -1;
	}

	switch (whence) {
	case SEEK_SET:
		fd->offset = offset;
	case SEEK_CUR:
		fd->offset += offset;
	case SEEK_END:
		err = proxyfs_get_stat(MOUNT_HANDLE(handle), fd->inum, &stat);
		if (err != 0) {
			errno = err;
			return -1;
		}

		offset = stat->size + offset;
		fd->offset = offset;

		free(stat);
	}

	return offset;
}

static ssize_t vfs_proxyfs_sendfile(struct vfs_handle_struct *handle,
                                    int tofd,
                                    files_struct *fromfsp,
                                    const DATA_BLOB *hdr,
                                    off_t offset,
                                    size_t n)
{
	DEBUG(10, ("vfs_proxyfs_sendfile: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return -1;
}

static ssize_t vfs_proxyfs_recvfile(struct vfs_handle_struct *handle,
                                    int fromfd,
                                    files_struct *tofsp,
                                    off_t offset,
                                    size_t n)
{
	DEBUG(10, ("vfs_proxyfs_recvfile: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return -1;
}

static int vfs_proxyfs_rename(struct vfs_handle_struct *handle,
                              const struct smb_filename *smb_fname_src,
                              const struct smb_filename *smb_fname_dst)
{
	int err;
	char *src_path, *dst_path;
	char *src_tmp_path, *dst_tmp_path, *src_parent, *dst_parent;

	src_path = resolve_path(handle, smb_fname_src->base_name);
	dst_path = resolve_path(handle, smb_fname_dst->base_name);

	src_tmp_path = strdup(src_path);
	src_parent = dirname(src_tmp_path);

	dst_tmp_path = strdup(dst_path);
	dst_parent = dirname(dst_tmp_path);

	DEBUG(10, ("vfs_proxyfs_rename: %s to %s\n",src_path, dst_path));

	if ((strcmp(src_parent, dst_parent) == 0) && (strcmp(src_parent, CWD(handle)) == 0)) {
		char *src_basename = basename(src_path);
		char *dst_basename = basename(dst_path);

		err = proxyfs_rename(MOUNT_HANDLE(handle), CWD_INUM(handle), src_basename, CWD_INUM(handle), dst_basename);
	} else {
		err = proxyfs_rename_path(MOUNT_HANDLE(handle), src_path, dst_path);
	}

	free(src_path);
	free(src_tmp_path);
	free(dst_path);
	free(dst_tmp_path);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_fsync(struct vfs_handle_struct *handle,
                             files_struct *fsp)
{
	DEBUG(10, ("vfs_proxyfs_fsync: %s\n", fsp->fsp_name->base_name));

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	int err = proxyfs_flush(MOUNT_HANDLE(handle), fd->inum);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static struct tevent_req *vfs_proxyfs_fsync_send(struct vfs_handle_struct *handle,
                                                 TALLOC_CTX *mem_ctx,
                                                 struct tevent_context *ev,
                                                 struct files_struct *fsp)
{
	DEBUG(10, ("vfs_proxyfs_fsync_send: %s\n", fsp->fsp_name->base_name));

	errno = ENOTSUP;
	return NULL;
}

static int vfs_proxyfs_fsync_recv(struct tevent_req *req,
                                  int *err)
{
	DEBUG(10, ("vfs_proxyfs_fsync_recv: \n"));

	errno = ENOTSUP;
	return -1;
}

#if SAMBA_VERSION_MINOR >= 5
static int vfs_proxyfs_fsync_recv_4_4(struct tevent_req *req,
                                  struct vfs_aio_state *state)
{
	DEBUG(10, ("vfs_proxyfs_fsync_recv: \n"));
	errno = ENOTSUP;
	return -1;
}
#endif

// TBD: How do we handle stat of symbolic link. Currently we are not resolving 
// symlink inode to target inode and then doing stat on it. stat behavior is same as
// lstat behavior for symbolic link.

static int vfs_proxyfs_stat(struct vfs_handle_struct *handle,
                            struct smb_filename *smb_fname)
{
	DEBUG(10, ("vfs_proxyfs_stat: file_name %s\n", smb_fname->base_name));

	struct   stat st;
	char    *path;
	int      err;

	path = resolve_path(handle, smb_fname->base_name);

	proxyfs_stat_t *pst;
	if (strcmp(path, CWD(handle)) == 0) {
		err = proxyfs_get_stat(MOUNT_HANDLE(handle), CWD_INUM(handle), &pst);
	} else {
		err = proxyfs_get_stat_path(MOUNT_HANDLE(handle), path, &pst);
	}
	free(path);

	if (err != 0) {
		errno = err;
		return -1;
	}

	smb_stat_ex_from_stat(&smb_fname->st, pst);
	free(pst);

	DEBUG(10, ("vfs_proxyfs_stat: returning stat for : %s mode 0%o\n", smb_fname->base_name, smb_fname->st.st_ex_mode));
	return 0;
}

static int vfs_proxyfs_fstat(struct vfs_handle_struct *handle,
                             files_struct *fsp,
                             SMB_STRUCT_STAT *sbuf)
{
	DEBUG(10, ("vfs_proxyfs_fstat: %s\n", fsp->fsp_name->base_name));

	proxyfs_stat_t *pst;
	int err;

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	err = proxyfs_get_stat(MOUNT_HANDLE(handle), fd->inum, &pst);
	if (err != 0) {
		errno = err;
		return -1;
	}

	smb_stat_ex_from_stat(sbuf, pst);
	free(pst);

	DEBUG(10, ("vfs_proxyfs_fstat: returning stat for : %ld mode 0%o\n", fd->inum, sbuf->st_ex_mode));

	return 0;
}

static int vfs_proxyfs_lstat(struct vfs_handle_struct *handle,
                             struct smb_filename *smb_fname)
{
	// For now lstat and stat are the same. stat should probably be modified to
	// resolve symbolic link.
	DEBUG(10, ("vfs_proxyfs_lstat: file_name %s\n", smb_fname->base_name));

	return vfs_proxyfs_stat(handle, smb_fname);
}

static uint64_t vfs_proxyfs_get_alloc_size(struct vfs_handle_struct *handle,
                                           files_struct *fsp,
                                           const SMB_STRUCT_STAT *sbuf)
{
	DEBUG(10, ("vfs_proxyfs_get_alloc_size: conn path: %s %ld\n", handle->conn->connectpath, sbuf->st_ex_blocks * 512));
	return sbuf->st_ex_blocks * 512;
}

static int vfs_proxyfs_unlink(struct vfs_handle_struct *handle,
                              const struct smb_filename *smb_fname)
{
	DEBUG(10, ("vfs_proxyfs_unlink: file_name %s\n", smb_fname->base_name));

	char *path = resolve_path(handle, smb_fname->base_name);
	char *tmp_path = strdup(path);
	char *parent = dirname(tmp_path);
	int   err;

	if (strcmp(parent, CWD(handle)) == 0) {
		char *name = basename(path);

		err = proxyfs_unlink(MOUNT_HANDLE(handle), CWD_INUM(handle), name);
	} else {
		err = proxyfs_unlink_path(MOUNT_HANDLE(handle), path);
	}
	free(path);
	free(tmp_path);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_chmod(struct vfs_handle_struct *handle,
                             const char *smb_fname,
                             mode_t mode)
{
	DEBUG(10, ("vfs_proxyfs_chmod: %s mode 0x%x\n", smb_fname, mode));
	char *path = resolve_path(handle, smb_fname);

	int err = proxyfs_chmod_path(MOUNT_HANDLE(handle), path, mode);
	free(path);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

#if SAMBA_VERSION_MINOR >= 5
static int vfs_proxyfs_chmod_4_4(struct vfs_handle_struct *handle,
                             const struct smb_filename *smb_fname,
                             mode_t mode)
{
	return vfs_proxyfs_chmod(handle, smb_fname->base_name, mode);
}
#endif

static int vfs_proxyfs_fchmod(struct vfs_handle_struct *handle,
                              files_struct *fsp,
                              mode_t mode)
{
	DEBUG(10, ("vfs_proxyfs_fchmod: %s mode 0%o\n", fsp->fsp_name->base_name, mode));

	int err;

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	err = proxyfs_chmod(MOUNT_HANDLE(handle), fd->inum, mode);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_chown(struct vfs_handle_struct *handle,
                             const char *smb_fname,
                             uid_t uid,
                             gid_t gid)
{
	DEBUG(10, ("vfs_proxyfs_chown: %s uid %d gid %d\n", smb_fname, uid, gid));
	char *path = resolve_path(handle, smb_fname);

	int err = proxyfs_chown_path(MOUNT_HANDLE(handle), path, uid, gid);
	free(path);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

#if SAMBA_VERSION_MINOR >= 5
static int vfs_proxyfs_chown_4_4(struct vfs_handle_struct *handle,
                                 const struct smb_filename *smb_fname,
                                 uid_t uid,
                                 gid_t gid)
{
	return vfs_proxyfs_chown(handle, smb_fname->base_name, uid, gid);
}
#endif

static int vfs_proxyfs_fchown(struct vfs_handle_struct *handle,
                              files_struct *fsp,
                              uid_t uid,
                              gid_t gid)
{
	DEBUG(10, ("vfs_proxyfs_fchown: %s uid %d gid %d\n", fsp->fsp_name->base_name, uid, gid));
	int err;

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	err = proxyfs_chown(MOUNT_HANDLE(handle), fd->inum, uid, gid);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return -1;
}

static int vfs_proxyfs_lchown(struct vfs_handle_struct *handle,
                              const char *smb_fname,
                              uid_t uid,
                              gid_t gid)
{
	DEBUG(10, ("vfs_proxyfs_lchown: %s uid %d gid %d\n", smb_fname, uid, gid));
	return vfs_proxyfs_chown(handle, smb_fname, uid, gid);
}

#if SAMBA_VERSION_MINOR >= 5
static int vfs_proxyfs_lchown_4_4(struct vfs_handle_struct *handle,
                              const struct smb_filename *smb_fname,
                              uid_t uid,
                              gid_t gid)
{
	return vfs_proxyfs_lchown(handle, smb_fname->base_name, uid, gid);
}
#endif

static int vfs_proxyfs_chdir(struct vfs_handle_struct *handle,
                             const char *path)
{
	DEBUG(10, ("vfs_proxyfs_chdir: %s\n", path));

	uint64_t inum;
	char *rpath = resolve_path(handle, path);
	int err = proxyfs_lookup_path(MOUNT_HANDLE(handle), rpath, &inum);
	if (err != 0) {
		errno = err;
		free(rpath);
		return -1;
	}

	fs_ctx_t *ctx = (fs_ctx_t *)handle->data;
	if (ctx->cwd != NULL) {
		free(ctx->cwd);
	}

	ctx->cwd = rpath;
	ctx->cwd_inum = inum;

	return 0;
}

static char *vfs_proxyfs_getwd(struct vfs_handle_struct *handle)
{
	fs_ctx_t *ctx = (fs_ctx_t *)handle->data;

	char *path = strdup(ctx->cwd);

	DEBUG(10, ("vfs_proxyfs_getwd: %s\n", path));

	return path;
}

static int vfs_proxyfs_ntimes(struct vfs_handle_struct *handle,
                              const struct smb_filename *smb_fname,
                              struct smb_file_time *ft)
{
	DEBUG(10, ("vfs_proxyfs_ntimes: file_name %s\n", smb_fname->base_name));

	struct timespec times[2];

	if (null_timespec(ft->atime)) {
		times[0].tv_sec = smb_fname->st.st_ex_atime.tv_sec;
		times[0].tv_nsec = smb_fname->st.st_ex_atime.tv_nsec;
	} else {
		times[0].tv_sec = ft->atime.tv_sec;
		times[0].tv_nsec = ft->atime.tv_nsec;
	}

	if (null_timespec(ft->mtime)) {
		times[1].tv_sec = smb_fname->st.st_ex_mtime.tv_sec;
		times[1].tv_nsec = smb_fname->st.st_ex_mtime.tv_nsec;
	} else {
		times[1].tv_sec = ft->mtime.tv_sec;
		times[1].tv_nsec = ft->mtime.tv_nsec;
	}

	if ((timespec_compare(&times[0],
			      &smb_fname->st.st_ex_atime) == 0) &&
	    (timespec_compare(&times[1],
			      &smb_fname->st.st_ex_mtime) == 0)) {
		return 0;
	}

	uint64_t inum;
	char *path = resolve_path(handle, smb_fname->base_name);
	int err = proxyfs_lookup_path(MOUNT_HANDLE(handle), path, &inum);
	free(path);
	if (err != 0) {
		errno = err;
		return -1;
	}

	proxyfs_timespec_t atim, mtim;
	atim.sec  = times[0].tv_sec;
	atim.nsec = times[0].tv_nsec;
	mtim.sec  = times[1].tv_sec;
	mtim.nsec = times[1].tv_nsec;

	err = proxyfs_settime(MOUNT_HANDLE(handle), inum, &atim, &mtim);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_ftruncate(struct vfs_handle_struct *handle,
                                 files_struct *fsp,
                                 off_t offset)
{
	DEBUG(10, ("vfs_proxyfs_ftruncate: %s offset: %ld\n", fsp->fsp_name->base_name, offset));

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	fd->offset = offset;

	int err = proxyfs_resize(MOUNT_HANDLE(handle), fd->inum, offset);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_fallocate(struct vfs_handle_struct *handle,
                                 struct files_struct *fsp,
                                 uint32_t mode,
                                 off_t offset, off_t len)
{
	DEBUG(10, ("vfs_proxyfs_fallocate: %s mode o%o offset: %ld len: %ld\n", fsp->fsp_name->base_name, mode, offset, len));

	// TBD: VFS makes assumptions about proxyfs, it should not! The logic should move to proxyfs and vfs should just call the proxyfs fallocate() interface.

	// Proxyfs doesn't have a block reservaction mechanism, so hope we have enough space :)
	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

#ifdef FALLOC_FL_KEEP_SIZE // Currently defined in <linux/falloc.h>, conditionally included based on os type.
	if (mode == FALLOC_FL_KEEP_SIZE) {
		return 0;
	}
#endif

	if (mode != 0) {
		// At this time we don't support any operation other than extending the file.
		return -1;
	}

	// 1. First get the size of the file.
	// 2. If the size is less than (offset + len) - extend the file, otherwise return.

	proxyfs_stat_t *stat_info;

	int err = proxyfs_get_stat(MOUNT_HANDLE(handle), fd->inum, &stat_info);
	if (err != 0) {
		errno = err;
		return -1;
	}

	uint64_t new_size = offset + len;
	if (stat_info->size > new_size) {
		free(stat_info);
		return 0;
	}

	free(stat_info);

	err = proxyfs_resize(MOUNT_HANDLE(handle), fd->inum, new_size);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static bool vfs_proxyfs_lock(struct vfs_handle_struct *handle,
                             files_struct *fsp,
                             int op,
                             off_t offset,
                             off_t count,
                             int type)
{
	DEBUG(10, ("vfs_proxyfs_lock: %s op: %d offset: %ld count: %ld type: %d\n", fsp->fsp_name->base_name, op, offset, count, type));

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);

	// for op and type values refer to fcntl(2), cmd and l_type.
	// op(cmd): F_GETLK, F_SETLK and F_SETLKW are used to acquire, release, and test for the existence of record locks.
	// type (l_type): Type of lock: F_RDLCK, F_WRLCK, F_UNLCK

	struct flock flock;

	flock.l_type = type;
	flock.l_whence = SEEK_SET;
	flock.l_start = offset;
	flock.l_len = count;
	flock.l_pid = getpid();

	DEBUG(10, ("vfs_proxyfs_lock: Calling proxyfs_flock: type: %d start %ld count %ld pid %d\n", flock.l_type, flock.l_start, flock.l_len, flock.l_pid));

	int err = proxyfs_flock(MOUNT_HANDLE(handle), fd->inum, op, &flock);
	if (err != 0) {
		DEBUG(10, ("vfs_proxyfs_lock: flock failed: %d\n", err));
		errno = err;
		return false;
	}

    if ((op == F_GETLK) && (flock.l_type != F_UNLCK)) {
		// Lock is already held by someone, not possible to get the lock.
		DEBUG(10, ("vfs_proxyfs_lock: F_GETLK failed, conflicting lock: lock_type = %d error = %d\n", type, err));
		return false;
	}

	return true;
}

static int vfs_proxyfs_kernel_flock(struct vfs_handle_struct *handle,
                                    files_struct *fsp,
                                    uint32_t share_mode,
                                    uint32_t access_mask)
{
	DEBUG(10, ("vfs_proxyfs_kernel_flock: %s file : %s share_mode 0%o mask 0x%x\n", handle->conn->connectpath,
		      fsp->fsp_name->base_name, share_mode, access_mask));
	errno = ENOSYS;
	return 0;
}

static int vfs_proxyfs_linux_setlease(struct vfs_handle_struct *handle,
                                      files_struct *fsp,
                                      int leasetype)
{
	DEBUG(10, ("vfs_proxyfs_linux_setlease: %s leasetype:%d\n", fsp->fsp_name->base_name, leasetype));
	errno = ENOSYS;
	return -1;
}

static bool vfs_proxyfs_getlock(struct vfs_handle_struct *handle,
                                files_struct *fsp, off_t *poffset,
                                off_t *pcount, int *ptype, pid_t *ppid)
{
	DEBUG(10, ("vfs_proxyfs_getlock: %s\n", fsp->fsp_name->base_name));

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);

	// for op and type values refer to fcntl(2), cmd and l_type.
	// op(cmd): F_GETLK, F_SETLK and F_SETLKW are used to acquire, release, and test for the existence of record locks.
	// type (l_type): Type of lock: F_RDLCK, F_WRLCK, F_UNLCK

	struct flock flock = { 0, };

	flock.l_type = *ptype;
	flock.l_whence = SEEK_SET;
	flock.l_start = *poffset;
	flock.l_len = *pcount;
	flock.l_pid = getpid();

	int err = proxyfs_flock(MOUNT_HANDLE(handle), fd->inum, F_GETLK, &flock);
	if (err != 0) {
		DEBUG(10, ("vfs_proxyfs_getlock: failed - %d\n", err));
		errno = err;
		return false;
	}

	*ptype = flock.l_type;
	*poffset = flock.l_start;
	*pcount = flock.l_len;
	*ppid = flock.l_pid;

	return true;
}

static int vfs_proxyfs_symlink(struct vfs_handle_struct *handle,
                               const char *oldpath, const char *newpath)
{
	DEBUG(10, ("vfs_proxyfs_symlink: %s --> %s\n", newpath, oldpath));

	char *path = resolve_path(handle, newpath);

	uid_t uid = get_current_uid(handle->conn);
	gid_t gid = get_current_gid(handle->conn);
	int err = proxyfs_symlink_path(MOUNT_HANDLE(handle), path, (char *)oldpath, uid, gid);
	free(path);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_readlink(struct vfs_handle_struct *handle,
                                const char *path, char *buf, size_t bufsize)
{
	uint64_t ino;

	char *rpath = resolve_path(handle, (char *)path);

	const char *target;

	int err = proxyfs_read_symlink_path(MOUNT_HANDLE(handle), rpath, &target);
	if (err != 0) {
		errno = err;
		return -1;
	}

	DEBUG(10, ("vfs_proxyfs_readlink: %s --> %s\n", rpath, target));

	int len = strlen(target) + 1;

	if (len > bufsize) {
		errno = E2BIG;
		return -1;
	}

	bzero(buf, len);
	memcpy(buf, target, strlen(target));

	free((char *)target);
	free(rpath);

	return len;
}

static int vfs_proxyfs_link(struct vfs_handle_struct *handle,
                            const char *oldpath, const char *newpath)
{
	DEBUG(10, ("vfs_proxyfs_link: oldpath %s newpath %s\n", oldpath, newpath));

	char *old_path = resolve_path(handle, oldpath);
	char *new_path = resolve_path(handle, newpath);

	int err = proxyfs_link_path(MOUNT_HANDLE(handle), new_path, old_path);
	free(old_path);
	free(new_path);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_mknod(struct vfs_handle_struct *handle,
                             const char *path, mode_t mode, SMB_DEV_T dev)
{
	DEBUG(10, ("vfs_proxyfs_mknod: %s\n", handle->conn->connectpath));

	errno = ENOTSUP;
	return -1;
}

static char *vfs_proxyfs_realpath(struct vfs_handle_struct *handle,
                                  const char *path)
{
	char *rpath = resolve_path(handle, path);

	if (strcmp(rpath, handle->conn->connectpath) == 0) {
		free(rpath);
		rpath = strdup("/");
	}

	DEBUG(10, ("vfs_proxyfs_realpath: %s -> %s\n", path, rpath));

	return rpath;
}

static int vfs_proxyfs_chflags(struct vfs_handle_struct *handle,
                               const char *path,
                               unsigned int flags)
{
	DEBUG(10, ("vfs_proxyfs_chflags: %s\n", handle->conn->connectpath));
//	errno = ENOSYS;
	return 0;
}

static struct file_id vfs_proxyfs_file_id_create(struct vfs_handle_struct *handle,
                                                 const SMB_STRUCT_STAT *sbuf)
{
	DEBUG(10, ("vfs_proxyfs_file_id_create\n"));

	struct file_id *fd = (struct file_id *)malloc(sizeof(struct file_id));
	bzero(fd, sizeof(struct file_id));
	return *fd;
}

static struct tevent_req *vfs_proxyfs_copy_chunk_send(struct vfs_handle_struct *handle,
                                                      TALLOC_CTX *mem_ctx,
                                                      struct tevent_context *ev,
                                                      struct files_struct *src_fsp,
                                                      off_t src_off,
                                                      struct files_struct *dest_fsp,
                                                      off_t dest_off,
                                                      off_t num)
{
	DEBUG(10, ("vfs_proxyfs_copy_chunk_send: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NULL;
}


static NTSTATUS vfs_proxyfs_copy_chunk_recv(struct vfs_handle_struct *handle,
                                            struct tevent_req *req,
                                            off_t *copied)
{
	DEBUG(10, ("vfs_proxyfs_copy_chunk_recv: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_get_compression(struct vfs_handle_struct *handle,
                                            TALLOC_CTX *mem_ctx,
                                            struct files_struct *fsp,
                                            struct smb_filename *smb_fname,
                                            uint16_t *_compression_fmt)
{
	DEBUG(10, ("vfs_proxyfs_get_compression: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_set_compression(struct vfs_handle_struct *handle,
                                            TALLOC_CTX *mem_ctx,
                                            struct files_struct *fsp,
                                            uint16_t compression_fmt)
{
	DEBUG(10, ("vfs_proxyfs_set_compression: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_snap_check_path(struct vfs_handle_struct *handle,
                                            TALLOC_CTX *mem_ctx,
                                            const char *service_path,
                                            char **base_volume)
{
	DEBUG(10, ("vfs_proxyfs_snap_check_path: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_snap_create(struct vfs_handle_struct *handle,
                                        TALLOC_CTX *mem_ctx,
                                        const char *base_volume,
                                        time_t *tstamp,
                                        bool rw,
                                        char **base_path,
                                        char **snap_path)
{
	DEBUG(10, ("vfs_proxyfs_snap_create: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_snap_delete(struct vfs_handle_struct *handle,
                                        TALLOC_CTX *mem_ctx,
                                        char *base_path,
                                        char *snap_path)
{
	DEBUG(10, ("vfs_proxyfs_snap_delete: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_streaminfo(struct vfs_handle_struct *handle,
                                       struct files_struct *fsp,
                                       const struct smb_filename *smb_fname,
                                       TALLOC_CTX *mem_ctx,
                                       unsigned int *num_streams,
                                       struct stream_struct **streams)
{
	DEBUG(10, ("vfs_proxyfs_streaminfo: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static int vfs_proxyfs_get_real_filename(struct vfs_handle_struct *handle,
                                         const char *path, const char *name,
                                         TALLOC_CTX *mem_ctx, char **found_name)
{
	DEBUG(10, ("vfs_proxyfs_get_real_filename: %s\n", path));
	errno = ENOTSUP;
	return -1;

}

static const char *vfs_proxyfs_connectpath(struct vfs_handle_struct *handle,
                                           const char *filename)
{
	DEBUG(10, ("vfs_proxyfs_connectpath: %s\n", handle->conn->connectpath));
	return handle->conn->connectpath;
}

static NTSTATUS vfs_proxyfs_brl_lock_windows(struct vfs_handle_struct *handle,
                                             struct byte_range_lock *br_lck,
                                             struct lock_struct *plock,
                                             bool blocking_lock)
{
	DEBUG(10, ("vfs_proxyfs_brl_lock_windows: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static bool vfs_proxyfs_brl_unlock_windows(struct vfs_handle_struct *handle,
                                           struct messaging_context *msg_ctx,
                                           struct byte_range_lock *br_lck,
                                           const struct lock_struct *plock)
{
	DEBUG(10, ("vfs_proxyfs_brl_unlock_windows: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return false;
}

static bool vfs_proxyfs_brl_cancel_windows(struct vfs_handle_struct *handle,
                                           struct byte_range_lock *br_lck,
                                           struct lock_struct *plock)
{
	DEBUG(10, ("vfs_proxyfs_brl_cancel_windows: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return false;
}

static bool vfs_proxyfs_strict_lock(struct vfs_handle_struct *handle,
                                    struct files_struct *fsp,
                                    struct lock_struct *plock)
{
	DEBUG(10, ("vfs_proxyfs_strict_lock: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return false;
}

static void vfs_proxyfs_strict_unlock(struct vfs_handle_struct *handle,
                                      struct files_struct *fsp,
                                      struct lock_struct *plock)
{
	DEBUG(10, ("vfs_proxyfs_strict_unlock: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
}

static NTSTATUS vfs_proxyfs_translate_name(struct vfs_handle_struct *handle,
                                           const char *name,
                                           enum vfs_translate_direction direction,
                                           TALLOC_CTX *mem_ctx,
                                           char **mapped_name)
{
	DEBUG(10, ("vfs_proxyfs_translate_name: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_fsctl(struct vfs_handle_struct *handle,
                                  struct files_struct *fsp,
                                  TALLOC_CTX *ctx,
                                  uint32_t function,
                                  uint16_t req_flags,
                                  const uint8_t *_in_data,
                                  uint32_t in_len,
                                  uint8_t **_out_data,
                                  uint32_t max_out_len,
                                  int32_t *out_len)
{
	DEBUG(10, ("vfs_proxyfs_fsctl is not supported\n"));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}
static NTSTATUS vfs_proxyfs_get_dos_attributes(struct vfs_handle_struct *handle,
                                               struct smb_filename *smb_fname,
                                               uint32_t *dosmode)
{
	DEBUG(10, ("vfs_proxyfs_get_dos_attributes: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_fget_dos_attributes(struct vfs_handle_struct *handle,
                                                struct files_struct *fsp,
                                                uint32_t *dosmode)
{
	DEBUG(10, ("vfs_proxyfs_fget_dos_attributes: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}
static NTSTATUS vfs_proxyfs_set_dos_attributes(struct vfs_handle_struct *handle,
                                               const struct smb_filename *smb_fname,
                                               uint32_t dosmode)
{
	DEBUG(10, ("vfs_proxyfs_set_dos_attributes: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_fset_dos_attributes(struct vfs_handle_struct *handle,
                                                struct files_struct *fsp,
                                                uint32_t dosmode)
{
	DEBUG(10, ("vfs_proxyfs_fset_dos_attributes: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}
	/* NT ACL operations. */

static NTSTATUS vfs_proxyfs_fget_nt_acl(struct vfs_handle_struct *handle,
                                        struct files_struct *fsp,
                                        uint32_t security_info,
                                        TALLOC_CTX *mem_ctx,
                                        struct security_descriptor **ppdesc)
{
	DEBUG(10, ("vfs_proxyfs_fget_nt_acl: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_get_nt_acl(struct vfs_handle_struct *handle,
                                       const struct smb_filename *smb_fname,
                                       uint32_t security_info,
                                       TALLOC_CTX *mem_ctx,
                                       struct security_descriptor **ppdesc)
{
	DEBUG(10, ("vfs_proxyfs_get_nt_acl: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_fset_nt_acl(struct vfs_handle_struct *handle,
                                        struct files_struct *fsp,
                                        uint32_t security_info_sent,
                                        const struct security_descriptor *psd)
{
	DEBUG(10, ("vfs_proxyfs_fset_nt_acl: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_audit_file(struct vfs_handle_struct *handle,
                                       struct smb_filename *file,
                                       struct security_acl *sacl,
                                       uint32_t access_requested,
                                       uint32_t access_denied)
{
	DEBUG(10, ("vfs_proxyfs_audit_file: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

/* POSIX ACL operations. */
static SMB_ACL_T mode_to_smb_acls(proxyfs_stat_t *st, TALLOC_CTX *mem_ctx) {
	struct smb_acl_t *result;
	int count;

	count = 3;
	result = sys_acl_init(mem_ctx);
	if (!result) {
		errno = ENOMEM;
		return NULL;
	}

	result->acl = talloc_array(result, struct smb_acl_entry, count);
	if (!result->acl) {
		errno = ENOMEM;
		talloc_free(result);
		return NULL;
	}

	result->count = count;

	result->acl[0].a_type = SMB_ACL_USER_OBJ;
	result->acl[0].a_perm = (st->mode & S_IRWXU) >> 6;

	result->acl[1].a_type = SMB_ACL_GROUP_OBJ;
	result->acl[1].a_perm = (st->mode & S_IRWXG) >> 3;

	result->acl[2].a_type = SMB_ACL_OTHER;
	result->acl[2].a_perm = st->mode & S_IRWXO;

	return result;
}

static SMB_ACL_T blob_to_smb_acl(DATA_BLOB *blob, TALLOC_CTX *mem_ctx) {
	enum ndr_err_code ndr_err;

	SMB_ACL_T acl = talloc_size(mem_ctx, sizeof(struct smb_acl_t));
	if (!acl) {
		DEBUG(1, ("Failed to allocate memory for acl when converting buf to acl\n"));
		errno = ENOMEM;
		return NULL;
	}

	ndr_err = ndr_pull_struct_blob(blob, acl, acl, (ndr_pull_flags_fn_t)ndr_pull_smb_acl_t);
	if (!NDR_ERR_CODE_IS_SUCCESS(ndr_err)) {
		DEBUG(1, ("ndr_pull_acl_t failed: %s\n", ndr_errstr(ndr_err)));
		TALLOC_FREE(acl);
		return NULL;
	}

	return acl;
}

static void smb_acl_to_blob(SMB_ACL_T acl, TALLOC_CTX *mem_ctx, DATA_BLOB *blob) {

	enum ndr_err_code ndr_err;
	blob->data = NULL;

	ndr_err = ndr_push_struct_blob(blob, mem_ctx, acl, (ndr_push_flags_fn_t)ndr_push_smb_acl_t);
	if (!NDR_ERR_CODE_IS_SUCCESS(ndr_err)) {
		DEBUG(1, ("ndr_push_acl_t failed: %s\n", ndr_errstr(ndr_err)));
	}
}

static SMB_ACL_T get_xattr_error_out(struct vfs_handle_struct *handle,
						 char *rpath,
						 TALLOC_CTX *mem_ctx,
						 ssize_t ret)
{
	proxyfs_stat_t *st;
	SMB_ACL_T result;

	ret = proxyfs_get_stat_path(MOUNT_HANDLE(handle), rpath, &st);
	if (ret != 0) {
		errno = ret;
		free(rpath);
		return NULL;
	}

	free(rpath);
	result = mode_to_smb_acls(st, mem_ctx);
	free(st);
	return result;
}

static SMB_ACL_T vfs_proxyfs_sys_acl_get_file(struct vfs_handle_struct *handle,
                                              const char *path_p,
                                              SMB_ACL_TYPE_T type,
                                              TALLOC_CTX *mem_ctx)
{
	DEBUG(10, ("vfs_proxyfs_sys_acl_get_file: %s ACL_TYPE: %d\n", path_p, type));

	SMB_ACL_T result;
	struct stat st;
	uint8_t *buf = NULL;
	const char *key;
	ssize_t ret, size;

	switch (type) {
	case SMB_ACL_TYPE_ACCESS:
		key = "system.posix_acl_access";
		break;
	case SMB_ACL_TYPE_DEFAULT:
		key = "system.posix_acl_default";
		break;
	default:
		errno = EINVAL;
		return NULL;
	}

	char *rpath = resolve_path(handle, path_p);

	// First find out how large the buf should be by passing 0 for the size.
	//
	// Then malloc() a buf and do the call again with the buf pointer.
	size = 0;
	ret = proxyfs_get_xattr_path(MOUNT_HANDLE(handle), rpath, key, NULL, &size);
	if ((ret != 0) || (size == 0)) {
		return get_xattr_error_out(handle, rpath, mem_ctx, ret);
	}

	buf = malloc(size+1);

	ret = proxyfs_get_xattr_path(MOUNT_HANDLE(handle), rpath, key, buf, &size);
	if ((ret != 0) || (size == 0)) {
		free(buf);
		return get_xattr_error_out(handle, rpath, mem_ctx, ret);
	}

	free(rpath);

	DATA_BLOB blob;
	blob.data = buf;
	blob.length = size;
	result = blob_to_smb_acl(&blob, mem_ctx);

	free(buf);

	return result;
}

static SMB_ACL_T vfs_proxyfs_sys_acl_get_fd(struct vfs_handle_struct *handle,
                                            struct files_struct *fsp,
                                            TALLOC_CTX *mem_ctx)
{
	DEBUG(10, ("vfs_proxyfs_sys_acl_get_fd: %s ACL_TYPE: system.posix_acl_access\n", fsp->fsp_name->base_name));
	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);

	SMB_ACL_T result;
	struct stat st;
	char *buf;
	const char *key = "system.posix_acl_access";
	ssize_t ret, size;

	ret = proxyfs_get_xattr(MOUNT_HANDLE(handle), fd->inum, key, (void **)&buf, &size);
	if ((ret != 0) || (size == 0)) {
		proxyfs_stat_t *st;
		ret = proxyfs_get_stat(MOUNT_HANDLE(handle), fd->inum, &st);
		if (ret != 0) {
			errno = ret;
			return NULL;
		}

		result = mode_to_smb_acls(st, mem_ctx);
		free(st);
		return result;
	}

	DATA_BLOB blob;
	blob.data = buf;
	blob.length = size;
	result = blob_to_smb_acl(&blob, mem_ctx);

	free(buf);

	return result;
}

static int vfs_proxyfs_sys_acl_set_file(struct vfs_handle_struct *handle,
                                        const char *name, SMB_ACL_TYPE_T acltype, SMB_ACL_T theacl)
{

	const char *key;

	switch (acltype) {
	case SMB_ACL_TYPE_ACCESS:
		key = "system.posix_acl_access";
		break;
	case SMB_ACL_TYPE_DEFAULT:
		key = "system.posix_acl_default";
		break;
	default:
		errno = EINVAL;
		return -1;
	}

	char *rpath = resolve_path(handle, name);

	DEBUG(10, ("vfs_proxyfs_sys_acl_set_file: %s SMB_ACL_TYPE_T %d Key %s\n", name, acltype, key));

	TALLOC_CTX *mem_ctx = talloc_init("proxyfs_set_acl");
	if (!mem_ctx) {
		DEBUG(10, ("Failed to initialize memory context for writing acl to xattr\n"));
		free(rpath);
		errno = ENOMEM;
		return -1;
	}

	DATA_BLOB blob;
	smb_acl_to_blob(theacl, mem_ctx, &blob);
	if (!blob.data) {
		DEBUG(1, ("Failed to get memory to convert acl to blob\n"));
		free(rpath);
		talloc_free(mem_ctx);
		errno = ENOMEM;
		return -1;
	}

	int ret = proxyfs_set_xattr_path(MOUNT_HANDLE(handle), rpath, key, blob.data, blob.length, 0);
	if (ret != 0) {
		DEBUG(1, ("Failed to store acl blob in xattr\n"));
		talloc_free(blob.data);
		free(rpath);
		errno = ret;
		ret = -1;
	}

	talloc_free(blob.data);
	free(rpath);
	talloc_free(mem_ctx);

	DEBUG(10, ("SUCCESSFULLY stored the acl\n"));
	return ret;
}

static int vfs_proxyfs_sys_acl_set_fd(struct vfs_handle_struct *handle,
                                      struct files_struct *fsp, SMB_ACL_T theacl)
{
	DEBUG(10, ("vfs_proxyfs_sys_acl_set_fd: %s\n", fsp->fsp_name->base_name));
	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);

	const char *key = "system.posix_acl_access";

	TALLOC_CTX *mem_ctx = talloc_init("proxyfs_set_acl");
	if (!mem_ctx) {
		DEBUG(10, ("Failed to initialize memory context for writing acl to xattr\n"));
		errno = ENOMEM;
		return -1;
	}

	DATA_BLOB blob;
	smb_acl_to_blob(theacl, mem_ctx, &blob);
	if (!blob.data) {
		DEBUG(1, ("Failed to get memory to convert acl to blob\n"));
		errno = ENOMEM;
		talloc_free(mem_ctx);
		return -1;
	}

	int ret = proxyfs_set_xattr(MOUNT_HANDLE(handle), fd->inum, key, blob.data, blob.length, 0);
	talloc_free(blob.data);
	talloc_free(mem_ctx);

	if (ret != 0) {
		errno = ret;
		ret = -1;
	}

	return ret;
}

static int vfs_proxyfs_sys_acl_delete_def_file(struct vfs_handle_struct *handle, const char *path)
{
	DEBUG(10, ("vfs_proxyfs_sys_acl_delete_def_file: %s\n", path));

	const char *key = "system.posix_acl_default";
	char *rpath = resolve_path(handle, path);

	int ret = proxyfs_remove_xattr_path(MOUNT_HANDLE(handle), rpath, key);
	free(rpath);

	if (ret != 0) {
		errno = ret;
		ret = -1;
	}

	return ret;
}

/* EA Operations */

static ssize_t vfs_proxyfs_getxattr(struct vfs_handle_struct *handle,
                                    const char *path, const char *name,
                                    void *value, size_t size)
{
	DEBUG(10, ("vfs_proxyfs_getxattr: %s name %s\n", path, name));

	char *rpath = resolve_path(handle, path);

	int err = proxyfs_get_xattr_path(MOUNT_HANDLE(handle), rpath, name, value, &size);
	free(rpath);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return size;
}

static ssize_t vfs_proxyfs_fgetxattr(struct vfs_handle_struct *handle,
                                     files_struct *fsp, const char *name,
                                     void *value, size_t size)
{
	DEBUG(10, ("vfs_proxyfs_fgetxattr: %s name %s\n", fsp->fsp_name->base_name, name));
	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	int err = proxyfs_get_xattr(MOUNT_HANDLE(handle), fd->inum, name, value, &size);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return size;
}

static ssize_t vfs_proxyfs_listxattr(struct vfs_handle_struct *handle,
                                     const char *path, char *list, size_t size)
{
	DEBUG(10, ("vfs_proxyfs_listxattr: %s\n", path));

	char *rpath = resolve_path(handle, path);
	int err = proxyfs_list_xattr_path(MOUNT_HANDLE(handle), rpath, &list, &size);
	free(rpath);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return size;
}

static ssize_t vfs_proxyfs_flistxattr(struct vfs_handle_struct *handle,
                                      files_struct *fsp, char *list,
                                      size_t size)
{
	DEBUG(10, ("vfs_proxyfs_flistxattr: %s\n", fsp->fsp_name->base_name));
	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	int err = proxyfs_list_xattr(MOUNT_HANDLE(handle), fd->inum, &list, &size);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return size;
}

static int vfs_proxyfs_removexattr(struct vfs_handle_struct *handle,
                                   const char *path, const char *name)
{
	DEBUG(10, ("vfs_proxyfs_removexattr: %s name %s\n", path, name));
	char *rpath = resolve_path(handle, path);

	int err = proxyfs_remove_xattr_path(MOUNT_HANDLE(handle), rpath, name);
	DEBUG(10, ("File %s Remove xattr %s errcode %d\n", rpath, name, err));

	free(rpath);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_fremovexattr(struct vfs_handle_struct *handle,
                                    files_struct *fsp, const char *name)
{
	DEBUG(10, ("vfs_proxyfs_fremovexattr: %s name %s\n", fsp->fsp_name->base_name, name));

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	int err = proxyfs_remove_xattr(MOUNT_HANDLE(handle), fd->inum, name);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_setxattr(struct vfs_handle_struct *handle,
                                const char *path, const char *name,
                                const void *value, size_t size, int flags)
{
	DEBUG(10, ("vfs_proxyfs_setxattr: %s name %s\n", path, name));

	char *rpath = resolve_path(handle, path);

	int err = proxyfs_set_xattr_path(MOUNT_HANDLE(handle), rpath, name, value, size, flags);
	free(rpath);

	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

static int vfs_proxyfs_fsetxattr(struct vfs_handle_struct *handle,
                                 files_struct *fsp, const char *name,
                                 const void *value, size_t size, int flags)
{
	DEBUG(10, ("vfs_proxyfs_fsetxattr: %s name %s\n", fsp->fsp_name->base_name, name));

	file_handle_t *fd = *(file_handle_t **)VFS_FETCH_FSP_EXTENSION(handle, fsp);
	if (fd == NULL) {
		errno = EINVAL;
		return -1;
	}

	int err = proxyfs_set_xattr(MOUNT_HANDLE(handle), fd->inum, name, value, size, flags);
	if (err != 0) {
		errno = err;
		return -1;
	}

	return 0;
}

/* AIO Operations */
static bool vfs_proxyfs_aio_force(struct vfs_handle_struct *handle, struct files_struct *fsp)
{
	DEBUG(10, ("vfs_proxyfs_aio_force: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return false;
}


/* Offline Operations */

static bool vfs_proxyfs_is_offline(struct vfs_handle_struct *handle,
                                   const struct smb_filename *fname,
                                   SMB_STRUCT_STAT *sbuf)
{
	DEBUG(10, ("vfs_proxyfs_is_offline: %s\n", handle->conn->connectpath));
	return false;
}

static int vfs_proxyfs_set_offline(struct vfs_handle_struct *handle,
                                   const struct smb_filename *fname)
{
	DEBUG(10, ("vfs_proxyfs_set_offline: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return -1;
}

	/* durable handle operations */
static NTSTATUS vfs_proxyfs_durable_cookie(struct vfs_handle_struct *handle,
                                           struct files_struct *fsp,
                                           TALLOC_CTX *mem_ctx,
                                           DATA_BLOB *cookie)
{
	DEBUG(10, ("vfs_proxyfs_durable_cookie: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_durable_disconnect(struct vfs_handle_struct *handle,
                                               struct files_struct *fsp,
                                               const DATA_BLOB old_cookie,
                                               TALLOC_CTX *mem_ctx,
                                               DATA_BLOB *new_cookie)
{
	DEBUG(10, ("vfs_proxyfs_durable_disconnect: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_durable_reconnect(struct vfs_handle_struct *handle,
                                              struct smb_request *smb1req,
                                              struct smbXsrv_open *op,
                                              const DATA_BLOB old_cookie,
                                              TALLOC_CTX *mem_ctx,
                                              struct files_struct **fsp,
                                              DATA_BLOB *new_cookie)
{
	DEBUG(10, ("vfs_proxyfs_durable_reconnect: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static NTSTATUS vfs_proxyfs_readdir_attr(struct vfs_handle_struct *handle,
                                         const struct smb_filename *fname,
                                         TALLOC_CTX *mem_ctx,
                                         struct readdir_attr_data **attr_data)
{
	DEBUG(10, ("vfs_proxyfs_readdir_attr: %s\n", handle->conn->connectpath));
	errno = ENOTSUP;
	return NT_STATUS_NOT_IMPLEMENTED;
}

static struct vfs_fn_pointers proxyfs_fns = {

	.connect_fn = vfs_proxyfs_connect,
	.disconnect_fn = vfs_proxyfs_disconnect,
	.disk_free_fn = vfs_proxyfs_disk_free,
#if (SAMBA_VERSION_MINOR >= 4)
	.get_quota_fn = vfs_proxyfs_get_quota_4_4,
#else
	.get_quota_fn = vfs_proxyfs_get_quota,
#endif
	.set_quota_fn = vfs_proxyfs_set_quota,
	.get_shadow_copy_data_fn = vfs_get_shadow_copy_data,
	.statvfs_fn = vfs_proxyfs_statvfs,
	.fs_capabilities_fn = vfs_proxyfs_fs_capabilities,
	.get_dfs_referrals_fn = NULL,

	/* Directory Operations */

#if SAMBA_VERSION_MINOR >= 5
	.opendir_fn = vfs_proxyfs_opendir_4_4,
#else
	.opendir_fn = vfs_proxyfs_opendir,
#endif
	.fdopendir_fn = vfs_proxyfs_fdopendir,
	.readdir_fn = vfs_proxyfs_readdir,
	.seekdir_fn = vfs_proxyfs_seekdir,
	.telldir_fn = vfs_proxyfs_telldir,
	.rewind_dir_fn = vfs_proxyfs_rewinddir,
#if SAMBA_VERSION_MINOR >= 5
	.mkdir_fn = vfs_proxyfs_mkdir_4_4,
	.rmdir_fn = vfs_proxyfs_rmdir_4_4,
#else
	.mkdir_fn = vfs_proxyfs_mkdir,
	.rmdir_fn = vfs_proxyfs_rmdir,
#endif
	.closedir_fn = vfs_proxyfs_closedir,
	.init_search_op_fn = vfs_proxyfs_init_search_op,

	/* File Operations */

	.open_fn = vfs_proxyfs_open,
	.create_file_fn = NULL,
//	.create_file_fn = vfs_proxyfs_create,
	.close_fn = vfs_proxyfs_close,
	.read_fn = vfs_proxyfs_read,
	.pread_fn = vfs_proxyfs_pread,
	.pread_send_fn = vfs_proxyfs_pread_send,
#if SAMBA_VERSION_MINOR >= 5
	.pread_recv_fn = vfs_proxyfs_read_recv_4_4,
#else
	.pread_recv_fn = vfs_proxyfs_read_recv,
#endif
	.write_fn = vfs_proxyfs_write,
	.pwrite_fn = vfs_proxyfs_pwrite,
	.pwrite_send_fn = vfs_proxyfs_pwrite_send,
#if SAMBA_VERSION_MINOR >= 5
	.pwrite_recv_fn = vfs_proxyfs_write_recv_4_4,
#else
	.pwrite_recv_fn = vfs_proxyfs_write_recv,
#endif
	.lseek_fn = vfs_proxyfs_lseek,
	.sendfile_fn = vfs_proxyfs_sendfile,
	.recvfile_fn = vfs_proxyfs_recvfile,
	.rename_fn = vfs_proxyfs_rename,
	.fsync_fn = vfs_proxyfs_fsync,
	.fsync_send_fn = vfs_proxyfs_fsync_send,
#if SAMBA_VERSION_MINOR >= 5
	.fsync_recv_fn = vfs_proxyfs_fsync_recv_4_4,
#else
	.fsync_recv_fn = vfs_proxyfs_fsync_recv,
#endif

	.stat_fn = vfs_proxyfs_stat,
	.fstat_fn = vfs_proxyfs_fstat,
	.lstat_fn = vfs_proxyfs_lstat,
	.get_alloc_size_fn = vfs_proxyfs_get_alloc_size,
	.unlink_fn = vfs_proxyfs_unlink,

#if SAMBA_VERSION_MINOR >= 5
	.chmod_fn = vfs_proxyfs_chmod_4_4,
#else
	.chmod_fn = vfs_proxyfs_chmod,
#endif

	.fchmod_fn = vfs_proxyfs_fchmod,

#if SAMBA_VERSION_MINOR >= 5
	.chown_fn = vfs_proxyfs_chown_4_4,
	.lchown_fn = vfs_proxyfs_lchown_4_4,
#else
	.chown_fn = vfs_proxyfs_chown,
	.lchown_fn = vfs_proxyfs_lchown,
#endif

	.fchown_fn = vfs_proxyfs_fchown,
	.chdir_fn = vfs_proxyfs_chdir,
	.getwd_fn = vfs_proxyfs_getwd,
	.ntimes_fn = vfs_proxyfs_ntimes,
	.ftruncate_fn = vfs_proxyfs_ftruncate,
	.fallocate_fn = vfs_proxyfs_fallocate,
	.lock_fn = vfs_proxyfs_lock,
	.kernel_flock_fn = vfs_proxyfs_kernel_flock,
	.linux_setlease_fn = vfs_proxyfs_linux_setlease,
	.getlock_fn = vfs_proxyfs_getlock,
	.symlink_fn = vfs_proxyfs_symlink,
	.readlink_fn = vfs_proxyfs_readlink,
	.link_fn = vfs_proxyfs_link,
	.mknod_fn = vfs_proxyfs_mknod,
	.realpath_fn = vfs_proxyfs_realpath,
	.chflags_fn = vfs_proxyfs_chflags,
	.file_id_create_fn = NULL,
	.copy_chunk_send_fn = NULL,
	.copy_chunk_recv_fn = NULL,

#if SAMBA_VERSION_MINOR >= 5
	.get_compression_fn = NULL,
	.set_compression_fn = NULL,
	.snap_check_path_fn = NULL,
	.snap_create_fn = NULL,
	.snap_delete_fn = NULL,
#endif

	.streaminfo_fn = NULL,
	.get_real_filename_fn = vfs_proxyfs_get_real_filename,
	.connectpath_fn = vfs_proxyfs_connectpath,

	.brl_lock_windows_fn = NULL,
	.brl_unlock_windows_fn = NULL,
	.brl_cancel_windows_fn = NULL,
	.strict_lock_fn = NULL,
	.strict_unlock_fn = NULL,
	.translate_name_fn = NULL,
	.fsctl_fn = NULL,

#if SAMBA_VERSION_MINOR >= 5
	.get_dos_attributes_fn = NULL,
	.fget_dos_attributes_fn = NULL,
	.set_dos_attributes_fn = NULL,
	.fset_dos_attributes_fn = NULL,
#endif

	/* NT ACL Operations */
	.fget_nt_acl_fn = NULL,
	.get_nt_acl_fn = NULL,
	.fset_nt_acl_fn = NULL,
	.audit_file_fn = NULL,

	/* Posix ACL Operations */
	.chmod_acl_fn = NULL,	/* passthrough to default */
	.fchmod_acl_fn = NULL,	/* passthrough to default */
	.sys_acl_get_file_fn = vfs_proxyfs_sys_acl_get_file,
	.sys_acl_get_fd_fn = vfs_proxyfs_sys_acl_get_fd,
	.sys_acl_blob_get_file_fn = posix_sys_acl_blob_get_file,
	.sys_acl_blob_get_fd_fn = posix_sys_acl_blob_get_fd,
	.sys_acl_set_file_fn = vfs_proxyfs_sys_acl_set_file,
	.sys_acl_set_fd_fn = vfs_proxyfs_sys_acl_set_fd,
	.sys_acl_delete_def_file_fn = vfs_proxyfs_sys_acl_delete_def_file,

	/* EA Operations */
	.getxattr_fn = vfs_proxyfs_getxattr,
	.fgetxattr_fn = vfs_proxyfs_fgetxattr,
	.listxattr_fn = vfs_proxyfs_listxattr,
	.flistxattr_fn = vfs_proxyfs_flistxattr,
	.removexattr_fn = vfs_proxyfs_removexattr,
	.fremovexattr_fn = vfs_proxyfs_fremovexattr,
	.setxattr_fn = vfs_proxyfs_setxattr,
	.fsetxattr_fn = vfs_proxyfs_fsetxattr,

	/* AIO Operations */
	.aio_force_fn = NULL,

#if SAMBA_VERSION_MINOR < 6
	/* Offline Operations */
	.is_offline_fn = vfs_proxyfs_is_offline,
	.set_offline_fn = vfs_proxyfs_set_offline,
#endif

	/* Durable handle Operations */
	.durable_cookie_fn = NULL,
	.durable_disconnect_fn = NULL,
	.durable_reconnect_fn = NULL,

//	.readdir_attr_fn = NULL,
};

NTSTATUS vfs_proxyfs_init(void);
NTSTATUS vfs_proxyfs_init(void)
{
	return smb_register_vfs(SMB_VFS_INTERFACE_VERSION, "proxyfs", &proxyfs_fns);
}
