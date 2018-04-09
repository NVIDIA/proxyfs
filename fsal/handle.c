/*
 * Copyright © 2012, CohortFS, LLC.
 * Author: Adam C. Emerson <aemerson@linuxbox.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * -------------
 */

/**
 * @file   FSAL_CEPH/handle.c
 * @author Adam C. Emerson <aemerson@linuxbox.com>
 * @date   Mon Jul  9 15:18:47 2012
 *
 * @brief Interface to handle functionality
 *
 * This function implements the interfaces on the struct
 * fsal_obj_handle type.
 */

#ifdef LINUX
#include <sys/sysmacros.h> /* for makedev(3) */
#endif
#include <fcntl.h>
#include "../../include/fsal.h"
#include "../../include/fsal_types.h"
#include "../../include/fsal_convert.h"
#include "../../include/fsal_api.h"
#include "../../include/nfs_exports.h"
#include "../../include/sal_data.h"
#include "handle.h"

/**
 * @brief Release an object
 *
 * This function looks up an object by name in a directory.
 *
 * @param[in] obj_pub The object to release
 *
 * @return FSAL status codes.
 */

static void pfs_release(struct fsal_obj_handle *obj_pub)
{
	/* The private 'full' handle */
	proxyfs_handle_t *obj = container_of(obj_pub, proxyfs_handle_t, handle);

	if (obj != obj->export->root) {
		pfs_deconstruct_handle(obj);
	}
}

/**
 * @brief Look up an object by name
 *
 * This function looks up an object by name in a directory.
 *
 * @param[in]  dir_pub The directory in which to look up the object.
 * @param[in]  path    The name to look up.
 * @param[out] obj_pub The looked up object.
 *
 * @return FSAL status codes.
 */
static fsal_status_t pfs_lookup(struct fsal_obj_handle *dir_pub,
			    const char *path, struct fsal_obj_handle **handle,
			    struct attrlist *attrs_out)
{
	/* Generic status return */
	int err = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };

	proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	struct handle *dir = container_of(dir_pub, struct handle, handle);
	struct handle *obj = NULL;

	LogFullDebug(COMPONENT_FSAL, "Lookup %s", path);

	int ino;
	err = proxyfs_lookup(export->mount_handle, dir->inum, path, &ino);
	if (err != 0) {
		return proxyfs2fsal_error(err);
	}

	proxyfs_stat_t *pst;
	err = proxyfs_get_stat(export->mount_handle, ino, &pst);

	obj = pfs_construct_handle(export, ino, pst->mode);

	if (attrs_out != NULL) {
		proxyfs2fsal_attributes(pst, attrs_out);
	}

	free(pst);

	*obj_pub = &obj->handle;

	return fsalstat(0, 0);
}

/**
 * @brief Read a directory
 *
 * This function reads the contents of a directory (excluding . and
 * .., which is ironic since the Ceph readdir call synthesizes them
 * out of nothing) and passes dirent information to the supplied
 * callback.
 *
 * @param[in]  dir_pub     The directory to read
 * @param[in]  whence      The cookie indicating resumption, NULL to start
 * @param[in]  dir_state   Opaque, passed to cb
 * @param[in]  cb          Callback that receives directory entries
 * @param[out] eof         True if there are no more entries
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_readdir(struct fsal_obj_handle *dir_pub,
				  fsal_cookie_t *whence, void *dir_state,
				  fsal_readdir_cb cb, attrmask_t attrmask,
				  bool *eof)
{
	int err = 0;
	proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *dir = container_of(dir_pub, proxyfs_handle_t, handle);

	uint64_t start = 0;

	fsal_status_t fsal_status = { ERR_FSAL_NO_ERROR, 0 };

	// TBD: open dir to do credential check: op_ctx->creds.

	if (whence != NULL) {
		start = *whence;
	}

	while (!(*eof)) {
		struct dirent *dir_ent = NULL;
		proxyfs_stat_t *sts;
		struct attrlist attrs;

		err = proxyfs_readdir_plus_by_loc(export->mount_handle, dir->inum, start, &dir_ent, &sts);
		if (err != 0) {
			return posix2fsal_error(err);
		}

		if (dir_ent == NULL) {
			*eof = true;
			return fsal_status;
		}

		/* skip . and .. */
		if ((strcmp(dir_ent->d_name, ".") == 0) || (strcmp(dir_ent->d_name, "..") == 0)) {
			continue;
		}

		proxyfs_handle_t *obj = pfs_construct_handle(export, dir_ent->d_ino, sts->mode);
		fsal_prepare_attrs(&attrs, attrmask);
		proxyfs2fsal_attributes(sts, &attrs);

		enum fsal_dir_result cb_rc;

		cb_rc = cb(dir_ent->d_name, &obj->handle, &attrs, dir_state, dir_ent->d_off);

		fsal_release_attrs(&attrs);
		free(sts);
		free(dir_ent);

		/* Read ahead not supported by this FSAL. */
		if (cb_rc >= DIR_READAHEAD) {
			return fsal_status;
		}
	}

	return fsal_status;
}

/**
 * @brief Create a directory
 *
 * This function creates a new directory.
 *
 * For support_ex, this method will handle attribute setting. The caller
 * MUST include the mode attribute and SHOULD NOT include the owner or
 * group attributes if they are the same as the op_ctx->cred.
 *
 * @param[in]     dir_hdl Directory in which to create the directory
 * @param[in]     name    Name of directory to create
 * @param[in]     attrib  Attributes to set on newly created object
 * @param[out]    new_obj Newly created object
 *
 * @note On success, @a new_obj has been ref'd
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_mkdir(struct fsal_obj_handle *dir_hdl,
				const char *name, struct attrlist *attrib,
				struct fsal_obj_handle **new_obj,
				struct attrlist *attrs_out)
{
	int err = 0;

	proxyfs_export_t *export =
	    container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *dir = container_of(dir_hdl, proxyfs_handle_t, handle);

	mode_t unix_mode;
	/* Newly created object */
	proxyfs_handle_t *obj = NULL;
	fsal_status_t status;

	LogFullDebug(COMPONENT_FSAL,
		     "mode = %o uid=%d gid=%d",
		     attrib->mode, (int) op_ctx->creds->caller_uid,
		     (int) op_ctx->creds->caller_gid);

	unix_mode = fsal2unix_mode(attrib->mode) & ~op_ctx->fsal_export->exp_ops.fs_umask(op_ctx->fsal_export);

	err = proxyfs_mkdir(export->mount_handle, dir->inum, name, op_ctx->creds->caller_uid, op_ctx->creds->caller_gid, unix_mode, &inum);
	if (err < 0) {
		return posix2fsal_error(err);
	}

	obj = pfs_construct_handle(export, inum, unix_mode);
	*new_obj = &obj->handle;

	/* We handled the mode above. */
	FSAL_UNSET_MASK(attrib->valid_mask, ATTR_MODE);

	if (attrib->valid_mask) {
		/* Now per support_ex API, if there are any other attributes
		 * set, go ahead and get them set now.
		 */
		status = (*new_obj)->obj_ops.setattr2(*new_obj, false, NULL,
						      attrib);
		if (FSAL_IS_ERROR(status)) {
			/* Release the handle we just allocated. */
			LogFullDebug(COMPONENT_FSAL,
				     "setattr2 status=%s",
				     fsal_err_txt(status));
			(*new_obj)->obj_ops.release(*new_obj);
			*new_obj = NULL;
		}
	} else {
		status = fsalstat(ERR_FSAL_NO_ERROR, 0);

		if (attrs_out != NULL) {
			proxyfs_stat_t *pst;
			err = proxyfs_get_stat(export->mount_handle, inum, &pst);
			if (err != 0) {
				proxyfs_rmdir(export->mount_handle, dir->inum, name);
				return posix2fsal_error(err);
			}

			proxyfs2fsal_attributes(pst, attrs_out);
			free(pst);
		}
	}

	FSAL_SET_MASK(attrib->valid_mask, ATTR_MODE);

	return status;
}

/**
 * @brief Create a symbolic link
 *
 * This function creates a new symbolic link.
 *
 * For support_ex, this method will handle attribute setting. The caller
 * MUST include the mode attribute and SHOULD NOT include the owner or
 * group attributes if they are the same as the op_ctx->cred.
 *
 * @param[in]     dir_hdl   Directory in which to create the object
 * @param[in]     name      Name of object to create
 * @param[in]     link_path Content of symbolic link
 * @param[in]     attrib    Attributes to set on newly created object
 * @param[out]    new_obj   Newly created object
 *
 * @note On success, @a new_obj has been ref'd
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_symlink(struct fsal_obj_handle *dir_hdl,
				  const char *name, const char *link_path,
				  struct attrlist *attrib,
				  struct fsal_obj_handle **new_obj,
				  struct attrlist *attrs_out)
{
	int err = 0;

	proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *dir = container_of(dir_hdl, proxyfs_handle_t, handle);

	proxyfs_stat_t *pfs_stats;

	unix_mode = fsal2unix_mode(attrib->mode) & ~op_ctx->fsal_export->exp_ops.fs_umask(op_ctx->fsal_export);

	/* Newly created object */
	proxyfs_handle_t *obj = NULL;
	fsal_status_t status;

	LogFullDebug(COMPONENT_FSAL,
		     "mode = %o uid=%d gid=%d",
		     attrib->mode, (int) op_ctx->creds->caller_uid,
		     (int) op_ctx->creds->caller_gid);

	err = proxyfs_symlink(export->mount_handle, dir->inum, name, link_path, op_ctx->creds->caller_uid, op_ctx->creds->caller_gid);
	if (err < 0) {
		return posix2fsal_error(err);
	}

	obj = pfs_construct_handle(export, inum, unix_mode);
	*new_obj = &obj->handle;

	return status;
}


/**
 * @brief Retrieve the content of a symlink
 *
 * This function allocates a buffer, copying the symlink content into
 * it.
 *
 * @param[in]  link_pub    The handle for the link
 * @param[out] content_buf Buffdesc for symbolic link
 * @param[in]  refresh     true if the underlying content should be
 *                         refreshed.
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_readlink(struct fsal_obj_handle *link_pub,
				   struct gsh_buffdesc *content_buf,
				   bool refresh)
{
	int err = 0;

	proxyfs_export_t *export =
	    container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *link = container_of(link_pub, proxyfs_handle_t, handle);

	char *target;
	err = proxyfs_read_symlink(export->mount_handle, link->inum, &target);
	if (err != 0) {
		return posix2fsal_error(err);
	}

	content_buf->addr = gsh_strldup(target, MIN(strlen(target), (PATH_MAX-1)), &content_buf->len);
	free(target);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/**
 * @brief Freshen and return attributes
 *
 * This function freshens and returns the attributes of the given
 * file.
 *
 * @param[in]  handle_pub Object to interrogate
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_getattrs(struct fsal_obj_handle *handle_pub,
			      struct attrlist *attrs)
{
	int err = 0;

	proxyfs_export_t *export =
	    container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *handle = container_of(handle_pub, proxyfs_handle_t, handle);

	proxyfs_stat_t *pst;
	err = proxyfs_get_stat(export->mount_handle, handle->fd.inum, &pst);
	if (err != 0) {
		return posix2fsal_error(err);
	}

	proxyfs2fsal_attributes(pst, attrs);
	free(pst);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/**
 * @brief Create a hard link
 *
 * This function creates a link from the supplied file to a new name
 * in a new directory.
 *
 * @param[in] handle_pub  File to link
 * @param[in] destdir_pub Directory in which to create link
 * @param[in] name        Name of link
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_link(struct fsal_obj_handle *handle_pub,
			       struct fsal_obj_handle *destdir_pub,
			       const char *name)
{
	int err = 0;

	proxyfs_export_t *export =
	    container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *handle = container_of(handle_pub, proxyfs_handle_t, handle);
	proxyfs_handle_t *destdir = container_of(destdir_pub, proxyfs_handle_t, handle);

	err = proxyfs_link(export->mount_handle, destdir->inum, name, handle->inum);
	if (err != 0) {
		return posix2fsal_error(err);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/**
 * @brief Rename a file
 *
 * This function renames a file, possibly moving it into another
 * directory.  We assume most checks are done by the caller.
 *
 * @param[in] olddir_pub Source directory
 * @param[in] old_name   Original name
 * @param[in] newdir_pub Destination directory
 * @param[in] new_name   New name
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_rename(struct fsal_obj_handle *obj_hdl,
				 struct fsal_obj_handle *olddir_pub,
				 const char *old_name,
				 struct fsal_obj_handle *newdir_pub,
				 const char *new_name)
{
	int err = 0;

	proxyfs_export_t *export =
	    container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *olddir = container_of(olddir_pub, proxyfs_handle_t, olddir);
	proxyfs_handle_t *newdir = container_of(newdir_pub, proxyfs_handle_t, newdir);

	err = proxyfs_rename(export->mount_handle, olddir->inum, old_name, newddir->inum, new_name);
	if (err != 0) {
		return posix2fsal_error(err)
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/**
 * @brief Remove a name
 *
 * This function removes a name from the filesystem and possibly
 * deletes the associated file.  Directories must be empty to be
 * removed.
 *
 * @param[in] dir_pub Parent directory
 * @param[in] name    Name to remove
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_unlink(struct fsal_obj_handle *dir_pub,
				      struct fsal_obj_handle *obj_pub,
				      const char *name)
{
	int err = 0;

	proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *dir = container_of(dir_pub, proxyfs_handle_t, dir);
	proxyfs_handle_t *obj = container_of(obj_pub, proxyfs_handle_t, obj);

	LogFullDebug(COMPONENT_FSAL,
		     "Unlink %s, I think it's a %s",
		     name, object_file_type_to_str(obj_pub->type));

	if (obj_pub->type != DIRECTORY) {
		err = proxyfs_unlink(export->mount_handle, dir->inum, name);
	} else {
		err = proxyfs_rmdir(export->mount_handle, dir->inum, name);
	}

	if (err != 0) {
		LogDebug(COMPONENT_FSAL, "Unlink %s returned %s (%d)", name, strerror(-rc), -rc);
		return posix2fsal_error(err);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

fsal_status_t pfs_open_my_fd(proxyfs_handle_t *handle, fsal_openflags_t openflags, proxyfs_fd_t *my_fd)
{
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};

	int posix_flags = 0;
	fsal2posix_openflags(openflags, &posix_flags);

	assert(handle->inum != 0 && my_fd->openflags == FSAL_O_CLOSED && openflags != 0);

	LogFullDebug(COMPONENT_FSAL,
		     "handle->inum = %x openflags = %x, posix_flags = %x",
		     handle->inum, openflags, posix_flags);

	// TBD: We should call open to proxyfs with credential check. We are not doing cred check!

	my_fd->openflags = openflags;

	return status;
}

fsal_status_t pfs_close_my_fd(proxyf_handle_t *handle, proxyfs_fd_t *fd)
{
	int err = 0;
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};

	// TBD: with introduction of open, we need to close it here:
	/*
	 * if (fd->openflags != FSAL_O_CLOSED) {
	 * 		err = proxyfs_close()
	 */

	fd->openflags = FSAL_O_CLOSED;
	return status;
}

/**
 * @brief Function to open an fsal_obj_handle's global file descriptor.
 *
 * @param[in]  obj_hdl     File on which to operate
 * @param[in]  openflags   Mode for open
 * @param[out] fd          File descriptor that is to be used
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_open_func(struct fsal_obj_handle *obj_hdl,
				    fsal_openflags_t openflags,
				    struct fsal_fd *fd)
{
	proxyfs_handle_t *handle = container_of(obj_hdl, struct handle, handle);

	return pfs_open_my_fd(handle, openflags, fd);
}

/**
 * @brief Function to close an fsal_obj_handle's global file descriptor.
 *
 * @param[in]  obj_hdl     File on which to operate
 * @param[in]  fd          File handle to close
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_close_func(struct fsal_obj_handle *obj_hdl,
				     struct fsal_fd *fd)
{
	proxyfs_handle_t *handle = container_of(obj_hdl, struct handle, handle);

	return pfs_close_my_fd(handle, fd);
}

/**
 * @brief Close a file
 *
 * This function closes a file, freeing resources used for read/write
 * access and releasing capabilities.
 *
 * @param[in] obj_hdl File to close
 *
 * @return FSAL status.
 */

static fsal_status_t pfs_close(struct fsal_obj_handle *obj_hdl)
{
	fsal_status_t status = fsalstat(ERR_FSAL_NO_ERROR, 0);

	/* The private 'full' object handle */
	struct handle *handle = container_of(obj_hdl, struct handle, handle);

	if (handle->fd.openflags == FSAL_O_CLOSED) {
		return fsalstat(ERR_FSAL_NOT_OPENED, 0);
	}

	/* Take write lock on object to protect file descriptor.
	 * This can block over an I/O operation.
	 */
	PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

	status = pfs_close_my_fd(handle, &handle->fd);

	PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);

	return status;
}

/**
 * @brief Allocate a state_t structure
 *
 * Note that this is not expected to fail since memory allocation is
 * expected to abort on failure.
 *
 * @param[in] exp_hdl               Export state_t will be associated with
 * @param[in] state_type            Type of state to allocate
 * @param[in] related_state         Related state if appropriate
 *
 * @returns a state structure.
 */

struct state_t *pfs_alloc_state(struct fsal_export *exp_hdl,
				 enum state_type state_type,
				 struct state_t *related_state)
{
	proxyfs_state_fd_t *state;
	proxyfs_fd_t *my_fd;

	state = init_state(gsh_calloc(1, sizeof(proxyfs_state_fd_t)),
			   exp_hdl, state_type, related_state);

	my_fd = &container_of(state, proxyfs_state_fd_t, state)->fd;

	my_fd->openflags = FSAL_O_CLOSED;
	PTHREAD_RWLOCK_init(&my_fd->fdlock, NULL);

	return state;
}

/**
 * @brief free a ceph_state_fd structure
 *
 * @param[in] exp_hdl  Export state_t will be associated with
 * @param[in] state    Related state if appropriate
 *
 */
void pfs_free_state(struct fsal_export *exp_hdl, struct state_t *state)
{
	proxyfs_state_fd_t *state_fd = container_of(state, proxyfs_fd_t, state);
	proxyfs_fd_t *my_fd = &state_fd->fd;

	PTHREAD_RWLOCK_destroy(&my_fd->fdlock);

	gsh_free(state_fd);
}

/**
 * @brief Merge a duplicate handle with an original handle
 *
 * This function is used if an upper layer detects that a duplicate
 * object handle has been created. It allows the FSAL to merge anything
 * from the duplicate back into the original.
 *
 * The caller must release the object (the caller may have to close
 * files if the merge is unsuccessful).
 *
 * @param[in]  orig_hdl  Original handle
 * @param[in]  dupe_hdl Handle to merge into original
 *
 * @return FSAL status.
 *
 */

fsal_status_t pfs_merge(struct fsal_obj_handle *orig_hdl,
			 struct fsal_obj_handle *dupe_hdl)
{
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};

	if (orig_hdl->type == REGULAR_FILE &&
	    dupe_hdl->type == REGULAR_FILE) {
		/* We need to merge the share reservations on this file.
		 * This could result in ERR_FSAL_SHARE_DENIED.
		 */
		proxyfs_handle_t *orig, *dupe;

		orig = container_of(orig_hdl, struct handle, handle);
		dupe = container_of(dupe_hdl, struct handle, handle);

		/* This can block over an I/O operation. */
		PTHREAD_RWLOCK_wrlock(&orig_hdl->obj_lock);

		status = merge_share(&orig->share, &dupe->share);

		PTHREAD_RWLOCK_unlock(&orig_hdl->obj_lock);
	}

	return status;
}

static bool
pfs_check_verifier_stat(struct attrlist *st, fsal_verifier_t verifier)
{
	uint32_t verf_hi, verf_lo;

	memcpy(&verf_hi, verifier, sizeof(uint32_t));
	memcpy(&verf_lo, verifier + sizeof(uint32_t), sizeof(uint32_t));

	LogFullDebug(COMPONENT_FSAL,
		     "Passed verifier %"PRIx32" %"PRIx32
		     " file verifier %"PRIx32" %"PRIx32,
		     verf_hi, verf_lo,
		     (uint32_t)st->atime.tv_sec,
		     (uint32_t)st->mtime.tv_sec);

	return st->atime.tv_sec == verf_hi && st->mtime.tv_sec == verf_lo;
}

/**
 * @brief Open a file descriptor for read or write and possibly create
 *
 * This function opens a file for read or write, possibly creating it.
 * If the caller is passing a state, it must hold the state_lock
 * exclusive.
 *
 * state can be NULL which indicates a stateless open (such as via the
 * NFS v3 CREATE operation), in which case the FSAL must assure protection
 * of any resources. If the file is being created, such protection is
 * simple since no one else will have access to the object yet, however,
 * in the case of an exclusive create, the common resources may still need
 * protection.
 *
 * If Name is NULL, obj_hdl is the file itself, otherwise obj_hdl is the
 * parent directory.
 *
 * On an exclusive create, the upper layer may know the object handle
 * already, so it MAY call with name == NULL. In this case, the caller
 * expects just to check the verifier.
 *
 * On a call with an existing object handle for an UNCHECKED create,
 * we can set the size to 0.
 *
 * If attributes are not set on create, the FSAL will set some minimal
 * attributes (for example, mode might be set to 0600).
 *
 * If an open by name succeeds and did not result in Ganesha creating a file,
 * the caller will need to do a subsequent permission check to confirm the
 * open. This is because the permission attributes were not available
 * beforehand.
 *
 * @param[in] obj_hdl               File to open or parent directory
 * @param[in,out] state             state_t to use for this operation
 * @param[in] openflags             Mode for open
 * @param[in] createmode            Mode for create
 * @param[in] name                  Name for file if being created or opened
 * @param[in] attrib_set            Attributes to set on created file
 * @param[in] verifier              Verifier to use for exclusive create
 * @param[in,out] new_obj           Newly created object
 * @param[in,out] caller_perm_check The caller must do a permission check
 *
 * @return FSAL status.
 */
fsal_status_t proxyfs_open2(struct fsal_obj_handle *obj_hdl,
			 struct state_t *state,
			 fsal_openflags_t openflags,
			 enum fsal_create_mode createmode,
			 const char *name,
			 struct attrlist *attrib_set,
			 fsal_verifier_t verifier,
			 struct fsal_obj_handle **new_obj,
			 struct attrlist *attrs_out,
			 bool *caller_perm_check)
{
	int posix_flags = 0;
	int retval = 0;
	mode_t unix_mode = 0;
	fsal_status_t status = {0, 0};
	proxyfs_fd_t *my_fd = NULL;
	proxyfs_handle_t *myself, *hdl = NULL;
	proxyfs_stat_t *sts = NULL;

	bool truncated;
	bool created = false;

	proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	LogAttrlist(COMPONENT_FSAL, NIV_FULL_DEBUG, "attrs ", attrib_set, false);

	if (state != NULL) {
		my_fd = &container_of(state, proxyfs_state_fd_t, state)->fd;
	}
	myself = container_of(obj_hdl, proxyfs_handle_t, handle);

	fsal2posix_openflags(openflags, &posix_flags);

	truncated = (posix_flags & O_TRUNC) != 0;

	if (createmode >= FSAL_EXCLUSIVE) {
		/* Now fixup attrs for verifier if exclusive create */
		set_common_verifier(attrib_set, verifier);
	}

	if (name == NULL) {
		/* This is an open by handle */
		if (state != NULL) {
			/* Prepare to take the share reservation, but only if we
			 * are called with a valid state (if state is NULL the
			 * caller is a stateless create such as NFS v3 CREATE).
			 */

			/* This can block over an I/O operation. */
			PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

			/* Check share reservation conflicts. */
			status = check_share_conflict(&myself->share, openflags, false);

			if (FSAL_IS_ERROR(status)) {
				PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
				return status;
			}

			/* Take the share reservation now by updating the
			 * counters.
			 */
			update_share_counters(&myself->share, FSAL_O_CLOSED, openflags);

			PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
		} else {
			/* We need to use the global fd to continue, and take
			 * the lock to protect it.
			 */
			my_fd = &myself->fd;
			PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);
		}

		if (my_fd->openflags != FSAL_O_CLOSED) {
			pfs_close_my_fd(myself, my_fd);
		}
		status = pfs_open_my_fd(myself, openflags, my_fd);

		if (FSAL_IS_ERROR(status)) {
			if (state == NULL) {
				/* Release the lock taken above, and return
				 * since there is nothing to undo.
				 */
				PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
				return status;
			} else {
				/* Error - need to release the share */
				goto undo_share;
			}
		}

		if (createmode >= FSAL_EXCLUSIVE || truncated) {
			/* Refresh the attributes */
			retval = pfs_getattrs(obj_hdl, attrs_out);

			if (retval == 0) {
				LogFullDebug(COMPONENT_FSAL, "New size = %"PRIx64, stx.stx_size);
			} else {
				/* Because we have an inode ref, we never
				 * get EBADF like other FSALs might see.
				 */
				status = posix2fsal_error(retval);
			}

			/* Now check verifier for exclusive, but not for
			 * FSAL_EXCLUSIVE_9P.
			 */
			if (!FSAL_IS_ERROR(status) &&
			    createmode >= FSAL_EXCLUSIVE &&
			    createmode != FSAL_EXCLUSIVE_9P &&
			    !pfs_check_verifier_stat(attrs_out, verifier)) {
				/* Verifier didn't match, return EEXIST */
				status =
				    fsalstat(posix2fsal_error(EEXIST), EEXIST);
			}

		} else if (attrs_out && attrs_out->request_mask &
			   ATTR_RDATTR_ERR) {
			attrs_out->valid_mask &= ATTR_RDATTR_ERR;
		}

		if (state == NULL) {
			/* If no state, release the lock taken above and return
			 * status. If success, we haven't done any permission
			 * check so ask the caller to do so.
			 */
			PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
			*caller_perm_check = !FSAL_IS_ERROR(status);
			return status;
		}

		if (!FSAL_IS_ERROR(status)) {
			/* Return success. We haven't done any permission
			 * check so ask the caller to do so.
			 */
			*caller_perm_check = true;
			return status;
		}

		(void) proxyfs_close_my_fd(myself, my_fd);

 undo_share:

		/* Can only get here with state not NULL and an error */

		/* On error we need to release our share reservation
		 * and undo the update of the share counters.
		 * This can block over an I/O operation.
		 */
		PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

		update_share_counters(&myself->share, openflags, FSAL_O_CLOSED);

		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);

		return status;
	}

	/* In this path where we are opening by name, we can't check share
	 * reservation yet since we don't have an object_handle yet. If we
	 * indeed create the object handle (there is no race with another
	 * open by name), then there CAN NOT be a share conflict, otherwise
	 * the share conflict will be resolved when the object handles are
	 * merged.
	 */

	if (createmode == FSAL_NO_CREATE) {
		struct fsal_obj_handle *temp = NULL;

		/* We don't have open by name... */
		status = obj_hdl->obj_ops.lookup(obj_hdl, name, &temp, NULL);

		if (FSAL_IS_ERROR(status)) {
			LogFullDebug(COMPONENT_FSAL, "lookup returned %s", fsal_err_txt(status));
			return status;
		}

		/* Now call ourselves without name and attributes to open. */
		status = obj_hdl->obj_ops.open2(temp, state, openflags,
						FSAL_NO_CREATE, NULL, NULL,
						verifier, new_obj,
						attrs_out,
						caller_perm_check);

		if (FSAL_IS_ERROR(status)) {
			/* Release the object we found by lookup. */
			temp->obj_ops.release(temp);
			LogFullDebug(COMPONENT_FSAL, "open returned %s", fsal_err_txt(status));
		}

		return status;
	}

	/* Now add in O_CREAT and O_EXCL.
	 * Even with FSAL_UNGUARDED we try exclusive create first so
	 * we can safely set attributes.
	 */
	if (createmode != FSAL_NO_CREATE) {
		/* Now add in O_CREAT and O_EXCL. */
		posix_flags |= O_CREAT;

		/* And if we are at least FSAL_GUARDED, do an O_EXCL create. */
		if (createmode >= FSAL_GUARDED)
			posix_flags |= O_EXCL;

		/* Fetch the mode attribute to use in the openat system call. */
		unix_mode = fsal2unix_mode(attrib_set->mode) &
		    ~op_ctx->fsal_export->exp_ops.fs_umask(op_ctx->fsal_export);

		/* Don't set the mode if we later set the attributes */
		FSAL_UNSET_MASK(attrib_set->valid_mask, ATTR_MODE);
	}

	if (createmode == FSAL_UNCHECKED && (attrib_set->valid_mask != 0)) {
		/* If we have FSAL_UNCHECKED and want to set more attributes
		 * than the mode, we attempt an O_EXCL create first, if that
		 * succeeds, then we will be allowed to set the additional
		 * attributes, otherwise, we don't know we created the file
		 * and this can NOT set the attributes.
		 */
		posix_flags |= O_EXCL;
	}

	// TBD: How to handle open flags? we are right now ignoring it.
	uint64_t ino;
	retval = proxyfs_create(export->mount_handle, obj_hdl->inum, name, export->saveduid, export->savedgid, unix_mode, &ino);
	if (retval < 0) {
		LogFullDebug(COMPONENT_FSAL, "Create %s failed with %s", name, strerror(-retval));
	}

	created = true;
#if 0
	if (retval == EEXIST && createmode == FSAL_UNCHECKED) {
		/* We tried to create O_EXCL to set attributes and failed.
		 * Remove O_EXCL and retry, also remember not to set attributes.
		 * We still try O_CREAT again just in case file disappears out
		 * from under us.
		 *
		 * Note that because we have dropped O_EXCL, later on we will
		 * not assume we created the file, and thus will not set
	.	 * additional attributes. We don't need to separately track
		 * the condition of not wanting to set attributes.
		 */
		posix_flags &= ~O_EXCL;
		retval = proxyfs_create(export->mount_handle, obj_hdl->inum, name, export->saveduid, export->savedgid, unix_mode, &ino);

		if (retval < 0) {
			LogFullDebug(COMPONENT_FSAL, "Non-exclusive Create %s failed with %s", name, strerror(-retval));
		}
	}
#endif

	if (retval < 0) {
		return posix2fsal_error(retval);
	}

	retval = proxyfs_get_stat(export->mount_handle, ino, &sts);
	if (retval < 0) {
		return posix2fsal_error(retval);
	}

	created = true;

	/* Since we did the create using the caller's credentials,
	 * we don't need to do an additional permission check.
	 */
	*caller_perm_check = false;

	pfs_construct_handle(export, ino, sts->mode);
	free(sts);

	/* If we didn't have a state above, use the global fd. At this point,
	 * since we just created the global fd, no one else can have a
	 * reference to it, and thus we can mamnipulate unlocked which is
	 * handy since we can then call setattr2 which WILL take the lock
	 * without a double locking deadlock.
	 */
	if (my_fd == NULL) {
		my_fd = &hdl->fd;
	}

	my_fd->openflags = openflags;

	*new_obj = &hdl->handle;

	if (created && attrib_set->valid_mask != 0) {
		/* Set attributes using our newly opened file descriptor as the
		 * share_fd if there are any left to set (mode and truncate
		 * have already been handled).
		 *
		 * Note that we only set the attributes if we were responsible
		 * for creating the file and we have attributes to set.
		 */
		status = (*new_obj)->obj_ops.setattr2(*new_obj,
						      false,
						      state,
						      attrib_set);

		if (FSAL_IS_ERROR(status))
			goto fileerr;

		if (attrs_out != NULL) {
			status = (*new_obj)->obj_ops.getattrs(*new_obj, attrs_out);
			if (FSAL_IS_ERROR(status) &&
			    (attrs_out->request_mask & ATTR_RDATTR_ERR) == 0) {
				/* Get attributes failed and caller expected
				 * to get the attributes. Otherwise continue
				 * with attrs_out indicating ATTR_RDATTR_ERR.
				 */
				goto fileerr;
			}
		}
	} else if (attrs_out != NULL) {
		/* Since we haven't set any attributes other than what was set
		 * on create (if we even created), just use the stat results
		 * we used to create the fsal_obj_handle.
		 */
		pfs2fsal_attributes(sts, attrs_out);
	}

	if (state != NULL) {
		/* Prepare to take the share reservation, but only if we are
		 * called with a valid state (if state is NULL the caller is
		 * a stateless create such as NFS v3 CREATE).
		 */

		/* This can block over an I/O operation. */
		PTHREAD_RWLOCK_wrlock(&(*new_obj)->obj_lock);

		/* Take the share reservation now by updating the counters. */
		update_share_counters(&hdl->share, FSAL_O_CLOSED, openflags);

		PTHREAD_RWLOCK_unlock(&(*new_obj)->obj_lock);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);

 fileerr:

	/* Close the file we just opened. */
	(void) pfs_close_my_fd(container_of(*new_obj, proxyfs_handle_t, handle), my_fd);

	/* Release the handle we just allocated. */
	(*new_obj)->obj_ops.release(*new_obj);
	*new_obj = NULL;

	if (created) {
		/* Remove the file we just created */
		proxyfs_unlink(export->mount_handle, obj_hdl->inum, name);
	}

	return status;
}

/**
 * @brief Return open status of a state.
 *
 * This function returns open flags representing the current open
 * status for a state. The state_lock must be held.
 *
 * @param[in] obj_hdl     File on which to operate
 * @param[in] state       File state to interrogate
 *
 * @retval Flags representing current open status
 */

fsal_openflags_t proxyfs_status2(struct fsal_obj_handle *obj_hdl,
			     struct state_t *state)
{
	proxyfs_file_handle_t *my_fd = (proxyfs_file_handle_t *)(state + 1);

	return my_fd->openflags;
}

/**
 * @brief Re-open a file that may be already opened
 *
 * This function supports changing the access mode of a share reservation and
 * thus should only be called with a share state. The state_lock must be held.
 *
 * This MAY be used to open a file the first time if there is no need for
 * open by name or create semantics. One example would be 9P lopen.
 *
 * @param[in] obj_hdl     File on which to operate
 * @param[in] state       state_t to use for this operation
 * @param[in] openflags   Mode for re-open
 *
 * @return FSAL status.
 */

fsal_status_t proxyfs_reopen2(struct fsal_obj_handle *obj_hdl,
			   struct state_t *state,
			   fsal_openflags_t openflags)
{
	fsal_status_t status = {0, 0};

	int posix_flags = 0;
	fsal_openflags_t old_openflags;

	proxyfs_fd_t temp_fd;
	bzero(&temp_fd, sizeof(proxyfs_fd_t);
	temp_fd.openflags = FSAL_O_CLOSED;
	temp_fd.fdlock = PTHREAD_RWLOCK_INITIALIZER;

	proxtfs_fd_t *my_fd = &temp_fd;

	proxyfs_handle_t *myself = container_of(obj_hdl, proxyfs_handle_t, handle);

	proxyfs_fd_t *my_share_fd = &container_of(state, proxyfs_state_fd_t, state)->fd;

	/* This can block over an I/O operation. */
	PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

	old_openflags = my_share_fd->openflags;

	/* We can conflict with old share, so go ahead and check now. */
	status = check_share_conflict(&myself->share, openflags, false);

	if (FSAL_IS_ERROR(status)) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);

		return status;
	}

	/* Set up the new share so we can drop the lock and not have a
	 * conflicting share be asserted, updating the share counters.
	 */
	update_share_counters(&myself->share, old_openflags, openflags);

	PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);

	status = pfs_open_my_fd(myself, openflags, my_fd);

	if (!FSAL_IS_ERROR(status)) {
		/* Close the existing file descriptor and copy the new
		 * one over. Make sure no one is using the fd that we are
		 * about to close!
		 */
		PTHREAD_RWLOCK_wrlock(&my_share_fd->fdlock);

		pfs_close_my_fd(myself, my_share_fd);
		my_share_fd = my_fd;

		PTHREAD_RWLOCK_unlock(&my_share_fd->fdlock);
	} else {
		/* We had a failure on open - we need to revert the share.
		 * This can block over an I/O operation.
		 */
		PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

		update_share_counters(&myself->share, openflags, old_openflags);

		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	return status;
}

/**
 * @brief Find a file descriptor for a read or write operation.
 *
 * We do not need file descriptors for non-regular files, so this never has to
 * handle them.
 */
fsal_status_t pfs_find_fd(proxyfs_ **fd,
			   struct fsal_obj_handle *obj_hdl,
			   bool bypass,
			   struct state_t *state,
			   fsal_openflags_t openflags,
			   bool *has_lock,
			   bool *closefd,
			   bool open_for_locks)
{
	proxyfs_handle_t *myself = container_of(obj_hdl, proxyfs_handle_t, handle);
	proxyfs_fd_t temp_fd;
	bzero(&temp_fd, sizeof(proxyfs_fd_t);
	temp_fd.openflags = FSAL_O_CLOSED;
	temp_fd.fdlock = PTHREAD_RWLOCK_INITIALIZER;

	proxyfs_fd_t *out_fd = &temp_fd;
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};
	bool reusing_open_state_fd = false;

	status = fsal_find_fd((struct fsal_fd **)&out_fd, obj_hdl,
			      (struct fsal_fd *)&myself->fd, &myself->share,
			      bypass, state, openflags,
			      pfs_open_func, pfs_close_func,
			      has_lock, closefd, open_for_locks,
			      &reusing_open_state_fd);

	LogFullDebug(COMPONENT_FSAL,
		     "fd = %p", out_fd->fd);
	*fd = out_fd->fd;
	return status;
}

/**
 * @brief Read data from a file
 *
 * This function reads data from the given file. The FSAL must be able to
 * perform the read whether a state is presented or not. This function also
 * is expected to handle properly bypassing or not share reservations.
 *
 * @param[in]     obj_hdl        File on which to operate
 * @param[in]     bypass         If state doesn't indicate a share reservation,
 *                               bypass any deny read
 * @param[in]     state          state_t to use for this operation
 * @param[in]     offset         Position from which to read
 * @param[in]     buffer_size    Amount of data to read
 * @param[out]    buffer         Buffer to which data are to be copied
 * @param[out]    read_amount    Amount of data read
 * @param[out]    end_of_file    true if the end of file has been reached
 * @param[in,out] info           more information about the data
 *
 * @return FSAL status.
 */

fsal_status_t pfs_read2(struct fsal_obj_handle *obj_hdl,
			bool bypass,
			struct state_t *state,
			uint64_t offset,
			size_t buffer_size,
			void *buffer,
			size_t *read_amount,
			bool *end_of_file,
			struct io_info *info)
{
	proxyfs_handle_t *handle = container_of(obj_hdl, proxyfs_handle_t, handle);

	proxyfs_fd_t *my_fd;
	proxyfs_fd_t *state_fd;

	bool has_lock = false;
	bool closefd = false;
	fsal_openflags_t openflags = FSAL_O_READ;


	int err;
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};

	proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	if (info != NULL) {
		/* Currently we don't support READ_PLUS */
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	}

	proxyfs_io_request_t *req = (proxyfs_io_request_t *)malloc(sizeof(proxyfs_io_request_t));
	if (req == NULL) {
		errno = ENOMEM;
		return proxyfs2fsal_error(ENOMEM);
	}
	bzero(req, sizeof(proxyfs_io_request_t));

	/* Acquire state's fdlock to prevent OPEN upgrade closing the
	 * file descriptor while we use it.
	 */
	if (state) {
		state_fd = &container_of(state, proxyfs_state_fd_t, state)->fd;

		PTHREAD_RWLOCK_rdlock(&state_fd->fdlock);
	}

	/* Get a usable file descriptor */
	status = pfs_find_fd(&my_fd, obj_hdl, bypass, state, openflags,
			      &has_lock, &closefd, false);

	req->op = IO_READ;
	req->mount_handle = export->mount_handle;
	req->inode_number = handle->inum;
	req->offset = offset;
	req->length = buffer_size;
	req->data = buffer
	req->error = 0;
	req->done_cb = NULL;
	req->done_cb_arg = NULL;
	req->done_cb_fd = -1;


	err = proxyfs_sync_io(req);

	if(stat) {
		PTHREAD_RWLOCK_unlock(&state_fd->fdlock);
	}

	if (closefd) {
		proxyfs_close_fd(my_fd);
	}

	if (has_lock) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	if (err != 0) {
		free(req);
		errno = EIO;
		return proxyfs2fsal_error(EIO);
	}

	if (req->error != 0) {
		errno = req->error;
		free(req);
		return proxyfs2fsal_error(errno);
	}

	*read_amount = req->out_size;
	*end_of_file = *read_amount == 0;
	free(req);
	return status;
}

/**
 * @brief Write data to a file
 *
 * This function writes data to a file. The FSAL must be able to
 * perform the write whether a state is presented or not. This function also
 * is expected to handle properly bypassing or not share reservations. Even
 * with bypass == true, it will enforce a mandatory (NFSv4) deny_write if
 * an appropriate state is not passed).
 *
 * The FSAL is expected to enforce sync if necessary.
 *
 * @param[in]     obj_hdl        File on which to operate
 * @param[in]     bypass         If state doesn't indicate a share reservation,
 *                               bypass any non-mandatory deny write
 * @param[in]     state          state_t to use for this operation
 * @param[in]     offset         Position at which to write
 * @param[in]     buffer         Data to be written
 * @param[in,out] fsal_stable    In, if on, the fsal is requested to write data
 *                               to stable store. Out, the fsal reports what
 *                               it did.
 * @param[in,out] info           more information about the data
 *
 * @return FSAL status.
 */

fsal_status_t pfs_write2(struct fsal_obj_handle *obj_hdl,
			 bool bypass,
			 struct state_t *state,
			 uint64_t offset,
			 size_t buffer_size,
			 void *buffer,
			 size_t *wrote_amount,
			 bool *fsal_stable,
			 struct io_info *info)
{
	proxyfs_handle_t *handle = container_of(obj_hdl, proxyfs_handle_t, handle);

	proxyfs_fd_t *my_fd;
	proxyfs_fd_t *state_fd;

	bool has_lock = false;
	bool closefd = false;
	fsal_openflags_t openflags = FSAL_O_WRITE;

	int err;
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};

	struct export *export =
	    container_of(op_ctx->fsal_export, struct export, export);

	if (info != NULL) {
		/* Currently we don't support READ_PLUS */
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	}

	proxyfs_io_request_t *req = (proxyfs_io_request_t *)malloc(sizeof(proxyfs_io_request_t));
	if (req == NULL) {
		errno = ENOMEM;
		return proxyfs2fsal_error(ENOMEM);
	}

	/* Acquire state's fdlock to prevent OPEN upgrade closing the
	 * file descriptor while we use it.
	 */
	if (state) {
		state_fd = &container_of(state, proxyfs_state_fd_t, state)->fd;

		PTHREAD_RWLOCK_rdlock(&state_fd->fdlock);
	}

	status = pfs_find_fd(&my_fd, obj_hdl, bypass, state, openflags,
				&has_lock, &closefd, false);

	if (FSAL_IS_ERROR(status)) {
		LogDebug(COMPONENT_FSAL,
			 "find_fd failed %s", msg_fsal_err(status.major));
		goto out;
	}

	req->op = IO_WRITE;
	req->mount_handle = export->mount_handle;
	req->inode_number = handle->inum;
	req->offset = offset;
	req->length = buffer_size;
	req->data = buffer
	req->error = 0;
	req->done_cb = NULL;
	req->done_cb_arg = NULL;
	req->done_cb_fd = -1;

	err = proxyfs_sync_io(req);

	if (state) {
		PTHREAD_RWLOCK_unlock(&state_fd->fdlock);
	}

	if (has_lock) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	if (closefd) {
		proxyfs_close_fd(my_fd);
	}

	if (err != 0) {
		free(req);
		errno = EIO;
		return proxyfs2fsal_error(EIO);
	}

	if (req->error != 0) {
		errno = req->error;
		free(req);
		return proxyfs2fsal_error(errno);
	}

	*wrote_amount = req->out_size;
	free(req);

	if (*fsal_stable) {
		err = proxyfs_flush(export->mount_handle, handle->inum);
	}

	return status;
}

/**
 * @brief Commit written data
 *
 * This function flushes possibly buffered data to a file. This method
 * differs from commit due to the need to interact with share reservations
 * and the fact that the FSAL manages the state of "file descriptors". The
 * FSAL must be able to perform this operation without being passed a specific
 * state.
 *
 * @param[in] obj_hdl          File on which to operate
 * @param[in] state            state_t to use for this operation
 * @param[in] offset           Start of range to commit
 * @param[in] len              Length of range to commit
 *
 * @return FSAL status.
 */

fsal_status_t pfs_commit2(struct fsal_obj_handle *obj_hdl,
			   off_t offset,
			   size_t len)
{
	proxyfs_export_t *export =
	    container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	proxyfs_handle_t *myself = container_of(obj_hdl, proxyfs_handle_t, handle);
	fsal_status_t status = {ERR_FSAL_NO_ERROR, 0};

	proxyfs_fd_t temp_fd;
	bzero(&temp_fd, sizeof(proxyfs_fd_t);
	temp_fd.openflags = FSAL_O_CLOSED;
	temp_fd.fdlock = PTHREAD_RWLOCK_INITIALIZER;

	proxyfs_fd_t *out_fd = &temp_fd;

	bool has_lock = false;
	bool closefd = false;

	/* Make sure file is open in appropriate mode.
	 * Do not check share reservation.
	 */
	status = fsal_reopen_obj(obj_hdl, false, false, FSAL_O_WRITE,
				 (struct fsal_fd *)&myself->fd, &myself->share,
				 pfs_open_func, pfs_close_func,
				 (struct fsal_fd **)&out_fd, &has_lock,
				 &closefd);

	int err = proxyfs_flush(export->mount_handle, myself->inum);
	if (err != 0) {
		return proxyfs2fsal_error(err);
	}

	if (closefd) {
		pfs_close_my_fd(myself, out_fd);
	}

	if (has_lock) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	return status;
}

/**
 * @brief Perform a lock operation
 *
 * This function performs a lock operation (lock, unlock, test) on a
 * file. This method assumes the FSAL is able to support lock owners,
 * though it need not support asynchronous blocking locks. Passing the
 * lock state allows the FSAL to associate information with a specific
 * lock owner for each file (which may include use of a "file descriptor".
 *
 * For FSAL_VFS etc. we ignore owner, implicitly we have a lock_fd per
 * lock owner (i.e. per state).
 *
 * @param[in]  obj_hdl          File on which to operate
 * @param[in]  state            state_t to use for this operation
 * @param[in]  owner            Lock owner
 * @param[in]  lock_op          Operation to perform
 * @param[in]  request_lock     Lock to take/release/test
 * @param[out] conflicting_lock Conflicting lock
 *
 * @return FSAL status.
 */
fsal_status_t pfs_lock_op2(struct fsal_obj_handle *obj_hdl,
			    struct state_t *state,
			    void *owner,
			    fsal_lock_op_t lock_op,
			    fsal_lock_param_t *request_lock,
			    fsal_lock_param_t *conflicting_lock)
{
	proxyfs_handle_t *myself = container_of(obj_hdl, proxyfs_handle_t, handle);
	struct flock lock_args;
	fsal_status_t status = {0, 0};
	int retval = 0;

	proxyfs_fd_t *my_fd;
	proxyfs_fd_t *state_fd;

	bool has_lock = false;
	bool closefd = false;
	bool bypass = false;

	fsal_openflags_t openflags = FSAL_O_RDWR;

	proxyfs_export_t *export =
	    container_of(op_ctx->fsal_export, struct export, export);

	LogFullDebug(COMPONENT_FSAL,
		     "Locking: op:%d type:%d start:%" PRIu64 " length:%"
		     PRIu64 " ",
		     lock_op, request_lock->lock_type, request_lock->lock_start,
		     request_lock->lock_length);

	if (lock_op == FSAL_OP_LOCKT) {
		/* We may end up using global fd, don't fail on a deny mode */
		bypass = true;
		openflags = FSAL_O_ANY;
	} else if (lock_op == FSAL_OP_LOCK) {
		if (request_lock->lock_type == FSAL_LOCK_R)
			openflags = FSAL_O_READ;
		else if (request_lock->lock_type == FSAL_LOCK_W)
			openflags = FSAL_O_WRITE;
	} else if (lock_op == FSAL_OP_UNLOCK) {
		openflags = FSAL_O_ANY;
	} else {
		LogDebug(COMPONENT_FSAL,
			 "ERROR: Lock operation requested was not TEST, READ, or WRITE.");
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	}

	if (lock_op != FSAL_OP_LOCKT && state == NULL) {
		LogCrit(COMPONENT_FSAL, "Non TEST operation with NULL state");
		return fsalstat(posix2fsal_error(EINVAL), EINVAL);
	}

	if (request_lock->lock_type == FSAL_LOCK_R) {
		lock_args.l_type = F_RDLCK;
	} else if (request_lock->lock_type == FSAL_LOCK_W) {
		lock_args.l_type = F_WRLCK;
	} else {
		LogDebug(COMPONENT_FSAL,
			 "ERROR: The requested lock type was not read or write.");
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	}

	if (lock_op == FSAL_OP_UNLOCK)
		lock_args.l_type = F_UNLCK;

	lock_args.l_pid = 0;
	lock_args.l_len = request_lock->lock_length;
	lock_args.l_start = request_lock->lock_start;
	lock_args.l_whence = SEEK_SET;

	/* flock.l_len being signed long integer, larger lock ranges may
	 * get mapped to negative values. As per 'man 3 fcntl', posix
	 * locks can accept negative l_len values which may lead to
	 * unlocking an unintended range. Better bail out to prevent that.
	 */
	if (lock_args.l_len < 0) {
		LogCrit(COMPONENT_FSAL,
			"The requested lock length is out of range- lock_args.l_len(%ld), request_lock_length(%"
			PRIu64 ")",
			lock_args.l_len, request_lock->lock_length);
		return fsalstat(ERR_FSAL_BAD_RANGE, 0);
	}

	if (lock_op == FSAL_OP_LOCKT) {
		lock_cmd = F_GETLK;
	} else {
		lock_cmd = F_SETLK;
	}

	if (state) {
		state_fd = &container_of(state, proxyfs_state_fd_t, state)->fd;
		PTHREAD_RWLOCK_rdlock(&state_fd->fdlock);
	}

	status = pfs_find_fd(&my_fd, obj_hdl, bypass, state, openflags, &has_lock, &closefd, true);

	int err = proxyfs_flock(export->mount_handle, myself->inum, lock_cmd, &lock_args);

	if (state) {
		PTHREAD_RWLOCK_unlock(&state_fd->fdlock);
	}

	if (closefd) {
		pfs_close_my_fd(myself, my_fd);
	}

	if (has_lock) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	if (conflicting_lock != NULL) {
		conflicting_lock->lock_length = lock_args.l_len;
		conflicting_lock->lock_start = lock_args.l_start;
		conflicting_lock->lock_type = lock_args.l_type;
	}

	if (err != 0) {
		return posix2fsal_error(err);
	}

	/* F_UNLCK is returned then the tested operation would be possible. */
	if (conflicting_lock != NULL) {
		if (lock_op == FSAL_OP_LOCKT && lock_args.l_type != F_UNLCK) {
			conflicting_lock->lock_length = lock_args.l_len;
			conflicting_lock->lock_start = lock_args.l_start;
			conflicting_lock->lock_type = lock_args.l_type;
		} else {
			conflicting_lock->lock_length = 0;
			conflicting_lock->lock_start = 0;
			conflicting_lock->lock_type = FSAL_NO_LOCK;
		}
	}

	return status;
}

/**
 * @brief Set attributes on an object
 *
 * This function sets attributes on an object.  Which attributes are
 * set is determined by attrib_set->valid_mask. The FSAL must manage bypass
 * or not of share reservations, and a state may be passed.
 *
 * @param[in] obj_hdl    File on which to operate
 * @param[in] state      state_t to use for this operation
 * @param[in] attrib_set Attributes to set
 *
 * @return FSAL status.
 */
fsal_status_t pfs_setattr2(struct fsal_obj_handle *obj_hdl,
			    bool bypass,
			    struct state_t *state,
			    struct attrlist *attrib_set)
{
	proxyfs_handle_t *myself = container_of(obj_hdl, proxyfs_handle_t, handle);
	fsal_status_t status = {0, 0};
	int rc = 0;

	bool has_lock = false;
	bool closefd = false;

	proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);
	/* Stat buffer */
	proxyfs_stat_t stx;

	/* Mask of attributes to set */
	uint32_t mask = 0;
	bool reusing_open_state_fd = false;

	if (attrib_set->valid_mask & ~CEPH_SETTABLE_ATTRIBUTES) {
		LogDebug(COMPONENT_FSAL,
			 "bad mask %"PRIx64" not settable %"PRIx64,
			 attrib_set->valid_mask,
			 attrib_set->valid_mask & ~CEPH_SETTABLE_ATTRIBUTES);
		return fsalstat(ERR_FSAL_INVAL, 0);
	}

	LogAttrlist(COMPONENT_FSAL, NIV_FULL_DEBUG,
		    "attrs ", attrib_set, false);

	/* apply umask, if mode attribute is to be changed */
	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MODE))
		attrib_set->mode &=
		    ~op_ctx->fsal_export->exp_ops.fs_umask(op_ctx->fsal_export);

	/* Test if size is being set, make sure file is regular and if so,
	 * require a read/write file descriptor.
	 */
	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_SIZE)) {
		if (obj_hdl->type != REGULAR_FILE) {
			LogFullDebug(COMPONENT_FSAL,
				     "Setting size on non-regular file");
			return fsalstat(ERR_FSAL_INVAL, EINVAL);
		}

		/* We don't actually need an open fd, we are just doing the
		 * share reservation checking, thus the NULL parameters.
		 */
		status = fsal_find_fd(NULL, obj_hdl, NULL, &myself->share,
				      bypass, state, FSAL_O_RDWR, NULL, NULL,
				      &has_lock, &closefd, false,
				      &reusing_open_state_fd);

		if (FSAL_IS_ERROR(status)) {
			LogFullDebug(COMPONENT_FSAL,
				     "fsal_find_fd status=%s",
				     fsal_err_txt(status));
			goto out;
		}
	}

	memset(&stx, 0, sizeof(stx));

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_SIZE)) {
		mask |= PROXYFS_SETATTR_SIZE;
		stx.size = attrib_set->filesize;
		LogDebug(COMPONENT_FSAL,
			     "setting size to %lu", stx.stx_size);
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MODE)) {
		mask |= PROXYFS_SETATTR_MODE;
		stx.mode = fsal2unix_mode(attrib_set->mode);
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_OWNER)) {
		mask |= PROXYFS_SETATTR_UID;
		stx.uid = attrib_set->owner;
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_GROUP)) {
		mask |= PROXYFS_SETATTR_GID;
		stx.gid = attrib_set->group;
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_ATIME)) {
		mask |= PROXYFS_SETATTR_ATIME;
		stx.atim = attrib_set->atime;
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_ATIME_SERVER)) {
		struct timespec timestamp;

		mask |= PROXYFS_SETATTR_ATIME;
		rc = clock_gettime(CLOCK_REALTIME, &timestamp);
		if (rc != 0) {
			LogDebug(COMPONENT_FSAL,
				 "clock_gettime returned %s (%d)",
				 strerror(errno), errno);
			status = fsalstat(posix2fsal_error(errno), errno);
			goto out;
		}
		stx.atim = timestamp;
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MTIME)) {
		mask |= PROXYFS_SETATTR_MTIME;
		stx.mtim = attrib_set->mtime;
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_MTIME_SERVER)) {
		struct timespec timestamp;

		mask |= PROXYFS_SETATTR_MTIME;
		rc = clock_gettime(CLOCK_REALTIME, &timestamp);
		if (rc != 0) {
			LogDebug(COMPONENT_FSAL,
				 "clock_gettime returned %s (%d)",
				 strerror(errno), errno);
			status = fsalstat(posix2fsal_error(errno), errno);
			goto out;
		}
		stx.mtim = timestamp;
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_CTIME)) {
		mask |= PROXYFS_SETATTR_CTIME;
		stx.ctim = attrib_set->ctime;
	}

	if (FSAL_TEST_MASK(attrib_set->valid_mask, ATTR_CREATION)) {
		mask |= PROXYFS_SETATTR_CRTIME;
		stx.crtim = attrib_set->creation;
	}

	rc = proxyfs_setattr(export->mount_handle, myself->inum, &stx, mask);
	if (rc < 0) {
		LogDebug(COMPONENT_FSAL, "setattrx returned %s (%d)", strerror(-rc), -rc);
		status = posix2fsal_error(rc);
	} else {
		/* Success */
		status = fsalstat(ERR_FSAL_NO_ERROR, 0);
	}

 out:

	if (has_lock) {
		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	return status;
}

/**
 * @brief Manage closing a file when a state is no longer needed.
 *
 * When the upper layers are ready to dispense with a state, this method is
 * called to allow the FSAL to close any file descriptors or release any other
 * resources associated with the state. A call to free_state should be assumed
 * to follow soon.
 *
 * @param[in] obj_hdl    File on which to operate
 * @param[in] state      state_t to use for this operation
 *
 * @return FSAL status.
 */

fsal_status_t pfs_close2(struct fsal_obj_handle *obj_hdl,
			 struct state_t *state)
{
	proxyfs_handle_t *myself = container_of(obj_hdl, proxyfs_handle_t, handle);
	proxyfs_fd_t *my_fd = &container_of(state, proxyfs_fd_t, state)->fd;

	if (state->state_type == STATE_TYPE_SHARE ||
	    state->state_type == STATE_TYPE_NLM_SHARE ||
	    state->state_type == STATE_TYPE_9P_FID) {
		/* This is a share state, we must update the share counters */

		/* This can block over an I/O operation. */
		PTHREAD_RWLOCK_wrlock(&obj_hdl->obj_lock);

		update_share_counters(&myself->share,my_fd->openflags, FSAL_O_CLOSED);

		PTHREAD_RWLOCK_unlock(&obj_hdl->obj_lock);
	}

	return pfs_close_my_fd(myself, my_fd);
}

/**
 * @brief Write wire handle
 *
 * This function writes a 'wire' handle to be sent to clients and
 * received from the.
 *
 * @param[in]     handle_pub  Handle to digest
 * @param[in]     output_type Type of digest requested
 * @param[in,out] fh_desc     Location/size of buffer for
 *                            digest/Length modified to digest length
 *
 * @return FSAL status.
 */

static fsal_status_t handle_to_wire(const struct fsal_obj_handle *handle_pub,
				    uint32_t output_type,
				    struct gsh_buffdesc *fh_desc)
{
	/* The private 'full' object handle */
	const proxyfs_handle_t *handle = container_of(handle_pub, const proxyfs_handle_t, handle);

	switch (output_type) {
		/* Digested Handles */
	case FSAL_DIGEST_NFSV3:
	case FSAL_DIGEST_NFSV4:
		if (fh_desc->len < sizeof(handle->vi)) {
			LogMajor(COMPONENT_FSAL,
				 "digest_handle: space too small for handle.  Need %zu, have %zu",
				 sizeof(vinodeno_t), fh_desc->len);
			return fsalstat(ERR_FSAL_TOOSMALL, 0);
		} else {
			memcpy(fh_desc->addr, &handle->vi, sizeof(vinodeno_t));
			fh_desc->len = sizeof(handle->vi);
		}
		break;

	default:
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);
	}

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/**
 * @brief Give a hash key for file handle
 *
 * This function locates a unique hash key for a given file.
 *
 * @param[in]  handle_pub The file whose key is to be found
 * @param[out] fh_desc    Address and length of key
 */

static void handle_to_key(struct fsal_obj_handle *handle_pub,
			  struct gsh_buffdesc *fh_desc)
{
	/* The private 'full' object handle */
	proxyfs_handle_t *handle = container_of(handle_pub, proxyfs_handle_t, handle);

	fh_desc->addr = &handle->vi;
	fh_desc->len = sizeof(vinodeno_t);
}

/**
 * @brief Override functions in ops vector
 *
 * This function overrides implemented functions in the ops vector
 * with versions for this FSAL.
 *
 * @param[in] ops Handle operations vector
 */

void handle_ops_init(struct fsal_obj_ops *ops)
{
	ops->get_ref = pfs_get_ref; // Not sure if needed?
	ops->put_ref = pfs_put_ref; // Not sure if needed?

	ops->release = pfs_release; // Releases private resource associated with a fh.
	ops->merge   = pfs_merge;   // Merge a fh to original fh.

	// Directory operations:
	ops->lookup  = pfs_lookup;
	ops->readdir = pfs_readdir;
//	ops->compute_readdir = pfs_compute_readdir_cookie; // probably we can determine cookie from name..
//	ops->dirent_cmp = pfs_dirent_cmp; // We should be able to support this function.
	ops->mkdir 	 = pfs_mkdir;
	ops->mknode  = NULL; // NOT supported
	ops->symlink = pfs_symlink;
	ops->readlink = pfs_readlink;
//	ops->test_access = NULL; // Check access for a given user against a given object, we should be able to support it.
	ops->getattrs = pfs_getattrs;
	ops->link = pfs_link; // hard link
//	ops->fs_locations = NULL; // do we need to support?
	ops->rename = pfs_rename;
	ops->unlink = pfs_unlink;
//	ops->seek = pfs_seek;
//	ops->io_advise = NULL:
	ops->close = pfs_close;

#if 0 // XATTRS related
	ops->list_ext_attrs = pfs_list_ext_attrs;
	ops->getexattr_id_by_name = pfs_getexattr_id_by_name;
	ops->getexattr_value_by_name = pfs_getexattr_value_by_name;
	ops->getexattr_value_by_id = pfs_getexattr_value_by_id;
	ops->setexattr_value = pfs_setexattr_value;
	op->setexattr_value_by_id = pfs_setexattr_value_by_id;
	ops->remove_exattr_by_id = pfs_remove_exattr_by_id;
	ops->remove_exattr_by_name = pfs_remove_exattr_by_name;
	ops->listxattrs = pfs_listxattrs;
#endif

	ops->handle_is = NULL;
	ops->handle_to_wire = handle_to_wire;
	ops->handle_to_key = handle_to_key;
	ops->handle_cmp = NULL;

	// pNFS functions:
	ops->layoutget = NULL;
	ops->layoutreturn = NULL;
	ops->layoutcommit = NULL;

#if 0 // XATTRS related
	ops->getxattrs = pfs_getxattrs;
	ops->setxattrs = pfs_setxattrs;
	ops->removexattrs = pfs_removexattrs;
	ops->listxattrs = pfs_listxattrs;
#endif

	// Extended API functions:
	ops->open2 = pfs_open2;
	ops->check_verifier = pfs_check_verifier;
	ops->status2 = pfs_status2;
	ops->reopen2 = pfs_reopen2;
	ops->read2 = pfs_read2;
	ops->write2 = pfs_write2;
//	ops->seek2 = pfs_seek2;
//	ops->io_advise2 = NULL;
	ops->commit2 = pfs_commit2;
	ops->lock_op2 = pfs_lock_op2;
//	ops->lease_op2 = pfs_lease_op2;
	ops->setattr2 = pfs_setattr2;
	ops->close2 = pfs_close2;
}
