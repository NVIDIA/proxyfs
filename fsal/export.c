/**
 * @brief Implements PROXYFS FSAL moduleoperation create_export
 */
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include "fsal.h"
#include "FSAL/fsal_config.h"
#include "fsal_convert.h"
#include "config_parsing.h"
#include "nfs_exports.h"
#include "export_mgr.h"
#include "pnfs_utils.h"
#include "sal_data.h"
#include "FSAL/fsal_commonlib.h"

#include "handle.h"

static struct config_item export_params[] = {
    CONF_ITEM_NOOP("name"),
    CONF_MAND_STR("volume", 1, MAXPATHLEN, NULL, proxyfs_export, volname),
    CONFIG_EOL
};

static struct config_block export_param_block = {
    .dbus_interface_name = "org.ganesha.nfsd.config.fsal.proxyfs-export%d",
	.blk_desc.name = "FSAL",
	.blk_desc.type = CONFIG_BLOCK,
	.blk_desc.u.blk.init = noop_conf_init,
	.blk_desc.u.blk.params = export_params,
	.blk_desc.u.blk.commit = noop_conf_commit
};

static void export_ops_init(struct export_ops *ops);

fsal_status_t proxyfs_create_export(struct fsal_module *fsal_hdl,
				      void *parse_node,
				      struct config_error_type *err_type,
				      const struct fsal_up_vector *up_ops)
{
	int err = 0;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	proxyfs_export_t *export = gsh_calloc(1, sizeof(proxyfs_export_t));

	LogDebug(COMPONENT_FSAL, "In args: export path = %s",
		 op_ctx->ctx_export->fullpath);

	err = load_config_from_node(parse_node,
				    &export_param_block,
				    export,
				    true,
				    err_type);
	if (err != 0) {
		LogCrit(COMPONENT_FSAL,
			"Incorrect or missing parameters for export %s",
			op_ctx->ctx_export->fullpath);
		status.major = ERR_FSAL_INVAL;
        gsh_free(export);
		return status;
    }

	LogEvent(COMPONENT_FSAL, "Volume %s exported at : '%s'",
		export->volname, op_ctx->ctx_export->fullpath);

	fsal_export_init(&export->export);

	export_ops_init(&export->export.exp_ops);
    export->saveduid = geteuid();
    export->savedgid = getegid();

    // Mount the volume:
    // TBD: Get the mount_option from op_ctx. For now just using RW.
    err = proxyfs_mount(export->volname, 0, export->saveduid, export->savedgid, &export->mount_handle);
    if (err != 0) {
        gsh_free(export);
        status.major = ERR_FSAL_SERVERFAULT;
        return status;
    }

    struct statvfs* stat_vfs = NULL;
	err = proxyfs_statvfs(export->mount_handle, &stat_vfs);
    if (err != 0) {
        gsh_free(export);
        status.major = ERR_FSAL_SERVERFAULT;
        return status;
    }

    export->fsid.major = 0;
    export->fsid.minor = stat_vfs->f_fsid;
    free(stat_vfs);

    // Do stats on proxyfs root directory
    proxyfs_stat_t *pst;
    err = proxyfs_get_stat(export->mount_handle, export->mount_handle->root_dir_inode_num, &pst);
    if (err != 0) {
        gsh_free(export);
        status.major = ERR_FSAL_SERVERFAULT;
        return status;
    }

    export->root = pfs_construct_handle(export, export->mount_handle->root_dir_inode_num, pst->mode);
    free(pst);
	err = fsal_attach_export(fsal_hdl, &export->export.exports);
	if (err != 0) {
        gsh_free(export);
		status.major = ERR_FSAL_SERVERFAULT;
		LogCrit(COMPONENT_FSAL, "Unable to attach export. Export: %s",
			op_ctx->ctx_export->fullpath);
		return status;
	}

    export->export.fsal = fsal_hdl;
    export->export.up_ops = up_ops;
	op_ctx->fsal_export = &export->export;

    return status;
}

/**
 * Export information
 */


/**
 * @brief Finalize an export
 *
 * This function is called as part of cleanup when the last reference to
 * an export is released and it is no longer part of the list.  It
 * should clean up all private resources and destroy the object.
 *
 * @param[in] exp_hdl The export to release.
 */
void pfs_export_release(struct fsal_export *exp_hdl) {
    proxyfs_export_t *export = container_of(exp_hdl, proxyfs_export_t, export);

    pfs_deconstruct_handle(export->root);
    export->root = NULL;
    fsal_detach_export(export->export.fsal, &export->export.exports);
    free_export_ops(&export->export);

    proxyfs_unmount(export->mount_handle);
    gsh_free(export);
}

// TBD need to fix the conflict between this implimentation and in internal.c
#if 0
void proxyfs2fsal_attributes(proxyfs_stat_t *pst, struct attrlist *fsalattr) {
    fsalattr->valid_mask |= ATTR_TYPE|ATTR_FILEID|ATTR_MODE|ATTR_OWNER|ATTR_GROUP|ATTR_SIZE|ATTR_NUMLINKS|ATTR_ATIME|ATTR_MTIME|ATTR_CTIME;
    fsalattr->supported = ((const attrmask_t) (ATTRS_POSIX));
    fsalattr->type = posix2fsal_type(pst->mode);
    fsalattr->fileid = pst->ino;
    fsalattr->mode = unix2fsal_mode(pst->mode);
    fsalattr->owner = pst->uid;
    fsalattr->group = pst->gid;
    fsalattr->filesize = pst->size;
    fsalattr->numlinks = pst->nlink;
    copy_ts(&fsalattr->atime, &pst->atim);
    copy_ts(&fsalattr->ctime, &pst->ctim);
    copy_ts(&fsalattr->mtime, &pst->mtim);
	fsalattr->chgtime =
			gsh_time_cmp(&fsalattr->mtime, &fsalattr->ctime) > 0 ?
			fsalattr->mtime : fsalattr->ctime;
	fsalattr->change = timespec_to_nsecs(&fsalattr->chgtime);
    copy_ts(&fsalattr->creation, &pst->crtim);
}
#endif

/**
 * @brief Look up a path
 *
 * This function looks up a path within the export, it is typically
 * used to get a handle for the root directory of the export.
 *
 * The caller will set the request_mask in attrs_out to indicate the attributes
 * of interest. ATTR_ACL SHOULD NOT be requested and need not be provided. If
 * not all the requested attributes can be provided, this method MUST return
 * an error unless the ATTR_RDATTR_ERR bit was set in the request_mask.
 *
 * Since this method instantiates a new fsal_obj_handle, it will be forced
 * to fetch at least some attributes in order to even know what the object
 * type is (as well as it's fileid and fsid). For this reason, the operation
 * as a whole can be expected to fail if the attributes were not able to be
 * fetched.
 *
 * @param[in]     exp_hdl   The export in which to look up
 * @param[in]     path      The path to look up
 * @param[out]    handle    The object found
 * @param[in,out] attrs_out Optional attributes for newly created object
 *
 * @note On success, @a handle has been ref'd
 *
 * @return FSAL status.
 */
fsal_status_t pfs_lookup_path(struct fsal_export *exp_hdl,
				      const char *path,
				      struct fsal_obj_handle **handle,
				      struct attrlist *attrs_out) {

    proxyfs_export_t *export = container_of(exp_hdl, proxyfs_export_t, export);

    fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };

    const char *realpath = path;

    if (*path != '/') {
        realpath = strchr(path, ':');
        if (realpath == NULL) {
            status.major = ERR_FSAL_INVAL;
            return status;
        }

        if (*(++realpath) != '/') {
            status.major = ERR_FSAL_INVAL;
            return status;
        }
    }

    if (strstr(realpath, op_ctx->ctx_export->fullpath) != realpath) {
        status.major = ERR_FSAL_SERVERFAULT;
        return status;
    }

    realpath += strlen(op_ctx->ctx_export->fullpath);

    *handle = NULL;
    // special case - if asking for root:
    if (strcmp(realpath, "/") == 0) {
        *handle = &export->root->handle;
        return status;
    }

    proxyfs_stat_t *pst;
    int ret = proxyfs_get_stat_path(export->mount_handle, (char *)realpath, &pst);
    if (ret != 0) {
        status.major = ERR_FSAL_SERVERFAULT;
        return status;
    }

    proxyfs_handle_t *phandle = pfs_construct_handle(export, pst->ino, pst->mode);
    *handle = &phandle->handle;

    if (attrs_out != NULL) {
        proxyfs2fsal_attributes(pst, attrs_out);
    }

    return status;
}

/**
 * @brief Convert a wire handle to a host handle
 *
 * This function extracts a host handle from a wire handle.  That
 * is, when given a handle as passed to a client, this method will
 * extract the handle to create objects.
 *
 * @param[in]     exp_hdl Export handle
 * @param[in]     in_type Protocol through which buffer was received.
 * @param[in]     flags   Flags to describe the wire handle. Example, if
 *			  the handle is a big endian handle.
 * @param[in,out] fh_desc Buffer descriptor.  The address of the
 *                        buffer is given in @c fh_desc->buf and must
 *                        not be changed.  @c fh_desc->len is the
 *                        length of the data contained in the buffer,
 *                        @c fh_desc->len must be updated to the correct
 *                        host handle size.
 *
 * @return FSAL type.
 */
fsal_status_t pfs_wire_to_host(struct fsal_export *exp_hdl,
					 fsal_digesttype_t in_type,
					 struct gsh_buffdesc *fh_desc,
					 int flags) {
	switch (in_type) {
		/* Digested Handles */
	case FSAL_DIGEST_NFSV3:
	case FSAL_DIGEST_NFSV4:
		/* wire handles */
		fh_desc->len = sizeof(proxyfs_file_handle_t);
		break;
	default:
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);
    }

    return fsalstat(ERR_FSAL_NO_ERROR, 0);
}


/**@{*/
/**
 * Statistics and configuration for this filesystem
 */

/**
 * @brief Get filesystem statistics
 *
 * This function gets information on inodes and space in use and free
 * for a filesystem.  See @c fsal_dynamicinfo_t for details of what to
 * fill out.
 *
 * @param[in]  exp_hdl Export handle to interrogate
 * @param[in]  obj_hdl Directory
 * @param[out] info    Buffer to fill with information
 *
 * @retval FSAL status.
 */
fsal_status_t pfs_get_fs_dynamic_info(struct fsal_export *exp_hdl,
					      struct fsal_obj_handle *obj_hdl,
					      fsal_dynamicfsinfo_t *info) {
    proxyfs_export_t *export = container_of(exp_hdl, proxyfs_export_t, export);

    struct statvfs* stat_vfs = NULL;
	int err = proxyfs_statvfs(export->mount_handle, &stat_vfs);
	if (err != 0) {
		errno = err;
		return posix2fsal_status(err);
	}

    info->total_bytes = stat_vfs->f_frsize * stat_vfs->f_blocks;
	info->free_bytes = stat_vfs->f_frsize * stat_vfs->f_bfree;
	info->avail_bytes = stat_vfs->f_frsize * stat_vfs->f_bavail;
	info->total_files = stat_vfs->f_files;
	info->free_files = stat_vfs->f_ffree;
	info->avail_files = stat_vfs->f_favail;
    info->maxread = 0;
    info->maxwrite = 0;
	info->time_delta.tv_sec = 1;
	info->time_delta.tv_nsec = 0;

    free(stat_vfs);
    return posix2fsal_status(0);
}

/**
 * @brief Export feature test
 *
 * This function checks whether a feature is supported on this
 * filesystem.  The features that can be interrogated are given in the
 * @c fsal_fsinfo_options_t enumeration.
 *
 * @param[in] exp_hdl The export to interrogate
 * @param[in] option  The feature to query
 *
 * @retval true if the feature is supported.
 * @retval false if the feature is unsupported or unknown.
 */
bool pfs_supports(struct fsal_export *exp_hdl,
			     fsal_fsinfo_options_t option) {
    proxyfs_fsal_module_t *myself = container_of(exp_hdl->fsal, proxyfs_fsal_module_t, fsal);
	return fsal_supports(&myself->fs_info, option);
}

/**
 * @brief Get the greatest file size supported
 *
 * @param[in] exp_hdl Filesystem to interrogate

 * @return Greatest file size supported.
 */
uint64_t pfs_maxfilesize(struct fsal_export *exp_hdl) {
    return UINT64_MAX;
}

/**
 * @brief Get the greatest read size supported
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return Greatest read size supported.
 */
uint32_t pfs_maxread(struct fsal_export *exp_hdl) {
    return 0x400000;
}

/**
 * @brief Get the greatest write size supported
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return Greatest write size supported.
 */
uint32_t pfs_maxwrite(struct fsal_export *exp_hdl) {
    return 0x400000;
}

/**
 * @brief Get the greatest link count supported
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return Greatest link count supported.
 */
uint32_t pfs_maxlink(struct fsal_export *exp_hdl) {
    return 1024;
}

/**
 * @brief Get the greatest name length supported
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return Greatest name length supported.
 */
uint32_t pfs_maxnamelen(struct fsal_export *exp_hdl) {
    return UINT32_MAX;
}

/**
 * @brief Get the greatest path length supported
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return Greatest path length supported.
 */
uint32_t pfs_maxpathlen(struct fsal_export *exp_hdl) {
    return UINT32_MAX;
}

/**
 * @brief Get the lease time for this filesystem
 *
 * @note Currently this value has no effect, with lease time being
 * configured globally for all filesystems at once.
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return Lease time.
 */
struct timespec pfs_lease_time(struct fsal_export *exp_hdl) {
	struct timespec lease = { 300, 0 };

	return lease;
}

/**
 * @brief Get supported ACL types
 *
 * This function returns a bitmask indicating whether it supports
 * ALLOW, DENY, neither, or both types of ACL.
 *
 * @note Could someone with more ACL support tell me if this is sane?
 * Is it legitimate for an FSAL supporting ACLs to support just ALLOW
 * or just DENY without supporting the other?  It seems fishy to
 * me. -- ACE
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return supported ACL types.
 */
fsal_aclsupp_t pfs_acl_support(struct fsal_export *exp_hdl) {
    return FSAL_ACLSUPPORT_ALLOW;
}

/**
 * @brief Get supported attributes
 *
 * This function returns a list of all attributes that this FSAL will
 * support.  Be aware that this is specifically the attributes in
 * struct attrlist, other NFS attributes (fileid and so forth) are
 * supported through other means.
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return supported attributes.
 */
attrmask_t pfs_supported_attrs(struct fsal_export *exp_hdl) {
    return ATTRS_POSIX;
}

/**
 * @brief Get umask applied to created files
 *
 * @note This seems fishy to me.  Is this actually supported properly?
 * And is it something we want the FSAL being involved in?  We already
 * have the functions in Protocol/NFS specifying a default mode. -- ACE
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return creation umask.
 */
uint32_t pfs_umask(struct fsal_export *exp_hdl) {
    proxyfs_fsal_module_t *myself = container_of(exp_hdl->fsal, proxyfs_fsal_module_t, fsal);

    return fsal_umask(&myself->fs_info);
}

/**
 * @brief Get permissions applied to names attributes
 *
 * @note This doesn't make sense to me as an export-level parameter.
 * Permissions on named attributes could reasonably vary with
 * permission and ownership of the associated file, and some
 * attributes may be read/write while others are read-only. -- ACE
 *
 * @param[in] exp_hdl Filesystem to interrogate
 *
 * @return permissions on named attributes.
 */
uint32_t pfs_xattr_access_rights(struct fsal_export *exp_hdl) {
    proxyfs_fsal_module_t *myself = container_of(exp_hdl->fsal, proxyfs_fsal_module_t, fsal);

    return fsal_xattr_access_rights(&myself->fs_info);
}

/**
 * @brief free a proxyfs_state_fd_t
 *
 * @param[in] exp_hdl  Export state_t will be associated with
 * @param[in] state    Related state if appropriate
 *
 */
void proxyfs_free_state(struct fsal_export *exp_hdl, struct state_t *state)
{
	proxyfs_state_fd_t *state_fd = container_of(state, proxyfs_state_fd_t, state);
	proxyfs_fd_t *my_fd = &state_fd->fd;

	PTHREAD_RWLOCK_destroy(&my_fd->fdlock);

	gsh_free(state_fd);
}

static fsal_status_t pfs_create_handle(struct fsal_export *export_pub,
				   struct gsh_buffdesc *fh_desc,
				   struct fsal_obj_handle **pub_handle,
				   struct attrlist *attrs_out) {

    fsal_status_t status = { ERR_FSAL_NO_ERROR, 0};

    //proxyfs_handle_t *phandle = pfs_construct_handle(export, inum, mode);
    //TBD: UNDER CONSTRUCTION
    return status;
}

void export_ops_init(struct export_ops *ops) {
    ops->release = pfs_export_release;
    ops->lookup_path = pfs_lookup_path;
    ops->wire_to_host = pfs_wire_to_host;

    ops->create_handle = pfs_create_handle;
//    ops->get_fs_dynamic_info = pfs_get_dynamic_info;
    ops->fs_supports = pfs_supports;
    ops->fs_maxfilesize = pfs_maxfilesize;
    ops->fs_maxread = pfs_maxread;
    ops->fs_maxwrite = pfs_maxwrite;
    ops->fs_maxlink = pfs_maxlink;
    ops->fs_maxpathlen = pfs_maxpathlen;
    ops->fs_lease_time = pfs_lease_time;
    ops->fs_acl_support = pfs_acl_support;
    ops->fs_supported_attrs = pfs_supported_attrs;
    ops->fs_umask = pfs_umask;
    ops->fs_xattr_access_rights = pfs_xattr_access_rights;
//    ops->alloc_state = pfs_alloc_state;
//    ops->free_state = pfs_free_state;

    /* pNFS ops that are not supported:
    ops->getdeviceinfo = getdeviceinfo;
	ops->getdevicelist = getdevicelist;
	ops->fs_layouttypes = fs_layouttypes;
	ops->fs_layout_blocksize = fs_layout_blocksize;
	ops->fs_maximum_segments = fs_maximum_segments;
	ops->fs_loc_body_size = fs_loc_body_size;
	ops->fs_da_addr_size = fs_da_addr_size;
    */
}
