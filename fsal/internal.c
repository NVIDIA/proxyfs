#include "handle.h"
#include <sys/types.h>
#include <attr/xattr.h> /* ENOATTR */
#include "fsal_api.h"
#include "fsal_convert.h"
#include "FSAL/fsal_commonlib.h"

proxyfs_handle_t *pfs_construct_handle(proxyfs_export_t *export, uint64_t inum, uint32_t mode)
{
	proxyfs_handle_t *handle = gsh_calloc(1, sizeof(proxyfs_handle_t));
	handle->inum = inum;
	handle->export = export;
	//handle->up_ops = export->export.up_ops;
	fsal_obj_handle_init(&handle->handle, &export->export, posix2fsal_type(mode));
	handle_ops_init(&handle->handle.obj_ops);
	handle->handle.fsid = export->fsid;
	handle->fid.vol_id_hi =export->fsid.major;
	handle->fid.vol_id_lo = export->fsid.minor;
	handle->fid.inode_num = inum;

	// We don't have a fsid, how will that work?
	handle->export = export;
	return handle;
}

void pfs_decosntruct_handle(proxyfs_handle_t *handle)
{
	fsal_obj_handle_fini(&handle->handle);
	gsh_free(handle);
}

void copy_ts(struct timespec *ts_dst, proxyfs_timespec_t *pts_src) {
    ts_dst->tv_sec = pts_src->sec;
    ts_dst->tv_nsec = pts_src->nsec;
}

void proxyfs2fsal_attributes(proxyfs_stat_t *pst, struct attrlist *fsalattr) {
    proxyfs_export_t *export = container_of(op_ctx->fsal_export, proxyfs_export_t, export);

	fsalattr->supported = op_ctx->fsal_export->exp_ops.fs_supported_attrs(
							op_ctx->fsal_export);

	/* Fills the output struct */
	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_TYPE))
		fsalattr->type = posix2fsal_type(pst->mode);

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_SIZE))
		fsalattr->filesize = pst->size;

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_FSID))
		fsalattr->fsid = export->fsid;

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_FILEID))
		fsalattr->fileid = pst->ino;

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_MODE))
		fsalattr->mode = unix2fsal_mode(pst->mode);

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_NUMLINKS))
		fsalattr->numlinks = pst->nlink;

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_OWNER))
		fsalattr->owner = pst->uid;

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_GROUP))
		fsalattr->group = pst->gid;

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_ATIME)) {
	    copy_ts(&fsalattr->atime, &pst->atim);
	}

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_CTIME)) {
    	copy_ts(&fsalattr->ctime, &pst->ctim);
	}

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_MTIME)) {
		copy_ts(&fsalattr->mtime, &pst->mtim);
	}

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_CHGTIME)) {
		fsalattr->chgtime =
			gsh_time_cmp(&fsalattr->mtime, &fsalattr->ctime) > 0 ?
			fsalattr->mtime : fsalattr->ctime;
		fsalattr->change = timespec_to_nsecs(&fsalattr->chgtime);
	}

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_SPACEUSED)) {
		fsalattr->spaceused = pst->size;
    }

	if (FSAL_TEST_MASK(fsalattr->valid_mask, ATTR_RAWDEV)) {
		fsalattr->rawdev = posix2fsal_devt(pst->dev);
    }
}
