#ifndef __FSAL_PROXYFS_HANDLE_H__
#define __FSAL_PROXYFS_HANDLE_H__

#include <proxyfs.h>
#include "fsal.h"
#include "sal_data.h"

typedef struct proxyfs_config_params_s {
    char *pfsd_addr;
    uint32_t pfsd_rpc_port;
    uint32_t pfsd_fast_port;
} proxyfs_config_params_t;

typedef struct proxyfs_fsal_module {
	struct fsal_staticfsinfo_t fs_info;
	struct fsal_module fsal;
	proxyfs_config_params_t config;

	struct glist_head fs_obj;            /* list of proxyfs filesystem objects */
	pthread_mutex_t   lock;              /* lock to protect above list */
} proxyfs_fsal_module_t;

proxyfs_fsal_module_t ProxyFS;

// Do we need proxyfs_t struct?
#if 0
typedef struct proxyfs_s {
	struct glist_head fs_obj; /* Entry in global proxyfs filesystem objects */
	char              *volname;
	mount_handle_t    *mnt_handle;

	const struct fsal_up_vector *up_ops;    /*< Upcall operations */

	int64_t    refcnt;
	pthread_t  up_thread; /* upcall thread */
	int8_t     destroy_mode;
	uint64_t   up_poll_usec;
	bool       enable_upcall;
} proxyfs_t;
#endif

typedef struct proxyfs_file_handle_s {
	uint64_t vol_id_hi;
	uint64_t vol_id_lo;
	uint64_t inode_num;
} proxyfs_file_handle_t;

typedef struct proxyfs_fd {
	/** The open and share mode etc. This MUST be first in every
	 *  file descriptor structure.
	 */
	fsal_openflags_t openflags;

	/* rw lock to protect the file descriptor */
	pthread_rwlock_t fdlock;

	struct user_cred creds; /* user creds opening fd */
} proxyfs_fd_t;

typedef struct proxyfs_state_fd {
	struct state_t state;
	proxyfs_fd_t   fd;
} proxyfs_state_fd_t;

struct proxyfs_export;

typedef struct proxyfs_handle {
	struct fsal_obj_handle handle;	/* public FSAL handle */
	struct fsal_share      share;   /* share_reservations */
	struct proxyfs_export  *export;
	proxyfs_fd_t           fd;
	uint64_t               inum;    /* inum for the file/directory */

	/* following added for pNFS support */
	uint64_t rd_issued;
	uint64_t rd_serial;
	uint64_t rw_issued;
	uint64_t rw_serial;
	uint64_t rw_max_len;
} proxyfs_handle_t;

typedef struct proxyfs_export {
    struct fsal_export export;
    mount_handle_t     *mount_handle;
    char               *volname;
    proxyfs_handle_t   *root;
	fsal_fsid_t        fsid;
	uid_t              saveduid;
	gid_t              savedgid;
} proxyfs_export_t;

/* Structures defined for PNFS */

struct proxyfs_file_layout {
	uint32_t stripe_length;
	uint64_t stripe_type;
	uint32_t devid;
};

struct proxyfs_ds_wire {
	unsigned char pfid[16];
	struct proxyfs_file_layout layout; /*< Layout information */
};

struct proxyfs_ds_handle {
	struct fsal_ds_handle  ds;
	struct proxyfs_ds_wire wire;
	stable_how4  stability_got;
	bool connected;
};

proxyfs_handle_t *pfs_construct_handle(proxyfs_export_t *export, uint64_t inum, uint32_t mode);
void pfs_deconstruct_handle(proxyfs_handle_t *handle);
void proxyfs2fsal_attributes(proxyfs_stat_t *pst, struct attrlist *attrs);

fsal_status_t proxyfs_create_export(struct fsal_module *fsal_hdl,
				      void *parse_node,
				      struct config_error_type *err_type,
				      const struct fsal_up_vector *up_ops);

void handle_ops_init(struct fsal_obj_ops *ops);
void copy_ts(struct timespec *ts_dst, proxyfs_timespec_t *pts_src);

#endif // __FSAL_PROXYFS_HANDLE_H__