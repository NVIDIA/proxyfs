#ifndef __FSAL_PROXYFS_CONFIG_H__
#define __FSAL_PROXYFS_CONFIG_H__

#include "handle.h"

typedef struct proxyfs_config_params_s {
    char *pfsd_addr;
    uint32 pfsd_rpc_port;
    uint32 pfsd_fast_port;
} proxyfs_config_params_t;

typedef struct proxyfs_fsal_module {
	struct fsal_module module;
	struct fsal_staticfsinfo_t fsinfo;
	proxyfs_config_params_t config;
} proxyfs_fsal_module_t;

#endif // __PROXYFS_CONFIG_H__

/*

TODO: (Remove) Temp NOTEs:
static struct config_item proxyfs_params[] = {
	CONF_ITEM_BOOL("link_support", true,
		       pxy_fsal_module, fsinfo.link_support),
	CONF_ITEM_BOOL("symlink_support", true,
		       pxy_fsal_module, fsinfo.symlink_support),
	CONF_ITEM_BOOL("cansettime", true,
		       pxy_fsal_module, fsinfo.cansettime),
	CONF_ITEM_UI64("maxread", 512,
		       FSAL_MAXIOSIZE - SEND_RECV_HEADER_SPACE,
		       DEFAULT_MAX_WRITE_READ,
		       pxy_fsal_module, fsinfo.maxread),
	CONF_ITEM_UI64("maxwrite", 512,
		       FSAL_MAXIOSIZE - SEND_RECV_HEADER_SPACE,
		       DEFAULT_MAX_WRITE_READ,
		       pxy_fsal_module, fsinfo.maxwrite),
	CONF_ITEM_MODE("umask", 0,
		       pxy_fsal_module, fsinfo.umask),
	CONF_ITEM_BOOL("auth_xdev_export", false,
		       pxy_fsal_module, fsinfo.auth_exportpath_xdev),
	CONF_ITEM_MODE("xattr_access_rights", 0400,
		       pxy_fsal_module, fsinfo.xattr_access_rights),
	CONF_ITEM_BLOCK("Remote_Server", proxy_remote_params,
		       noop_conf_init, remote_commit,
		       pxy_fsal_module, special),

	CONFIG_EOL
};
*/