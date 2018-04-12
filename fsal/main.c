/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) Red Hat  Inc., 2013
 * Author: Anand Subramanian anands@redhat.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 * -------------
 */

/**
 * @file main.c
 * @author Anand Subramanian <anands@redhat.com>
 *
 * @author Shyamsundar R     <srangana@redhat.com>
 *
 * @brief Module core functions for FSAL_PROXYFS functionality, init etc.
 *
 */

#include "fsal.h"
#include "FSAL/fsal_init.h"
#include "handle.h"
#include "FSAL/fsal_commonlib.h"
#include "fsal_pnfs.h"
/* Swift ProxyFS FSAL module private storage
 */

static const char *module_name = "ProxyFS";

/* filesystem info for ProxyFS */
static struct fsal_staticfsinfo_t default_proxyfs_info = {
	.maxfilesize = UINT64_MAX,
	.maxlink = _POSIX_LINK_MAX,
	.maxnamelen = 1024,
	.maxpathlen = 1024,
	.no_trunc = true,
	.chown_restricted = true,
	.case_insensitive = false,
	.case_preserving = true,
	.link_support = true,
	.symlink_support = true,
	.lock_support = true,
	.lock_support_async_block = false,
	.named_attr = true,
	.unique_handles = true,
	.lease_time = {10, 0},
	.acl_support = FSAL_ACLSUPPORT_ALLOW | FSAL_ACLSUPPORT_DENY,
	.cansettime = true,
	.homogenous = true,
	.supported_attrs = ATTRS_POSIX,
	.maxread = 0,
	.maxwrite = 0,
	.umask = 0,
	.auth_exportpath_xdev = false,
	.xattr_access_rights = 0400,	/* root=RW, owner=R */
	.pnfs_mds = false,
	.pnfs_ds = true,
	.link_supports_permission_checks = true,
};

static struct config_item proxyfs_params[] = {
	CONF_MAND_IP_ADDR("ProxyFS_Addr", "127.0.0.1",
			  proxyfs_fsal_module, config.pfsd_addr),
	CONF_ITEM_UI16("ProxyFS_RPC_Port", 0, UINT16_MAX, 12345,
		       proxyfs_fsal_module, config.pfsd_rpc_port),
	CONF_ITEM_UI16("ProxyFS_FAST_IO_Port", 0, UINT16_MAX, 32345,
				proxyfs_fsal_module, config.pfsd_fast_port),

	CONFIG_EOL
};

struct config_block proxyfs_param = {
	.dbus_interface_name = "org.ganesha.nfsd.config.fsal.proxyfs",
	.blk_desc.name = "PROXYFS",
	.blk_desc.type = CONFIG_BLOCK,
	.blk_desc.u.blk.init = noop_conf_init,
	.blk_desc.u.blk.params = proxyfs_params,
	.blk_desc.u.blk.commit = noop_conf_commit
};

static fsal_status_t init_config(struct fsal_module *fsal_hdl,
				 config_file_t config_struct,
				 struct config_error_type *err_type)
{
	proxyfs_fsal_module_t *proxyfsal_module = container_of(fsal_hdl, proxyfs_fsal_module_t, fsal);

	proxyfsal_module->fs_info = default_proxyfs_info;
	(void) load_config_from_parse(config_struct,
				      &proxyfs_param,
				      &proxyfsal_module,
				      true,
				      err_type);

	/*
	 * Global block is not mandatory, so evenif
	 * it is not parsed correctly, don't consider
	 * that as an error
	 */
	if (!config_error_is_harmless(err_type))
		LogDebug(COMPONENT_FSAL, "Parsing Export Block failed");

	display_fsinfo(&proxyfsal_module->fs_info);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

/* Module methods
 */

MODULE_INIT void proxyfs_init(void)
{
	struct fsal_module *myself = &ProxyFS.fsal;

	/* register_fsal seems to expect zeroed memory. */
	memset(myself, 0, sizeof(*myself));

	if (register_fsal(myself, module_name, FSAL_MAJOR_VERSION,
			  FSAL_MINOR_VERSION, 9) != 0) {
		LogCrit(COMPONENT_FSAL,
			"ProxyFS FSAL module failed to register.");
		return;
	}

	/* set up module operations */
	myself->m_ops.create_export = proxyfs_create_export;

	/* setup global handle internals */
	myself->m_ops.init_config = init_config;
	/*
	 * Following inits needed for pNFS support
	 * get device info will used by pnfs meta data server
	 */
//	myself->m_ops.fsal_pnfs_ds_ops = pnfs_ds_ops_init;

	PTHREAD_MUTEX_init(&ProxyFS.lock, NULL);
	glist_init(&ProxyFS.fs_obj);

	LogDebug(COMPONENT_FSAL, "FSAL ProxyFS initialized");
}

MODULE_FINI void proxyfs_unload(void)
{
	if (unregister_fsal(&ProxyFS.fsal) != 0) {
		LogCrit(COMPONENT_FSAL,
			"FSAL ProxyFS unable to unload.  Dying ...");
		return;
	}

	/* All the shares should have been unexported */
	if (!glist_empty(&ProxyFS.fs_obj)) {
		LogWarn(COMPONENT_FSAL,
			"FSAL ProxyFS still contains active shares.");
	}
	PTHREAD_MUTEX_destroy(&ProxyFS.lock);
	LogDebug(COMPONENT_FSAL, "FSAL ProxyFS unloaded");
}
