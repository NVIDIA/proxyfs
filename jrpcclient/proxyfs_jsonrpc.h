#ifndef __PFS_JSONRPC_H__
#define __PFS_JSONRPC_H__

#include <stdbool.h>
#include <json_utils.h>
#include <time_utils.h>

// Override config file
void rpc_config_override(const char *override_string);

// Context open/close
jsonrpc_handle_t* pfs_rpc_open();
void pfs_rpc_close(jsonrpc_handle_t* handle);

// Execute a JSON request, blocking for the response.
int jsonrpc_exec_request_blocking(jsonrpc_context_t* ctx);

// Execute a JSON request, non-blocking. Callback is provided for handling the response.
int jsonrpc_exec_request_nonblocking(jsonrpc_context_t* ctx, jsonrpc_internal_callback_t internal_cb);

// In proxyfs_req_resp.c; here because exported to proxyfs_api.c
jsonrpc_context_t* jsonrpc_get_request_by_cookie(void* cookie);

// Context open/close: In proxyfs_req_resp.c; here because exported to proxyfs_api.c
jsonrpc_context_t* jsonrpc_open(jsonrpc_handle_t* handle, const char* method);
void jsonrpc_close(jsonrpc_context_t* ctx);

// Set timing profiler
void jsonrpc_set_profiler(jsonrpc_context_t* ctx, profiler_t* profiler);

#endif
