#ifndef __PFS_REQ_RESP_H__
#define __PFS_REQ_RESP_H__

#include <json_utils.h>
#include <json_utils_internal.h>

// Setter/getter for internal callback
void jsonrpc_set_internal_callback(jsonrpc_context_t* ctx, jsonrpc_internal_callback_t internal_callback);
jsonrpc_internal_callback_t jsonrpc_get_internal_callback(jsonrpc_context_t* ctx);

// Save the request context somewhere
void jsonrpc_store_request(jsonrpc_context_t* ctx);

// Remove the request from the list. caller still must free the ctx.
void jsonrpc_remove_request(jsonrpc_context_t* ctx);

// Return the request context that corresponds to the request_id in the response
jsonrpc_context_t* jsonrpc_get_request(jsonrpc_response_t* resp);

// Callback-related
//
// API to save read-callback-related stuff for later
void jsonrpc_set_callback_info(jsonrpc_context_t*      ctx,
                               jsonrpc_done_callback_t callback,
                               void*                   cookie,
                               void*                   in_mount_handle,
                               uint8_t*                in_bufptr,
                               size_t                  in_bufsize,
                               uint64_t                in_inode_number,
                               uint64_t                in_offset,
                               uint64_t                in_length,
                               void*                   in_request);

// API to retrieve read-callback-related stuff
int jsonrpc_get_callback_info(jsonrpc_context_t*       ctx,
                              jsonrpc_done_callback_t* callback,
                              void**                   cookie,
                              void**                   mount_handle,
                              uint8_t**                bufptr,
                              size_t*                  bufsize,
                              uint64_t*                inode_number,
                              uint64_t*                offset,
                              uint64_t*                length,
                              void**                   request);

// API to save callback-related stuff for later
//void jsonrpc_set_callback_info(jsonrpc_context_t*         ctx,
//                               jsonrpc_done_callback_t callback,
//                               void*                      cookie);
// API to retrieve read-callback-related stuff
int jsonrpc_get_done_callback(jsonrpc_context_t*       ctx,
                              jsonrpc_done_callback_t* callback,
                              void**                   cookie,
                              void**                   request);

// Get timing profiler
profiler_t* jsonrpc_get_profiler(jsonrpc_context_t* ctx);

#endif
