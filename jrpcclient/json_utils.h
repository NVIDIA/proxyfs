#ifndef __PFS_JSON_UTILS_H__
#define __PFS_JSON_UTILS_H__

#include <stdbool.h>

// Struct magic so that we don't have to expose the internals of
// our context structure into proxyfs_api.c
//
struct jsonrpc_internal_t;
typedef struct jsonrpc_internal_t jsonrpc_context_t;

#ifndef __PROXYFS_H__
struct rpc_handle_t;
typedef struct rpc_handle_t jsonrpc_handle_t;
#endif

// Request-related
void jsonrpc_set_req_param_str(jsonrpc_context_t* ctx, char* key, char* value);
void jsonrpc_set_req_param_int(jsonrpc_context_t* ctx, char* key, int value);
void jsonrpc_set_req_param_int64(jsonrpc_context_t* ctx, char* key, int64_t value);
void jsonrpc_set_req_param_uint64(jsonrpc_context_t* ctx, char* key, uint64_t value);
void jsonrpc_set_req_param_buf(jsonrpc_context_t* ctx, char* key, uint8_t* buf, size_t buf_size);

// Response-related
int         jsonrpc_get_resp_status(jsonrpc_context_t* ctx);
const char* jsonrpc_get_resp_str(jsonrpc_context_t* ctx, char* key);
int         jsonrpc_get_resp_int(jsonrpc_context_t* ctx, char* key);
uint64_t    jsonrpc_get_resp_uint64(jsonrpc_context_t* ctx, char* key);
int64_t     jsonrpc_get_resp_int64(jsonrpc_context_t* ctx, char* key);
bool        jsonrpc_get_resp_bool(jsonrpc_context_t* ctx, char* key);
void        jsonrpc_get_resp_buf(jsonrpc_context_t* ctx, char* key, uint8_t* buf, size_t buf_size, size_t* bytes_written);
uint64_t    jsonrpc_get_resp_array_uint64(jsonrpc_context_t* ctx, char* array_key, int index, char* key);
int         jsonrpc_get_resp_array_int(jsonrpc_context_t* ctx, char* array_key, int index, char* key);
const char* jsonrpc_get_resp_array_str(jsonrpc_context_t* ctx, char* array_key, int index, char* key);
const char* jsonrpc_get_resp_array_str_value(jsonrpc_context_t* ctx, char* array_key, int index);
int         jsonrpc_get_resp_array_length(jsonrpc_context_t *ctx, char *array_key);

// Callbacks
typedef void (*jsonrpc_done_callback_t)(void* in_cookie);
typedef void (*jsonrpc_internal_callback_t)(jsonrpc_context_t* ctx);


#endif
