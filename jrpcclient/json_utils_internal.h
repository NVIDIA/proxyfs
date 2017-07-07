#ifndef __PFS_JSON_UTILS_INTERNAL_H__
#define __PFS_JSON_UTILS_INTERNAL_H__

#include <json-c/json.h>
#include <time_utils.h>


// json object for request context
typedef struct {
    int               request_id;
    json_object*      request;
    json_object*      request_params;
} jsonrpc_request_t;

// json object for response context
typedef struct jsonrpc_response {
    int               response_id;
    json_object*      response;
    json_object*      response_result;
    int               rsp_err;

    // stuff for deferred response handling
    char*             readBuf;
} jsonrpc_response_t;

// user callback context
typedef struct {
    jsonrpc_done_callback_t done_callback;

    void*    cookie; // caller's cookie
    void*    in_mount_handle;

    // Specific to read
    uint8_t* in_bufptr;
    size_t   in_bufsize;

    // Not strictly needed, but used for debugging
    uint64_t in_inode_number;
    uint64_t in_offset;
    uint64_t in_length;

    // For eliminating read/write_recv
    void* in_request;

} jsonrpc_user_callback_info_t;

// user cv context
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t  got_resp;
    bool            have_response;
} jsonrpc_cv_info_t;

// json object for request and response in context, use context to request fields
struct jsonrpc_internal_t {
    jsonrpc_handle_t* rpc_handle;

    jsonrpc_request_t  req;
    jsonrpc_response_t resp;

    // For blocking calls
    jsonrpc_cv_info_t  cv_info;

    // For non-blocking calls
    jsonrpc_user_callback_info_t user_callback;
    jsonrpc_internal_callback_t  internal_callback;

    struct jsonrpc_internal_t* next;

    // For timing profiling
    profiler_t* profiler;
};

// Get result object
json_object* get_jrpc_result(json_object* jobj);

#endif
