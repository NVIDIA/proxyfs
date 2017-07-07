#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <proxyfs_jsonrpc.h>
#include <json_utils_internal.h>
#include <socket.h>
#include <debug.h>


// Set to nonzero to enable LIST_PRINTFs
int list_debug_flag = 0;

// Global for JSON RPC request ID.
//
static int jsonrpc_request_id = 0;

// Lock to protect request ID
pthread_mutex_t request_id_lock = PTHREAD_MUTEX_INITIALIZER;

int get_request_id()
{
    int reqId = 0;

    pthread_mutex_lock(&request_id_lock);
    reqId = jsonrpc_request_id++;
    pthread_mutex_unlock(&request_id_lock);

    return reqId;
}

void jsonrpc_init_request(jsonrpc_request_t* req, const char* method)
{
    // Alloc JSON object for request
    req->request         = json_object_new_object();
    req->request_params  = NULL;

    // Create request ID
    req->request_id = get_request_id();

    // Set the request method
    jsonrpc_set_req_method(req, method);
}

void jsonrpc_init_response(jsonrpc_response_t* resp)
{
    resp->response_id     = -1;
    resp->response        = NULL;
    resp->response_result = NULL;
    resp->rsp_err         = -1;
    resp->readBuf         = NULL;
}

void jsonrpc_print_response(char* prefix, jsonrpc_response_t* resp)
{
    DPRINTF("%s\n response_id=%d\n response=%p\n response_result=%p\n rsp_err=%d\n readBuf=%p\n\n",
            prefix, resp->response_id, resp->response, resp->response_result, resp->rsp_err, resp->readBuf);
}

void jsonrpc_init_cv_info(jsonrpc_cv_info_t* cv_info)
{
    pthread_mutex_init(&cv_info->mutex, NULL);
    pthread_cond_init(&cv_info->got_resp, NULL);
    cv_info->have_response = false;
}

void jsonrpc_cleanup_cv_info(jsonrpc_cv_info_t* cv_info)
{
    pthread_mutex_destroy(&cv_info->mutex);
    pthread_cond_destroy(&cv_info->got_resp);
}

// Wait on cv until we get the response
void jsonrpc_block_for_response(jsonrpc_context_t* ctx)
{
    DPRINTF("Blocking until we get the response for ctx=%p.\n",ctx);

    // protect have_response flag
    pthread_mutex_lock(&ctx->cv_info.mutex);

    // If we don't have a response, then wait
    while (!ctx->cv_info.have_response) {
        pthread_cond_wait(&ctx->cv_info.got_resp, &ctx->cv_info.mutex);
    }

    //DPRINTF("Blocking until we get the response for ctx=%p (pre-unlock).\n",ctx);

    // release have_response flag
    pthread_mutex_unlock(&ctx->cv_info.mutex);

    DPRINTF("Finished blocking until we get the response for ctx=%p (post-unlock), have_response=%s.\n",
            ctx,(ctx->cv_info.have_response?"true":"false"));
}

// Unblock whoever is waiting for the response
void jsonrpc_unblock_for_response(jsonrpc_context_t* ctx)
{
    DPRINTF("Unblocking whoever is waiting for the response for ctx=%p.\n",ctx);

    // protect have_response flag
    pthread_mutex_lock(&ctx->cv_info.mutex);

    // Set the have_response flag
    ctx->cv_info.have_response = true;

    // Wake up the waiter
    pthread_cond_signal(&ctx->cv_info.got_resp);

    //DPRINTF("Unblocking whoever is waiting for the response for ctx=%p (pre-unlock).\n",ctx);

    // release have_response flag
    pthread_mutex_unlock(&ctx->cv_info.mutex);

    DPRINTF("Unblocking done. for ctx=%p (post-unlock)\n",ctx);
}

// Copy response info into ctx
void jsonrpc_copy_response(jsonrpc_context_t* ctx, jsonrpc_response_t* resp)
{
    ctx->resp.response_id     = resp->response_id;
    ctx->resp.response        = resp->response;
    ctx->resp.response_result = resp->response_result;
    ctx->resp.rsp_err         = resp->rsp_err;
    ctx->resp.readBuf         = resp->readBuf;
}

void jsonrpc_set_read_buf(jsonrpc_response_t* resp, char* newReadBuf)
{
    //DPRINTF("Setting resp=%p read buf to %p.\n",resp, newReadBuf);
    // Make sure we aren't overwriting a non-null pointer, since we'd leak it
    if (resp->readBuf != NULL) {
        DPRINTF("ERROR, overwriting read buf %p with %p.\n",resp->readBuf, newReadBuf);
    }
    resp->readBuf  = newReadBuf;
}

void jsonrpc_init_user_callback(jsonrpc_user_callback_info_t* user_callback)
{
    user_callback->done_callback    = NULL;
    user_callback->cookie           = NULL;
    user_callback->in_mount_handle  = NULL;
    user_callback->in_bufptr        = NULL;
    user_callback->in_bufsize       = 0;
    user_callback->in_inode_number  = 0;
    user_callback->in_offset        = 0;
    user_callback->in_length        = 0;
    user_callback->in_request       = NULL;
}

void jsonrpc_set_callback_info(jsonrpc_context_t*      ctx,
                               jsonrpc_done_callback_t callback,
                               void*                   cookie,
                               void*                   in_mount_handle,
                               uint8_t*                in_bufptr,
                               size_t                  in_bufsize,
                               uint64_t                in_inode_number,
                               uint64_t                in_offset,
                               uint64_t                in_length,
                               void*                   in_request)
{
    ctx->user_callback.done_callback    = callback;
    ctx->user_callback.cookie           = cookie;
    ctx->user_callback.in_mount_handle  = in_mount_handle;
    ctx->user_callback.in_bufptr        = in_bufptr;
    ctx->user_callback.in_bufsize       = in_bufsize;
    ctx->user_callback.in_inode_number  = in_inode_number;
    ctx->user_callback.in_offset        = in_offset;
    ctx->user_callback.in_length        = in_length;
    ctx->user_callback.in_request       = in_request;
}

int jsonrpc_get_callback_info(jsonrpc_context_t*       ctx,
                              jsonrpc_done_callback_t* callback,
                              void**                   cookie,
                              void**                   mount_handle,
                              uint8_t**                bufptr,
                              size_t*                  bufsize,
                              uint64_t*                inode_number,
                              uint64_t*                offset,
                              uint64_t*                length,
                              void**                   request)
{
    if ((callback == NULL) || (cookie == NULL)   || (mount_handle == NULL) ||
        (bufptr == NULL)   || (bufsize == NULL)  || (inode_number == NULL) ||
        (offset == NULL)   || (length == NULL)) {
        return EINVAL;
    }

    *callback     = ctx->user_callback.done_callback;
    *cookie       = ctx->user_callback.cookie;
    *mount_handle = ctx->user_callback.in_mount_handle;
    *bufptr       = ctx->user_callback.in_bufptr;
    *bufsize      = ctx->user_callback.in_bufsize;
    *inode_number = ctx->user_callback.in_inode_number;
    *offset       = ctx->user_callback.in_offset;
    *length       = ctx->user_callback.in_length;
    *request      = ctx->user_callback.in_request ;
    return 0;
}

#if 0
void jsonrpc_set_callback_info(jsonrpc_context_t*      ctx,
                               jsonrpc_done_callback_t callback,
                               void*                   cookie)
{
    ctx->user_callback.done_callback    = callback;
    ctx->user_callback.cookie           = cookie;
    ctx->user_callback.in_mount_handle  = NULL;
    ctx->user_callback.in_bufptr        = NULL;
    ctx->user_callback.in_bufsize       = 0;
    ctx->user_callback.in_inode_number  = 0;
    ctx->user_callback.in_offset        = 0;
    ctx->user_callback.in_length        = 0;
}
#endif

int jsonrpc_get_done_callback(jsonrpc_context_t*       ctx,
                              jsonrpc_done_callback_t* callback,
                              void**                   cookie,
                              void**                   request)
{
    if ((callback == NULL) || (cookie == NULL)) {
        return EINVAL;
    }

    *callback     = ctx->user_callback.done_callback;
    *cookie       = ctx->user_callback.cookie;
    *request      = ctx->user_callback.in_request ;
    return 0;
}

void jsonrpc_dump_user_callback(jsonrpc_context_t* ctx)
{
    if (ctx == NULL) return;

    printf("User callback info for ctx=%p: done_callback=%p "
           "cookie=%p in_mount_handle=%p in_bufptr=%p, in_bufsize=%" PRIu64 ", in_inode_number=%" PRIu64 ", "
           "in_offset=%" PRIu64 ", in_length=%" PRIu64 ", in_request =%p\n",
           ctx,
           ctx->user_callback.done_callback,
           ctx->user_callback.cookie,
           ctx->user_callback.in_mount_handle,
           ctx->user_callback.in_bufptr,
           ctx->user_callback.in_bufsize,
           ctx->user_callback.in_inode_number,
           ctx->user_callback.in_offset,
           ctx->user_callback.in_length,
           ctx->user_callback.in_request);
}

// Make sure at least one of the callbacks has been set for this context
bool jsonrpc_check_user_callback(jsonrpc_context_t* ctx)
{
    if (ctx->user_callback.done_callback != NULL) {
        // callback is set, good
        return true;
    }

    // Otherwise either multiple callbacks are set or no callback is set
    printf("%s: ERROR, callback check failed. done_callback=%p.\n",
           __FUNCTION__, ctx->user_callback.done_callback);
    return false;
}

void jsonrpc_set_internal_callback(jsonrpc_context_t* ctx, jsonrpc_internal_callback_t internal_callback)
{
    ctx->internal_callback = internal_callback;
}

jsonrpc_internal_callback_t jsonrpc_get_internal_callback(jsonrpc_context_t* ctx)
{
    return ctx->internal_callback;
}

void jsonrpc_set_profiler(jsonrpc_context_t* ctx, profiler_t* profiler)
{
    ctx->profiler = profiler;
}

profiler_t* jsonrpc_get_profiler(jsonrpc_context_t* ctx)
{
    return ctx->profiler;
}

// XXX TODO: create a ctx pool so that we don't have to alloc/free these on the fly?
//
jsonrpc_context_t* construct_ctx(jsonrpc_handle_t* handle, const char* method)
{
    // Alloc memory for context to return
    jsonrpc_context_t* ctx = (jsonrpc_context_t*)malloc(sizeof(jsonrpc_context_t));
    bzero(ctx, sizeof(jsonrpc_context_t));

    // Set RPC handle
    ctx->rpc_handle = handle;

    // Initialize request and response
    jsonrpc_init_request(&ctx->req, method);
    jsonrpc_init_response(&ctx->resp);
    jsonrpc_init_cv_info(&ctx->cv_info);

    // Initialize callback stuff
    jsonrpc_init_user_callback(&ctx->user_callback);

    // Initialize internal callback
    ctx->internal_callback = NULL;

    // Initialize next pointer
    ctx->next = NULL;

    // Initialize timing profiler
    ctx->profiler = NULL;

    return ctx;
}

void destruct_ctx(jsonrpc_context_t* ctx)
{
    if (ctx == NULL) return;

    //DPRINTF("Locking so that we have exclusive access to ctx=%p (pre-lock).\n",ctx);

    // protect have_response flag
    pthread_mutex_lock(&ctx->cv_info.mutex);

    // Free json objects
    json_object_put(ctx->req.request);
    json_object_put(ctx->resp.response);

    // Initialize timing profiler
    ctx->profiler = NULL;

    //DPRINTF("Freeing ctx=%p (post-lock).\n",ctx);

    // Free context. Note that we don't free the rpc_handle, because
    // we didn't allocate it.
    free(ctx);
}

// Create a jsonrpc context and set the request method
jsonrpc_context_t* jsonrpc_open(jsonrpc_handle_t* handle, const char* method)
{
    return construct_ctx(handle, method);
}

void jsonrpc_close(jsonrpc_context_t* ctx)
{
    destruct_ctx(ctx);
}

int jsonrpc_num_in_list(jsonrpc_context_t* head, pthread_mutex_t* lock)
{
    int num_items= 0;

    // Not locking here; if enabled and you call this function while holding
    // the lock, it hangs.
    pthread_mutex_lock(lock);

    jsonrpc_context_t* currPtr = head;
    while (currPtr != NULL) {
        num_items++;
        currPtr = currPtr->next;
    }

	pthread_mutex_unlock(lock);

    return num_items;
}

// Save the request context somewhere
void jsonrpc_store_in_list(jsonrpc_context_t* ctx, jsonrpc_context_t** head, pthread_mutex_t* lock, char* list_name)
{
    pthread_mutex_lock(lock);

    LIST_PRINTF("%s: current head is %p\n", list_name, *head);

    // Store the request at the head of the list
    if (*head != NULL) {
        ctx->next = *head;
    } else {
        ctx->next = NULL;
    }
    *head = ctx;
    LIST_PRINTF("%s: setting %p->next to %p; new head is %p\n", list_name, ctx, ctx->next, *head);

    pthread_mutex_unlock(lock);

    LIST_PRINTF("%s: stored request %p with id %d; list size is %d\n", list_name, ctx, ctx->req.request_id, jsonrpc_num_in_list(*head, lock));
}

// remove the request from the list
// caller still must free the ctx.
void jsonrpc_remove_from_list(jsonrpc_context_t* ctx, jsonrpc_context_t** head, pthread_mutex_t* lock, char* list_name)
{
    if (ctx == NULL) return;

    pthread_mutex_lock(lock);

    jsonrpc_context_t* currPtr = *head;
    jsonrpc_context_t* prevPtr = NULL;
    bool request_found = false;

    int count = 0;
    for (; currPtr != NULL; count++, prevPtr = currPtr, currPtr = currPtr->next) {

        if (currPtr == ctx) {
            // Found it
            if (prevPtr == NULL) {
                // Need to update the head of the list
                *head = currPtr->next;
            } else {
                // Make previous node point to next one
                prevPtr->next = currPtr->next;
            }
            request_found = true;
            ctx->next = NULL;

            break;
        }
    }

    pthread_mutex_unlock(lock);

    if (request_found) {
        LIST_PRINTF("removed request %p with id %d from list %s; list size is %d\n", ctx, ctx->req.request_id, list_name, jsonrpc_num_in_list(*head, lock));
    } else {
        LIST_PRINTF("could not find request %p with id %d in the list\n", ctx, ctx->req.request_id);
    }
}

// Return the request context that corresponds to the response_id
jsonrpc_context_t* jsonrpc_find_in_list_by_id(int request_id, jsonrpc_context_t** head, pthread_mutex_t* lock, char* list_name)
{
    jsonrpc_context_t* currPtr = *head;

    pthread_mutex_lock(lock);

    int count = 0;
    for (; currPtr != NULL; count++, currPtr = currPtr->next) {
        if (currPtr->req.request_id == request_id) {
            pthread_mutex_unlock(lock);

            // Found it
            return currPtr;
        }
    }

    DPRINTF("Did not find the request in list - find by id: %d\n", request_id);
    pthread_mutex_unlock(lock);

    // If we get here, we didn't find it
    return NULL;
}

// Return the request context that corresponds to the cookie
jsonrpc_context_t* jsonrpc_find_in_list_by_cookie(void* cookie, jsonrpc_context_t** head, pthread_mutex_t* lock, char* list_name)
{
    jsonrpc_context_t* currPtr = *head;

    pthread_mutex_lock(lock);

    int count = 0;
    for (; currPtr != NULL; count++, currPtr = currPtr->next) {
        if (currPtr->user_callback.cookie == cookie) {
            pthread_mutex_unlock(lock);
            // Found it
            return currPtr;
        }
    }

    pthread_mutex_unlock(lock);

    // If we get here, we didn't find it
    LIST_PRINTF("Unable to find cookie %p in list. List size: %d\n", cookie, jsonrpc_num_in_list(*head, lock));
    return NULL;
}

// Global pointer to linked list of outstanding requests
jsonrpc_context_t* requests_in_progress = NULL;
pthread_mutex_t    requests_in_progress_lock = PTHREAD_MUTEX_INITIALIZER;
char               request_list_name[]  = "request_list";

// Return the number of outstanding requests
int jsonrpc_num_requests()
{
    return jsonrpc_num_in_list(requests_in_progress, &requests_in_progress_lock);
}

// Save the request context somewhere
void jsonrpc_store_request(jsonrpc_context_t* ctx)
{
    jsonrpc_store_in_list(ctx, &requests_in_progress, &requests_in_progress_lock, request_list_name);
}

// remove the request from the list
// caller still must free the ctx.
void jsonrpc_remove_request(jsonrpc_context_t* ctx)
{
    jsonrpc_remove_from_list(ctx, &requests_in_progress, &requests_in_progress_lock, request_list_name);
}

// Return the request context that corresponds to the request_id
jsonrpc_context_t* jsonrpc_get_request(jsonrpc_response_t* resp)
{
    return jsonrpc_find_in_list_by_id(resp->response_id, &requests_in_progress, &requests_in_progress_lock, request_list_name);
}

// Return the request context that corresponds to the caller-provided cookie
jsonrpc_context_t* jsonrpc_get_request_by_cookie(void* cookie)
{
    return jsonrpc_find_in_list_by_cookie(cookie, &requests_in_progress, &requests_in_progress_lock, request_list_name);
}

