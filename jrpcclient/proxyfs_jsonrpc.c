#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <ioworker.h>
#include <proxyfs_jsonrpc.h>
#include <json_utils_internal.h>
#include <proxyfs_req_resp.h>
#include <socket.h>
#include <debug.h>
#include <fault_inj.h>
#include <string.h>

static char rpc_server[128];
static int  rpc_port;
static int  rpc_fast_port;

static char* configFile	       = "/tmp/rpc_server.conf";
static char* configIPAddrKey   = "IPAddr";
static char* configPortKey     = "TCPPort";
static char* configFastPortKey = "FastTCPPort";

char *rpc_override_string = (char *)NULL;

void rpc_config_override(const char *override_string)
{
    rpc_override_string = strdup(override_string);
}

void get_rpc_config()
{
    char* buffer = NULL;
    char* bufPtr = NULL;
    long length = 0;
    char *line, *key, *value, *brkt, *brkb;
    int slash_pos;
    int colon_pos;
    char rpc_port_string[16];
    char rpc_fast_port_string[16];

    if ((char *)NULL != rpc_override_string) {
        // Instead of reading config file, parse IPAddr:TCPPort:FastTCPPort rpc_override_string

        length = strlen(rpc_override_string);

        slash_pos = length - 1;
        while ((0 <= slash_pos) && ('/' != rpc_override_string[slash_pos])) slash_pos--;
        if (0 > slash_pos) DPANIC("Failed to find delimiting '|' between TCPPort & FastTCPPort in rpc_override_string");
        if (1 == (length - slash_pos)) DPANIC("FastTCPPort following '|' zero-length");
        if (16 < (length - slash_pos)) DPANIC("FastTCPPort field too long (%d - should be no more than 15)", length - slash_pos - 1);

        colon_pos = slash_pos - 1;
        while ((0 <= colon_pos) && (':' != rpc_override_string[colon_pos])) colon_pos--;
        if (0 > colon_pos) DPANIC("Failed to find delimiting ':' between IPAddr & TCPPort in rpc_override_string");
        if (1 == (slash_pos - colon_pos)) DPANIC("TCPPort following ':' zero-length");
        if (16 < (slash_pos - colon_pos)) DPANIC("TCPPort field too long (%d - should be no more than 15)", slash_pos - colon_pos - 1);

        if (0 == colon_pos) DPANIC("IPAddr preceding ':' zero-length");
        if (128 < colon_pos) DPANIC("IPAddr field too long (%d - should be no more than 127)", colon_pos - 1);

        strncpy(&rpc_server[0], &rpc_override_string[0], colon_pos);
        rpc_server[colon_pos] = '\0';
        strncpy(&rpc_port_string[0], &rpc_override_string[colon_pos + 1], slash_pos - colon_pos - 1);
        rpc_port_string[slash_pos - colon_pos - 1] = '\0';
        rpc_port = atoi(rpc_port_string);
        strncpy(&rpc_fast_port_string[0], &rpc_override_string[slash_pos + 1], length - slash_pos - 1);
        rpc_fast_port_string[length - slash_pos - 1] = '\0';
        rpc_fast_port = atoi(rpc_fast_port_string);

        return;
    }

    // Open /tmp/rpc_server.conf
    FILE* fd = fopen(configFile, "r");
    if (fd) {
        //DPRINTF(">>>> ProxyFS API: Opened config file %s.\n", configFile);

        // Find the file length
        fseek(fd, 0, SEEK_END);
        length = ftell(fd);
        fseek(fd, 0, SEEK_SET);

        // Alloc a buffer and then read the file into it
        buffer = malloc(length+1);
        if (buffer)
        {
            fread (buffer, 1, length, fd);
            buffer[length] = '\0';
        }
        fclose (fd);
    } else {
        DPRINTF(">>>> ProxyFS API: FAILED to open config file %s.\n", configFile);
    }

    // Set our temp pointer to the beginning of the buffer
    bufPtr = buffer;

    //printf("conf file length is %ld\n",strlen(buffer));
    if (buffer == NULL)
    {
        DPRINTF(">>>> ProxyFS API: ERROR, unable to read config file %s; default settings will be used.\n",
                configFile);
    }

    // Look for key: value lines
    while (line = strtok_r(bufPtr, "\n", &bufPtr))
    {
        // Get this line's key and value
        key = strtok_r(line, ": ", &brkb);
        if (key != NULL) {
            value = brkb + 1;

            if (strcmp(key, configIPAddrKey) == 0) {
                // Found the RPC server IPAddr
                strcpy(rpc_server, value);
            } else if (strcmp(key, configPortKey) == 0) {
                // Found the RPC TCPPort
                rpc_port = atoi(value);
            } else if (strcmp(key, configFastPortKey) == 0) {
                // Found the RPC FastTCPPort
                rpc_fast_port = atoi(value);
            }
        }
    }

    free(buffer);

    DPRINTF(">>>> ProxyFS API: Using JSON RPC server %s:%d and %s:%d\n", rpc_server, rpc_port, rpc_server, rpc_fast_port);
}


// Internal struct for our RPC handle
struct rpc_handle_t {
    int sockfd;
};

// XXX TODO: Since sock_read and sock_write now use global_sockfd, we don't really need
//           to keep handle->sockfd around any more.
//           Should we keep it in case we use different sockets for different mounts later,
//           or remove it because it's confusing to fetch and store it and not use it?

// Open proxyfs RPC context. If successful, returns non-null handle.
//
// Implementation notes:
// * This API will be called probably on a per-mount basis. We'll need to decide whether to
//   create a new TCP connection per call, or cache it underneath.
//
jsonrpc_handle_t* pfs_rpc_open()
{
    if ( fail(RPC_CONNECT_FAULT) ) {
        // Fault-inject case
        return NULL;
    }

    // Get the config of the RPC server so that we know who to connect to.
    get_rpc_config();

    // Alloc memory for handle to return
    jsonrpc_handle_t* handle = (jsonrpc_handle_t*)malloc(sizeof(jsonrpc_handle_t));

    int ret = io_workers_start(rpc_server, rpc_fast_port, 128);
    if (ret != 0) {
        free(handle);
        handle = NULL;
        printf("Failed to start worker pool for %s port: %d\n", rpc_server, rpc_fast_port);
        return handle;
    }

    // Open the IO socket, if it's not already open
    if (io_sock_fd == -1) {
        io_sock_fd = sock_open(rpc_server, rpc_fast_port);
        if (io_sock_fd < 0) {
            free(handle);
            handle = NULL;
            //printf("Failed to open io socket: %d\n", rpc_fast_port);
            return handle;
        }
    } else {
        DPRINTF("pfs_rpc_open: no need to open io_sock_fd=%d, already open.\n", io_sock_fd);
    }

    // TODO: NOT using any lock to test. Can cause issue in concurrent mounts.
    if (global_sock_pool == NULL) {
        global_sock_pool = sock_pool_create(rpc_server, rpc_port, GLOBAL_SOCK_POOL_COUNT);
        if (global_sock_pool == NULL) {
            free(handle);
            handle = NULL;
        }
    } else {
        DPRINTF("pfs_rpc_open: no need to create global_sock_pool=%p, already open.\n", global_sock_pool);
    }

    return handle;
}


// Close proxyfs RPC context. Returns errno indicating success/failure.
void pfs_rpc_close(jsonrpc_handle_t* handle)
{
    if (handle == NULL) {
        return;
    }

    io_workers_stop();

    // TODO: Socket pool once created remains active.. do not remove it.
    // sock_pool_t *local_pool = global_sock_pool;
    // global_sock_pool = NULL;

    // sock_pool_destroy(local_pool, TRUE);

    // Free handle
    free(handle);
}

// Close and reopen proxyfs RPC context, to clear errors
void jsonrpc_rpc_bounce(jsonrpc_handle_t** handle)
{
    // TODO: NOT using any lock to close...
    sock_pool_t *local_pool = global_sock_pool;
    global_sock_pool = NULL;

    sock_pool_destroy(local_pool, TRUE);

    global_sock_pool = sock_pool_create(rpc_server, rpc_port, GLOBAL_SOCK_POOL_COUNT);
    if (global_sock_pool == NULL) {
        // There was an error opening the socket.
        DPRINTF("Error recreating socket pool.\n");
    }
}

void jsonrpc_free_read_buf(jsonrpc_context_t* ctx)
{
    // Free the buffer; the data is all in the json response now.
    if (ctx->resp.readBuf != NULL) {
        free(ctx->resp.readBuf);
        ctx->resp.readBuf = NULL;
    }
}

// NOTE on response handling:
//
// rpc_lock is primarily used to ensure that we serialize sending of requests
// and reading of responses so that only one is trying to use the socket at
// any given time. It is also used to control the count of the number of
// responses we need.
//
// The counting of responses needed is done just after a request is sent,
// with rpc_lock held; this will ensure that we don't get the actual response
// before we have recorded that we need one.
//

// Lock to serialize request/response pairs and protect response work count
// TODO: With socket pool we may not need the rpc_lock anymore.

pthread_mutex_t rpc_lock                     = PTHREAD_MUTEX_INITIALIZER;
int32_t         responses_needed             = 0;
bool            response_work_thread_running = false;
pthread_cond_t  response_work_to_do          = PTHREAD_COND_INITIALIZER;
pthread_t       response_work_thread;

// TODO: With socket pool and select mechanism for event notificaiton when a reply arrives, we may not
// need request send triggering the worker thread to look for a reply and work complete handshake:
// wait_for_response_work(), record_resp_work_locked() and complete_response_work().
void wait_for_response_work()
{
    DPRINTF("Blocking until we have response work to do.\n");

    // protect responses_needed
    pthread_mutex_lock(&rpc_lock);

    // rpc_get_response will decrement responses_needed based on how
    // many responses it gets. If it gets multiple response, we could
    // get a cv signal without needing to do anything.

    // If we don't have a response, then wait
    while (!responses_needed) {
        pthread_cond_wait(&response_work_to_do, &rpc_lock);
    }

    //DPRINTF("Have work to do; unlocking mutex.\n");

    // release responses_needed
    pthread_mutex_unlock(&rpc_lock);

    DPRINTF("Have response work to do.\n");
}

// Increment responses_needed and signal cv.
//
// NOTE: This API must be called with rpc_lock held.
//
void record_resp_work_locked(int response_id)
{
    // Record that there's a response needed. This number is incremented
    // here and decremented by rpc_get_response
    responses_needed++;

    DPRINTF("resp_id:%d responses_needed incremented, now=%d\n", response_id, responses_needed);

    // XXX TODO: add response_id to the list of responses we're expecting,
    //           so that we can make sure we get what we're looking for.

    // Wake up the waiter
    pthread_cond_signal(&response_work_to_do);

    DPRINTF("resp_id:%d Unblocking of response work waiter done\n", response_id);
}

// Decrement responses_needed, indicating that the response processing is complete.
void complete_response_work(int response_id)
{
    DPRINTF("resp_id:%d Decrementing responses_needed.\n", response_id);

    // Lock around response work control
    pthread_mutex_lock(&rpc_lock);

    // Record that one responses's work has been completed.
    //
    if (responses_needed > 0) {
        responses_needed--;
    } else {
        // We've had some sort of race condition. PANIC so that we can find the logic problem.
        DPANIC("FATAL, called to decrement responses_needed for response_id=%d but it's already %d!\n", response_id, responses_needed);
        return;
    }

    // XXX TODO: remove response_id from the list of responses we're expecting,
    //           so that we can make sure we get what we're looking for.

    DPRINTF("resp_id:%d responses_needed decremented, now=%d\n", response_id, responses_needed);

    // release response work control
    pthread_mutex_unlock(&rpc_lock);
}

// API to send a request over the socket. It is layered on top of sock_write() which will send the data
// over an available socket in the socket pool.
// All send requests are asynchronous. The reply will arrive in the receive thread and the socket used for
// this send will be released there.
int rpc_send_request(jsonrpc_context_t* ctx)
{
    profiler_t* profiler = jsonrpc_get_profiler(ctx);
    //AddProfilerEvent(profiler, BEFORE_RPC_SEND);

    // Return value
    int rc = 0;

    // Send something
    const char* writeBuf = json_object_to_json_string_ext(ctx->req.request, JSON_C_TO_STRING_PLAIN);
    if (debug_flag > 0) {
        if (strlen(writeBuf) <= MAX_PRINT_SIZE) {
            DPRINTF("Sending data: %s\n",writeBuf);
        } else {
            // Emit a truncated version
            // Save the byte we are changing
            char byte = writeBuf[MAX_PRINT_SIZE];
            ((char*)writeBuf)[MAX_PRINT_SIZE] = 0;
            DPRINTF("Sending data: %s<snip>\n",writeBuf);
            ((char*)writeBuf)[MAX_PRINT_SIZE] = byte;
        }
    }
    AddProfilerEvent(profiler, RPC_SEND_AFTER_JSON);

    // Store request before sending so that it's available if we get a response before we return.
    jsonrpc_store_request(ctx);

    // sock_write success is 0, all else is an error
    rc = sock_write(writeBuf);
    // NOTE: This one is commented out since it races with delivery timestamps
    //AddProfilerEvent(profiler, RPC_SEND_AFTER_SOCK_WRITE);
    if (rc != 0) {
        DPRINTF("Error %d writing to socket.\n", rc);
        jsonrpc_remove_request(ctx);
        goto done;
    }

    DPRINTF("Calling rpc_schedule_resp_work_locked, ctx=%p\n", ctx);

    // Read response (response locking is in rpc_get_response)
    rc = rpc_schedule_resp_work_locked(ctx->req.request_id);
    if (rc != 0) {
        DPRINTF("Error %d from rpc_schedule_resp_work_locked for ctx=%p\n", rc, ctx);
        goto done;
    }

    DPRINTF("Returned %d from rpc_schedule_resp_work_locked for ctx=%p\n", rc, ctx);

done:
    if ((rc == EPIPE) || (rc == ENODEV) || (rc == EBADF)) {
        // The socket got disconnected.
        // Close and reopen the socket to clear the error
        jsonrpc_rpc_bounce(&ctx->rpc_handle);
    }

    //AddProfilerEvent(profiler, AFTER_RPC_SEND);

    return rc;
}

// XXX TODO: Fix this limitation
#define MAX_CONCURRENT_RESPONSES 16

// Generically read a response from the socket.
// This API can be called directly, or spawned with pthread_create.
//
// Returns the response_id, for request/response matching
//
void rpc_get_response(int sockfd)
{
    int num_responses = 0;

    // Lock around request/response block

    // Array of response structures, just init the first one for now
    jsonrpc_response_t resp[MAX_CONCURRENT_RESPONSES];
    jsonrpc_context_t* ctx[MAX_CONCURRENT_RESPONSES] = { NULL };
    jsonrpc_init_response(&resp[0]);

    //DPRINTF("Reading from socket.\n");
    // sock_read returns bytes read, negative values or non-zero rtnError are errors

    // NOTE: If we time the start of the sock_read here, it also includes time
    //       spent waiting for a response to come over the socket.
    //
    //AddProfilerEvent(profiler, BEFORE_SOCK_READ);
    int bytesRead = sock_read(sockfd, &resp[0].readBuf, &resp[0].rsp_err);

    // Start timing
    profiler_t* profiler = NewProfiler(SOCK_RECEIVE);
    AddProfilerEvent(profiler, AFTER_SOCK_READ);

    if ((resp[0].rsp_err == 0) && (bytesRead < 0)) {
        resp[0].rsp_err = EIO;
        DPRINTF("Error, read %d bytes from socket, returning error=%d.\n", bytesRead, resp[0].rsp_err);
    }

    if (resp[0].rsp_err != 0) {
        DPRINTF("Error %d reading from socket.\n", resp[0].rsp_err);
        if ((resp[0].rsp_err == EPIPE) || (resp[0].rsp_err == ENODEV) || (resp[0].rsp_err == EBADF)) {
            // The socket got disconnected. Force a DPANIC.
            // TBD: Build a proper error handling mechanism to retry the operation.
            PANIC("Failed to read reply from proxyfsd <-> rpc client socket.\n");
            goto done;
        }
    }
    //DPRINTF("Read from socket: %s\n",resp[0].readBuf);
    DPRINTF("Read %ld bytes into resp[0].readBuf %p from socket.\n", strlen(resp[0].readBuf),resp[0].readBuf);
    if (bytesRead != strlen(resp[0].readBuf)) {
        PRINTF("ERROR, sock_read returned bytesRead=%d but strlen(resp.ReadBuf)=%ld resp.ReadBuf[%d]=0x%x resp.ReadBuf[%d]=0x%x!!\n",
               bytesRead, strlen(resp[0].readBuf), bytesRead, resp[0].readBuf[bytesRead], bytesRead+1, resp[0].readBuf[bytesRead+1]);
        DPRINTF("Raw full response: \n---\n%s\n---\n", resp[0].readBuf);
        fflush(stdout);
    }

    // Sometimes we can receive more than one response in a single sock_read.
    // Because of this, we need to break the readBuf at CRLF into as many buffers as are present
    //
    // This has the side effect of cutting CRLF off of the end of the buffers.
    // But that's ok because the json code doesn't care, and it makes the debug prints easier to read.
    //
    char* raw_json_response      = NULL;
    char* bufPtr                 = resp[0].readBuf;
    int   resp_index             = 0;
    jsonrpc_response_t* resp_ptr = NULL;

    while (raw_json_response = strtok_r(bufPtr, "\r\n", &bufPtr))
    {
        num_responses++;
        resp_index = num_responses-1;
        resp_ptr   = &resp[resp_index];

        //DPRINTF("Raw JSON response %d: \n---\n%s\n---\n", num_responses, raw_json_response);
        //fflush(stdout);

        // For the first response, we've already allocated buffers and initialized the response struct.
        if (num_responses == 1) {
            continue;
        }
        // Else we have multiple responses.

        // Range check for our response structures
        if (num_responses > MAX_CONCURRENT_RESPONSES) {
            // Oops, can't handle this one!
            DPANIC("FATAL: received more responses (%d) than we can handle!\n", num_responses);
            return;
        }

        // Init a new response struct.
        jsonrpc_init_response(resp_ptr);

        // XXX TODO: So for now, we copy responses > 1 into their own buffer.
        //           Better could be to keep refcount on the single one. But we can do that later.
        //
        size_t readSize = strlen(raw_json_response)+1;
        char*  readBuf  = malloc(readSize);
        if (readBuf == NULL) {
            DPANIC("FATAL: unable to allocate %ld bytes for response %d!\n", readSize, num_responses);
            return;
        }
        // Copy response into readBuf and make sure it is null-terminated
        strncpy(readBuf, raw_json_response, readSize);
        readBuf[readSize-1] = 0;

        // Save away readBuf.
        jsonrpc_set_read_buf(resp_ptr, readBuf);

        DPRINTF("Copied %ld bytes from socket into resp[%d].readBuf %p.\n",
                strlen(resp_ptr->readBuf),resp_index,resp_ptr->readBuf);

#ifdef TEST_MULTI_RESPONSE_HANDLING
        // Temporarily sleep in the multi-response scenario so that we can know it happened
        //
        // This code is only enabled if you define TEST_MULTI_RESPONSE_HANDLING...
        //
        DPRINTF("SLEEPING so that you can find me!\n");
        fflush(stdout);
        sleep(2*60);
        DPRINTF("Done SLEEPING!\n");
        fflush(stdout);
#endif
    }

    AddProfilerEvent(profiler, AFTER_GET_RESPONSES);

    // Now handle all our responses
    for (resp_index = 0, resp_ptr = &resp[0]; resp_index < num_responses; resp_index++, resp_ptr = &resp[resp_index]) {

        resp_ptr->response = json_tokener_parse(resp_ptr->readBuf);
        //DPRINTF("response:\n---\n%s\n---\n", json_object_to_json_string_ext(resp_ptr->response,
        //        JSON_C_TO_STRING_SPACED | JSON_C_TO_STRING_PRETTY));

        // Get id from response
        resp_ptr->response_id = jsonrpc_get_resp_id(resp_ptr);

        // use the resp.response_id to find the request ctx
        ctx[resp_index] = jsonrpc_get_request(resp_ptr);
        if (ctx[resp_index] == NULL) {
            PRINTF("ERROR, unable to find context for id=%d\n",resp_ptr->response_id);
            goto done;
        }
        //DPRINTF("Found ctx[%d]=%p for id=%d\n", resp_index, ctx[resp_index], resp_ptr->response_id);

        if (resp_ptr->response_id != ctx[resp_index]->req.request_id) {
            // This shouldn't happen, since we specifically looked for this id
            PRINTF("ERROR, expected id=%d, received id=%d\n", ctx[resp_index]->req.request_id, resp_ptr->response_id);

            // Tell the caller we've been disconnected from the far end
            // XXX TODO: this isn't really true though...
            resp_ptr->rsp_err = EPIPE;

            // XXX TODO: need to do the request/response handling even in this case...
            goto done;
        } else {
            DPRINTF("Response id = %d\n",resp_ptr->response_id);
        }

        // Was there an error?
        const char* err_str = NULL;
        resp_ptr->rsp_err = jsonrpc_get_resp_error(resp_ptr, &err_str);
        if (resp_ptr->rsp_err != 0) {
            DPRINTF("error=%d (%s) was returned.\n",resp_ptr->rsp_err, err_str);
        }
    }

    AddProfilerEvent(profiler, AFTER_JSON_RESPONSES);

done:
    for (resp_index = 0, resp_ptr = &resp[0]; resp_index < num_responses; resp_index++, resp_ptr = &resp[resp_index]) {

        // Set the response result
        resp_ptr->response_result = get_jrpc_result(resp_ptr->response);

        // Logic to skip ctx-related stuff if we failed to get ctx above
        if (ctx[resp_index] != NULL) {
            // Save the response in the context
            jsonrpc_copy_response(ctx[resp_index], resp_ptr);

            // Free the buffer; the data is all in the json response now.
            jsonrpc_free_read_buf(ctx[resp_index]);

            if ((resp_ptr->rsp_err == EPIPE) || (resp_ptr->rsp_err == ENODEV) || (resp_ptr->rsp_err == EBADF)) {
                // The socket got disconnected.
                // Close and reopen the socket to clear the error
                jsonrpc_rpc_bounce(&ctx[resp_index]->rpc_handle);
            }
        }
    }

    AddProfilerEvent(profiler, AFTER_COPY_RESPONSES);

    for (resp_index = 0, resp_ptr = &resp[0]; resp_index < num_responses; resp_index++, resp_ptr = &resp[resp_index]) {

        if (ctx[resp_index] == NULL) {
            goto really_done;
        }

        // Add profiler events to this op's context, now that we know what it is
        profiler_t* op_profiler = jsonrpc_get_profiler(ctx[resp_index]);
        //AddProfilerEvents(op_profiler, profiler);

        // Mark this response as completed.
        // We do this before the callback because the ctx gets freed there.
        //complete_response_work(ctx[resp_index]->req.request_id);

        // If there is a callback, invoke it now. If not, signal that we have the response.
        jsonrpc_internal_callback_t internal_cb = jsonrpc_get_internal_callback(ctx[resp_index]);
        if (internal_cb == NULL) {
            DPRINTF("ctx=%p No callback to call for blocking call; signal waiter.\n", ctx[resp_index]);

            // Remove request context from outstanding request list
            jsonrpc_remove_request(ctx[resp_index]);

            // Find the cv and signal it
            jsonrpc_unblock_for_response(ctx[resp_index]);

        } else {
            DPRINTF("ctx=%p Calling callback %p.\n", ctx[resp_index], internal_cb);
            (*internal_cb)(ctx[resp_index]);

            // NOTE: Don't remove ctx when we get response in nonblocking case;
            //       need to wait until proxyfs_read_recv has been called.
        }
    }

    AddProfilerEvent(profiler, AFTER_RESPONSE_CALLBACKS);

really_done:
    // Stop timing and print latency
    StopProfiler(profiler);
    // NOTE: Not dumping here since we've folded the events into the appropriate operation's profile
    //DumpProfiler(profiler);
    DeleteProfiler(profiler);

    return;
}

// Response thread main loop
void* jsonrpc_response_thread(void* not_used)
{
    DPRINTF("Spawned thread.\n");

    while (1) {
        // Wait for something to do.
        //wait_for_response_work();
        //DPRINTF("calling sock_pool_select.\n");
        int sockfd = sock_pool_select(global_sock_pool, 5); // timeout == 5 seconds.
        //DPRINTF("sock_pool_select returned sockfd=%d.\n",sockfd);
        if (sockfd < 0) {
            DPRINTF("ERROR faild to select on a socket\n");
            response_work_thread_running = false;
            return;
        }

        // Did we timeout on the select?
        if (sockfd == 0) {
            DPRINTF("Timeout on sock select; retrying.\n");
            // NOTE:
            // We seem to periodically hit this, causing 5-second delays when it occurs.
            // This can happen if we did a sock_pool_destroy/create after the select started.
            continue;
        }

        // There's work to do, do it
        //DPRINTF("responses_needed=%d; calling rpc_get_response.\n", responses_needed);
        rpc_get_response(sockfd);
    }

    // XXX TODO: what is the best way to clean this thread up when we're done?
    //           currently we just leave it running; when the process who called us exits,
    //           it will clean up this thread for us.

    DPRINTF("Thread done.\n");
}

// Request/response threading:
//
// We have a single thread for response handling. When we finish sending a request
// and thus have a response to get, rpc_send_request calls rpc_schedule_resp_work_locked,
// who manages work for the thread.
//

// XXX TODO future performance improvements:
//  - buffer pool?

// Trigger response handling.
//
// TODO: With socket pool we don't neet the rpc_lock anymore.
//
int rpc_schedule_resp_work_locked(int expected_response_id)
{
    int rc = 0;

    // Record that response handling is required
    //record_resp_work_locked(expected_response_id);

    if (!response_work_thread_running) {
        rc = pthread_create(&response_work_thread, NULL, &jsonrpc_response_thread, NULL);
        if (rc != 0) {
            DPRINTF("Error %d spawning thread to get the response\n", rc);
        } else {
            DPRINTF("Spawned thread %p to read the response\n",(void*)response_work_thread);

            response_work_thread_running = true;

            // Set thread to be detached, so that the memory is cleaned up when it exits.
            // Otherwise the thread's memory shows as leaked if there is no pthread_join.
            rc = pthread_detach(response_work_thread);
            if (rc != 0) {
                DPRINTF("Error %d detaching thread %p\n", rc, (void*)response_work_thread);
            } else {
                DPRINTF("Detached thread %p to read the response\n",(void*)response_work_thread);
            }
        }
    }

done:

    return rc;
}

int jsonrpc_exec_request_blocking(jsonrpc_context_t* ctx)
{
    profiler_t* profiler = jsonrpc_get_profiler(ctx);

    // Send request
    int rc = rpc_send_request(ctx);
    if (rc != 0) {
        DPRINTF("Error %d sending request.\n", rc);
        return rc;
    }
    // else rc == 0

    // Block until we get this response
    //AddProfilerEvent(profiler, BEFORE_RPC_RX);
    jsonrpc_block_for_response(ctx);
    //AddProfilerEvent(profiler, AFTER_RPC_RX);

    // Extract status to return
    rc = ctx->resp.rsp_err;

    return rc;
}

int jsonrpc_exec_request_nonblocking(jsonrpc_context_t* ctx, jsonrpc_internal_callback_t internal_cb)
{
    // Make sure an external callback context has been saved; bail if not
    //
    // XXX TODO: This check doesn't make sense if we have some async calls (like remount)
    //           that don't have a user-level callback.
    if (!jsonrpc_check_user_callback(ctx)) {
        return -1;
    }

    // Save internal callback
    jsonrpc_set_internal_callback(ctx, internal_cb);

    // Send request and return
    int rc = rpc_send_request(ctx);
    if (rc != 0) {
        DPRINTF("Error %d sending request.\n", rc);
    }
    return rc;
}

