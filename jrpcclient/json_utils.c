#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <base64.h>
#include <json_utils.h>
#include <json_utils_internal.h>
#include <debug.h>


// All of the ProxyFS JSON RPCs are prefixed with this
const char method_prefix[] = "Server";

// Constants for JSON RPC keys
//
const char ID_KEY[]      = "id";
const char METHOD_KEY[]  = "method";
const char PARAMS_KEY[]  = "params";
const char ERROR_KEY[]   = "error";
const char JSONRPC_KEY[] = "jsonrpc";
const char JSONRPC_VAL[] = "2.0";
const char RESULT_KEY[]  = "result";

void print_json_object(struct json_object *jobj, const char *msg) {
    printf("\n%s: \n", msg);
    printf("---\n%s\n---\n", json_object_to_json_string(jobj));
}

struct json_object * find_something(json_object *jobj, const char *key) {
    struct json_object *tmp;

    json_object_object_get_ex(jobj, key, &tmp);

    return tmp;
}

void jsonrpc_set_req_method(jsonrpc_request_t* req, const char* method)
{
    char full_method[80];
    sprintf(full_method,"%s.%s",method_prefix,method);

    // Add the top-level key-value pairs
    json_object_object_add(req->request, ID_KEY, json_object_new_int(req->request_id));
    json_object_object_add(req->request, METHOD_KEY, json_object_new_string(full_method));
    json_object_object_add(req->request, JSONRPC_KEY, json_object_new_string(JSONRPC_VAL));

    // Create the params array, which will consist of key-value pairs
    // but for now is empty
    json_object* pobj = json_object_new_array();
    json_object* params_obj  = json_object_new_object();
    json_object_array_add(pobj,params_obj);

    // Save params_obj in the req to make it easier to add params later
    req->request_params = params_obj;

    // Add the params array to the top-level object
    json_object_object_add(req->request, PARAMS_KEY, pobj);
}

void jsonrpc_set_req_param_str(jsonrpc_context_t* ctx, char* key, char* value)
{
    json_object_object_add(ctx->req.request_params, key, json_object_new_string(value));
}

void jsonrpc_set_req_param_int(jsonrpc_context_t* ctx, char* key, int value)
{
    json_object_object_add(ctx->req.request_params, key, json_object_new_int(value));
}

void jsonrpc_set_req_param_int64(jsonrpc_context_t* ctx, char* key, int64_t value)
{
    json_object_object_add(ctx->req.request_params, key, json_object_new_int64(value));
}

void jsonrpc_set_req_param_uint64(jsonrpc_context_t* ctx, char* key, uint64_t value)
{
    json_object_object_add(ctx->req.request_params, key, json_object_new_int64((int64_t)value));
}

void jsonrpc_set_req_param_buf(jsonrpc_context_t* ctx, char* key, uint8_t* buf, size_t buf_size)
{
    // Encode binary data as a base64 string
    char* encoded_data = encode_binary(buf, buf_size);

    // Set the encoded string in the request header
    json_object_object_add(ctx->req.request_params, key, json_object_new_string(encoded_data));

    // Free the temporary encoded string; it's been copied into the json header
    free(encoded_data);
}

int jsonrpc_get_resp_id(jsonrpc_response_t* resp)
{
    json_object* obj = find_something(resp->response, ID_KEY);

    enum json_type type = json_object_get_type(obj);
    if (type != json_type_int) {
        DPRINTF("Error, id field is not an int (type=%d)!\n",type);
        return -1;
    }

    return json_object_get_int(obj);
}

int jsonrpc_get_resp_error(jsonrpc_response_t* resp, const char** error_string)
{
    json_object* obj = NULL;
    int rtnVal = -1;

    if (json_object_object_get_ex(resp->response, ERROR_KEY, &obj)) {
        // key was found, as it should be
        enum json_type type = json_object_get_type(obj);
        if (type == json_type_string) {
            // Found an error
            *error_string = json_object_get_string(obj);

            //DPRINTF("error=%s was returned.\n",*error_string);

            // Convert error string to errno

            // Look for key: value lines
            char* bufPtr = (char*)*error_string;
            char *line, *key, *value, *brkt, *brkb = NULL;
            while (line = strtok_r(bufPtr, "\n", &bufPtr))
            {
                //printf("Line: %s\n", line);

                // Get this line's key and value
                key = strtok_r(line, ": ", &brkb);
                if (key != NULL) {
                    value = strtok_r(NULL, ": ", &brkb);
                }

                // Look for errno
                if (strcmp(key, "errno") == 0) {
                    // Found the errno
                    rtnVal = atoi(value);
                    //printf("Errno is %d\n", rtnVal);

                // Look for HTTP status
                } else if (strcmp(key, "httpStatus") == 0) {
                    // Found the http status
                    //printf("HTTP status is %s\n", value);
                }
            }
        } else {
            // Error field present but is null. This means success.
            rtnVal = 0;
        }
    } else {
        DPRINTF("Error field not found in response!\n");
    }

    //DPRINTF("Returning %d\n", rtnVal);
    return rtnVal;
}

int jsonrpc_get_resp_status(jsonrpc_context_t* ctx)
{
    return ctx->resp.rsp_err;
}

const char* jsonrpc_get_resp_str(jsonrpc_context_t* ctx, char* key)
{
    json_object* obj = NULL;
    if (!json_object_object_get_ex(ctx->resp.response_result, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return NULL;
    }

    const char* value = json_object_get_string(obj);
    DPRINTF("Returned %s: %s\n", key, value);
    return value;
}

int jsonrpc_get_resp_int(jsonrpc_context_t* ctx, char* key)
{
    json_object* obj = NULL;
    if (!json_object_object_get_ex(ctx->resp.response_result, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return -1;
    }

    int value = json_object_get_int(obj);
    DPRINTF("Returned %s: %d\n", key, value);
    return value;
}

uint64_t jsonrpc_get_resp_uint64(jsonrpc_context_t* ctx, char* key)
{
    json_object* obj = NULL;
    if (!json_object_object_get_ex(ctx->resp.response_result, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return -1;
    }

    uint64_t value = (uint64_t)json_object_get_int64(obj);
    DPRINTF("Returned %s: %" PRIu64 "\n", key, value);
    return value;
}

int64_t jsonrpc_get_resp_int64(jsonrpc_context_t* ctx, char* key)
{
    json_object* obj = NULL;
    if (!json_object_object_get_ex(ctx->resp.response_result, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return -1;
    }

    int64_t value = json_object_get_int64(obj);
    DPRINTF("Returned %s: %" PRId64 "\n", key, value);
    return value;
}

bool jsonrpc_get_resp_bool(jsonrpc_context_t* ctx, char* key)
{
    json_object* obj = NULL;
    if (!json_object_object_get_ex(ctx->resp.response_result, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return -1;
    }

    bool value = json_object_get_boolean(obj);
    DPRINTF("Returned %s: %s\n", key, (value?"true":"false"));
    return value;
}

void jsonrpc_get_resp_buf(jsonrpc_context_t* ctx, char* key, uint8_t* buf, size_t buf_size, size_t* bytes_written)
{
    // Init return values
    *bytes_written = 0;

    json_object* obj = NULL;
    if (!json_object_object_get_ex(ctx->resp.response_result, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return;
    }

    const char* value = json_object_get_string(obj);
    //DPRINTF("Returned %s: %s\n", key, value);

    // Decode from base64 into binary
    decode_binary(value, buf, buf_size, bytes_written);
    DPRINTF("Decoded %s: %p, len=%zu\n", key, buf, *bytes_written);
}

json_object* jsonrpc_get_resp_obj(jsonrpc_context_t* ctx, char* key)
{
    json_object* obj = NULL;
    if (!json_object_object_get_ex(ctx->resp.response_result, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return NULL;
    }

    //DPRINTF("Returned %s: %p\n", key, obj);
    return obj;
}

json_object* get_jrpc_result(json_object* jobj)
{
    return find_something(jobj, RESULT_KEY);
}

uint64_t get_jrpc_mount_id(json_object* jobj)
{
    json_object* obj = NULL;
    // MountID is inside the result object
    json_object* robj = get_jrpc_result(jobj);
    if (!json_object_object_get_ex(robj, "MountID", &obj)) {
        // key was not found
        DPRINTF("MountID field not found in response!\n");
        return 0;
    }

    return json_object_get_int64(obj);
}

json_object* get_jrpc_resp_array_elem(jsonrpc_context_t* ctx, char* array_key, int index)
{
    // Get the dirent array from the result
    json_object* dobj = jsonrpc_get_resp_obj(ctx, array_key);
    //DPRINTF("Got %s: %s\n", array_key, json_object_to_json_string_ext(dobj, JSON_C_TO_STRING_PLAIN));

    int num_entries = json_object_array_length(dobj);
    if (index > num_entries-1) {
        DPRINTF("Error, requested index (%d) is greater than array length: %d\n",index,num_entries);
        return NULL;
    }

    json_object* jvalue = json_object_array_get_idx(dobj, index);
    //DPRINTF("value[%d]: %s\n",index, json_object_get_string(jvalue));
    return jvalue;
}

uint64_t jsonrpc_get_resp_array_uint64(jsonrpc_context_t* ctx,
                                       char*              array_key,
                                       int                index,
                                       char*              key)
{
    // So that this API can be called for both array and non-array gets (which makes
    // stat_resp_to_struct simpler), we do a non-array get if array_key is null.
    if (array_key == NULL) {
        return jsonrpc_get_resp_uint64(ctx, key);
    }

    // Get the json object for the requested index
    json_object* dir_entry = get_jrpc_resp_array_elem(ctx, array_key, index);

    json_object* obj = NULL;
    if (!json_object_object_get_ex(dir_entry, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return -1;
    }

    uint64_t value = (uint64_t)json_object_get_int64(obj);
//    DPRINTF("Returned %s: %" PRIu64 "\n", key, value);
    return value;
}

int jsonrpc_get_resp_array_int(jsonrpc_context_t* ctx,
                               char*              array_key,
                               int                index,
                               char*              key)
{
    // So that this API can be called for both array and non-array gets (which makes
    // stat_resp_to_struct simpler), we do a non-array get if array_key is null.
    if (array_key == NULL) {
        return jsonrpc_get_resp_int(ctx, key);
    }

    // Get the json object for the requested index
    json_object* dir_entry = get_jrpc_resp_array_elem(ctx, array_key, index);

    json_object* obj = NULL;
    if (!json_object_object_get_ex(dir_entry, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return -1;
    }

    int value = json_object_get_int(obj);
//    DPRINTF("Returned %s: %d\n", key, value);
    return value;
}

const char* jsonrpc_get_resp_array_str(jsonrpc_context_t* ctx,
                                       char*              array_key,
                                       int                index,
                                       char*              key)
{
    // So that this API can be called for both array and non-array gets (which makes
    // stat_resp_to_struct simpler), we do a non-array get if array_key is null.
    if (array_key == NULL) {
        return jsonrpc_get_resp_str(ctx, key);
    }

    // Get the json object for the requested index
    json_object* dir_entry = get_jrpc_resp_array_elem(ctx, array_key, index);

    json_object* obj = NULL;
    if (!json_object_object_get_ex(dir_entry, key, &obj)) {
        DPRINTF("%s field not found in response!\n",key);
        return NULL;
    }

    const char* value = json_object_get_string(obj);
//    DPRINTF("Returned %s: %s\n", key, value);
    return value;
}

// This will get a string value from an array of strings at the specified index.
const char* jsonrpc_get_resp_array_str_value(jsonrpc_context_t* ctx,
                                             char*              array_key,
                                             int                index)
{
    if (array_key == NULL) {
        return NULL;
    }

    // Get the json object for the requested index
    json_object* str_loc = get_jrpc_resp_array_elem(ctx, array_key, index);

    if (str_loc == NULL) {
        DPRINTF("Entry %d not found in %s\n", index, array_key);
        return NULL;
    }

    const char* value = json_object_get_string(str_loc);
//    DPRINTF("Returned %s: %d %s\n", array_key, index, value);
    return value;
}

int jsonrpc_get_resp_array_length(jsonrpc_context_t *ctx,
                                  char              *array_key)
{
    if (array_key == NULL) {
        return 0;
    }

    json_object* dobj = jsonrpc_get_resp_obj(ctx, array_key);

    int num_entries = json_object_array_length(dobj);

    return num_entries;

}