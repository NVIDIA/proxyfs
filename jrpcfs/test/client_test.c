#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <json-c/json.h>

void error(const char *msg)
{
    perror(msg);
    //exit(0);
}

static int sockfd = 0;
static int portno = 12345;
static char* hostname = "localhost";

int sock_open() {
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        error("ERROR opening socket");
		return -1;
	}
    //printf("socket %s:%d opened successfully.\n",hostname,portno);
    server = gethostbyname(hostname);
    if (server == NULL) {
        printf("ERROR, no such host\n");
		return -1;
    }
    //printf("got server for hostname %s.\n",hostname);
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(portno);

    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) {
        //printf("ERROR connecting");
        error("ERROR connecting");
		return -1;
	}

	return 0;
}

int sock_close() {
    close(sockfd);

	return 0;
}

int sock_read(char* buf) {
    int n = read(sockfd,buf,255);
    if (n < 0) {
        error("ERROR reading from socket");
		return -1;
	}
	return 0;
}

int sock_write(const char* buf) {
    int n = write(sockfd,buf,strlen(buf));
    if (n < 0) {
        error("ERROR writing to socket");
		return -1;
	}
	return 0;
}

/*printing the value corresponding to boolean, double, integer and strings*/
void print_json_object(struct json_object *jobj, const char *msg) {
	printf("\n%s: \n", msg);
	printf("---\n%s\n---\n", json_object_to_json_string(jobj));
}

struct json_object * find_something(struct json_object *jobj, const char *key) {
	struct json_object *tmp;

	json_object_object_get_ex(jobj, key, &tmp);

	return tmp;
}

int get_jrpc_id(json_object* jobj)
{
    json_object* obj = find_something(jobj, "id");

    enum json_type type = json_object_get_type(obj);
	if (type != json_type_int) {
		printf("Error, id field is not an int (type=%d)!\n",type);
		return -1;
	}

	return json_object_get_int(obj);
}

int get_jrpc_error(json_object* jobj)
{
    json_object* obj = NULL;
	if (json_object_object_get_ex(jobj, "error", &obj)) {
	    // key was found, as it should be
        enum json_type type = json_object_get_type(obj);
		if (type == json_type_string) {
			// Found an error
            printf("Error value: %s\n", json_object_get_string(obj));
			return -1;
		}
	} else {
		printf("Error field not found in response!\n");
	}

	return 0;
}

json_object* get_jrpc_result(json_object* jobj)
{
    return find_something(jobj, "result");
}

uint64_t get_jrpc_mount_id(json_object* jobj)
{
    json_object* obj = NULL;
	// MountID is inside the result object
    json_object* robj = get_jrpc_result(jobj);
	if (!json_object_object_get_ex(robj, "MountID", &obj)) {
	    // key was not found
		printf("MountID field not found in response!\n");
	    return 0;
	}

	return json_object_get_int64(obj);
}

json_object* build_jrpc_request(int id, char* method)
{
	// Create a new JSON object to populate and return
	json_object* jobj = json_object_new_object();

	// Add the top-level key-value pairs
	json_object_object_add(jobj, "id", json_object_new_int(id));
	json_object_object_add(jobj, "method", json_object_new_string(method));
	json_object_object_add(jobj, "jsonrpc", json_object_new_string("2.0"));

	// Create the params array, consisting of key-value pairs
	json_object* pobj = json_object_new_array();
	json_object* tmp  = json_object_new_object();
	json_object_object_add(tmp, "VolumeName", json_object_new_string("CommonVolume"));
	json_object_object_add(tmp, "MountOptions", json_object_new_int(0));
	json_object_object_add(tmp, "AuthUser", json_object_new_string("balajirao"));
	json_object_array_add(pobj,tmp);

	// Add the params array to the top-level object
	json_object_object_add(jobj,"params",pobj);

	return jobj;
}


int main(int argc, char *argv[])
{
    char buffer[256];
	int id = 1;

	// Open socket
    //printf("Opening socket.\n");
	if (sock_open() < 0) {
		goto done;
	}

	// Build our request header
    json_object* myobj = build_jrpc_request(id, "Server.RpcMount");

	// Send something
	const char* writeBuf = json_object_to_json_string_ext(myobj, JSON_C_TO_STRING_PLAIN);
    printf("Sending data: %s\n",writeBuf);
	if (sock_write(writeBuf) < 0) {
        printf("Error writing to socket.\n");
		goto sock_close_and_done;
	}

	// Read response
    //printf("Reading from socket.\n");
    bzero(buffer,256);
	if (sock_read(buffer) < 0) {
        printf("Error reading from socket.\n");
		goto sock_close_and_done;
	}
    //printf("Read %s\n",buffer);
	json_object* jobj = json_tokener_parse(buffer);
	//printf("response:\n---\n%s\n---\n", json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_SPACED | JSON_C_TO_STRING_PRETTY));

	// Get id from response
    int rsp_id = get_jrpc_id(jobj);
    if (rsp_id != id) {
        printf("Error, expected id=%d, received id=%d", id, rsp_id);
	} else {
	   // printf("Response id = %d\n",rsp_id);
	}

	// Was there an error?
    if (get_jrpc_error(jobj) != 0) {
	    printf("Error was set in response.\n");
	}

	// Try to find the mount ID
    printf("Returned MountID: %lld\n", get_jrpc_mount_id(jobj));

sock_close_and_done:
	// Close socket
    //printf("Closing socket.\n");
	sock_close();

done:
    return 0;
}
