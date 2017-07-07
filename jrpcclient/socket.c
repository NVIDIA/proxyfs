#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <errno.h>
#include <json-c/json.h>
#include <pthread.h>
#include "fault_inj.h"
#include "debug.h"
#include "pool.h"

// If errno is set, return that. Otherwise, return -1.
int set_err_return()
{
    int rtnVal = -1;
    if (errno != 0) {
        rtnVal = errno;
    }
    return rtnVal;
}

// Global sockfd, populated on successful sock_open.
sock_pool_t *global_sock_pool = NULL;
int io_sock_fd = -1;

int sock_open(char* rpc_server, int rpc_port)
{
    struct sockaddr_in serv_addr;
    struct sockaddr_in6 serv_addr6;
    struct hostent *server;

    char* hostname = rpc_server;
    int   portno   = rpc_port;
    int   sockfd   = -1;
    int   flag     = 0;

    // Set errno to zero before system calls
    errno = 0;

    // Error injection?
    if ( fail(RPC_CONNECT_FAULT) ) {
        // Fault-inject case
        errno = ECONNREFUSED;
    }

    // Determine socket address family
    server = gethostbyname2(hostname, AF_INET6); // Try IPv6 first
    if (server == NULL) {
        server = gethostbyname2(hostname, AF_INET); // If no IPv6, try IPv4
        if (server == NULL) {
            DPRINTF("ERROR %s, no such host %s\n", strerror(errno), hostname);
            return -1;
        } else {
            DPRINTF("got IPv4 server for hostname %s.\n", hostname);
            bzero((char *) &serv_addr, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
            serv_addr.sin_port = htons(portno);

            // Set errno to zero before system calls
            errno = 0;

            // Create the socket
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) {
                DPRINTF("ERROR %s opening AF_INET socket\n", strerror(errno));
                return -1;
            }

            // Connect to the far end
            if (connect(sockfd,(struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
                DPRINTF("ERROR %s connecting socket\n", strerror(errno));
                return -1;
            }
        }
    } else {
        DPRINTF("got IPv6 server for hostname %s.\n", hostname);
        bzero((char *) &serv_addr6, sizeof(serv_addr6));
        serv_addr6.sin6_flowinfo = 0;
        serv_addr6.sin6_family = AF_INET6;
        bcopy((char *)server->h_addr, (char *)&serv_addr6.sin6_addr.s6_addr, server->h_length);
        serv_addr6.sin6_port = htons(portno);

        // Set errno to zero before system calls
        errno = 0;

        // Create the socket
        sockfd = socket(AF_INET6, SOCK_STREAM, 0);
        if (sockfd < 0) {
            DPRINTF("ERROR %s opening AF_INET6 socket\n", strerror(errno));
            return -1;
        }

        // Connect to the far end
        if (connect(sockfd,(struct sockaddr *) &serv_addr6, sizeof(serv_addr6)) < 0) {
            DPRINTF("ERROR %s connecting socket\n", strerror(errno));
            return -1;
        }
    }

    flag = 1;
    if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int)) < 0) {
        DPRINTF("ERROR %s setting TCP_NODELAY option\n", strerror(errno));
        return -1;
    }

    DPRINTF("socket %s:%d opened successfully.\n",hostname,portno);

    return sockfd;
}

void sock_close(int sockfd)
{
    close(sockfd);
}

// NOTE on buffer sizes for reading from our socket:
//
// NORMAL_REQUEST_SIZE:
// This is the default size for what we read off the socket for "normal" RPC
// requests, i.e. those that do not return a lot of data. Calls like proxyfs_read
// and proxyfs_readdir may return more data than this.
//
// MAX_READ_SIZE:
// Since when we are doing an actual read we tend to be more performance-critical
// than other operations, let's be less memory-efficient for normal requests
// but as fast as possible for reads by always using a buffer big enough
// for a 64k read so that we don't realloc in sock_read on the read path.
#define NORMAL_REQUEST_SIZE   4  * 1024
#define MAX_READ_SIZE         64 * 1024

int alloc_read_buf(char** bufPtr)
{
    int readSize = NORMAL_REQUEST_SIZE + MAX_READ_SIZE * 4/3; // *4/3 is to account for base64 encoding

    // We'll allocate a buffer for our response, and set the read buffer in the context
    *bufPtr = malloc(readSize);
    if (*bufPtr == NULL) {
        PANIC("FATAL: unable to allocate %d bytes for socket read!\n", readSize);
        return -1;
    }

    return readSize;
}

// NOTE: This is the largest socket read buffer we support right now.
int big_buffer_size = 128 * 1024 * 4/3; // *4/3 is to account for base64 encoding

int sock_read(int sockfd, char** bufPtr, int* error)
{
    size_t allBytesRecd  = 0;
    size_t bytesRecd     = 0;
    int    max_read_size = alloc_read_buf(bufPtr);
    char*  buf           = *bufPtr;

    // Set errno to zero to start
    *error = 0;

    while (1) {
        bytesRecd = read(sockfd, buf + allBytesRecd, max_read_size - allBytesRecd);
        if (bytesRecd < 0) {
            DPRINTF("ERROR %s reading from socket\n", strerror(errno));
            *error = errno;
            allBytesRecd = bytesRecd;
            return -1;
        } else if (bytesRecd == 0) {
            DPRINTF("far end disconnected while reading from socket.\n");
            *error = EPIPE;
            allBytesRecd = bytesRecd;
            return -1;
        }

        // otherwise data is good
        allBytesRecd += bytesRecd;

        // Are we done? Check for CR as last character.
        if (buf[allBytesRecd-1] == 0xa) {
            // Read terminates in a CR; we're done.
            DPRINTF("read %ld/%ld bytes from socket; found CR, done. (max=%d).\n",
                    bytesRecd,allBytesRecd, max_read_size);
            break;
        } else {
            DPRINTF("read %ld/%ld bytes from socket; keep trying. (max=%d, last-char=0x%x).\n",
                    bytesRecd,allBytesRecd, max_read_size,buf[allBytesRecd-1]);

            if (allBytesRecd == max_read_size) {
                // We've run out of buffer space but aren't done reading.
                // Let's realloc to a bigger buffer size.
                //
                // XXX TODO: If we end up needing a buffer larger than big_buffer_size,
                //           we are hosed. If that happens, we trigger a panic here.
                //
                // Let's make sure we haven't already reallocated...
                if (max_read_size == big_buffer_size) {
                    // Uh oh, we ran out of space in our biggest buffer.
                    PANIC("FATAL: Ran out of buffer space when reading socket! Bytes read: %d\n", max_read_size);
                    return -1;
                }

                // XXX TODO: can we make this more efficient?
                //
                DPRINTF("Ran out of buffer space at size %d but not done reading. "
                        "Reallocating to bigger size %d.\n", max_read_size, big_buffer_size);
                *bufPtr = realloc(*bufPtr, big_buffer_size);
                DPRINTF("Old bufPtr was %p, new one is %p.\n", buf, *bufPtr);
                buf = *bufPtr;
                max_read_size = big_buffer_size;
            }
            continue;
        }
    }

    // We got the socket in sock_write() and since we are done with it, lets put it back to pool.
    sock_pool_put(global_sock_pool, sockfd);

    // Just in case, make sure the buffer we return is null-terminated.
    buf[allBytesRecd] = 0;

    DPRINTF("returning %ld bytes read, error=%d.\n",allBytesRecd, *error);
    return allBytesRecd;
}

int sock_write(const char* buf) {
    int rtnVal = 0; // success
    int n = 0;

    if (global_sock_pool == NULL) {
        return ENODEV;
    }

    if ( fail(WRITE_BROKEN_PIPE_FAULT) ) {
        errno = EPIPE;
        n = 0;
    } else {
        int sockfd = sock_pool_get(global_sock_pool);
        if (sockfd == -1) {
            return ENODEV;
        }

        DPRINTF("Sending data on socket: %d\n", sockfd);
        n = write(sockfd, buf, strlen(buf));
    }
    if (n <= 0) {
        DPRINTF("ERROR %s writing to socket\n", strerror(errno));
        rtnVal = set_err_return();
    }

    // Note: The socket will be put back to free pool after getting the response in read path.

    return rtnVal;
}
