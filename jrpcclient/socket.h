#ifndef __PFS_SOCKET_H__
#define __PFS_SOCKET_H__

#include <stdio.h>
#include <stdlib.h>
#include "pool.h"

int  sock_open(char* rpc_server, int rpc_port);
void sock_close(int sockfd);
int  sock_read(int sock_read, char** buf, int* error);
int  sock_write(const char* buf);

#define GLOBAL_SOCK_POOL_COUNT 2
extern sock_pool_t *global_sock_pool;
extern int io_sock_fd;

#endif
