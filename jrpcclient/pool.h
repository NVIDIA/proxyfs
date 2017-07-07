#ifndef __PFS_POOL_H__
#define __PFS_POOL_H__

#include <stdbool.h>

typedef struct sock_info_s {
    int sock_fd;
    struct sock_info_s *next;
} sock_info_t;

typedef struct sock_pool_s {
    char            *server;
    int             port;
    char            *network;
    int             pool_count;
    pthread_mutex_t pool_lock;
    pthread_cond_t  pool_cv;

    int             available_count;
    int             *fd_list;
    sock_info_t     *free_pool;
    sock_info_t     *busy_pool;
} sock_pool_t;

sock_pool_t *sock_pool_create(char *server, int port, int count);
int sock_pool_get(sock_pool_t *pool);
void sock_pool_put(sock_pool_t *pool, int sock_fd);
int sock_pool_select(sock_pool_t *pool, int timeout_in_secs);
int sock_pool_destroy(sock_pool_t *pool, bool force);

#endif // __PFS_POOL_H__
