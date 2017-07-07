// Worker threads to handle aio requests from file server. Each worker will do synchronous request to 
// proxyfs file server. That means the max outstanding concurrent request will be equal to thread pool size.

// API:
// int io_workers_start(char *server, int port, int count);
// int io_workers_stop();
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/queue.h>

#include "socket.h"
#include "debug.h"
#include "pool.h"
#include "proxyfs.h"

typedef struct io_worker_s {
    int num_ops_started;
    int num_ops_finished;
} io_worker_t;

typedef struct io_worker_config_s {
    char *server;
    int  port;
    int  worker_count;

    pthread_mutex_t request_queue_lock;
    pthread_cond_t  request_queue_cv;
    TAILQ_HEAD(, proxyfs_io_request_t) request_queue;

    io_worker_t *worker_pool;
} io_worker_config_t;


io_worker_config_t *worker_config = NULL;

void *io_worker(void *arg);

// Lock for max concurrent workers tracking
pthread_mutex_t concurrent_worker_lock = PTHREAD_MUTEX_INITIALIZER;
int num_conc_workers = 0;
int hwm_conc_workers = 0;

int times_inc = 0;
int times_dec = 0;
int times_enter[128] = {0};
int times_exit[128]  = {0};

int64_t         concDurationUs[128] = {0};
struct timespec concStartTime[128];
struct timespec zeroTime = (struct timespec){ 0 };

bool timeIsZero(struct timespec theTime)
{
    return ((theTime.tv_sec == zeroTime.tv_sec) && (theTime.tv_nsec == zeroTime.tv_nsec));
}

void enterLevel(int level, struct timespec timeNow)
{
    times_enter[level]++;

    // Time how long we are at this level; start the clock
    concStartTime[level] = timeNow;
}

void exitLevel(int level, struct timespec timeNow)
{
    times_exit[level]++;

    if (!timeIsZero(concStartTime[level])) {
        concDurationUs[level] += diffUs(concStartTime[level], timeNow);
    }
}

// keep track of how long we spend running at each number of concurrent workers > 0
//
void inc_running_worker()
{
    pthread_mutex_lock(&concurrent_worker_lock);
    times_inc++;

    struct timespec timeNow;
    clock_gettime(CLOCK_REALTIME, &timeNow);

    // Record how long we spent in the previous level
    exitLevel(num_conc_workers, timeNow);

    num_conc_workers++;

    // Time how long we are at this level; start the clock
    enterLevel(num_conc_workers, timeNow);

    if (num_conc_workers > hwm_conc_workers) {
        hwm_conc_workers = num_conc_workers;
    }
    pthread_mutex_unlock(&concurrent_worker_lock);
}

void dec_running_worker()
{
    pthread_mutex_lock(&concurrent_worker_lock);
    times_dec++;

    struct timespec timeNow;
    clock_gettime(CLOCK_REALTIME, &timeNow);

    // Record how long we spent in the previous level
    exitLevel(num_conc_workers, timeNow);

    num_conc_workers--;

    // Time how long we are at this level; start the clock
    enterLevel(num_conc_workers, timeNow);

    pthread_mutex_unlock(&concurrent_worker_lock);
}

bool debug_concurrency = false;

void dump_running_workers()
{
    if (!debug_concurrency) return;

    int     i           = 0;
    int64_t timeMs      = 0;
    int64_t totalTimeMs = 0;

    PRINTF("running_workers: %d, max running_workers: %d\n", num_conc_workers, hwm_conc_workers);
    if (worker_config == NULL) return;

    for (i = 0; i < worker_config->worker_count; i++) {
        if (concDurationUs[i] > 0) {
            timeMs       = concDurationUs[i]/1000;
            totalTimeMs += i * timeMs;
            PRINTF("  %d workers: %ld ms\n", i, timeMs);
        }
    }
    PRINTF("  total worker-thread runtime: %ld ms\n", totalTimeMs);

    PRINTF("  times inc called: %d dec called: %d\n", times_inc, times_dec);
    for (i = 0; i < worker_config->worker_count; i++) {
        if (times_enter[i] > 0) {
            PRINTF("  level %d enter: %d exit %d\n", i, times_enter[i], times_exit[i]);
        }
    }

#if 0
    for (i = 0; i < worker_config->worker_count; i++) {
        io_worker_t* worker = &worker_config->worker_pool[i];

        if (worker->num_ops_started == worker->num_ops_finished) {
            PRINTF("  worker %d, num_ops_handled %d\n", i, worker->num_ops_started);
        } else {
            PRINTF("  worker %d, num_ops_started %d num_ops_finished %d\n", i,
                   worker->num_ops_started, worker->num_ops_finished);
        }
    }
#endif
}

// Return an array of pipe write file descriptors: The worker pool will be blocked on reading a request address from the caller.
int io_workers_start(char *server, int port, int count)
{
    // Note this needs to be done holding a lock, for now we are assuming it is okay to do it in a single thread:
    if (worker_config != NULL) {
        return 0; // already initialized..
    }

    worker_config = (io_worker_config_t *)malloc(sizeof(io_worker_config_t));
    if (worker_config == NULL) {
        return ENOMEM;
    }

    worker_config->worker_pool = (io_worker_t *)malloc(sizeof(io_worker_t) * count);
    if (worker_config->worker_pool == NULL) {
        free(worker_config);
        return ENOMEM;
    }
    bzero(worker_config->worker_pool, sizeof(io_worker_t) * count);

    worker_config->server = strdup(server);
    worker_config->port = port;
    worker_config->worker_count = count;

    pthread_mutex_init(&worker_config->request_queue_lock, NULL);
    pthread_cond_init(&worker_config->request_queue_cv, NULL);
    TAILQ_INIT(&worker_config->request_queue);

    int i;
    for (i = 0; i < count; i++) {

        concDurationUs[i] = 0;

        pthread_t worker_thread;
        int ret = pthread_create(&worker_thread, NULL, &io_worker, &worker_config->worker_pool[i]);
        if (ret != 0) {
            DPRINTF("Failed to create io worker thread #%d: error: %d\n", i, ret);
            free(worker_config->worker_pool);
            free(worker_config);
            return; // TODO cleanup
        }

        ret = pthread_detach(worker_thread);
        if (ret != 0) {
            DPRINTF("Failed to detach the io worker thread #%d: error: %d\n", i, ret);
            free(worker_config->worker_pool);
            free(worker_config);
            return; // TODO cleanup
        }
    }

    return 0;
}

void *io_worker(void *arg)
{
    io_worker_t *worker = (io_worker_t *)arg;

    // Init number of ops handled by this worker to zero
    worker->num_ops_started  = 0;
    worker->num_ops_finished = 0;

    int sock_fd = -1;
    while (1) {
        pthread_mutex_lock(&worker_config->request_queue_lock);
        while (TAILQ_EMPTY(&worker_config->request_queue)) {
           pthread_cond_wait(&worker_config->request_queue_cv, &worker_config->request_queue_lock);
        }
        worker->num_ops_started++;
        inc_running_worker();

        proxyfs_io_request_t *req = TAILQ_FIRST(&worker_config->request_queue);
        TAILQ_REMOVE(&worker_config->request_queue, req, request_queue_entry);
        pthread_mutex_unlock(&worker_config->request_queue_lock);

        if (sock_fd < 0) {
            sock_fd = sock_open(worker_config->server, worker_config->port);
            if (sock_fd < 0) {
                DPRINTF("Failed to open the socket, exiting ..\n");
                // io should fail:
                req->error = EIO;
                goto callback;
            }
        }

        int ret = 0;
        switch (req->op) {
        case IO_READ: ret = proxyfs_read_req(req, sock_fd);
                 break;
        case IO_WRITE: ret = proxyfs_write_req(req, sock_fd);
                 break;
        default: req->error = EINVAL;
        }

        if (ret != 0) {
            DPRINTF("Socket communication to proxyfs server failed\n");
            sock_close(sock_fd);
            sock_fd = -1;
        }

callback:
        req->done_cb(req);
        worker->num_ops_finished++;
        dec_running_worker();
    }
}

int schedule_io_work(proxyfs_io_request_t *req)
{
    pthread_mutex_lock(&worker_config->request_queue_lock);
    TAILQ_INSERT_TAIL(&worker_config->request_queue, req, request_queue_entry);
    pthread_cond_signal(&worker_config->request_queue_cv);
    pthread_mutex_unlock(&worker_config->request_queue_lock);

    return 0;
}
