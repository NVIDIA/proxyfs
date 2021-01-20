// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

typedef struct {
    pthread_t      thread;
    int            passes_completed;
} readdir_thread_t;

#define MAX_DIR_ENTRIES         1000
#define MAX_PASSES_PER_THREAD 100000
#define MAX_THREADS               80 // Note: Line-length of status display limited

#define SECONDS_PER_POLL           1

#define DIR_CREATION_MODE     (S_IRWXU | S_IRWXG | S_IRWXO)
#define FILE_CREATION_MODE    (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)

static const char convert_int_modulo_0_thru_9_A_thru_Z[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

char            *dir_path;
size_t           dirent_malloc_size;
pthread_mutex_t  mutex = PTHREAD_MUTEX_INITIALIZER;
int              num_dir_entries;
int              num_passes_in_total;
int              num_passes_per_thread;
int              num_threads;
int              passes_completed_in_total;

void usage(const char *argv_0) {
    fprintf(stderr, "%s <dir-path> <# dir-entries> <# threads> <# passes/thread>\n", argv_0);
    fprintf(stderr, "  <dir-path>:        directory to create (must not exist; parent must exist)\n");
    fprintf(stderr, "  <# dir-entries>:   # of files created in <dir-path>-specified directory\n");
    fprintf(stderr, "  <# threads:        # of threads simultaneously performing readdir_r(3)'s\n");
    fprintf(stderr, "  <# passes/thread>: # of times to read the <dir-path>-specified directory\n");
}

void *readdir_start_routine(void *arg) {
    DIR              *dir;
    struct dirent    *entry;
    int               file_index;
    int               pass_index;
    int               posix_ret;
    readdir_thread_t *readdir_thread = (readdir_thread_t *)arg;
    struct dirent    *result;

    entry = (struct dirent *)malloc(dirent_malloc_size);
    if (NULL == entry) {
        fprintf(stderr, "entry = malloc(dirent_malloc_size) failed: %s\n", strerror(errno));
        exit(1);
    }

    for (pass_index = 0; pass_index < num_passes_per_thread; pass_index++) {
        dir = opendir(dir_path);
        if (NULL == dir) {
            fprintf(stderr, "opendir(%s) failed: %s\n", dir_path, strerror(errno));
            exit(1);
        }

        // Read ".", "..", and all but the last entry
        for (file_index = -1; file_index <= num_dir_entries; file_index++) {
            result = NULL;
            posix_ret = readdir_r(dir, entry, &result);
            if (0 != posix_ret) {
                fprintf(stderr, "readdir_r(%s,,) failed: %s\n", dir_path, strerror(posix_ret));
                exit(1);
            }
            if (NULL == result) { // For all but the last entry
                fprintf(stderr, "readdir_r(%s,,) should have returned non-NULL result\n", dir_path);
                exit(1);
            }
        }

        // Read what should be the last entry
        result = NULL;
        posix_ret = readdir_r(dir, entry, &result);
        if (0 != posix_ret) {
            fprintf(stderr, "readdir_r(%s,,) failed: %s\n", dir_path, strerror(posix_ret));
            exit(1);
        }
        if (NULL != result) { // For the last entry
            fprintf(stderr, "readdir_r(%s,,) should have returned NULL result\n", dir_path);
            exit(1);
        }

        posix_ret = closedir(dir);
        if (0 != posix_ret) {
            fprintf(stderr, "closedir(%s) failed: %s\n", dir_path, strerror(errno));
            exit(1);
        }

        readdir_thread->passes_completed = 1 + pass_index;

        posix_ret = pthread_mutex_lock(&mutex);
        if (0 != posix_ret) {
            fprintf(stderr, "pthread_mutex_lock(&mutex) failed: %s\n", strerror(posix_ret));
            exit(1);
        }

        passes_completed_in_total++;

        posix_ret = pthread_mutex_unlock(&mutex);
        if (0 != posix_ret) {
            fprintf(stderr, "pthread_mutex_unlock(&mutex) failed: %s\n", strerror(posix_ret));
            exit(1);
        }
    }

    free(entry);

    return NULL;
}

int main(int argc, const char * argv[]) {
    DIR              *dir_path_dir;
    char             *dir_path_dirname;
    char             *dir_path_dirname_buf;
    size_t            dirent_d_name_offset;
    int               fd;
    int               file_index;
    char             *file_path;
    size_t            file_path_len;
    int               posix_ret;
    readdir_thread_t *readdir_thread;
    readdir_thread_t *readdir_thread_array;
    struct stat       stat_buf;
    char             *status_string;
    int               thread_index;
    
    // Validate arguments

    if (5 != argc) {
        usage(argv[0]);
        exit(1);
    }

    dir_path = strdup(argv[1]);
    if (NULL == dir_path) {
        fprintf(stderr, "strdup(%s) failed: %s\n", argv[1], strerror(errno));
        exit(1);
    }

    num_dir_entries = atoi(argv[2]);
    num_threads = atoi(argv[3]);
    num_passes_per_thread = atoi(argv[4]);

    dir_path_dirname_buf = strdup(dir_path);
    if (NULL == dir_path_dirname_buf) {
        fprintf(stderr, "strdup(%s) failed: %s\n", dir_path, strerror(errno));
        exit(1);
    }
    dir_path_dirname = dirname(dir_path_dirname_buf);
    
    dir_path_dir = opendir(dir_path_dirname);
    if (NULL == dir_path_dir) {
        fprintf(stderr, "%s must exist\n", dir_path_dirname);
        exit(1);
    } else {
        posix_ret = closedir(dir_path_dir);
        if (0 != posix_ret) {
            fprintf(stderr, "closedir(%s) failed: %s\n", dir_path_dirname, strerror(errno));
            exit(1);
        }
    }

    posix_ret = stat(dir_path, &stat_buf);
    if (0 == posix_ret) {
        fprintf(stderr, "%s must not pre-exist\n", dir_path);
        exit(1);
    } else {
        if (ENOENT != errno) {
            fprintf(stderr, "stat(%s,) failed: %s\n", dir_path, strerror(errno));
            exit(1);
        }
    }

    if ((1 > num_dir_entries) || (MAX_DIR_ENTRIES < num_dir_entries)) {
        fprintf(stderr, "num_dir_entries (%d) must be between 1 and %d (inclusive)\n", num_dir_entries, MAX_DIR_ENTRIES);
        exit(1);
    }
    if ((1 > num_threads) || (MAX_THREADS < num_threads)) {
        fprintf(stderr, "num_threads (%d) must be between 1 and %d (inclusive)\n", num_threads, MAX_THREADS);
        exit(1);
    }
    if ((1 > num_passes_per_thread) || (MAX_PASSES_PER_THREAD < num_passes_per_thread)) {
        fprintf(stderr, "num_passes_per_thread (%d) must be between 1 and %d (inclusive)\n", num_passes_per_thread, MAX_PASSES_PER_THREAD);
        exit(1);
    }

    // Build test directory

    posix_ret = mkdir(dir_path, DIR_CREATION_MODE);
    if (0 != posix_ret) {
        fprintf(stderr, "mkdir(%s,) failed: %s\n", dir_path, strerror(errno));
        exit(1);
    }

    file_path_len = strlen(dir_path) + 1 + 5 + 4; // "<dir_path>/file_XXXX"
    file_path = (char *)malloc(file_path_len + 1);
    if (NULL == file_path) {
        fprintf(stderr, "file_path = malloc(file_path_len + 1) failed: %s\n", strerror(errno));
        exit(1);
    }

    for (file_index = 0; file_index < num_dir_entries; file_index++) {
        posix_ret = sprintf(file_path, "%s/file_%04X", dir_path, file_index);
        if (file_path_len != posix_ret) {
            fprintf(stderr, "sprintf(,,,) returned unexpected length: %d\n", posix_ret);
            exit(1);
        }

        fd = open(file_path, O_WRONLY|O_CREAT|O_EXCL, FILE_CREATION_MODE);
        if (-1 == fd) {
            fprintf(stderr, "open(%s,,) failed: %s\n", file_path, strerror(errno));
            exit(1);
        }

        posix_ret = close(fd);
        if (0 != posix_ret) {
            fprintf(stderr, "close(fd[%s]) failed: %s\n", file_path, strerror(errno));
            exit(1);
        }
    }

    // Start pthreads

    dirent_d_name_offset = offsetof(struct dirent, d_name);
    dirent_malloc_size = dirent_d_name_offset + 5 + 4 + 1; // "file_XXXX"

    num_passes_in_total       = num_threads * num_passes_per_thread;
    passes_completed_in_total = 0;
    
    readdir_thread_array = (readdir_thread_t *)malloc(num_threads * sizeof(readdir_thread_t));
    if (NULL == readdir_thread_array) {
        fprintf(stderr, "readdir_thread_array = malloc(%d * sizeof(readdir_thread_t)) failed: %s\n", num_threads, strerror(errno));
        exit(1);
    }

    for (thread_index = 0; thread_index < num_threads; thread_index++) {
        readdir_thread = &readdir_thread_array[thread_index];

        readdir_thread->passes_completed = 0;

        posix_ret = pthread_create(&readdir_thread->thread, NULL, readdir_start_routine, readdir_thread);
        if (0 != posix_ret) {
            fprintf(stderr, "pthread_create(,,,) failed: %s\n", strerror(posix_ret));
            exit(1);
        }
    }

    // Poll reader_thread's until they are done

    status_string = (char *)malloc(num_threads + 1);
    if (NULL == status_string) {
        fprintf(stderr, "status_string = malloc(num_threads + 1) failed: %s\n", strerror(errno));
        exit(1);
    }
    status_string[num_threads] = '\0';

    for (;;) {
        (void)sleep(SECONDS_PER_POLL);

        for (thread_index = 0; thread_index < num_threads; thread_index++) {
            readdir_thread = &readdir_thread_array[thread_index];
            status_string[thread_index] = convert_int_modulo_0_thru_9_A_thru_Z[readdir_thread->passes_completed % 36];
        }

        printf("%s\n", status_string);

        if (num_passes_in_total == passes_completed_in_total) {
            break;
        }
    }

    // Clean-up
    
    for (thread_index = 0; thread_index < num_threads; thread_index++) {
        readdir_thread = &readdir_thread_array[thread_index];

        posix_ret = pthread_join(readdir_thread->thread, NULL);
        if (0 != posix_ret) {
            fprintf(stderr, "pthread_join(,) failed: %s\n", strerror(posix_ret));
            exit(1);
        }
    }

    for (file_index = 0; file_index < num_dir_entries; file_index++) {
        posix_ret = sprintf(file_path, "%s/file_%04X", dir_path, file_index);
        if (file_path_len != posix_ret) {
            fprintf(stderr, "sprintf(,,,) returned unexpected length: %d\n", posix_ret);
            exit(1);
        }

        posix_ret = unlink(file_path);
        if (0 != posix_ret) {
            fprintf(stderr, "unlink(%s) failed: %s\n", file_path, strerror(errno));
            exit(1);
        }
    }

    posix_ret = rmdir(dir_path);
    if (0 != posix_ret) {
        fprintf(stderr, "rmdir(%s) failed: %s\n", dir_path, strerror(errno));
        exit(1);
    }

    free(status_string);
    free(readdir_thread_array);
    free(file_path);
    free(dir_path_dirname_buf);
    free(dir_path);

    exit(0);
}
