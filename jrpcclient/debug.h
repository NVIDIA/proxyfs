#ifndef __PFS_DEBUG_H__
#define __PFS_DEBUG_H__

#include <stdio.h>
#include <inttypes.h>

// Defined in proxyfs_api.c. Go there to change the default for debug printfs.
extern int debug_flag;

// Defined in proxyfs_jsonrpc.c. Go there to change the default for list printfs.
extern int list_debug_flag;

#define MAX_PRINT_SIZE  2048

// This is a roll-your-own panic. Sigh.
#define PANIC(fmt, ...) \
    do { printf("PANIC [%p]: " fmt, ((void*)((uint64_t)pthread_self())), ##__VA_ARGS__); fflush(stdout); if (debug_flag>0) abort(); } while (0)

#define DPANIC(fmt, ...) \
    do { if (debug_flag>0) PANIC(fmt, ##__VA_ARGS__); PRINTF(fmt, ##__VA_ARGS__); } while (0)

#define DPRINTF(fmt, ...) \
    do { if (debug_flag>0) printf("  [%p] %s: " fmt, ((void*)((uint64_t)pthread_self())), __FUNCTION__, ##__VA_ARGS__); fflush(stdout); } while (0)

#define PRINTF(fmt, ...) \
    do { printf("  [%p] %s: " fmt, ((void*)((uint64_t)pthread_self())), __FUNCTION__, ##__VA_ARGS__); fflush(stdout); } while (0)

#define LIST_PRINTF(fmt, ...) \
    do { if (list_debug_flag>0) printf("  [%p] %s: " fmt, ((void*)((uint64_t)pthread_self())), __FUNCTION__, ##__VA_ARGS__); fflush(stdout); } while (0)

#endif
