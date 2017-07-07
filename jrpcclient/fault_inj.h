#ifndef __FAULT_INJ_H__
#define __FAULT_INJ_H__

#include <stdio.h>
#include <stdbool.h>

// NOTE: Add new faults above __MAX_FAULT__
#define FOREACH_FAULT(FAULT)            \
        FAULT(READ_DISC_FAULT)          \
        FAULT(WRITE_BROKEN_PIPE_FAULT)  \
        FAULT(BAD_MOUNT_ID)             \
        FAULT(RPC_CONNECT_FAULT)        \
        FAULT(__MAX_FAULT__)

// Generate the fault enum from FOREACH_FAULT above
#define GENERATE_ENUM(ENUM) ENUM,
typedef enum {
    FOREACH_FAULT(GENERATE_ENUM)
} faults_t;

// Returns true if fault is set
bool fail(faults_t fault);

// APIs for setting/clearing faults, to be called from test code
void set_fault(faults_t fault);
void clear_fault(faults_t fault);

// Suppress/enable prints from fault injection code
void disable_fault_prints();
void enable_fault_prints();

// Fault-checking macros
//#define DONT_DO_IF_FAULT(fault, ...) \
//    do { if (fail(fault)) { printf("  %s: " fmt, __FUNCTION__, ##__VA_ARGS__);
//	} while (0)


#endif
