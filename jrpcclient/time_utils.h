#ifndef __TIME_UTILS_H__
#define __TIME_UTILS_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>

typedef struct {
	struct timespec StartTime;
	struct timespec StopTime;
	struct timespec ElapsedTime;
	bool            IsRunning;
} stopwatch_t;

stopwatch_t* NewStopwatch();
void DeleteStopwatch(stopwatch_t* sw);
int64_t Stop(stopwatch_t* sw);
void Restart(stopwatch_t* sw);
int64_t Elapsed(stopwatch_t* sw);
int64_t ElapsedSec(stopwatch_t* sw);
int64_t ElapsedMs(stopwatch_t* sw);
int64_t ElapsedUs(stopwatch_t* sw);
int64_t ElapsedNs(stopwatch_t* sw);

// NOTE: Add new operations above __MAX_OP__
#define FOREACH_OP(OP)        \
        OP(READ)              \
        OP(WRITE)             \
        OP(FLUSH)             \
        OP(SOCK_RECEIVE)      \
        OP(__MAX_OP__)

// Generate the op enum from FOREACH_OP above
#define GENERATE_OP_ENUM(ENUM) ENUM,
typedef enum {
    FOREACH_OP(GENERATE_OP_ENUM)
} time_operations_t;


// NOTE: Add new events above __MAX_EVENT__
#define FOREACH_EVENT(EVENT)            \
        EVENT(BEFORE_BASE64_ENCODE)     \
        EVENT(AFTER_BASE64_ENCODE)      \
        EVENT(BEFORE_BASE64_DECODE)     \
        EVENT(AFTER_BASE64_DECODE)      \
        EVENT(BEFORE_RPC_CALL)          \
        EVENT(AFTER_RPC)                \
        EVENT(BEFORE_RPC_SEND)          \
        EVENT(RPC_SEND_TIMESTAMP)       \
        EVENT(RPC_SEND_AFTER_LOCK)      \
        EVENT(RPC_SEND_AFTER_JSON)      \
        EVENT(RPC_SEND_AFTER_SOCK_WRITE)\
        EVENT(AFTER_RPC_SEND)           \
        EVENT(BEFORE_RPC_RX)            \
        EVENT(AFTER_RPC_RX)             \
        EVENT(RPC_RESP_SEND_TIME)       \
        EVENT(RPC_REQ_DELIVERY_TIME)    \
        EVENT(BEFORE_SOCK_READ)         \
        EVENT(AFTER_SOCK_READ)          \
        EVENT(AFTER_GET_RESPONSES)      \
        EVENT(AFTER_JSON_RESPONSES)     \
        EVENT(AFTER_COPY_RESPONSES)     \
        EVENT(AFTER_RESPONSE_CALLBACKS) \
        EVENT(AFTER_SEND_FIELDS)        \
        EVENT(AFTER_SEND_DATA)          \
        EVENT(AFTER_READ_ERROR)         \
        EVENT(AFTER_READ_SIZE)          \
        EVENT(AFTER_READ_DATA)          \
        EVENT(__MAX_EVENT__)

// Generate the op enum from FOREACH_EVENT above
#define GENERATE_EVENT_ENUM(ENUM) ENUM,
typedef enum {
    FOREACH_EVENT(GENERATE_EVENT_ENUM)
} time_event_type_t;


typedef struct {
    time_event_type_t event;
	struct timespec   timestamp;
} time_event_t;

#define MAX_TIME_EVENTS 100

typedef struct {
    time_operations_t op;
    stopwatch_t       timer;
    time_event_t      events[MAX_TIME_EVENTS];
    int               numEvents;
} profiler_t;

// Create and start profiler for the specified operation
profiler_t* NewProfiler(time_operations_t op);

// Clean up profile
void DeleteProfiler(profiler_t* profiler);

// Stop the profiler
void StopProfiler(profiler_t* profiler);

// Add the specified time event to the profiler at time "now"
void AddProfilerEvent(profiler_t* profiler, time_event_type_t event);

// Add the specified time event to the profiler at time "now"
void AddProfilerEventTime(profiler_t* profiler, time_event_type_t event, struct timespec eventTime);

// Add events from one profiler to another
void AddProfilerEvents(profiler_t* dest_profiler, profiler_t* src_profiler);

// Dump profiler info to stdout, if enabled
void DumpProfiler(profiler_t* profiler);

// APIs to control whether profiler info is dumped to stdout.
// Default is to not print to stdout.
void enableDumpPrints();
void disableDumpPrints();

struct timespec addNs(struct timespec dest, int64_t ns_to_add);

typedef struct {
    int      numStats;
    int64_t  totalUs;
    int      numDurationStats;
    int      maxDurationStats;
    int64_t* durationStatsUs;
} duration_stats_t;

// Allocate/free duration stats data structure
duration_stats_t* allocStats(int numStats);
void freeDurationStats(duration_stats_t* stats);

// Add a duration to the overall stats
void addDurationStat(duration_stats_t* stats, int64_t durationUs);

// Print the stats summary to stdout
void printStats(duration_stats_t* stats, char* op_name);



#endif
