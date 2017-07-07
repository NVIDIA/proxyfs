#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <math.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <debug.h>
#include <time_utils.h>

#define TIME_NANOSECOND    (1)
#define TIME_MICROSECOND   (1000 * TIME_NANOSECOND)
#define TIME_MILLISECOND   (1000 * TIME_MICROSECOND)
#define TIME_SECOND        (1000 * TIME_MILLISECOND)

struct timespec diff(struct timespec start, struct timespec end)
{
	struct timespec temp;
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec  = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = TIME_SECOND+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec  = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	return temp;
}

int64_t diffNs(struct timespec start, struct timespec end)
{
	struct timespec temp = diff(start, end);
	return temp.tv_nsec + TIME_SECOND * temp.tv_sec;
}

int64_t diffUs(struct timespec start, struct timespec end)
{
	struct timespec temp = diff(start, end);
	return (temp.tv_nsec + TIME_SECOND * temp.tv_sec) / TIME_MICROSECOND;
}

struct timespec addNs(struct timespec dest, int64_t ns_to_add)
{
	struct timespec temp;
	if ((dest.tv_nsec + ns_to_add)<0) {
		dest.tv_sec  += 1;
		temp.tv_nsec += ns_to_add - TIME_SECOND;
	} else {
		//temp.tv_sec doesn't change
		temp.tv_nsec += ns_to_add;
	}
	return temp;
}

bool diffIsNegative(struct timespec* start, struct timespec* end)
{
	struct timespec tempDiff;
    tempDiff = diff(*start, *end);
    if ((tempDiff.tv_sec < 0) || (tempDiff.tv_nsec < 0)) {
        return true;
    }
    return false;
}

void reset(struct timespec timer)
{
    timer.tv_sec  = (time_t)-1;
    timer.tv_nsec = 0;
}

void InitStopwatch(stopwatch_t* sw) {
    if (sw == NULL) return;

    clock_gettime(CLOCK_REALTIME, &sw->StartTime); // Set StartTime to now
    reset(sw->StopTime);
    reset(sw->ElapsedTime);
    sw->IsRunning = true;
}

stopwatch_t* NewStopwatch() {
    stopwatch_t* sw = (stopwatch_t*)malloc(sizeof(stopwatch_t));
    InitStopwatch(sw);
    return sw;
}

void DeleteStopwatch(stopwatch_t* sw) {
    if (sw != NULL) {
        free(sw);
    }
}

int64_t Stop(stopwatch_t* sw) {
    clock_gettime(CLOCK_REALTIME, &sw->StopTime); // Set StopTime to now

	// Stopwatch should have been running when stopped, but
	// to avoid making callers do error checking we just
	// don't do calculations if it wasn't.
	if (sw->IsRunning) {
		sw->ElapsedTime = diff(sw->StartTime, sw->StopTime);
		sw->IsRunning = false;
	}
	return sw->ElapsedTime.tv_nsec + sw->ElapsedTime.tv_sec * TIME_SECOND;
}

void Restart(stopwatch_t* sw) {
	// Stopwatch should not be running when restarted, but
	// to avoid making callers do error checking we just
	// don't do anything if it wasn't.
	if (!sw->IsRunning) {
        reset(sw->StopTime);
        reset(sw->ElapsedTime);
        clock_gettime(CLOCK_REALTIME, &sw->StartTime); // Set StartTime to now
		sw->IsRunning = true;
	}
}

int64_t elapsedInNs(struct timespec* start, struct timespec* stop) {
    struct timespec timeElapsed;
	timeElapsed = diff(*start, *stop);
	int64_t elapsed = timeElapsed.tv_nsec + timeElapsed.tv_sec * TIME_SECOND;
    return elapsed;
}

int64_t Elapsed(stopwatch_t* sw) {
	int64_t elapsed = sw->ElapsedTime.tv_nsec + sw->ElapsedTime.tv_sec * TIME_SECOND;

	if (sw->IsRunning) {
        // Otherwise still running, return time so far
        struct timespec timeNow;
        clock_gettime(CLOCK_REALTIME, &timeNow);
	    elapsed = elapsedInNs(&sw->StartTime, &timeNow);
	}
	// else not running, return elapsed time when stopped

    if (elapsed < 0) {
        PANIC("*** Negative elapsed time=%ld! StopTime=%ld StartTime=%ld\n",
              elapsed, sw->StartTime.tv_nsec, sw->StopTime.tv_nsec);
        return -1;
    }

	return elapsed;
}

int64_t ElapsedSec(stopwatch_t* sw) {
	return (Elapsed(sw) / TIME_SECOND);
}

int64_t ElapsedMs(stopwatch_t* sw) {
	return (Elapsed(sw) / TIME_MILLISECOND);
}

int64_t ElapsedUs(stopwatch_t* sw) {
	return (Elapsed(sw) / TIME_MICROSECOND);
}

int64_t ElapsedNs(stopwatch_t* sw) {
	return (Elapsed(sw) / TIME_NANOSECOND);
}

// Create a op-to-string array
#define GENERATE_OP_STRING(STRING) #STRING,
static const char *OP_STRING[] = {
    FOREACH_OP(GENERATE_OP_STRING)
};

// Create a event-to-string array
#define GENERATE_EVENT_STRING(STRING) #STRING,
static const char *EVENT_STRING[] = {
    FOREACH_EVENT(GENERATE_EVENT_STRING)
};

profiler_t* NewProfiler(time_operations_t op)
{
    profiler_t* profiler = (profiler_t*)malloc(sizeof(profiler_t));

    // Init stuff
    profiler->op        = op;
    profiler->numEvents = 0;

    // Start the stopwatch
    InitStopwatch(&profiler->timer);

    return profiler;
}

void DeleteProfiler(profiler_t* profiler)
{
    if (profiler != NULL) {
        free(profiler);
    }
}

void AddProfilerEventTime(profiler_t* profiler, time_event_type_t event, struct timespec eventTime)
{
    if (profiler == NULL) return;
    if (profiler->numEvents >= MAX_TIME_EVENTS) {
        PANIC("Number of events is too big! numEvents=%d max=%d\n", profiler->numEvents, MAX_TIME_EVENTS);
        return;
    }

    // Set the event
    profiler->events[profiler->numEvents].event = event;
    profiler->events[profiler->numEvents].timestamp.tv_sec = eventTime.tv_sec;
    profiler->events[profiler->numEvents].timestamp.tv_nsec = eventTime.tv_nsec;
    profiler->numEvents++;
}

void AddProfilerEvent(profiler_t* profiler, time_event_type_t event)
{
    if (profiler == NULL) return;

    // Get "now" time and set the event
    struct timespec timeNow;
    clock_gettime(CLOCK_REALTIME, &timeNow);
    AddProfilerEventTime(profiler, event, timeNow);
}

void AddProfilerEvents(profiler_t* dest_profiler, profiler_t* src_profiler)
{
    int           i         = 0;
    time_event_t* currEvPtr = NULL;

    for (i=0; i < src_profiler->numEvents; i++) {
        currEvPtr  = &src_profiler->events[i];

        AddProfilerEventTime(dest_profiler, currEvPtr->event, currEvPtr->timestamp);
    }
}

void StopProfiler(profiler_t* profiler)
{
    if (profiler == NULL) return;

    Stop(&profiler->timer);
}

bool doDumpPrints = false;

void enableDumpPrints()
{
    doDumpPrints = true;
}

void disableDumpPrints()
{
    doDumpPrints = false;
}

void DumpProfiler(profiler_t* profiler)
{
    if (profiler == NULL) return;
    if (!doDumpPrints) return;

    int              i          = 0;
    time_event_t*    currEvPtr  = NULL;
    time_event_t*    prevEvPtr  = NULL;
    struct timespec* prevEvTime = &profiler->timer.StartTime;
    struct timespec* currEvTime = NULL;
    bool             verbose    = false;
    bool             debug      = false;

    printf("Profiler ");
    if (verbose) printf("%p ", profiler);
    printf("for op: %s ", OP_STRING[profiler->op]);
    if (debug) printf(" (StartTime.tv_sec = %ld, tv_nsec = %ld)", profiler->timer.StartTime.tv_sec,
                      profiler->timer.StartTime.tv_nsec);
    if (verbose) printf("\n");

    // Reorder if needed.
    // XXX TODO: Not particularly efficient...
    int num_tries = 0;
    for (num_tries=0; num_tries < 10; num_tries++) {
        for (i=0; i < profiler->numEvents; i++) {
            currEvPtr  = &profiler->events[i];
            currEvTime = &currEvPtr->timestamp;

            if (i > 0) {
                if (diffIsNegative(prevEvTime, currEvTime)) {
                    bool debug_swap = false;
                    if (debug_swap) {
                        printf("\nSwapping (event=%s currEvTime.tv_sec = %ld, tv_nsec = %ld) "
                               "and (event=%s prevEvTime.tv_sec = %ld, tv_nsec = %ld)\n",
                               EVENT_STRING[currEvPtr->event], currEvTime->tv_sec, currEvTime->tv_nsec,
                               EVENT_STRING[prevEvPtr->event], prevEvTime->tv_sec, prevEvTime->tv_nsec);
                    }

                    struct timespec   savedTime  = *currEvTime;
                    time_event_type_t savedEvent = currEvPtr->event;
                    *currEvTime      = *prevEvTime;
                    currEvPtr->event = prevEvPtr->event;
                    *prevEvTime      = savedTime;
                    prevEvPtr->event = savedEvent;

                    if (debug_swap) {
                        printf("\nNow      (event=%s currEvTime.tv_sec = %ld, tv_nsec = %ld) "
                               "and (event=%s prevEvTime.tv_sec = %ld, tv_nsec = %ld)\n",
                               EVENT_STRING[currEvPtr->event], currEvTime->tv_sec, currEvTime->tv_nsec,
                               EVENT_STRING[prevEvPtr->event], prevEvTime->tv_sec, prevEvTime->tv_nsec);
                    }
                }
            }
            prevEvPtr  = currEvPtr;
            prevEvTime = currEvTime;
        }
    }

    prevEvTime = &profiler->timer.StartTime;
    prevEvPtr  = NULL;
    for (i=0; i < profiler->numEvents; i++) {
        currEvPtr  = &profiler->events[i];
        currEvTime = &currEvPtr->timestamp;

        if (verbose) printf("  ");

        if (i > 0) printf(",");

        printf(" event %s + %ld us", EVENT_STRING[currEvPtr->event],
               elapsedInNs(prevEvTime, currEvTime) / TIME_MICROSECOND);

        if (debug) printf(" (currEvTime.tv_sec = %ld, tv_nsec = %ld)",
                          currEvTime->tv_sec, currEvTime->tv_nsec);
        if (debug && verbose) printf(" (prevEvTime.tv_sec = %ld, tv_nsec = %ld)",
                                     prevEvTime->tv_sec, prevEvTime->tv_nsec);
        if (verbose) printf("\n");

        prevEvTime = currEvTime;
    }

    if (verbose) printf("  ");
    printf(" total elapsedTime: %ld us", ElapsedUs(&profiler->timer));
    printf("\n"); fflush(stdout);
}

int maxStats = 8*1024;

duration_stats_t* allocStats(int numStats) {

    duration_stats_t* stats = malloc(sizeof(duration_stats_t));
    stats->numStats         = 0;
    stats->totalUs          = 0;
    stats->numDurationStats = 0;
    stats->durationStatsUs  = NULL;
    stats->maxDurationStats = 0;

    if (numStats > 0) {
        stats->maxDurationStats = numStats;
    } else {
        stats->maxDurationStats = maxStats;
    }
    stats->durationStatsUs = malloc(sizeof(int64_t) * stats->maxDurationStats);
    return stats;
}

void freeDurationStats(duration_stats_t* stats) {
    if (stats == NULL) return;

    if (stats->durationStatsUs != NULL) {
        free(stats->durationStatsUs);
    }
    free(stats);
}

void addDurationStat(duration_stats_t* stats, int64_t durationUs) {
    if (stats == NULL) return;

    stats->totalUs += durationUs;

    // Add to stats, if we have room
    if (stats->numDurationStats < stats->maxDurationStats) {
        stats->durationStatsUs[stats->numDurationStats] = durationUs;
        stats->numDurationStats++;
    }
    stats->numStats++;
}

// Print some stats
void printStats(duration_stats_t* stats, char* op_name) {
    if (stats == NULL) return;

    if (stats->numStats > 0) {
        printf("Total duration for %d %s: %ld us, average: %ld us",
               stats->numStats, op_name, stats->totalUs, stats->totalUs/stats->numStats);

        // Don't print std deviation if we gathered more stats than we had room
        // to save in detail, or if there was just one stat.
        if ((stats->numStats == stats->numDurationStats) && (stats->numStats > 1)) {
            float   stdDeviation = 0.0;
            float   allDurations = (float)stats->totalUs;
            int64_t deviationsUs = 0;
            int64_t avgDurationUs = stats->totalUs/stats->numDurationStats;

            int i = 0;
            for (i=0; i<stats->numDurationStats; i++) {
                stdDeviation += pow(stats->durationStatsUs[i] - avgDurationUs, 2);
            }

            printf(", standard deviation: %.02f us.", sqrt(stdDeviation/stats->numStats));
        }
        // Else we ran out of space to store duration stats. Don't do the standard deviation part.
        printf("\n");
    }
}

