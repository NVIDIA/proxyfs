#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <math.h>
#include <proxyfs.h>
#include <proxyfs_jsonrpc.h>
#include <time_utils.h>

typedef struct {
    int      numStats;
    int64_t  totalUs;
    int      numDurationStats;
    int      maxDurationStats;
    int64_t* durationStatsUs;
} duration_stats_t;

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

        if (stats->numStats == stats->numDurationStats) {
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

//                              10        20        30        40        50        60        70        80        90        100
// Each line is 100 bytes...
char ping_message[] = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
                      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";

size_t defaultPingSize = 64;
size_t maxPingSize     = sizeof(ping_message);

bool use_old = true;
bool verbose = true;

static volatile int keepRunning = 1;

void intHandler(int dummy) {
    printf("Got an interrupt!\n");
    keepRunning = 0;
}

void print_usage(char* func) {
	printf("ProxyFS ping.\n");
	printf("Usage: %s [-n <number of pings to send>|-h|-v]\n", func);
    printf("       -c: specify number of pings to send. Default is to run until killed.\n");
    printf("       -s: specify ping message size. Default is to run until killed.\n");
    printf("       -q: Quiet output.  Nothing is displayed except the summary lines when finished.\n");
    printf("       -h: print this message.\n");
}

int main(int argc, char *argv[])
{
    int num_pings     = 0;
    int ping_msg_size = 0;

	// See if the caller passed in any flags
	int c = 0;
	opterr = 0;
	while ((c = getopt (argc, argv, "hqc:s:")) != -1) {
		switch (c)
		{
			case 'h':
				print_usage(argv[0]);
				return 0;
				break;
			case 'q':
				verbose = false;
				break;
			case 'c':
                // Get number of pings from parameters
                num_pings = atoi(optarg);
                if ((num_pings <= 0) || (num_pings > 10000)) {
                    printf("Error, %d is not a supported number of pings.\n", num_pings);
                    return 0;
                }
                printf("Sending %d ping(s).\n", num_pings);
				break;
			case 's':
                // ping message size from parameters
                ping_msg_size = atoi(optarg);
                if ((ping_msg_size <= 0) || (ping_msg_size > maxPingSize)) {
                    printf("Error, %d is not a supported ping message size (max=%ld).\n", ping_msg_size, maxPingSize);
                    return 0;
                }
                // Shorten our ping message by inserting a null terminator at the appropriate spot
                ping_message[ping_msg_size] = '\0';
                printf("Ping size is %ld bytes.\n", strlen(ping_message));
				break;
			default:
				abort();
		}
	}

    // If ping message size wasn't set, make it the default.
    if (ping_msg_size == 0) {
        defaultPingSize = 64;
        ping_message[defaultPingSize] = '\0';
    }

	// We don't want debug prints from proxyfs code
	proxyfs_unset_verbose();

	// fill in mount handle
	//
	mount_handle_t  handle;
	handle.rpc_handle         = pfs_rpc_open();
	handle.mount_id           = 0;
	handle.root_dir_inode_num = 0;
	handle.mount_options      = 0;

	// Check that we were able to open an RPC connection to the server
	if (handle.rpc_handle == NULL) {
	    printf("Error opening RPC connection to server. Message was not sent.\n");

		return -1;
	}

    // Create somewhere to store stats
    duration_stats_t* durationStats = allocStats(num_pings);

    // Get notified when we are killed
    signal(SIGINT, intHandler);

    int i = 0;
    while (keepRunning) {
        if (verbose) printf("Sending ping %d over JSON RPC (size=%ld)...", i+1, strlen(ping_message));

        // Get timestamp before ping
        struct timespec timeStart;
        clock_gettime(CLOCK_REALTIME, &timeStart);

        int err        = 0;
	    int exp_status = 0;
        if (use_old) {
            err = proxyfs_ping(&handle, ping_message);
        } else {
            printf("Oh no, we don't support new ping yet!\n");
            //err = proxyfs_ping_new(&handle, ping_size);
        }

        // Get timestamp after ping and add to stats
        struct timespec timeStop;
        clock_gettime(CLOCK_REALTIME, &timeStop);
        int64_t durationUs = diffUs(timeStart, timeStop);
        addDurationStat(durationStats, durationUs);

        if (err == exp_status) {
            if (verbose) printf("done. (%ld us)\n", durationUs);
        } else {
            printf("\nERROR, got status=%d from proxyfs_ping, expected %d.\n",err,exp_status);
            keepRunning = false;
            break;
        }

        // Time to be done?
        i++;
        if ((num_pings > 0) && (i >= num_pings)) {
            keepRunning = false;
        }
    }

	pfs_rpc_close(handle.rpc_handle);

    // Print the stats
    char statsMsg[80];
    sprintf(statsMsg, "pings of size %ld bytes", strlen(ping_message));
    printStats(durationStats, statsMsg);
    freeDurationStats(durationStats);

    return 0;
}
