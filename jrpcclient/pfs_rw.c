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
#include <debug.h>

bool use_old_port = true;   // Whether to use old or new RPC server port
bool verbose      = false;  // Whether to print line-by-line detail
bool debug        = false;  // Whether to print jrpcclient debug info
bool perf_prints  = false;  // Whether to emit jrpcclient perf profiling prints
bool use_randfile = false;  // Whether to use write data from randfile (emulates test suite)

char reads[]     = "reads";
char writes[]    = "writes";
char read_api[]  = "proxyfs_read";
char write_api[] = "proxyfs_write";

static volatile int keepRunning = 1;

void intHandler(int dummy) {
    printf("Got an interrupt!\n");
    keepRunning = 0;
}

// XXX TODO: hmmm, how to make sure there is something there to read?
//           perhaps just have caller provide filename and just fail if it isn't
//           there or there is nothing to read/write?
//

void print_usage(char* func) {
	printf("ProxyFS read/write.\n");
	printf("Usage: %s {-r <filename> | -w  <filename>} -c <number of ops to send> [|-s <msg size>|-f|-h|-v|-p]\n", func);
    printf("       -r: specify filename to perform reads.\n");
    printf("       -w: specify filename to perform writes.\n");
    printf("       -c: specify number of reads/writes to send.\n");
    printf("       -s: specify read/write size.\n");
    printf("       -o: specify read/write offset.\n");
    printf("       -f: use new rpc server.\n");
    printf("       -v: verbose output.  Otherwise nothing is displayed except the summary lines when finished.\n");
    printf("       -p: enable perf profiling prints to stdout. Can affect performance.\n");
    printf("       -h: print this message.\n");
}

int main(int argc, char *argv[])
{
    int      err        = 0;
    int      num_ops    = 0;
    int      io_size    = 0;
    int      tmp_offset = 0;
    uint8_t* io_buffer  = NULL;
    bool     do_reads   = false;
    bool     do_writes  = false;
    char*    filename   = NULL;
    char*    op_string  = NULL;
    char*    api_string = NULL;
    uint64_t offset     = 0;

	// See if the caller passed in any flags
	int c = 0;
	opterr = 0;
	while ((c = getopt (argc, argv, "hr:w:c:s:fdvpo:")) != -1) {
		switch (c)
		{
			case 'h':
				print_usage(argv[0]);
				return 0;
				break;
			case 'r':
                if (do_writes) {
                    printf("Error, Both -r and -w set: must choose either reads or writes.\n");
                    return 0;
                }
                filename = optarg;
                if (strlen(filename) == 0) {
                    printf("Error, file name must be provided.\n");
                    return 0;
                }
				do_reads = true;
				break;
			case 'w':
                if (do_reads) {
                    printf("Error, both -r and -w set: must choose either reads or writes.\n");
                    return 0;
                }
                filename = optarg;
                if (strlen(filename) == 0) {
                    printf("Error, file name must be provided.\n");
                    return 0;
                }
				do_writes = true;
				break;
			case 'v':
				verbose = true;
				break;
			case 'p':
				perf_prints = true;
				break;
			case 'd':
				debug = true;
				break;
			case 'f':
				use_old_port = false;
				break;
			case 'c':
                // Get number of ops from parameters
                num_ops = atoi(optarg);
                if ((num_ops <= 0) || (num_ops > 10000)) {
                    printf("Error, %d is not a supported number of ops.\n", num_ops);
                    return 0;
                }
				break;
			case 's':
                // message size from parameters
                io_size = atoi(optarg);
                if (io_size <= 0) {
                    printf("Error, %d is not a valid message size.\n", io_size);
                    return 0;
                }
                // Allocate a buffer for the write message/read buffer
                io_buffer = (uint8_t*)malloc(io_size);
				break;
			case 'o':
                // Get offset from parameters
                tmp_offset = atoi(optarg);
                if (tmp_offset < 0) {
                    printf("Error, %d is an invalid offset.\n", tmp_offset);
                    return 0;
                }
                offset = (uint64_t)tmp_offset;
				break;
            case '?':
                // Option expected an argument but we didn't get one.
                if ((optopt == 'r') || (optopt == 'w')) {
                    printf("Error, file name must be provided.\n");
                } else if (optopt == 'c') {
                    printf("Error, number of ops must be provided.\n");
                } else if (optopt == 's') {
                    printf("Error, message size must be provided.\n");
                }
                return 1;
			default:
                printf("Unknown option `-%c`.\n", optopt);
				return 0;
                break;
		}
	}

    // If num ops not set, bail
    if (num_ops <= 0) {
        printf("Error, number of ops must be specified.\n");
        return 1;
    }

    // If ping message size wasn't set, bail
    if (io_size == 0) {
        printf("Error, read/write size must be specified.\n");
        return 1;
    }

    // Make sure either read or write was chosen
    if (!do_reads && !do_writes) {
        printf("Error, neither -r nor -w are set: must choose either reads or writes.\n");
        return 0;
    } else if (do_reads) {
        // Set helper strings
        op_string  = reads;
        api_string = read_api;
    } else if (do_writes) {
        // Set helper strings
        op_string  = writes;
        api_string = write_api;

        // If write, init the data in the buffer
        if (use_randfile) {
            // Read data from file
            FILE* fp = fopen("./randfile", "r");
            if (fp == NULL) {
                printf("Error opening randfile, errno=%s\n",strerror(errno));
                return -1;
            }
            // Read io_size bytes from randfile
            size_t readSize = fread(io_buffer, 1, io_size, fp);
            if (readSize != io_size) {
                printf("Error reading randfile, got %zu bytes, expected %u\n", readSize, io_size);
                fclose(fp);
                return -1;
            }
        } else {
            memset(io_buffer, 0x00, io_size);
        }
    }

    // Print out "helpful" stuff for user
    if (use_old_port) {
        printf("Sending %d OLD %s to file %s offset %ld.\n", num_ops, op_string, filename, offset);
        proxyfs_unset_rw_fastpath();
    } else {
        printf("Sending %d NEW %s to file %s offset %ld.\n", num_ops, op_string, filename, offset);
        proxyfs_set_rw_fastpath();
    }

	// We generally don't want debug prints from proxyfs code
	if (debug) {
	    proxyfs_set_verbose();
    } else {
	    proxyfs_unset_verbose();
    }

	// We generally don't want perf prints from proxyfs code
	if (perf_prints) {
	    enableDumpPrints();
    } else {
	    disableDumpPrints();
    }

	// get mount handle outside the loop, so that the cost of spinning up the thread(s)
    // is done before the ops
    char volName[] = "CommonVolume";
    char user[]    = "su";
	mount_handle_t* handle;
    err = proxyfs_mount(volName, 0, user, &handle);
	if (err != 0) {
	    printf("Error opening RPC connection to server. Message was not sent.\n");
		return -1;
	}

    // Get the inode for the file
    uint64_t file_inode = 0;
    err = proxyfs_lookup_path(handle, filename, &file_inode);
	if (err != 0) {
        // File doesn't exist. If this is a read, fail now.
        if (do_reads) {
	        printf("Error, lookup of file \"%s\" failed, errno=%d.\n", filename, err);
		    return -1;
        }
        // Else we should create the file now.
        err = proxyfs_create_path(handle, filename, 1, 1, 0774, &file_inode);
        if (err != 0) {
            printf("Error, file \"%s\" did not exist and could not create it, errno=%d.\n", filename, err);
            return -1;
        }
	}

    // Create somewhere to store stats
    duration_stats_t* durationStats = allocStats(num_ops);

    // Get notified when we are killed
    signal(SIGINT, intHandler);

    int i = 0;
    while (keepRunning) {
        if (verbose) printf("Sending %s %d over JSON RPC (size=%d)...", op_string, i+1, io_size);

        // Get timestamp before ping
        struct timespec timeStart;
        clock_gettime(CLOCK_REALTIME, &timeStart);

        // XXX TODO: Can't kill with ctrl-c if stuck waiting for a response...

        // XXX TODO: For now, always read/write from/to the same offset. Make it walk later.
        //

        uint64_t expectedBytes = 0;
        if (do_reads) {
            err = proxyfs_read(handle, file_inode, offset, io_size, io_buffer, io_size, &expectedBytes);
        } else if (do_writes) {
            err = proxyfs_write(handle, file_inode, offset, (uint8_t*)io_buffer, io_size, &expectedBytes);
        } else {
            PANIC("Neither read nor write is set!");
            err = EINVAL;
        }

        // Get timestamp after ping and add to stats
        struct timespec timeStop;
        clock_gettime(CLOCK_REALTIME, &timeStop);
        int64_t durationUs = diffUs(timeStart, timeStop);
        addDurationStat(durationStats, durationUs);

        if (err != 0) {
            printf("\nERROR, %s returned error=%d. Aborting test.\n", api_string, err);
            keepRunning = false;
            break;
        } else if (expectedBytes != io_size) {
            printf("\nERROR, %s returned %ld bytes read/written, expected %d. Aborting test.\n",
                   api_string, expectedBytes, io_size);
            keepRunning = false;
            break;
        } else {
            // success, by process of elimination
            if (verbose) printf("done. (%ld us)\n", durationUs);
        }

        // Time to be done?
        i++;
        if ((num_ops > 0) && (i >= num_ops)) {
            keepRunning = false;
        }
    }

done:
    // Unmount/close down
    err = proxyfs_unmount(handle);

    // Print the stats
    char statsMsg[80];
    sprintf(statsMsg, "ops of size %d bytes", io_size);
    printStats(durationStats, statsMsg);
    freeDurationStats(durationStats);

    // Free write message, if we allocated one
    if (io_buffer != NULL) {
        free(io_buffer);
    }

    return 0;
}
