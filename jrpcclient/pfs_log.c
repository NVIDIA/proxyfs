#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <proxyfs.h>
#include <proxyfs_jsonrpc.h>
#include <fault_inj.h>

void print_usage(char* func) {
	printf("Cause ProxyFS log message.\n");
	printf("Usage: %s -h | \"message to be logged\"\n", func);
}

// XXX TODO: make option handling better
int main(int argc, char *argv[])
{
	int c = 0;
	opterr = 0;
	char* log_message = NULL;

	if (argc != 2) {
		printf("Error, incorrect number of parameters.\n\n");
		print_usage(argv[0]);
		return 0;
	}

	// See if the caller passed in any flags
	while ((c = getopt (argc, argv, "h")) != -1) {
		switch (c)
		{
			case 'h':
				print_usage(argv[0]);
				return 0;
				break;
			default:
				abort();
		}
	}

	// Get log message from parameters
	log_message = argv[1];

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

	// Generate test begin log on proxyfs
	// NOTE: For now, has to be after mount because that's where our socket handle is stored
	int exp_status = 0;
	printf("Sending log \"%s\" over JSON RPC...", log_message);
    int err = proxyfs_log(&handle, log_message);
	if (err == exp_status) {
		printf("done.\n");
	} else {
	    printf("\nERROR, got status=%d from proxyfs_log, expected %d.\n",err,exp_status);
	}

	pfs_rpc_close(handle.rpc_handle);

    return 0;
}
