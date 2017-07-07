#include <stdio.h>
#include <stdlib.h>
#include "fault_inj.h"

// Create array of faults to be set/cleared
#define GENERATE_BOOL(FAULT) false,
static bool faults[] = {
    FOREACH_FAULT(GENERATE_BOOL)
};

// Create a fault-to-string array
#define GENERATE_STRING(STRING) #STRING,
static const char *FAULT_STRING[] = {
    FOREACH_FAULT(GENERATE_STRING)
};

// Suppress/enable prints from fault injection code
bool do_prints = true;

void disable_fault_prints()
{
    do_prints = false;
}

void enable_fault_prints()
{
    do_prints = true;
}

bool fail(faults_t fault) {
	if (fault >= __MAX_FAULT__) {
		// out of range
		return false;
	}

	// return fault value
	if (faults[fault]) {
        if (do_prints) printf("  FAULT INJECT: hit error %s\n", FAULT_STRING[fault]);
	}
	return faults[fault];
}

void set_fault(faults_t fault) {
	if (fault >= __MAX_FAULT__) {
		// out of range
		return;
	}

	// Set fault
	faults[fault] = true;
}

void clear_fault(faults_t fault) {
	if (fault >= __MAX_FAULT__) {
		// out of range
		return;
	}

	// Set fault
	faults[fault] = false;
}


