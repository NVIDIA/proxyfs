#!/usr/bin/env python2.7
#
# Collect periodic heap profiles from proxyfsd using the
# HTTP interface.  This requies that proxyfsd be compiled with the
# patch included below.
#
import os
import sys
import datetime
import argparse

cmd_name = os.path.basename(sys.argv[0])

heap_profile_url = "http://localhost:6060/debug/pprof/heap"
profile_interval_sec = 30

def timestamp():
    '''Return a timestamp suitable for use in filenames.
    '''
    now = datetime.datetime.now()
    ts = ("%04d-%02d-%02d_%02d:%02d:%02d" %
          (now.year, now.month, now.day, now.hour, now.minute, now.second))
    return ts


def patch_print():
    '''Print the patch that needs to be applied to proxyfsd to allow this
    script to work.

    '''

    patch = '''
a/proxyfsd/daemon.go b/proxyfsd/daemon.go
index 656412a..8c441b7 100644
--- a/proxyfsd/daemon.go
+++ b/proxyfsd/daemon.go
@@ -2,6 +2,8 @@ package proxyfsd

import (
"fmt"
+       "net/http"
+       _ "net/http/pprof"
"os"
"os/signal"
"sync"
@@ -246,6 +248,11 @@ func Daemon(confFile string, confStrings []string, signalHandlerIsArmed *bool, e
	wg.Done()
}()

+       go func() {
+               logger.Infof("proxyfsd.Daemon() starting debug HTTP server: %s",
+                       http.ListenAndServe("localhost:6060", nil))
+       }()
+
// Arm signal handler used to indicate termination and wait on it
//
// Note: signalled chan must be buffered to avoid race with window between
'''

    print "Apply this patch to proxyfsd sources and recompile:\n", patch, "\n"
    return

def main():
    '''
    Figure out what to do and do it.
    '''

    parser = argparse.ArgumentParser()
    parser.add_argument('--patch', action = 'store_true',
                        help = "Print patch for proxyfsd for profiling.")

    args = parser.parse_args()

    if (args.patch):
        patch_print()
        return 0

    ts = timestamp()
    print >> sys.stderr, ts, ": No action specified"
    return 2

if __name__ == '__main__':
    rc = main()
    sys.exit(rc)
