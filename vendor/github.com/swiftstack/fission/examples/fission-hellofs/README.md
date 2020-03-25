# fission/examples/fission-hellofs

Example fission (FUSE) file system implementing containing only a single file named "hello"
who's contents simply is "Hello World!\n". The lone argument to the program should be an
empty directory where the FUSE MountPoint will appear. A clean exit is triggered by sending
the process a SIGHUP, SIGINT, or SIGTERM.
