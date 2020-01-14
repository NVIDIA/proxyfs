# fission/examples/fission-ramfs

Example fission (FUSE) file system implementing in non-persistent ram. The lone argument to
the program should be an empty directory where the FUSE MountPoint will appear. A clean exit
is triggered by sending the process a SIGHUP, SIGINT, or SIGTERM.
