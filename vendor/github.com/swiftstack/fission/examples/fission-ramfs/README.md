# fission/examples/passthrough

Example fission (FUSE) file system in the style of the low-level `passthrough` file system
presented in the `libfuse` source tree.

Here, the program must be run with sudo (as package fission doesn't utilize setuid'd `fusermount`).
The first arg is the "source" directory to be presented via the 2nd arg's "mountpoint" directory.
