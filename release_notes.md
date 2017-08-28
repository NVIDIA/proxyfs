# ProxyFS Release Notes

## 0.52.0 (August 21, 2017)

### Features:

* Support for disabling volumes added

### Bug Fixes:

* Fixed missing flushing of modified files leading to zero-lengthed files

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.51.2 (August 15, 2017)

### Features:

* Improved metadata checkpointing mechanism (V2) performs optimized garbage collection

### Bug Fixes:

* Fixed clean-up of FUSE (and NFS) mount point upon restart after failure
* Fixed memory leaks in SMBd for readdir(), getxattr(), chdir() and list xattr
* Fixed race condition between time-based flushes and on-going write traffic
* Fixed multi-threaded socket management code in resolving DNS names
* Fixed missing support for file names containing special characters

### Known Issues:

* Named Streams are disabled in SMB (enabling this is TBD)
* Upgrading metadata checkpointing from V1 to V2 experiences process hangs in some cases

## 0.51.1 (August 3, 2017)

### Features:

* Enhanced tolerance for intermittent Swift errors
* Read Cache now consumes a configurable percentage of available memory
* Flow Controls now get a weighted fraction of total Read Cache memory
* Configuration reload now supported via SIGHUP signal

### Bug Fixes:

* Fixed embedded HTTP Server handling of "empty" URLs
* Removed memory leaks in SMB handling
* Resolved potential corruption when actively written files are flushed

### Known Issues:

* Memory leak in SMB directory reading and extended attribute reading
* Process restart may leave NFS mount point in a hung state
* Named Streams are disabled in SMB (enabling this is TBD)
