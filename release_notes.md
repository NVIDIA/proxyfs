# ProxyFS Release Notes

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

* Named Streams are disabled in SMB
