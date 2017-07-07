## Problem statement and constraints

ProxyFS needs an analogue of [`fsck`](https://en.wikipedia.org/wiki/Fsck) to periodically walk the filesystem tree and verify that inodes are valid, marking corrupted inodes and providing some sort of [`lost+found`](http://unix.stackexchange.com/questions/18154/what-is-the-purpose-of-the-lostfound-folder-in-linux-and-unix)-like functionality. It needs to be a client of package `fs` in order to respect DLM locks.

## Implementation plans

### inode validation

A `Validate()` API will be added to package `fs` and the `inode.VolumeHandle` interface. Validating an inode will involve fetching it from disk (_not_ the cache), validating that its metadata properties have sane values, and calling `Validate()` on the B+ tree for file and directory inodes: the tree (from our [`sortedmap` package](https://github.com/swiftstack/sortedmap)) already has a `Validate()` method. For file inodes, we should also verify the file integrity by getting a read plan, computing the highest offset that would be fetched from each log segment, and HEADing each log segment object to confirm thaat the Content-Length is sufficient.

If validation fails (either because the tree `Validate()`ion failed, or because we couldn't even get the tree because the trailer or payload log-segment-number/offset/length fields failed to match reality), we should mark the inode as corrupt in headhunter: we already [have a `CorruptionDetected` field](https://github.com/swiftstack/ProxyFS/blob/030c1e12a/inode/inode.go#L20) on the headhunter inode record struct. Attempts to fetch an inode that has been marked corrupted should fail.

### lost+found

We want `lost+found`-like functionality such that we can access a list of corrupted inodes in case some rescue operation is available. This can be a new bucket in the headhunter database: the Bolt README claims that [iterating over keys is fast](https://github.com/boltdb/bolt#iterating-over-keys). Adding to the lost+found bucket should be done in the same transaction as an inode is marked corrupted in the inode record bucket.

### checker goroutine

The checker should run in a goroutine concurrent with ordinary file service operations. It could be implemented in a separate package called `fsck`. It should walk the filesystem starting from the root directory, calling `Validate()` on each. 

-----

QUESTIONS—

* Presumably the checker should only run at some time interval controlled by a value in the config file? How often should it be? (Not important to answer in advance of writing the thing and getting empricial data.)
