# Headhunter Usage

## Old Model

### Implementation

BoltDB database for each Volume stored in a file in Headhunter.DBDirPath.
The database file for a Volume has the name of the Volume as its name.
Being accessible via local file system calls, each is inherently a single point of failure.

### Values/Tables

| Entity | Key | Value |
| ------ | --- | ----- |
| Nonce  | N/A | non-repeating uint64 used to uniquely name things like LogSegment Objects |
| InodeRec | InodeNumber | last LogSegmentNumber indicating the Object where the latest version of the "on disk" Inode structure is serialized |
| LogSegmentRec | LogSegmentNumber | ObjectPath for this LogSegmentNumber... though note that we didn't actually use this mapping anywhere instead opt'ing to replicate the ObjectPath every where we used it... making it impractical to ever actually change the mapping |

## New Model

### Implementation

Swift-checkpointed set of B+Trees to hold the three Key:Value tables listed below.
SnapShots are taken of the live in-memory instances of these three tables and, along with the in-memory uint64 tracking the next Nonce to issue, are preserved by setting a Header in the Swift Container used to persist the nodes of Headhunter's three B+Trees.
Destructive operations to Swift (e.g. garbage collection) can be held off until the necessary SnapShot has itself been persisted to Swift.
Similarly, delayed/background operations to Swift (e.g. lazily PUT LogSegment Objects) can hold off the Headhunter CheckPoint/SnapShot until such time as the SnapShot would be coherent with the Swift Objects that have completed their PUTs.

### Values/Tables

| Entity | Key | Value |
| ------ | --- | ----- |
| Nonce  | N/A | non-repeating uint64 used to uniquely name things like LogSegment Objects |
| InodeRec | InodeNumber | the cstruct-serialized Inode structure... note also that Symlink Inodes also include the TargetPath field |
| LogSegmentRec | LogSegmentNumber | ObjectPath for this LogSegmentNumber |
| BPlusTreeObject | ObjectNumber | the cstruct-serialized node of a B+Tree used to hold a DirInode's directory entries (mapping basename to InodeNumber) or a FileInode's extent map (mapping file offsets to LogSegmentNumber-identified Objects offsets) |
