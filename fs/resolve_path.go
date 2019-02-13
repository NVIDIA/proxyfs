package fs

import (
	"fmt"
	"strings"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
)

const (
	resolvePathFollowDirEntrySymlinks              uint32 = 1 << iota //
	resolvePathFollowDirSymlinks                                      //
	resolvePathCreateMissingPathElements                              //
	resolvePathDirEntryInodeMustBeDirectory                           // Otherwise, it must be a File
	resolvePathRequireExclusiveLockOnDirEntryInode                    //
	resolvePathRequireExclusiveLockOnDirInode                         // Presumably only useful if resolvePathRequireExclusiveLockOnDirEntryInode also specified
	resolvePathRequireSharedLockOnDirInode                            // Not valid if resolvePathRequireExclusiveLockOnDirInode also specified
)

func resolvePathOptionsCheck(optionsRequested uint32, optionsToCheckFor uint32) (optionsRequestedIncludesCheckFor bool) {
	optionsRequestedIncludesCheckFor = (optionsToCheckFor == (optionsToCheckFor & optionsRequested))
	return
}

type heldLocksStruct struct {
	exclusive map[inode.InodeNumber]*dlm.RWLockStruct
	shared    map[inode.InodeNumber]*dlm.RWLockStruct
}

func newHeldLocks() (heldLocks *heldLocksStruct) {
	heldLocks = &heldLocksStruct{
		exclusive: make(map[inode.InodeNumber]*dlm.RWLockStruct),
		shared:    make(map[inode.InodeNumber]*dlm.RWLockStruct),
	}
	return
}

func (heldLocks *heldLocksStruct) free() {
	var (
		heldLock *dlm.RWLockStruct
		err      error
	)

	for _, heldLock = range heldLocks.exclusive {
		err = heldLock.Unlock()
		if nil != err {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", heldLock.LockID, err)
		}
	}

	for _, heldLock = range heldLocks.shared {
		err = heldLock.Unlock()
		if nil != err {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", heldLock.LockID, err)
		}
	}
}

// resolvePath is used to walk the supplied path starting at the supplied DirInode and locate
// the "leaf" {Dir|File}Inode. Various options allow for traversing encountered SymlinkInodes as
// well as optionally creating missing Inodes along the way. The caller is allowed to hold locks
// at entry that will be used (and possibly upgraded from "shared" to "exclusive") as well as
// extended to include any additional locks obtained. All lock requests are attempted so as to
// always avoid deadlock and if attempts fail, retryRequired == TRUE will be returned and the
// caller will be responsible for releasing all held locks, backing off (via a delay), before
// restarting the sequence.
//
// The pattern should look something like this:
//
//     restartBackoff = time.Duration(0)
//
// Restart:
//
//     restartBackoff, _ = utils.PerformDelayAndComputeNextDelay(restartBackoff, backoffMin, backoffMax)
//
//     heldLocks = newHeldLocks()
//
//     dirInode, fileInode, retryRequired, err =
//         resolvePath(inode.RootDirInodeNumber, path, heldLocks, resolvePathFollowSymlnks|...)
//
//     if retryRequired {
//         heldLocks.free()
//         got Restart
//     }
//
//     // Do whatever needed to be done with returned [dirInode and] fileInode
//
//     heldLocks.free()
func (mS *mountStruct) resolvePath(startingInodeNumber inode.InodeNumber, path string, heldLocks *heldLocksStruct, options uint32) (dirInodeNumber inode.InodeNumber, dirEntryInodeNumber inode.InodeNumber, dirEntryBasename string, retryRequired bool, err error) {
	var (
		dirEntryInodeLock                 *dlm.RWLockStruct
		dirEntryInodeLockAlreadyExclusive bool
		dirEntryInodeLockAlreadyHeld      bool
		dirEntryInodeLockAlreadyShared    bool
		dirEntryInodeType                 inode.InodeType
		dirInodeLock                      *dlm.RWLockStruct
		dirInodeLockAlreadyExclusive      bool
		dirInodeLockAlreadyHeld           bool
		dirInodeLockAlreadyShared         bool
		dlmCallerID                       dlm.CallerID
		followSymlink                     bool
		inodeVolumeHandle                 inode.VolumeHandle
		internalErr                       error
		pathSplit                         []string
		pathSplitPart                     string
		pathSplitPartIndex                int
		symlinkCount                      uint16
		symlinkTarget                     string
	)

	// Setup default returns

	dirInodeNumber = inode.InodeNumber(0)
	dirEntryInodeNumber = inode.InodeNumber(0)
	dirEntryBasename = ""
	retryRequired = false
	err = nil

	// Validate options

	if resolvePathOptionsCheck(options, resolvePathRequireExclusiveLockOnDirInode) && resolvePathOptionsCheck(options, resolvePathRequireSharedLockOnDirInode) {
		err = blunder.NewError(blunder.InvalidArgError, "resolvePath(,,,options) includes both resolvePathRequireExclusiveLockOnDirInode & resolvePathRequireSharedLockOnDirInode")
		return
	}

	// Setup shortcuts/contants

	dlmCallerID = dlm.GenerateCallerID()
	inodeVolumeHandle = mS.volStruct.inodeVolumeHandle

	// Prepare for SymlinkInode-restart handling on canonicalized path

	symlinkCount = 0

	pathSplit, internalErr = canonicalizePath(path)

RestartAfterFollowingSymlink:

	if (nil != internalErr) || (0 == len(pathSplit)) {
		err = blunder.NewError(blunder.InvalidArgError, "resolvePath(,\"%s\",,) invalid", path)
		return
	}

	// Start loop from a ReadLock on startingInodeNumber

	dirInodeNumber = inode.InodeNumber(0)

	dirEntryInodeNumber = startingInodeNumber

	dirEntryInodeLock, dirEntryInodeLockAlreadyExclusive = heldLocks.exclusive[dirEntryInodeNumber]
	if dirEntryInodeLockAlreadyExclusive {
		dirEntryInodeLockAlreadyHeld = true
		dirEntryInodeLockAlreadyShared = false
	} else {
		dirEntryInodeLock, dirEntryInodeLockAlreadyShared = heldLocks.shared[dirEntryInodeNumber]
		if dirEntryInodeLockAlreadyShared {
			dirEntryInodeLockAlreadyHeld = true
		} else {
			dirEntryInodeLockAlreadyHeld = false
			dirEntryInodeLock, internalErr = inodeVolumeHandle.AttemptReadLock(dirEntryInodeNumber, dlmCallerID)
			if nil != internalErr {
				retryRequired = true
				return
			}
		}
	}

	// Now loop for each pathSplit part

	for pathSplitPartIndex, pathSplitPart = range pathSplit {
		// Shift dirEntryInode to dirInode as we recurse down pathSplit

		if inode.InodeNumber(0) != dirInodeNumber {
			if !dirInodeLockAlreadyHeld {
				internalErr = dirInodeLock.Unlock()
				if nil != internalErr {
					logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
				}
			}
		}

		dirInodeNumber = dirEntryInodeNumber
		dirInodeLock = dirEntryInodeLock
		dirInodeLockAlreadyExclusive = dirEntryInodeLockAlreadyExclusive
		dirInodeLockAlreadyHeld = dirEntryInodeLockAlreadyHeld
		dirInodeLockAlreadyShared = dirEntryInodeLockAlreadyShared

		// Lookup dirEntry

		dirEntryBasename = pathSplitPart // In case this is the last pathSplitPart that needs to be returned

		dirEntryInodeNumber, internalErr = inodeVolumeHandle.Lookup(dirInodeNumber, pathSplitPart)

		if nil == internalErr {
			// Lookup() succeeded... ensure we have at least a ReadLock on dirEntryInode

			dirEntryInodeLock, dirEntryInodeLockAlreadyExclusive = heldLocks.exclusive[dirEntryInodeNumber]
			if dirEntryInodeLockAlreadyExclusive {
				dirEntryInodeLockAlreadyHeld = true
				dirEntryInodeLockAlreadyShared = false
			} else {
				dirEntryInodeLock, dirEntryInodeLockAlreadyShared = heldLocks.shared[dirEntryInodeNumber]
				if dirEntryInodeLockAlreadyShared {
					dirEntryInodeLockAlreadyHeld = true
				} else {
					dirEntryInodeLockAlreadyHeld = false
					dirEntryInodeLock, internalErr = inodeVolumeHandle.AttemptReadLock(dirEntryInodeNumber, dlmCallerID)
					if nil != internalErr {
						// Caller must call heldLocks.free(), "backoff", and retry
						// But first, free locks not recorded in heldLocks (if any)

						if !dirInodeLockAlreadyHeld {
							internalErr = dirInodeLock.Unlock()
							if nil != internalErr {
								logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
							}
						}

						retryRequired = true
						return
					}
				}
			}

			// Handle SymlinkInode case if requested

			dirEntryInodeType, internalErr = inodeVolumeHandle.GetType(dirEntryInodeNumber)
			if nil != internalErr {
				logger.Fatalf("resolvePath(): failed obtaining InodeType for some part of path \"%s\"", path)
			}
			if dirEntryInodeType == inode.SymlinkType {
				if pathSplitPartIndex < (len(pathSplit) - 1) {
					followSymlink = resolvePathOptionsCheck(options, resolvePathFollowDirSymlinks)
				} else { // pathSplitPartIndex == (len(pathSplit) - 1)
					followSymlink = resolvePathOptionsCheck(options, resolvePathFollowDirEntrySymlinks)
				}

				if followSymlink {
					symlinkTarget, internalErr = inodeVolumeHandle.GetSymlink(dirEntryInodeNumber)
					if nil != internalErr {
						logger.Fatalf("resolvePath(): failure from inode.GetSymlink() on a known SymlinkInode: %v", err)
					}

					// Free locks not recorded in heldLocks (if any)

					if !dirEntryInodeLockAlreadyHeld {
						internalErr = dirEntryInodeLock.Unlock()
						if nil != internalErr {
							logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirEntryInodeLock.LockID, internalErr)
						}
					}
					if !dirInodeLockAlreadyHeld {
						internalErr = dirInodeLock.Unlock()
						if nil != internalErr {
							logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
						}
					}

					// Enforce SymlinkMax setting

					if 0 != globals.symlinkMax {
						symlinkCount++

						if symlinkCount > globals.symlinkMax {
							err = blunder.NewError(blunder.TooManySymlinksError, "resolvePath(): exceeded SymlinkMax")
							return
						}
					}

					// Apply symlinkTarget to path, reCanonicalize resultant path, and restart

					pathSplit, internalErr = reCanonicalizePathForSymlink(pathSplit, pathSplitPartIndex, symlinkTarget)
					goto RestartAfterFollowingSymlink
				} else {
					// Not following SymlinkInode... so its a failure if not last pathSplitPart

					if pathSplitPartIndex < (len(pathSplit) - 1) {
						// But first, free locks not recorded in heldLocks (if any)

						if !dirEntryInodeLockAlreadyHeld {
							internalErr = dirEntryInodeLock.Unlock()
							if nil != internalErr {
								logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirEntryInodeLock.LockID, internalErr)
							}
						}
						if !dirInodeLockAlreadyHeld {
							internalErr = dirInodeLock.Unlock()
							if nil != internalErr {
								logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
							}
						}

						err = blunder.NewError(blunder.InvalidArgError, "resolvePath(,\"%s\",,) invalid", path)
						return
					}
				}
			}
		} else {
			// Lookup() failed... is resolvePath() asked to create missing Inode?

			if resolvePathOptionsCheck(options, resolvePathCreateMissingPathElements) {
				// Must hold exclusive lock to create missing {Dir|File}Inode

				if !dirInodeLockAlreadyExclusive {
					if dirInodeLockAlreadyShared {
						// Promote heldLocks.shared InodeLock to .exclusive

						internalErr = dirInodeLock.Unlock()
						if nil != internalErr {
							logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
						}

						delete(heldLocks.shared, dirInodeNumber)
						dirInodeLockAlreadyHeld = false
						dirInodeLockAlreadyShared = false

						dirInodeLock, internalErr = inodeVolumeHandle.AttemptWriteLock(dirInodeNumber, dlmCallerID)
						if nil != internalErr {
							// Caller must call heldLocks.free(), "backoff", and retry

							retryRequired = true
							return
						}

						heldLocks.exclusive[dirInodeNumber] = dirInodeLock
						dirInodeLockAlreadyExclusive = true
						dirInodeLockAlreadyHeld = true
					} else {
						// Promote temporary ReadLock to heldLocks.exclusive

						internalErr = dirInodeLock.Unlock()
						if nil != internalErr {
							logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
						}

						dirInodeLock, internalErr = inodeVolumeHandle.AttemptWriteLock(dirInodeNumber, dlmCallerID)
						if nil != internalErr {
							// Caller must call heldLocks.free(), "backoff", and retry

							retryRequired = true
							return
						}

						heldLocks.exclusive[dirInodeNumber] = dirInodeLock
						dirInodeLockAlreadyExclusive = true
						dirInodeLockAlreadyHeld = true
					}
				}

				// Create missing {Dir|File}Inode

				if (pathSplitPartIndex < (len(pathSplit) - 1)) || resolvePathOptionsCheck(options, resolvePathDirEntryInodeMustBeDirectory) {
					// Create a DirInode to be inserted

					dirEntryInodeNumber, internalErr = inodeVolumeHandle.CreateDir(inode.InodeMode(0755), inode.InodeRootUserID, inode.InodeGroupID(0))
					if nil != internalErr {
						err = blunder.NewError(blunder.PermDeniedError, "resolvePath(): failed to create a DirInode: %v", err)
						return
					}
				} else {
					// Create a FileInode to be inserted

					dirEntryInodeNumber, internalErr = inodeVolumeHandle.CreateFile(inode.InodeMode(0644), inode.InodeRootUserID, inode.InodeGroupID(0))
					if nil != internalErr {
						err = blunder.NewError(blunder.PermDeniedError, "resolvePath(): failed to create a FileInode: %v", err)
						return
					}
				}

				// Obtain and record an exclusive lock on just created {Dir|File}Inode

				dirEntryInodeLock, internalErr = inodeVolumeHandle.AttemptWriteLock(dirEntryInodeNumber, dlmCallerID)
				if nil != internalErr {
					logger.Fatalf("resolvePath(): failed to exclusively lock just-created Inode 0x%016X: %v", dirEntryInodeNumber, internalErr)
				}

				heldLocks.exclusive[dirEntryInodeNumber] = dirEntryInodeLock
				dirEntryInodeLockAlreadyExclusive = true
				dirEntryInodeLockAlreadyHeld = true
				dirEntryInodeLockAlreadyShared = false

				// Now insert created {Dir|File}Inode

				internalErr = inodeVolumeHandle.Link(dirInodeNumber, pathSplitPart, dirEntryInodeNumber, false)
				if nil != internalErr {
					err = blunder.NewError(blunder.PermDeniedError, "resolvePath(): failed to Link created {Dir|File}Inode into path %s: %v", path, internalErr)
					internalErr = inodeVolumeHandle.Destroy(dirEntryInodeNumber)
					if nil != internalErr {
						logger.Errorf("resolvePath(): failed to Destroy() created {Dir|File}Inode 0x%016X: %v", dirEntryInodeNumber, internalErr)
					}
					return
				}
			} else {
				// Don't create missing Inode... so its a failure
				// But first, free locks not recorded in heldLocks (if any)

				if !dirInodeLockAlreadyHeld {
					internalErr = dirInodeLock.Unlock()
					if nil != internalErr {
						logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
					}
				}

				err = blunder.NewError(blunder.NotFoundError, "resolvePath(,\"%s\",,) invalid", path)
				return
			}
		}
	}

	if resolvePathOptionsCheck(options, resolvePathRequireExclusiveLockOnDirEntryInode) {
		if !dirEntryInodeLockAlreadyExclusive {
			if dirEntryInodeLockAlreadyShared {
				// Promote heldLocks.shared dirEntryInodeLock to .exclusive

				internalErr = dirEntryInodeLock.Unlock()
				if nil != internalErr {
					logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirEntryInodeLock.LockID, internalErr)
				}

				delete(heldLocks.shared, dirEntryInodeNumber)
				dirEntryInodeLockAlreadyHeld = false
				dirEntryInodeLockAlreadyShared = false

				dirEntryInodeLock, internalErr = inodeVolumeHandle.AttemptWriteLock(dirEntryInodeNumber, dlmCallerID)
				if nil != internalErr {
					// Caller must call heldLocks.free(), "backoff", and retry

					retryRequired = true
					return
				}

				heldLocks.exclusive[dirEntryInodeNumber] = dirEntryInodeLock
				dirEntryInodeLockAlreadyExclusive = true
				dirEntryInodeLockAlreadyHeld = true
			} else {
				// Promote temporary ReadLock dirEntryInodeLock to .exclusive

				internalErr = dirEntryInodeLock.Unlock()
				if nil != internalErr {
					logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirEntryInodeLock.LockID, internalErr)
				}

				dirEntryInodeLock, internalErr = inodeVolumeHandle.AttemptWriteLock(dirEntryInodeNumber, dlmCallerID)
				if nil != internalErr {
					// Caller must call heldLocks.free(), "backoff", and retry

					retryRequired = true
					return
				}

				heldLocks.exclusive[dirEntryInodeNumber] = dirEntryInodeLock
				dirEntryInodeLockAlreadyExclusive = true
				dirEntryInodeLockAlreadyHeld = true
			}
		}
	} else {
		if !dirEntryInodeLockAlreadyHeld {
			// Promote temporary ReadLock dirEntryInodeLock to heldLocks.shared

			heldLocks.shared[dirEntryInodeNumber] = dirEntryInodeLock
			dirEntryInodeLockAlreadyHeld = true
			dirEntryInodeLockAlreadyShared = true
		}
	}

	if resolvePathOptionsCheck(options, resolvePathRequireExclusiveLockOnDirInode) {
		if !dirInodeLockAlreadyExclusive {
			if dirInodeLockAlreadyShared {
				// Promote heldLocks.shared dirInodeLock to .exclusive

				internalErr = dirInodeLock.Unlock()
				if nil != internalErr {
					logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
				}

				delete(heldLocks.shared, dirInodeNumber)
				dirInodeLockAlreadyHeld = false
				dirInodeLockAlreadyShared = false

				dirInodeLock, internalErr = inodeVolumeHandle.AttemptWriteLock(dirInodeNumber, dlmCallerID)
				if nil != internalErr {
					// Caller must call heldLocks.free(), "backoff", and retry

					retryRequired = true
					return
				}

				heldLocks.exclusive[dirInodeNumber] = dirInodeLock
				dirInodeLockAlreadyExclusive = true
				dirInodeLockAlreadyHeld = true
			} else {
				// Promote temporary ReadLock dirInodeLock to .exclusive

				internalErr = dirInodeLock.Unlock()
				if nil != internalErr {
					logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
				}

				dirInodeLock, internalErr = inodeVolumeHandle.AttemptWriteLock(dirInodeNumber, dlmCallerID)
				if nil != internalErr {
					// Caller must call heldLocks.free(), "backoff", and retry

					retryRequired = true
					return
				}

				heldLocks.exclusive[dirInodeNumber] = dirInodeLock
				dirInodeLockAlreadyExclusive = true
				dirInodeLockAlreadyHeld = true
			}
		}
	} else if resolvePathOptionsCheck(options, resolvePathRequireSharedLockOnDirInode) {
		if !dirInodeLockAlreadyHeld {
			// Promote temporary ReadLock dirInodeLock to .shared

			heldLocks.shared[dirInodeNumber] = dirInodeLock
			dirInodeLockAlreadyHeld = true
			dirInodeLockAlreadyShared = true
		}
	} else {
		if !dirInodeLockAlreadyHeld {
			// If only temporary ReadLock dirInodeLock is held, release it

			internalErr = dirInodeLock.Unlock()
			if nil != internalErr {
				logger.Fatalf("resolvePath(): failed unlocking held LockID %s: %v", dirInodeLock.LockID, internalErr)
			}
		}
	}

	// Finally, return with values already set from above

	return
}

func canonicalizePath(path string) (canonicalizedPathSplit []string, err error) {
	var (
		canonicalizedPathSplitLen int
		pathSplit                 []string
		pathSplitElement          string
	)

	pathSplit = strings.Split(path, "/")

	canonicalizedPathSplit = make([]string, 0, len(pathSplit))

	for _, pathSplitElement = range pathSplit {
		switch pathSplitElement {
		case "":
			// drop pathSplitElement
		case ".":
			// drop pathSplitElement
		case "..":
			// backup one pathSplitElement
			canonicalizedPathSplitLen = len(canonicalizedPathSplit)
			if 0 == canonicalizedPathSplitLen {
				err = fmt.Errorf("\"..\" in path stepped beyond start of path")
				return
			}
			canonicalizedPathSplit = canonicalizedPathSplit[:canonicalizedPathSplitLen-1]
		default:
			// append pathSplitElement
			canonicalizedPathSplit = append(canonicalizedPathSplit, pathSplitElement)
		}
	}

	err = nil
	return
}

func reCanonicalizePathForSymlink(canonicalizedPathSplit []string, symlinkIndex int, symlinkTarget string) (reCanonicalizedPathSplit []string, err error) {
	var (
		updatedPath              string
		updatedPathAfterSymlink  string
		updatedPathBeforeSymlink string
	)

	if (0 == symlinkIndex) || strings.HasPrefix(symlinkTarget, "/") {
		updatedPathBeforeSymlink = ""
	} else {
		updatedPathBeforeSymlink = strings.Join(canonicalizedPathSplit[:symlinkIndex], "/")
	}

	if len(canonicalizedPathSplit) == (symlinkIndex - 1) {
		updatedPathAfterSymlink = ""
	} else {
		updatedPathAfterSymlink = strings.Join(canonicalizedPathSplit[symlinkIndex+1:], "/")
	}

	updatedPath = updatedPathBeforeSymlink + "/" + symlinkTarget + "/" + updatedPathAfterSymlink

	reCanonicalizedPathSplit, err = canonicalizePath(updatedPath)

	return
}
