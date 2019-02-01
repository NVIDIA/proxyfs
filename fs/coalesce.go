package fs

import (
	"fmt"
	"strings"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

type elementListEntryElementStruct struct {
	dirInodeNumber  inode.InodeNumber
	fileInodeNumber inode.InodeNumber
}

func (mS *mountStruct) freeHeldLocks(heldLocksMap map[inode.InodeNumber]*dlm.RWLockStruct) {
	var (
		heldLock  *dlm.RWLockStruct
		unlockErr error
	)

	for _, heldLock = range heldLocksMap {
		unlockErr = heldLock.Unlock()
		if nil != unlockErr {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", heldLock.LockID, unlockErr)
		}
	}
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
			} else {
				canonicalizedPathSplit = canonicalizedPathSplit[:canonicalizedPathSplitLen-1]
			}
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

func (mS *mountStruct) MiddlewareCoalesce(destPath string, elementPaths []string) (ino uint64, numWrites uint64, modificationTime uint64, err error) {
	var (
		coalesceElementList                    []*inode.CoalesceElement
		coalesceElementListIndex               int
		coalesceSize                           uint64
		coalesceTime                           time.Time
		destFileInodeNumber                    inode.InodeNumber
		destPathSplit                          []string
		destPathSplitPart                      string
		destPathSplitPartIndex                 int
		dlmCallerID                            dlm.CallerID
		elementPath                            string
		elementPathSplit                       []string
		elementPathSplitPart                   string
		elementPathSplitPartIndex              int
		dirEntryInodeLock                      *dlm.RWLockStruct
		dirEntryInodeLockAlreadyInHeldLocksMap bool
		dirEntryInodeNumber                    inode.InodeNumber
		dirEntryInodeType                      inode.InodeType
		dirInodeLock                           *dlm.RWLockStruct
		dirInodeLockAlreadyInHeldLocksMap      bool
		dirInodeNumber                         inode.InodeNumber
		heldLocksMap                           map[inode.InodeNumber]*dlm.RWLockStruct
		inodeVolumeHandle                      inode.VolumeHandle
		internalErr                            error
		restartBackoff                         time.Duration
		symlinkTarget                          string
	)

	startTime := time.Now()
	defer func() {
		globals.MiddlewareCoalesceUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.MiddlewareCoalesceBytes.Add(coalesceSize)
		if err != nil {
			globals.MiddlewareCoalesceErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	dlmCallerID = dlm.GenerateCallerID()
	inodeVolumeHandle = mS.volStruct.inodeVolumeHandle

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:
	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, TryLockBackoffMin, TryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareCoalesce failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocksMap for this restart

	heldLocksMap = make(map[inode.InodeNumber]*dlm.RWLockStruct)

	// Assemble WriteLock on each FileInode and their containing DirInode in elementPaths

	coalesceElementList = make([]*inode.CoalesceElement, len(elementPaths))

	for coalesceElementListIndex, elementPath = range elementPaths {
		// Canonicalize elementPath - allowing for resumption in the presence of SimlinkInodes

		elementPathSplit, err = canonicalizePath(elementPath)

	ResumeElementPath:
		// Resumption of processing will return to this point when a SymlinkInode is encountered
		// The elementPathSplit will have been recomputed via reCanonicalizePathForSymlink()

		if (nil != err) || (2 > len(elementPathSplit)) {
			mS.freeHeldLocks(heldLocksMap)
			err = blunder.NewError(blunder.InvalidArgError, "MiddlewareCoalesce elementPath \"%s\" invalid", elementPath)
			return
		}

		// Start loop from a ReadLock on '/'

		dirInodeNumber = inode.InodeNumber(0)
		dirEntryInodeNumber = inode.RootDirInodeNumber
		dirEntryInodeLock, dirEntryInodeLockAlreadyInHeldLocksMap = heldLocksMap[dirEntryInodeNumber]
		if !dirEntryInodeLockAlreadyInHeldLocksMap {
			dirEntryInodeLock, err = inodeVolumeHandle.AttemptReadLock(dirEntryInodeNumber, dlmCallerID)
			if nil != err {
				mS.freeHeldLocks(heldLocksMap)
				goto Restart
			}
		}

		// Now loop for each elementPath part

		for elementPathSplitPartIndex, elementPathSplitPart = range elementPathSplit {
			// Shift dirEntryInode to dirInode as we recurse down elementPath

			if dirInodeNumber != inode.InodeNumber(0) {
				// If not at '/', Unlock dirInode (if necessary) that is about to be replaced by dirEntryInode
				if !dirInodeLockAlreadyInHeldLocksMap {
					err = dirInodeLock.Unlock()
					if nil != err {
						logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
					}
				}
			}

			dirInodeNumber = dirEntryInodeNumber
			dirInodeLock = dirEntryInodeLock
			dirInodeLockAlreadyInHeldLocksMap = dirEntryInodeLockAlreadyInHeldLocksMap

			// Lookup dirEntry...failing if not found or dirInode isn't actually a DirInode

			dirEntryInodeNumber, err = inodeVolumeHandle.Lookup(dirInodeNumber, elementPathSplitPart)
			if nil != err {
				if !dirInodeLockAlreadyInHeldLocksMap {
					err = dirInodeLock.Unlock()
					if nil != err {
						logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
					}
				}
				mS.freeHeldLocks(heldLocksMap)
				err = blunder.NewError(blunder.InvalidArgError, "MiddlewareCoalesce elementPath \"%s\" not found", elementPath)
				return
			}

			// Ensure we have at least a ReadLock on dirEntryInode...restart if unavailable

			dirEntryInodeLock, dirEntryInodeLockAlreadyInHeldLocksMap = heldLocksMap[dirEntryInodeNumber]
			if !dirEntryInodeLockAlreadyInHeldLocksMap {
				dirEntryInodeLock, err = inodeVolumeHandle.AttemptReadLock(dirEntryInodeNumber, dlmCallerID)
				if nil != err {
					if !dirInodeLockAlreadyInHeldLocksMap {
						err = dirInodeLock.Unlock()
						if nil != err {
							logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
						}
					}
					mS.freeHeldLocks(heldLocksMap)
					goto Restart
				}
			}

			// Handle SymlinkInode case

			dirEntryInodeType, err = inodeVolumeHandle.GetType(dirEntryInodeNumber)
			if nil != err {
				logger.Fatalf("Failure obtaining InodeType for some part of elementPath \"%s\"", elementPath)
			}
			if dirEntryInodeType == inode.SymlinkType {
				symlinkTarget, err = inodeVolumeHandle.GetSymlink(dirEntryInodeNumber)
				if nil != err {
					logger.Fatalf("Failure from inode.GetSymlink() on a known SymlinkInode: %v", err)
				}
				if !dirEntryInodeLockAlreadyInHeldLocksMap {
					err = dirEntryInodeLock.Unlock()
					if nil != err {
						logger.Fatalf("Failure unlocking a held LockID %s: %v", dirEntryInodeLock.LockID, err)
					}
				}
				if dirInodeNumber != inode.InodeNumber(0) {
					if !dirInodeLockAlreadyInHeldLocksMap {
						err = dirInodeLock.Unlock()
						if nil != err {
							logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
						}
					}
				}
				elementPathSplit, err = reCanonicalizePathForSymlink(elementPathSplit, elementPathSplitPartIndex, symlinkTarget)
				goto ResumeElementPath
			}
		}

		// If dirEntryInode WriteLock not already held (it shouldn't be), attempt to promote its ReadLock to a WriteLock

		if !dirEntryInodeLockAlreadyInHeldLocksMap {
			err = dirEntryInodeLock.Unlock()
			if nil != err {
				logger.Fatalf("Failure unlocking a held LockID %s: %v", dirEntryInodeLock.LockID, err)
			}

			dirEntryInodeLock, err = inodeVolumeHandle.AttemptWriteLock(dirEntryInodeNumber, dlmCallerID)
			if nil != err {
				if !dirInodeLockAlreadyInHeldLocksMap {
					err = dirInodeLock.Unlock()
					if nil != err {
						logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
					}
				}
				mS.freeHeldLocks(heldLocksMap)
				goto Restart
			}

			heldLocksMap[dirEntryInodeNumber] = dirEntryInodeLock
		}

		// If dirInode WriteLock not already held, attempt to promote its ReadLock to a WriteLock

		if !dirInodeLockAlreadyInHeldLocksMap {
			err = dirInodeLock.Unlock()
			if nil != err {
				logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
			}

			dirInodeLock, err = inodeVolumeHandle.AttemptWriteLock(dirInodeNumber, dlmCallerID)
			if nil != err {
				mS.freeHeldLocks(heldLocksMap)
				goto Restart
			}

			heldLocksMap[dirInodeNumber] = dirInodeLock
		}

		// Record dirInode & dirEntryInode (fileInode) in elementList

		coalesceElementList[coalesceElementListIndex] = &inode.CoalesceElement{
			ContainingDirectoryInodeNumber: dirInodeNumber,
			ElementInodeNumber:             dirEntryInodeNumber,
			ElementName:                    elementPathSplitPart,
		}
	}

	// At this point, we should have all elementPaths FileInodes and their containing DirInodes WriteLock'd
	// Now it's time to do the same for destPath.
	// Here, though, things are a bit different:
	//  1 - destPath FileInode may not exist (if it does, it will be replaced)
	//  2 - intervening DirInodes may not exist and must be implicitly created

	// Canonicalize elementPath - allowing for resumption in the presence of SimlinkInodes

	destPathSplit, err = canonicalizePath(destPath)

ResumeDestPath:
	// Resumption of processing will return to this point when a SymlinkInode is encountered
	// The destPathSplit will have been recomputed via reCanonicalizePathForSymlink()

	if (nil != err) || (2 > len(destPathSplit)) {
		mS.freeHeldLocks(heldLocksMap)
		err = blunder.NewError(blunder.InvalidArgError, "MiddlewareCoalesce destPath \"%s\" invalid", destPath)
		return
	}

	// Start loop from a ReadLock on '/'

	dirInodeNumber = inode.InodeNumber(0)
	dirEntryInodeNumber = inode.RootDirInodeNumber
	dirEntryInodeLock, dirEntryInodeLockAlreadyInHeldLocksMap = heldLocksMap[dirEntryInodeNumber]
	if !dirEntryInodeLockAlreadyInHeldLocksMap {
		dirEntryInodeLock, err = inodeVolumeHandle.AttemptReadLock(dirEntryInodeNumber, dlmCallerID)
		if nil != err {
			mS.freeHeldLocks(heldLocksMap)
			goto Restart
		}
	}

	// Now loop for each destPath part

	for destPathSplitPartIndex, destPathSplitPart = range destPathSplit {
		// Shift dirEntryInode to dirInode as we recurse down destPath

		if dirInodeNumber != inode.InodeNumber(0) {
			// If not at '/', Unlock dirInode (if necessary) that is about to be replaced by dirEntryInode
			if !dirInodeLockAlreadyInHeldLocksMap {
				err = dirInodeLock.Unlock()
				if nil != err {
					logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
				}
			}
		}

		dirInodeNumber = dirEntryInodeNumber
		dirInodeLock = dirEntryInodeLock
		dirInodeLockAlreadyInHeldLocksMap = dirEntryInodeLockAlreadyInHeldLocksMap

		// Lookup dirEntry
		// If this is     the final destPath part, if it doesn't exist, we need to create a FileInode
		// If this is not the final destPath part, if it doesn't exist, we need to create a DirInode

		dirEntryInodeNumber, err = inodeVolumeHandle.Lookup(dirInodeNumber, destPathSplitPart)

		if nil == err {
			// Ensure we have at least a ReadLock on dirEntryInode...restart if unavailable

			dirEntryInodeLock, dirEntryInodeLockAlreadyInHeldLocksMap = heldLocksMap[dirEntryInodeNumber]
			if !dirEntryInodeLockAlreadyInHeldLocksMap {
				dirEntryInodeLock, err = inodeVolumeHandle.AttemptReadLock(dirEntryInodeNumber, dlmCallerID)
				if nil != err {
					if !dirInodeLockAlreadyInHeldLocksMap {
						err = dirInodeLock.Unlock()
						if nil != err {
							logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
						}
					}
					mS.freeHeldLocks(heldLocksMap)
					goto Restart
				}
			}

			// Handle SymlinkInode case

			dirEntryInodeType, err = inodeVolumeHandle.GetType(dirEntryInodeNumber)
			if nil != err {
				logger.Fatalf("Failure obtaining InodeType for some part of destPath \"%s\"", destPath)
			}
			if dirEntryInodeType == inode.SymlinkType {
				symlinkTarget, err = inodeVolumeHandle.GetSymlink(dirEntryInodeNumber)
				if nil != err {
					logger.Fatalf("Failure from inode.GetSymlink() on a known SymlinkInode: %v", err)
				}
				if !dirEntryInodeLockAlreadyInHeldLocksMap {
					err = dirEntryInodeLock.Unlock()
					if nil != err {
						logger.Fatalf("Failure unlocking a held LockID %s: %v", dirEntryInodeLock.LockID, err)
					}
				}
				if dirInodeNumber != inode.InodeNumber(0) {
					if !dirInodeLockAlreadyInHeldLocksMap {
						err = dirInodeLock.Unlock()
						if nil != err {
							logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
						}
					}
				}
				destPathSplit, err = reCanonicalizePathForSymlink(destPathSplit, destPathSplitPartIndex, symlinkTarget)
				goto ResumeDestPath
			}
		} else {
			// If dirInode WriteLock not already held, attempt to promote its ReadLock to a WriteLock

			if !dirInodeLockAlreadyInHeldLocksMap {
				err = dirInodeLock.Unlock()
				if nil != err {
					logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
				}

				dirInodeLock, err = inodeVolumeHandle.AttemptWriteLock(dirInodeNumber, dlmCallerID)
				if nil != err {
					mS.freeHeldLocks(heldLocksMap)
					goto Restart
				}

				heldLocksMap[dirInodeNumber] = dirInodeLock
				dirInodeLockAlreadyInHeldLocksMap = true
			}

			if destPathSplitPartIndex == (len(destPathSplit) - 1) {
				// Create a FileInode to be inserted

				dirEntryInodeNumber, err = inodeVolumeHandle.CreateFile(inode.InodeMode(0644), inode.InodeRootUserID, inode.InodeGroupID(0))
				if nil != err {
					mS.freeHeldLocks(heldLocksMap)
					err = blunder.NewError(blunder.InvalidArgError, "Coalesce destPath needed to create a FileInode but failed: %v", err)
					return
				}
			} else {
				// Create a DirInode to be inserted

				dirEntryInodeNumber, err = inodeVolumeHandle.CreateDir(inode.InodeMode(0755), inode.InodeRootUserID, inode.InodeGroupID(0))
				if nil != err {
					mS.freeHeldLocks(heldLocksMap)
					err = blunder.NewError(blunder.InvalidArgError, "Coalesce destPath needed to create a DirInode that failed: %v", err)
					return
				}
			}

			// Now insert created {File|Dir}Inode

			dirEntryInodeLock, err = inodeVolumeHandle.AttemptWriteLock(dirEntryInodeNumber, dlmCallerID)
			if nil != err {
				logger.Fatalf("Failure WriteLock-ing just created FileInode: %v", err)
			}

			heldLocksMap[dirEntryInodeNumber] = dirEntryInodeLock
			dirEntryInodeLockAlreadyInHeldLocksMap = true

			err = inodeVolumeHandle.Link(dirInodeNumber, destPathSplitPart, dirEntryInodeNumber, false)
			if nil != err {
				internalErr = inodeVolumeHandle.Destroy(dirEntryInodeNumber)
				if nil != internalErr {
					logger.Errorf("Coalesce Link() failure case... unable to destroy created {File|Dir}Inode: %v", internalErr)
				}
				mS.freeHeldLocks(heldLocksMap)
				err = blunder.NewError(blunder.InvalidArgError, "Coalesce destPath needed to link a created a {File|Dir}Inode but failed: %v", err)
				return
			}
		}
	}

	// If dirEntryInode WriteLock not already held, attempt to promote its ReadLock to a WriteLock

	if !dirEntryInodeLockAlreadyInHeldLocksMap {
		err = dirEntryInodeLock.Unlock()
		if nil != err {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", dirEntryInodeLock.LockID, err)
		}

		dirEntryInodeLock, err = inodeVolumeHandle.AttemptWriteLock(dirEntryInodeNumber, dlmCallerID)
		if nil != err {
			if !dirInodeLockAlreadyInHeldLocksMap {
				err = dirInodeLock.Unlock()
				if nil != err {
					logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
				}
			}
			mS.freeHeldLocks(heldLocksMap)
			goto Restart
		}

		heldLocksMap[dirEntryInodeNumber] = dirEntryInodeLock
	}

	// If dirInode WriteLock not already held, release its ReadLock

	if !dirInodeLockAlreadyInHeldLocksMap {
		err = dirInodeLock.Unlock()
		if nil != err {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", dirInodeLock.LockID, err)
		}
	}

	// Invoke package inode to actually perform the Coalesce operation

	destFileInodeNumber = dirEntryInodeNumber
	coalesceTime, numWrites, _, err = inodeVolumeHandle.Coalesce(destFileInodeNumber, coalesceElementList)

	// We can now release all the WriteLocks we are currently holding

	mS.freeHeldLocks(heldLocksMap)

	// Regardless of err return, fill in other return values

	ino = uint64(destFileInodeNumber)
	modificationTime = uint64(coalesceTime.UnixNano())

	return
}
