// Package fs, sitting on top of the inode manager, defines the filesystem exposed by ProxyFS.
package fs

import (
	"bytes"
	"container/list"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

// Shorthand for our internal API debug log id; global to the package
var internalDebug = logger.DbgInternal

type symlinkFollowState struct {
	seen      map[inode.InodeNumber]bool
	traversed int
}

// Let us sort an array of directory and file names
type dirAndFileName struct {
	dirName  string
	fileName string
}

// this has to be a named type to be a method receiver
type dirAndFileNameSlice []dirAndFileName

func (coll dirAndFileNameSlice) Len() int {
	return len(coll)
}

func (coll dirAndFileNameSlice) Less(i int, j int) bool {
	return coll[i].dirName < coll[j].dirName
}

func (coll dirAndFileNameSlice) Swap(i int, j int) {
	coll[i], coll[j] = coll[j], coll[i]
}

// trackInFlightFileInodeData is called to ensure a timely Flush occurs.
//
// Only Write() will call this while holding a WriteLock on the fileInode
// either just before or just after its call to inode.Write().
func (vS *volumeStruct) trackInFlightFileInodeData(inodeNumber inode.InodeNumber) {
	var (
		inFlightFileInodeData *inFlightFileInodeDataStruct
		ok                    bool
	)

	globals.Lock()
	vS.dataMutex.Lock()
	inFlightFileInodeData, ok = vS.inFlightFileInodeDataMap[inodeNumber]
	if !ok {
		inFlightFileInodeData = &inFlightFileInodeDataStruct{
			InodeNumber: inodeNumber,
			volStruct:   vS,
			control:     make(chan bool, inFlightFileInodeDataControlBuffering),
		}
		vS.inFlightFileInodeDataMap[inodeNumber] = inFlightFileInodeData
		inFlightFileInodeData.globalsListElement = globals.inFlightFileInodeDataList.PushBack(inFlightFileInodeData)
		inFlightFileInodeData.wg.Add(1)
		go inFlightFileInodeData.inFlightFileInodeDataTracker()
	}
	vS.dataMutex.Unlock()
	globals.Unlock()
}

// untrackInFlightInodeData is called once it is known a Flush() is no longer needed
// or to actually request a Flush() [as would be the case during unmounting a volume].
func (vS *volumeStruct) untrackInFlightFileInodeData(inodeNumber inode.InodeNumber, flushFirst bool) {
	var (
		inFlightFileInodeData *inFlightFileInodeDataStruct
		ok                    bool
	)

	globals.Lock()
	vS.dataMutex.Lock()
	inFlightFileInodeData, ok = vS.inFlightFileInodeDataMap[inodeNumber]
	if !ok {
		vS.dataMutex.Unlock()
		globals.Unlock()
		return
	}
	delete(vS.inFlightFileInodeDataMap, inodeNumber)
	if nil != inFlightFileInodeData.globalsListElement {
		_ = globals.inFlightFileInodeDataList.Remove(inFlightFileInodeData.globalsListElement)
		inFlightFileInodeData.globalsListElement = nil
	}
	inFlightFileInodeData.control <- flushFirst
	vS.dataMutex.Unlock()
	globals.Unlock()
	if flushFirst {
		inFlightFileInodeData.wg.Wait()
	}
}

// untrackInFlightFileInodeDataAll is called to flush all current elements
// of vS.inFlightFileInodeDataMap (if any) during SIGHUP or Down().
func (vS *volumeStruct) untrackInFlightFileInodeDataAll() {
	var (
		inFlightFileInodeNumber          inode.InodeNumber
		inFlightFileInodeNumbers         []inode.InodeNumber
		inFlightFileInodeNumbersCapacity int
	)

	// Snapshot list of inode.InodeNumber's currently in vS.inFlightFileInodeDataMap

	vS.dataMutex.Lock()
	inFlightFileInodeNumbersCapacity = len(vS.inFlightFileInodeDataMap)
	if 0 == inFlightFileInodeNumbersCapacity {
		vS.dataMutex.Unlock()
		return
	}
	inFlightFileInodeNumbers = make([]inode.InodeNumber, 0, inFlightFileInodeNumbersCapacity)
	for inFlightFileInodeNumber, _ = range vS.inFlightFileInodeDataMap {
		inFlightFileInodeNumbers = append(inFlightFileInodeNumbers, inFlightFileInodeNumber)
	}
	vS.dataMutex.Unlock()

	// Now go flush each of those

	for _, inFlightFileInodeNumber = range inFlightFileInodeNumbers {
		vS.untrackInFlightFileInodeData(inFlightFileInodeNumber, true)
	}
}

func (vS *volumeStruct) inFlightFileInodeDataFlusher(inodeNumber inode.InodeNumber) {
	var (
		err         error
		inodeLock   *dlm.RWLockStruct
		stillExists bool
	)

	// Act as if a package fs client called Flush()...

	inodeLock, err = vS.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if nil != err {
		logger.PanicfWithError(err, "InitInodeLock() for volume '%s' inode %d failed", vS.volumeName, inodeNumber)
	}
	err = inodeLock.WriteLock()
	if nil != err {
		logger.PanicfWithError(err, "dlm.Writelock() for volume '%s' inode %d failed", vS.volumeName, inodeNumber)
	}

	stillExists = vS.inodeVolumeHandle.Access(inodeNumber, inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.F_OK,
		inode.NoOverride)
	if stillExists {
		err = vS.inodeVolumeHandle.Flush(inodeNumber, false)
		if nil == err {
			vS.untrackInFlightFileInodeData(inodeNumber, false)
		} else {
			logger.ErrorfWithError(err, "Flush of file data failed on volume '%s' inode %d", vS.volumeName, inodeNumber)
		}
	}

	err = inodeLock.Unlock()
	if nil != err {
		logger.PanicfWithError(err, "dlm.Unlock() for volume '%s' inode %d failed", vS.volumeName, inodeNumber)
	}
}

func (inFlightFileInodeData *inFlightFileInodeDataStruct) inFlightFileInodeDataTracker() {
	var (
		flushFirst bool
	)

	logger.Tracef("fs.inFlightFileInodeDataTracker(): waiting to flush volume '%s' inode %d",
		inFlightFileInodeData.volStruct.volumeName, inFlightFileInodeData.InodeNumber)

	select {
	case flushFirst = <-inFlightFileInodeData.control:
		// All we needed was the value of flushFirst from control chan
	case <-time.After(inFlightFileInodeData.volStruct.maxFlushTime):
		flushFirst = true
	}

	logger.Tracef("fs.inFlightFileInodeDataTracker(): flush starting for volume '%s' inode %d flushfirst %t",
		inFlightFileInodeData.volStruct.volumeName, inFlightFileInodeData.InodeNumber, flushFirst)

	if flushFirst {
		inFlightFileInodeData.volStruct.inFlightFileInodeDataFlusher(inFlightFileInodeData.InodeNumber)
	}

	inFlightFileInodeData.wg.Done()
}

func mount(volumeName string, mountOptions MountOptions) (mountHandle MountHandle, err error) {
	var (
		mS        *mountStruct
		ok        bool
		volStruct *volumeStruct
	)

	startTime := time.Now()
	defer func() {
		globals.MountUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MountErrors.Add(1)
		}
	}()

	globals.Lock()

	volStruct, ok = globals.volumeMap[volumeName]
	if !ok {
		err = fmt.Errorf("Unknown volumeName passed to mount(): \"%s\"", volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		globals.Unlock()
		return
	}

	globals.lastMountID++

	mS = &mountStruct{
		id:        globals.lastMountID,
		options:   mountOptions,
		volStruct: volStruct,
	}

	globals.mountMap[mS.id] = mS

	volStruct.dataMutex.Lock()
	volStruct.mountList = append(volStruct.mountList, mS.id)
	volStruct.dataMutex.Unlock()

	globals.Unlock()

	mountHandle = mS
	err = nil

	return
}

func (mS *mountStruct) Access(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, accessMode inode.InodeMode) (accessReturn bool) {

	startTime := time.Now()
	defer func() {
		globals.AccessUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	accessReturn = mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, accessMode,
		inode.NoOverride)
	return
}

func (mS *mountStruct) CallInodeToProvisionObject() (pPath string, err error) {

	startTime := time.Now()
	defer func() {
		globals.CallInodeToProvisionObjectUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.CallInodeToProvisionObjectErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	pPath, err = mS.volStruct.inodeVolumeHandle.ProvisionObject()
	return
}

func (mS *mountStruct) Create(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, dirInodeNumber inode.InodeNumber, basename string, filePerm inode.InodeMode) (fileInodeNumber inode.InodeNumber, err error) {
	startTime := time.Now()
	defer func() {
		globals.CreateUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.CreateErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	err = validateBaseName(basename)
	if err != nil {
		return 0, err
	}

	// Lock the directory inode before doing the link
	dirInodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(dirInodeNumber, nil)
	if err != nil {
		return 0, err
	}
	err = dirInodeLock.WriteLock()
	if err != nil {
		return 0, err
	}
	defer dirInodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(dirInodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		return 0, blunder.NewError(blunder.NotFoundError, "ENOENT")
	}
	if !mS.volStruct.inodeVolumeHandle.Access(dirInodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK,
		inode.NoOverride) {
		return 0, blunder.NewError(blunder.PermDeniedError, "EACCES")
	}

	// create the file and add it to the directory
	fileInodeNumber, err = mS.volStruct.inodeVolumeHandle.CreateFile(filePerm, userID, groupID)
	if err != nil {
		return 0, err
	}

	err = mS.volStruct.inodeVolumeHandle.Link(dirInodeNumber, basename, fileInodeNumber, false)
	if err != nil {
		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(fileInodeNumber)
		if destroyErr != nil {
			logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Link() in fs.Create", fileInodeNumber)
		}
		return 0, err
	}

	return fileInodeNumber, nil
}

func (mS *mountStruct) FetchReadPlan(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, offset uint64, length uint64) (readPlan []inode.ReadPlanStep, err error) {
	var (
		inodeLock   *dlm.RWLockStruct
		inodeType   inode.InodeType
		localLength uint64
		localOffset uint64
	)

	startTime := time.Now()
	defer func() {
		globals.FetchReadPlanUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.FetchReadPlanErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err = mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if nil != err {
		return
	}
	err = inodeLock.ReadLock()
	if nil != err {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	inodeType, err = mS.volStruct.inodeVolumeHandle.GetType(inodeNumber)
	if nil != err {
		logger.ErrorfWithError(err, "couldn't get type for inode %v", inodeNumber)
		return
	}
	// Make sure the inode number is for a file inode
	if inodeType != inode.FileType {
		err = fmt.Errorf("%s: expected inode %v to be a file inode, got %v", utils.GetFnName(), inodeNumber, inodeType)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFileError)
		return
	}

	localOffset = offset
	localLength = length

	readPlan, err = mS.volStruct.inodeVolumeHandle.GetReadPlan(inodeNumber, &localOffset, &localLength)
	if err != nil {
		return
	}

	if localOffset != offset {
		err = blunder.NewError(blunder.BadSeekError, "supplied offset (0x%016X) could not be honored", offset)
		return
	}

	return
}

func (mS *mountStruct) doInlineCheckpointIfEnabled() {
	var (
		err error
	)

	if !mS.volStruct.doCheckpointPerFlush {
		return
	}

	err = mS.volStruct.headhunterVolumeHandle.DoCheckpoint()
	if nil != err {
		logger.Fatalf("fs.doInlineCheckpoint() call to headhunter.DoCheckpoint() failed: %v", err)
	}
}

func (mS *mountStruct) Flush(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (err error) {

	startTime := time.Now()
	defer func() {
		globals.FlushUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.FlushErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		return blunder.NewError(blunder.NotFoundError, "ENOENT")
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK,
		inode.OwnerOverride) {
		return blunder.NewError(blunder.PermDeniedError, "EACCES")
	}

	err = mS.volStruct.inodeVolumeHandle.Flush(inodeNumber, false)
	mS.volStruct.untrackInFlightFileInodeData(inodeNumber, false)

	mS.doInlineCheckpointIfEnabled()

	return
}

func (mS *mountStruct) getFileLockList(inodeNumber inode.InodeNumber) (flockList *list.List) {
	mS.volStruct.dataMutex.Lock()
	defer mS.volStruct.dataMutex.Unlock()

	flockList, ok := mS.volStruct.FLockMap[inodeNumber]
	if !ok {
		flockList = new(list.List)
		mS.volStruct.FLockMap[inodeNumber] = flockList
	}

	return
}

// Check for lock conflict with other Pids, if there is a conflict then it will return the first occurance of conflicting range.
func checkConflict(elm *FlockStruct, flock *FlockStruct) bool {

	if flock.Pid == elm.Pid {
		return false
	}

	if (elm.Start+elm.Len) <= flock.Start || (flock.Start+flock.Len) <= elm.Start {
		return false
	}

	if (flock.Type == syscall.F_WRLCK) || (elm.Type == syscall.F_WRLCK) {
		return true
	}

	return false
}

func (mS *mountStruct) verifyLock(inodeNumber inode.InodeNumber, flock *FlockStruct) (conflictLock *FlockStruct) {
	flockList := mS.getFileLockList(inodeNumber)

	for e := flockList.Front(); e != nil; e = e.Next() {
		elm := e.Value.(*FlockStruct)

		if checkConflict(elm, flock) == true {
			return elm
		}
	}

	return nil
}

// Insert a file lock range to corresponding lock list for the pid.
// Assumption: There is no lock conflict and the range that is being inserted has no conflict and is free.
func (mS *mountStruct) fileLockInsert(inodeNumber inode.InodeNumber, inFlock *FlockStruct) (err error) {
	err = nil
	flockList := mS.getFileLockList(inodeNumber)

	overlapList := new(list.List)
	var beforeElm *list.Element // Refers to the immediate element that starts before the start of the range.
	var afterElm *list.Element  // Refers to the immediate element that starts after the end of the range.

	// flockList is sorted by starting offset of the range.
	// Inserting a range happens in two steps. 1) Check if there is any conflict and also identify the
	// point in the list where the entry will be added (before and after elements) 2) Then check if
	// the range can extend the before element, if so adjust it. 3) Simillarly, check if the after
	// element can be collapsed if it forms a contiguous range.

	for e := flockList.Front(); e != nil; e = e.Next() {
		elm := e.Value.(*FlockStruct)

		if (elm.Start + elm.Len) <= inFlock.Start {
			beforeElm = e
			continue
		}

		if elm.Start > (inFlock.Start + inFlock.Len) {
			afterElm = e
			if overlapList.Len() == 0 {
				flockList.InsertBefore(inFlock, e)
				return
			}

			break
		}

		if checkConflict(elm, inFlock) {
			err = blunder.AddError(nil, blunder.TryAgainError)
			return
		}

		if elm.Pid == inFlock.Pid {
			overlapList.PushBack(e)
		}
	}

	if overlapList.Len() == 0 {
		if beforeElm != nil {
			elm := beforeElm.Value.(*FlockStruct)
			if elm.Pid == inFlock.Pid && elm.Type == inFlock.Type && (elm.Start+elm.Len) == inFlock.Start {
				elm.Len = inFlock.Start + inFlock.Len - elm.Len
			} else {
				flockList.InsertAfter(inFlock, beforeElm)
			}
		} else {
			flockList.PushBack(inFlock)
		}

		return
	}

	// Look at the last element in the overlapping list
	lastEnt := overlapList.Back()
	e := lastEnt.Value.(*list.Element)
	elm := e.Value.(*FlockStruct)
	if (elm.Start + elm.Len) > (inFlock.Start + inFlock.Len) {
		inFlock.Len = (elm.Start + elm.Len) - inFlock.Start
	}

	// We can delete all the entries in the overlapping list. These entries are replaced by
	// the range we are inserting.
	for e := overlapList.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*list.Element)
		flockList.Remove(entry)
	}

	// Now adjust the before and after entries:
	// First adjust the after:
	if afterElm != nil {
		elm := afterElm.Value.(*FlockStruct)
		if elm.Pid == inFlock.Pid && elm.Type == inFlock.Type && (inFlock.Start+inFlock.Len) == elm.Start {
			// We can collapse the entry:
			elm.Len = elm.Start + elm.Len - inFlock.Start
			elm.Start = inFlock.Start

			if beforeElm != nil {
				belm := beforeElm.Value.(*FlockStruct)
				if belm.Pid == elm.Pid && belm.Type == elm.Type && (belm.Start+belm.Len) == elm.Start {
					belm.Len = elm.Start + elm.Len - belm.Start
					flockList.Remove(afterElm)
				}
			}

			return
		}
	}

	if beforeElm != nil {
		belm := beforeElm.Value.(*FlockStruct)
		if belm.Pid == inFlock.Pid && belm.Type == inFlock.Type && (belm.Start+belm.Len) == inFlock.Start {
			belm.Len = inFlock.Start + inFlock.Len - belm.Start
		}

		flockList.InsertAfter(inFlock, beforeElm)
		return
	}

	if afterElm != nil {
		flockList.InsertBefore(inFlock, afterElm)
	} else {
		flockList.PushBack(inFlock)
	}

	return

}

// Unlock a given range. All locks held in this range by the process (indentified by Pid) are removed.
func (mS *mountStruct) fileUnlock(inodeNumber inode.InodeNumber, inFlock *FlockStruct) (err error) {

	flockList := mS.getFileLockList(inodeNumber)
	if flockList == nil {
		logger.Warnf("Unlock of a region not already locked - %+v", inFlock)
		return
	}

	start := inFlock.Start
	len := inFlock.Len

	removeList := new(list.List)

	for e := flockList.Front(); e != nil; e = e.Next() {
		elm := e.Value.(*FlockStruct)

		if elm.Pid != inFlock.Pid {
			continue
		}

		if (elm.Start + elm.Len) < start {
			continue
		}

		if elm.Start >= (start + len) {
			break
		}

		// If the lock falls completely in the range, delete it.
		if elm.Start >= start && (elm.Start+elm.Len) <= (start+len) {
			removeList.PushBack(e)
			continue
		}

		// This lock overlapps with the range - three possibalities 1) lock starts before the range, 2) end after range and 3) both.

		elmLen := elm.Start + elm.Len // Save the original length, it is required in case of #3 (both)

		if elm.Start < start { // Handle the first part - lock starts before the range.
			elm.Len = start - elm.Start
		}

		if elmLen > (start + len) { // Lock extends beyond the unlock range.
			if elm.Start > start { // case #2
				// use the existing record
				elm.Start = start + len
				elm.Len = elmLen - elm.Start
				break
			}

			// Create a new record - handle case #3 both (starts before the range and extends beyond the range)
			elmTail := new(FlockStruct)
			elmTail.Start = start + len
			elmTail.Len = elmLen - elm.Start
			elmTail.Pid = elm.Pid
			elmTail.Type = elm.Type
			elmTail.Whence = elm.Whence
			flockList.InsertAfter(elmTail, e)
			break
		}
	}

	for e := removeList.Front(); e != nil; e = e.Next() {
		elm := e.Value.(*list.Element)
		flockList.Remove(elm)
	}

	return
}

// Implements file locking conforming to fcntl(2) locking description. F_SETLKW is not implemented. Supports F_SETLW and F_GETLW.
// whence: FS supports only SEEK_SET - starting from 0, since it does not manage file handles, caller is expected to supply the start and length relative to offset ZERO.
func (mS *mountStruct) Flock(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, lockCmd int32, inFlock *FlockStruct) (outFlock *FlockStruct, err error) {

	startTime := time.Now()
	defer func() {
		switch lockCmd {

		case syscall.F_GETLK:
			globals.FlockGetUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
			if err != nil {
				globals.FlockGetErrors.Add(1)
			}

		case syscall.F_SETLK:
			if inFlock.Type == syscall.F_UNLCK {
				globals.FlockUnlockUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
				if err != nil {
					globals.FlockUnlockErrors.Add(1)
				}

			} else if inFlock.Type == syscall.F_WRLCK || inFlock.Type == syscall.F_RDLCK {
				globals.FlockLockUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
				if err != nil {
					globals.FlockLockErrors.Add(1)
				}
			} else {
				globals.FlockOtherErrors.Add(1)
			}

		default:
			globals.FlockOtherErrors.Add(1)
		}

	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	outFlock = inFlock

	if lockCmd == syscall.F_SETLKW {
		err = blunder.AddError(nil, blunder.NotSupportedError)
		return
	}

	// Make sure the inode does not go away, while we are applying the flock.
	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK, inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK, inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	if inFlock.Len == 0 { // If length is ZERO means treat it as whole file.
		inFlock.Len = ^uint64(0)
	}

	switch lockCmd {
	case syscall.F_GETLK:
		conflictLock := mS.verifyLock(inodeNumber, inFlock)
		if conflictLock != nil {
			outFlock = conflictLock
			err = blunder.AddError(nil, blunder.TryAgainError)
		} else {
			outFlock = inFlock
			outFlock.Type = syscall.F_UNLCK
		}
		break

	case syscall.F_SETLK:
		if inFlock.Type == syscall.F_UNLCK {
			err = mS.fileUnlock(inodeNumber, inFlock)

		} else if inFlock.Type == syscall.F_WRLCK || inFlock.Type == syscall.F_RDLCK {
			err = mS.fileLockInsert(inodeNumber, inFlock)

		} else {
			err = blunder.NewError(blunder.InvalidArgError, "EINVAL")
			return
		}
		break

	default:
		err = blunder.NewError(blunder.InvalidArgError, "EINVAL")
		return
	}

	return
}

func (mS *mountStruct) getstatHelper(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (stat Stat, err error) {

	lockID, err := mS.volStruct.inodeVolumeHandle.MakeLockID(inodeNumber)
	if err != nil {
		return
	}
	if !dlm.IsLockHeld(lockID, callerID, dlm.ANYLOCK) {
		err = fmt.Errorf("%s: inode %v lock must be held before calling", utils.GetFnName(), inodeNumber)
		return nil, blunder.AddError(err, blunder.NotFoundError)
	}

	stat = make(map[StatKey]uint64)

	metadata, err := mS.volStruct.inodeVolumeHandle.GetMetadata(inodeNumber)

	if err != nil {
		return nil, err
	}

	stat[StatCRTime] = uint64(metadata.CreationTime.UnixNano())
	stat[StatMTime] = uint64(metadata.ModificationTime.UnixNano())
	stat[StatCTime] = uint64(metadata.AttrChangeTime.UnixNano())
	stat[StatATime] = uint64(metadata.AccessTime.UnixNano())
	stat[StatSize] = metadata.Size
	stat[StatNLink] = metadata.LinkCount
	stat[StatFType] = uint64(metadata.InodeType)
	stat[StatINum] = uint64(inodeNumber)
	stat[StatMode] = uint64(metadata.Mode)
	stat[StatUserID] = uint64(metadata.UserID)
	stat[StatGroupID] = uint64(metadata.GroupID)
	stat[StatNumWrites] = metadata.NumWrites

	return stat, nil
}

func (mS *mountStruct) Getstat(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (stat Stat, err error) {

	startTime := time.Now()
	defer func() {
		globals.GetstatUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.GetstatErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	// Call getstat helper function to do the work
	return mS.getstatHelper(inodeNumber, inodeLock.GetCallerID())
}

func (mS *mountStruct) getTypeHelper(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (inodeType inode.InodeType, err error) {

	lockID, err := mS.volStruct.inodeVolumeHandle.MakeLockID(inodeNumber)
	if err != nil {
		return
	}
	if !dlm.IsLockHeld(lockID, callerID, dlm.ANYLOCK) {
		err = fmt.Errorf("%s: inode %v lock must be held before calling.", utils.GetFnName(), inodeNumber)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	inodeType, err = mS.volStruct.inodeVolumeHandle.GetType(inodeNumber)
	if err != nil {
		logger.ErrorWithError(err, "couldn't get inode type")
		return inodeType, err
	}
	return inodeType, nil
}

func (mS *mountStruct) GetType(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeType inode.InodeType, err error) {

	startTime := time.Now()
	defer func() {
		globals.GetTypeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.GetTypeErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	return mS.getTypeHelper(inodeNumber, inodeLock.GetCallerID())
}

func (mS *mountStruct) GetXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, streamName string) (value []byte, err error) {

	startTime := time.Now()
	defer func() {
		globals.GetXAttrUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.GetXAttrErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	value, err = mS.volStruct.inodeVolumeHandle.GetStream(inodeNumber, streamName)
	if err != nil {
		// Did not find the requested stream. However this isn't really an error since
		// samba will ask for acl-related streams and is fine with not finding them.
		logger.TracefWithError(err, "Failed to get XAttr %v of inode %v", streamName, inodeNumber)
	}

	return
}

func (mS *mountStruct) IsDir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeIsDir bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.IsDirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.IsDirErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	lockID, err := mS.volStruct.inodeVolumeHandle.MakeLockID(inodeNumber)
	if err != nil {
		return
	}
	if !dlm.IsLockHeld(lockID, inodeLock.GetCallerID(), dlm.ANYLOCK) {
		err = fmt.Errorf("%s: inode %v lock must be held before calling", utils.GetFnName(), inodeNumber)
		return false, blunder.AddError(err, blunder.NotFoundError)
	}

	inodeType, err := mS.volStruct.inodeVolumeHandle.GetType(inodeNumber)
	if err != nil {
		return false, err
	}
	return inodeType == inode.DirType, nil
}

func (mS *mountStruct) IsFile(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeIsFile bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.IsFileUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.IsFileErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	inodeType, err := mS.volStruct.inodeVolumeHandle.GetType(inodeNumber)
	if err != nil {
		return false, err
	}

	return inodeType == inode.FileType, nil
}

func (mS *mountStruct) IsSymlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeIsSymlink bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.IsSymlinkUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.IsSymlinkErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	inodeType, err := mS.volStruct.inodeVolumeHandle.GetType(inodeNumber)
	if err != nil {
		return false, err
	}

	return inodeType == inode.SymlinkType, nil
}

func (mS *mountStruct) Link(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, dirInodeNumber inode.InodeNumber, basename string, targetInodeNumber inode.InodeNumber) (err error) {

	startTime := time.Now()
	defer func() {
		globals.LinkUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.LinkErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	var (
		inodeType inode.InodeType
	)

	err = validateBaseName(basename)
	if err != nil {
		return
	}

	// We need both dirInodelock and the targetInode lock to make sure they
	// don't go away and linkCount is updated correctly.
	callerID := dlm.GenerateCallerID()
	dirInodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(dirInodeNumber, callerID)
	if err != nil {
		return
	}

	targetInodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(targetInodeNumber, callerID)
	if err != nil {
		return
	}

	// Lock the target inode to check its type and insure its not a directory (if it is a
	// directory then locking it after the target directory could result in deadlock).
	err = targetInodeLock.WriteLock()
	if err != nil {
		return
	}

	// make sure target inode is not a directory
	inodeType, err = mS.volStruct.inodeVolumeHandle.GetType(targetInodeNumber)
	if err != nil {
		targetInodeLock.Unlock()
		// Because we know that GetType() has already "blunderized" the error, we just pass it on
		logger.ErrorfWithError(err, "%s: couldn't get type for inode %v", utils.GetFnName(), targetInodeNumber)
		return err
	}
	if inodeType == inode.DirType {
		targetInodeLock.Unlock()
		// no need to print an error when its a mistake by the client
		err = fmt.Errorf("%s: inode %v cannot be a dir inode", utils.GetFnName(), targetInodeNumber)
		return blunder.AddError(err, blunder.LinkDirError)
	}

	// drop the target inode lock so we can get the directory lock then
	// reget the target inode lock
	targetInodeLock.Unlock()

	err = dirInodeLock.WriteLock()
	if err != nil {
		return
	}
	defer dirInodeLock.Unlock()

	err = targetInodeLock.WriteLock()
	if err != nil {
		return
	}
	defer targetInodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(dirInodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(targetInodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(dirInodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	err = mS.volStruct.inodeVolumeHandle.Link(dirInodeNumber, basename, targetInodeNumber, false)

	// if the link was successful and this is a regular file then any
	// pending data was flushed
	if err == nil && inodeType == inode.FileType {
		mS.volStruct.untrackInFlightFileInodeData(targetInodeNumber, false)
	}

	return err
}

func (mS *mountStruct) ListXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (streamNames []string, err error) {

	startTime := time.Now()
	defer func() {
		globals.ListXAttrUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.ListXAttrErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	metadata, err := mS.volStruct.inodeVolumeHandle.GetMetadata(inodeNumber)
	if err != nil {
		// Did not find the requested stream. However this isn't really an error since
		// samba will ask for acl-related streams and is fine with not finding them.
		logger.TracefWithError(err, "Failed to list XAttrs of inode %v", inodeNumber)
		return
	}

	streamNames = make([]string, len(metadata.InodeStreamNameSlice))
	copy(streamNames, metadata.InodeStreamNameSlice)
	return
}

func (mS *mountStruct) Lookup(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, dirInodeNumber inode.InodeNumber, basename string) (inodeNumber inode.InodeNumber, err error) {

	startTime := time.Now()
	defer func() {
		globals.LookupUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.LookupErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	dirInodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(dirInodeNumber, nil)
	if err != nil {
		return
	}
	dirInodeLock.ReadLock()
	defer dirInodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(dirInodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(dirInodeNumber, userID, groupID, otherGroupIDs, inode.X_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	inodeNumber, err = mS.volStruct.inodeVolumeHandle.Lookup(dirInodeNumber, basename)
	return inodeNumber, err
}

func (mS *mountStruct) LookupPath(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, fullpath string) (inodeNumber inode.InodeNumber, err error) {

	startTime := time.Now()
	defer func() {
		globals.LookupPathUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.LookupPathErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	// In the special case of a fullpath starting with "/", the path segment splitting above
	// results in a first segment that still begins with "/". Because this is not recognized
	// as a real path segment, by the underlying code, we have trouble looking it up.
	//
	// This is a hack to work around this case until I figure out a better way.
	newfullpath := strings.TrimPrefix(fullpath, "/")
	if strings.Compare(fullpath, newfullpath) != 0 {
		fullpath = newfullpath
	}

	pathSegments := strings.Split(path.Clean(fullpath), "/")

	cursorInodeNumber := inode.RootDirInodeNumber
	for _, segment := range pathSegments {
		cursorInodeLock, err1 := mS.volStruct.inodeVolumeHandle.InitInodeLock(cursorInodeNumber, nil)
		if err = err1; err != nil {
			return
		}
		err = cursorInodeLock.ReadLock()
		if err != nil {
			return
		}

		if !mS.volStruct.inodeVolumeHandle.Access(cursorInodeNumber, userID, groupID, otherGroupIDs, inode.X_OK,
			inode.NoOverride) {
			cursorInodeLock.Unlock()
			err = blunder.NewError(blunder.PermDeniedError, "EACCES")
			return
		}

		cursorInodeNumber, err = mS.volStruct.inodeVolumeHandle.Lookup(cursorInodeNumber, segment)
		cursorInodeLock.Unlock()

		if err != nil {
			return cursorInodeNumber, err
		}
	}

	return cursorInodeNumber, nil
}

func (mS *mountStruct) MiddlewareCoalesce(destPath string, elementPaths []string) (ino uint64, numWrites uint64, modificationTime uint64, err error) {

	var coalesceSize uint64
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

	// it'll hold a dir lock and a file lock for each element path, plus a lock on the destination dir and the root dir
	heldLocks := make([]*dlm.RWLockStruct, 0, 2*len(elementPaths)+2)
	defer func() {
		for _, lock := range heldLocks {
			if lock != nil {
				lock.Unlock()
			}
		}
	}()

	elementDirAndFileNames := make(dirAndFileNameSlice, 0, len(elementPaths))
	coalesceElements := make([]inode.CoalesceElement, 0, len(elementPaths))

	for _, path := range elementPaths {
		dirName, fileName := filepath.Split(path)
		if dirName == "" {
			err = fmt.Errorf("Files to coalesce must not be in the root directory")
			return
		} else {
			// filepath.Split leaves the trailing slash on the directory file name; the only time it doesn't is if you
			// split something lacking slashes, e.g. "file.txt", in which case dirName is "" and we've already returned
			// an error.
			dirName = dirName[0 : len(dirName)-1]
		}

		elementDirAndFileNames = append(elementDirAndFileNames, dirAndFileName{
			dirName:  dirName,
			fileName: fileName,
		})
	}

	destDirName, destFileName := filepath.Split(destPath)
	if destDirName == "" {
		// NB: the middleware won't ever call us with a destination file in the root directory, as that would look like
		// a container path.
		err = fmt.Errorf("Coalesce target must not be in the root directory")
		return
	}
	destDirName = destDirName[0 : len(destDirName)-1] // chop off trailing slash

	// We lock things in whatever order the caller provides them. To protect against deadlocks with other concurrent
	// calls to this function, we first obtain a write lock on the root inode.
	//
	// One might think there's some way to sort these paths and do something smarter, but then one would remember that
	// symlinks exist and so one cannot do anything useful with paths. For example, given two directory paths, which is
	// more deeply nested, a/b/c or d/e/f/g/h? It might be the first one, if b is a symlink to b1/b2/b3/b4/b5, or it
	// might not.
	//
	// This function is called infrequently enough that we can probably get away with the big heavy lock without causing
	// too much trouble.
	callerID := dlm.GenerateCallerID()

	rootDirInodeLock, err := mS.volStruct.inodeVolumeHandle.GetWriteLock(inode.RootDirInodeNumber, callerID)
	if err != nil {
		return
	}
	heldLocks = append(heldLocks, rootDirInodeLock)

	// Walk the path one directory at a time, stopping when we either encounter a missing path component or when we've
	// gone through the whole path.
	destDirPathComponents := strings.Split(destDirName, "/")

	cursorInodeNumber := inode.RootDirInodeNumber
	var cursorInodeLock *dlm.RWLockStruct // deliberately starts as nil; we have a lock on the root dir already

	defer func() {
		if cursorInodeLock != nil {
			cursorInodeLock.Unlock()
		}
	}()

	// Resolve as much of the path as exists
	for len(destDirPathComponents) > 0 {
		pathComponent := destDirPathComponents[0]
		// We have to look up the dirent ourselves instead of letting resolvePath do it so we can distinguish between
		// the following cases:
		//
		// (A) dirent doesn't exist, in which case we break out of this loop and start creating directories
		//
		// (B) dirent does exist, but references a broken symlink, so resolvePath returns NotFoundError, in which case
		// we return an error
		_, err1 := mS.volStruct.inodeVolumeHandle.Lookup(cursorInodeNumber, pathComponent)
		if err1 != nil {
			if blunder.Is(err1, blunder.NotFoundError) {
				// We found a dir entry that doesn't exist; now we start making directories.
				break
			} else {
				// Mystery error; bail out
				err = err1
				return
			}
		}

		// Resolve one path component and advance the cursor
		nextCursorInodeNumber, nextCursorInodeType, nextCursorInodeLock, err1 := mS.resolvePath(pathComponent, callerID, cursorInodeNumber, mS.volStruct.inodeVolumeHandle.EnsureWriteLock)
		if err1 != nil {
			err = err1
			return
		}
		if nextCursorInodeType != inode.DirType {
			// Every path component must resolve to a directory. There may be symlinks along the way, but resolvePath
			// takes care of following those.
			if nextCursorInodeLock != nil {
				nextCursorInodeLock.Unlock()
			}
			err = blunder.NewError(blunder.NotDirError, "%v is not a directory", pathComponent)
			return
		}

		if cursorInodeLock != nil {
			cursorInodeLock.Unlock()
		}
		cursorInodeNumber = nextCursorInodeNumber
		cursorInodeLock = nextCursorInodeLock
		destDirPathComponents = destDirPathComponents[1:]
	}

	// Make any missing directory entires
	for len(destDirPathComponents) > 0 {
		pathComponent := destDirPathComponents[0]
		// can't use Mkdir since it wants to take its own lock, so we make and link the dir ourselves

		newDirInodeNumber, err1 := mS.volStruct.inodeVolumeHandle.CreateDir(inode.InodeMode(0755), inode.InodeRootUserID, inode.InodeGroupID(0))
		if err1 != nil {
			logger.ErrorWithError(err1)
			err = err1
			return
		}

		err = mS.volStruct.inodeVolumeHandle.Link(cursorInodeNumber, pathComponent, newDirInodeNumber, false)
		if err != nil {
			destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(newDirInodeNumber)
			if destroyErr != nil {
				logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Link() in fs.MiddlewareCoalesce", newDirInodeNumber)
			}
			return
		}

		if cursorInodeLock != nil {
			cursorInodeLock.Unlock()
		}
		destDirPathComponents = destDirPathComponents[1:]
		cursorInodeNumber = newDirInodeNumber
		cursorInodeLock, err1 = mS.volStruct.inodeVolumeHandle.EnsureWriteLock(newDirInodeNumber, callerID)
		if err1 != nil {
			err = err1
			return
		}
	}

	for _, entry := range elementDirAndFileNames {
		dirInodeNumber, dirInodeType, dirInodeLock, err1 := mS.resolvePathForWrite(entry.dirName, callerID)
		if err1 != nil {
			err = err1
			return
		}
		if dirInodeLock != nil {
			heldLocks = append(heldLocks, dirInodeLock)
		}

		if dirInodeType != inode.DirType {
			err = blunder.NewError(blunder.NotDirError, "%s is not a directory", entry.dirName)
			return
		}

		fileInodeNumber, err1 := mS.volStruct.inodeVolumeHandle.Lookup(dirInodeNumber, entry.fileName)
		if err1 != nil {
			err = err1
			return
		}

		fileInodeLock, err1 := mS.volStruct.inodeVolumeHandle.InitInodeLock(fileInodeNumber, callerID)
		if err1 != nil {
			err = err1
			return
		}
		if !fileInodeLock.IsWriteHeld() {
			err = fileInodeLock.WriteLock()
			if err != nil {
				return
			}
			heldLocks = append(heldLocks, fileInodeLock)
		}

		fileMetadata, err1 := mS.volStruct.inodeVolumeHandle.GetMetadata(fileInodeNumber)
		if err1 != nil {
			err = err1
			return
		}
		if fileMetadata.InodeType != inode.FileType {
			err = blunder.NewError(blunder.NotFileError, "%s/%s is not an ordinary file", entry.dirName, entry.fileName)
			return
		}

		coalesceElements = append(coalesceElements, inode.CoalesceElement{
			ContainingDirectoryInodeNumber: dirInodeNumber,
			ElementInodeNumber:             fileInodeNumber,
			ElementName:                    entry.fileName,
		})
	}

	// We've now jumped through all the requisite hoops to get the required locks, so now we can call inode.Coalesce and
	// do something useful
	destInodeNumber, mtime, numWrites, coalesceSize, err :=
		mS.volStruct.inodeVolumeHandle.Coalesce(cursorInodeNumber, destFileName, coalesceElements)
	ino = uint64(destInodeNumber)
	modificationTime = uint64(mtime.UnixNano())
	return
}

func (mS *mountStruct) MiddlewareDelete(parentDir string, baseName string) (err error) {

	startTime := time.Now()
	defer func() {
		globals.MiddlewareDeleteUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareDeleteErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	// Get the inode, type, and lock for the parent directory
	parentInodeNumber, parentInodeType, parentDirLock, err := mS.resolvePathForWrite(parentDir, nil)
	if err != nil {
		return err
	}
	defer parentDirLock.Unlock()
	if parentInodeType != inode.DirType {
		err = blunder.NewError(blunder.NotDirError, "%s is a file", parentDir)
	}

	// We will need both parentDir lock to Unlink() and baseInode lock.
	baseNameInodeNumber, err := mS.volStruct.inodeVolumeHandle.Lookup(parentInodeNumber, baseName)
	if err != nil {
		return err
	}
	baseInodeLock, err := mS.volStruct.inodeVolumeHandle.GetWriteLock(baseNameInodeNumber, parentDirLock.GetCallerID())
	if nil != err {
		return
	}
	defer baseInodeLock.Unlock()

	inodeType, err := mS.volStruct.inodeVolumeHandle.GetType(baseNameInodeNumber)
	if nil != err {
		return
	}

	var doDestroy bool

	if inodeType == inode.DirType {
		dirEntries, nonShadowingErr := mS.volStruct.inodeVolumeHandle.NumDirEntries(baseNameInodeNumber)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if 2 != dirEntries {
			err = fmt.Errorf("Directory not empty")
			err = blunder.AddError(err, blunder.NotEmptyError)
			return
		}

		// LinkCount must == 2 ("." and "..") since we don't allow hardlinks to DirInode's

		doDestroy = true
	} else { // inodeType != inode.DirType
		basenameLinkCount, nonShadowingErr := mS.volStruct.inodeVolumeHandle.GetLinkCount(baseNameInodeNumber)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		doDestroy = (1 == basenameLinkCount)
	}

	// At this point, we *are* going to Unlink... and optionally Destroy... the inode

	err = mS.volStruct.inodeVolumeHandle.Unlink(parentInodeNumber, baseName, false)
	if nil != err {
		return
	}

	if doDestroy {
		err = mS.volStruct.inodeVolumeHandle.Destroy(baseNameInodeNumber)
		if nil != err {
			return err
		}
		mS.volStruct.untrackInFlightFileInodeData(baseNameInodeNumber, false)
	}

	return
}

func (mS *mountStruct) MiddlewareGetAccount(maxEntries uint64, marker string) (accountEnts []AccountEntry, mtime uint64, ctime uint64, err error) {

	startTime := time.Now()
	defer func() {
		globals.MiddlewareGetAccountUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareGetAccountErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	statResult, err := mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber)
	if err != nil {
		return
	}
	mtime = statResult[StatMTime]
	ctime = statResult[StatCTime]

	// List the root directory, starting at the marker, and keep only
	// the directories. The Swift API doesn't let you have objects in
	// an account, so files or symlinks don't belong in an account
	// listing.
	areMoreEntries := true
	lastBasename := marker
	for areMoreEntries && uint64(len(accountEnts)) < maxEntries {
		var dirEnts []inode.DirEntry
		dirEnts, _, areMoreEntries, err = mS.Readdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, lastBasename, maxEntries-uint64(len(accountEnts)), 0)
		if err != nil {
			if blunder.Is(err, blunder.NotFoundError) {
				// Readdir gives you a NotFoundError if you ask for a
				// lastBasename that's lexicographically greater than
				// the last entry in the directory.
				//
				// For account listings, it's not an error to set
				// marker=$PAST_END where $PAST_END is greater than
				// the last container in the account; you simply get
				// back an empty listing.
				//
				// Therefore, we treat this as though Readdir returned
				// 0 entries.
				err = nil
				break
			} else {
				return
			}
		}

		for _, dirEnt := range dirEnts {
			if dirEnt.Basename == "." || dirEnt.Basename == ".." {
				continue
			}

			statResult, err1 := mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirEnt.InodeNumber)
			if err1 != nil {
				err = err1
				return
			}
			if inode.InodeType(statResult[StatFType]) != inode.DirType {
				// Yes, there might be files or symlinks in here, but the Swift API wouldn't know what to do with them.
				continue
			}
			accountEnts = append(accountEnts, AccountEntry{
				Basename:         dirEnt.Basename,
				ModificationTime: statResult[StatMTime],
				AttrChangeTime:   statResult[StatCTime],
			})
		}
		if len(dirEnts) == 0 {
			break
		} else {
			lastBasename = dirEnts[len(dirEnts)-1].Basename
		}
	}
	return
}

func (mS *mountStruct) MiddlewareGetContainer(vContainerName string, maxEntries uint64, marker string, prefix string, delimiter string) (containerEnts []ContainerEntry, err error) {

	startTime := time.Now()
	defer func() {
		globals.MiddlewareGetContainerUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareGetContainerErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	ino, _, inoLock, err := mS.resolvePathForRead(vContainerName, nil)
	if err != nil {
		return
	}
	// Because a container listing can take a long time to generate,
	// we don't hold locks for the whole time. While this might lead
	// to some small inconsistencies (like a new file created halfway
	// through a call to MiddlewareGetContainer being omitted from the
	// listing), this is mitigated by two things. First, this lets us
	// accept new PUTs and writes while generating a container
	// listing, and second, Swift container listings are subject to
	// all sorts of temporary inconsistencies, so this is no worse
	// than what a Swift client would normally have to put up with.
	inoLock.Unlock()

	containerEnts = make([]ContainerEntry, 0)
	var recursiveReaddirPlus func(dirName string, dirInode inode.InodeNumber) error
	recursiveReaddirPlus = func(dirName string, dirInode inode.InodeNumber) error {
		var dirEnts []inode.DirEntry
		var recursiveDescents []dirToDescend
		areMoreEntries := true
		lastBasename := ""
		var delimiterSet bool
		if delimiter != "" {
			delimiterSet = true
		} else {
			delimiterSet = false
		}
		prefixMatched := false

		// Note that we're taking advantage of the fact that
		// Readdir() returns things in lexicographic order, which
		// is the same as our desired order. This lets us avoid
		// reading the whole directory only to sort it.
		for (areMoreEntries || len(dirEnts) > 0 || len(recursiveDescents) > 0) && uint64(len(containerEnts)) < maxEntries {
			// If we've run out of real directory entries, load some more.
			if areMoreEntries && len(dirEnts) == 0 {
				dirEnts, _, areMoreEntries, err = mS.Readdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirInode, lastBasename, maxEntries-uint64(len(containerEnts)), 0)
				if err != nil {
					logger.ErrorfWithError(err, "MiddlewareGetContainer: error reading directory %s (inode %v)", dirName, dirInode)
					return err
				}
				if len(dirEnts) > 0 {
					// If there's no dirEnts here, then areMoreEntries
					// is false, so we'll never call Readdir again,
					// and thus it doesn't matter what the value of
					// lastBasename is.
					lastBasename = dirEnts[len(dirEnts)-1].Basename
				}
			}

			// Ignore these early so we can stop thinking about them
			if len(dirEnts) > 0 && (dirEnts[0].Basename == "." || dirEnts[0].Basename == "..") {
				dirEnts = dirEnts[1:]
				continue
			}

			// If we've got pending recursive descents that should go before the next dirEnt, handle them
			// If delimiter has been set and prefix has been matched, it means we're only interested in the
			// current level in the hierarchy, so we want to stop recursion.
			for len(recursiveDescents) > 0 && (len(dirEnts) == 0 || (recursiveDescents[0].name < dirEnts[0].Basename)) && (!delimiterSet || !prefixMatched) {
				err = recursiveReaddirPlus(recursiveDescents[0].path, recursiveDescents[0].ino)
				if err != nil {
					// already logged
					return err
				}
				if uint64(len(containerEnts)) >= maxEntries {
					// we're finished here
					return nil
				}
				recursiveDescents = recursiveDescents[1:]
			}

			// Handle just one dirEnt per loop iteration. That lets us
			// avoid having to refill dirEnts at more than one
			// location in the code.
			if !(len(dirEnts) > 0) {
				if delimiterSet && prefixMatched {
					// We ran out of dirEnts and we won't do more recursion because the delimiter
					// was set and the prefix was already matched => return!
					return nil
				} else {
					continue
				}
			}

			dirEnt := dirEnts[0]
			dirEnts = dirEnts[1:]

			fileName := dirEnt.Basename
			if len(dirName) > 0 {
				fileName = dirName + dirEnt.Basename
			}

			if fileName > prefix && !strings.HasPrefix(fileName, prefix) {
				// Remember that we're going over these in order, so the first time we see something that's greater that
				// the prefix but doesn't start with it, we can skip the entire rest of the directory entries since they
				// are *also* greater than the prefix but don't start with it.
				return nil
			}

			// Swift container listings are paginated; you
			// retrieve the first page with a simple GET
			// <container>, then you retrieve each subsequent page
			// with a GET <container>?marker=<last-obj-returned>.
			//
			// If we were given a marker, then we can prune the
			// directory tree that we're walking.
			//
			// For a regular file, if its container-relative path
			// is lexicographically less than or equal to the
			// marker, we skip it.
			//
			// For a directory, if its container-relative path is
			// lexicographically less than or equal to the marker
			// and the marker does not begin with the directory's
			// path, we skip it.
			//
			// Since no regular file's container-relative path
			// starts with another regular file's
			// container-relative path, we can make the following
			// test prior to any Getstat() calls, avoiding
			// unneeded IO.
			if fileName <= marker && strings.Index(marker, fileName) != 0 {
				continue
			}

			statResult, err := mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirEnt.InodeNumber) // TODO: fix this
			if err != nil {
				logger.ErrorfWithError(err, "MiddlewareGetContainer: error in Getstat of %s", fileName)
				return err
			}

			fileType := inode.InodeType(statResult[StatFType])

			if fileType == inode.FileType || fileType == inode.SymlinkType {
				if fileName <= marker {
					continue
				}
				if !strings.HasPrefix(fileName, prefix) {
					continue
				}
				if delimiterSet {
					// We only care about 'prefixMatched' if delimiter was set
					prefixMatched = true
				}

				// Alternate data streams live in the inode, so this is almost certainly still cached from the Getstat()
				// call, and hence is very cheap to retrieve.
				serializedMetadata, err := mS.volStruct.inodeVolumeHandle.GetStream(dirEnt.InodeNumber, MiddlewareStream)

				// It's okay if there's no such stream; we just treat it as empty metadata. The middleware handles it.
				if err != nil && blunder.IsNot(err, blunder.StreamNotFound) {
					return err
				}

				containerEnt := ContainerEntry{
					Basename:         fileName,
					FileSize:         statResult[StatSize],
					ModificationTime: statResult[StatMTime],
					AttrChangeTime:   statResult[StatCTime],
					NumWrites:        statResult[StatNumWrites],
					InodeNumber:      statResult[StatINum],
					IsDir:            false,
					Metadata:         serializedMetadata,
				}
				containerEnts = append(containerEnts, containerEnt)
			} else {
				if !strings.HasPrefix(fileName, prefix) && !strings.HasPrefix(prefix, fileName) {
					continue
				}

				// Directories are handled specially. For a directory
				// "some-dir", we put an entry for "some-dir" in the
				// container listing, then put "some-dir/" into
				// recursiveDescents (note the trailing slash). This
				// lets us put off the descent until we have handled
				// all dirEnts coming before "some-dir/".
				//
				// For example, consider a filesystem with a dir "d",
				// a file "d/f", and a file "d-README".
				// Lexicographically, these would be ordered "d",
				// "d-README", "d/f" ("-" is ASCII 45, "/" is ASCII
				// 47). If we recursed into d immediately upon
				// encountering it, we would have "d/f" before
				// "d-README", which is not what the Swift API
				// demands.
				if fileName > marker && strings.HasPrefix(fileName, prefix) {
					if delimiterSet {
						// We only care about 'prefixMatched' if delimiter was set
						prefixMatched = true
					}
					containerEnt := ContainerEntry{
						Basename:         fileName,
						FileSize:         0,
						ModificationTime: statResult[StatMTime],
						AttrChangeTime:   statResult[StatCTime],
						NumWrites:        statResult[StatNumWrites],
						InodeNumber:      statResult[StatINum],
						IsDir:            true,
					}
					containerEnts = append(containerEnts, containerEnt)
				}
				recursiveDescents = append(recursiveDescents, dirToDescend{path: fileName + "/", name: dirEnt.Basename + "/", ino: dirEnt.InodeNumber})
			}
		}
		return nil
	}

	err = recursiveReaddirPlus("", ino)
	if err != nil {
		// already logged
		return
	}
	return
}

func (mS *mountStruct) MiddlewareGetObject(volumeName string, containerObjectPath string, readRangeIn []ReadRangeIn, readRangeOut *[]inode.ReadPlanStep) (fileSize uint64, lastModified uint64, lastChanged uint64, ino uint64, numWrites uint64, serializedMetadata []byte, err error) {

	startTime := time.Now()
	defer func() {
		var totalReadBytes uint64
		for _, step := range *readRangeOut {
			totalReadBytes += step.Length
		}

		globals.MiddlewareGetObjectUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.MiddlewareGetObjectBytes.Add(totalReadBytes)
		if err != nil {
			globals.MiddlewareGetObjectErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeNumber, inodeType, inodeLock, err := mS.resolvePathForRead(containerObjectPath, nil)
	ino = uint64(inodeNumber)
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	// If resolvePathForRead succeeded, then inodeType is either
	// inode.DirType or inode.FileType; if it was inode.SymlinkType,
	// then err was not nil, and we bailed out before reaching this
	// point.
	if inode.DirType == inodeType {
		err = blunder.NewError(blunder.IsDirError, "%s: inode %v is a directory.", utils.GetFnName(), inodeNumber)
		return
	}

	// Find file size
	metadata, err := mS.volStruct.inodeVolumeHandle.GetMetadata(inodeNumber)
	if err != nil {
		return
	}
	fileSize = metadata.Size
	lastModified = uint64(metadata.ModificationTime.UnixNano())
	lastChanged = uint64(metadata.AttrChangeTime.UnixNano())
	numWrites = metadata.NumWrites

	// If no ranges are given then get range of whole file.  Otherwise, get ranges.
	if len(readRangeIn) == 0 {
		// Get ReadPlan for file
		volumeHandle, err1 := inode.FetchVolumeHandle(volumeName)
		if err1 != nil {
			err = err1
			logger.ErrorWithError(err)
			return
		}
		var offset uint64 = 0
		tmpReadEnt, err1 := volumeHandle.GetReadPlan(inodeNumber, &offset, &metadata.Size)
		if err1 != nil {
			err = err1
			return
		}
		appendReadPlanEntries(tmpReadEnt, readRangeOut)
	} else {
		volumeHandle, err1 := inode.FetchVolumeHandle(volumeName)
		if err1 != nil {
			err = err1
			logger.ErrorWithError(err)
			return
		}

		// Get ReadPlan for each range and append physical path ranges to result
		for i := range readRangeIn {
			// TODO - verify that range request is within file size
			tmpReadEnt, err1 := volumeHandle.GetReadPlan(inodeNumber, readRangeIn[i].Offset, readRangeIn[i].Len)
			if err1 != nil {
				err = err1
				return
			}
			appendReadPlanEntries(tmpReadEnt, readRangeOut)
		}
	}

	serializedMetadata, err = mS.volStruct.inodeVolumeHandle.GetStream(inodeNumber, MiddlewareStream)
	// If someone makes a directory or file via SMB/FUSE and then
	// accesses it via HTTP, we'll see StreamNotFound. We treat it as
	// though there is no metadata. The middleware is equipped to
	// handle receiving empty metadata.
	if err != nil && !blunder.Is(err, blunder.StreamNotFound) {
		return
	} else {
		err = nil
	}
	return
}

func (mS *mountStruct) MiddlewareHeadResponse(entityPath string) (response HeadResponse, err error) {

	startTime := time.Now()
	defer func() {
		globals.MiddlewareHeadResponseUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareHeadResponseErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	ino, inoType, inoLock, err := mS.resolvePathForRead(entityPath, nil)
	if err != nil {
		return
	}
	defer inoLock.Unlock()

	statResult, err := mS.getstatHelper(ino, inoLock.GetCallerID())
	if err != nil {
		return
	}
	response.ModificationTime = statResult[StatMTime]
	response.AttrChangeTime = statResult[StatCTime]
	response.FileSize = statResult[StatSize]
	response.IsDir = (inoType == inode.DirType)
	response.InodeNumber = ino
	response.NumWrites = statResult[StatNumWrites]

	response.Metadata, err = mS.volStruct.inodeVolumeHandle.GetStream(ino, MiddlewareStream)
	if err != nil {
		response.Metadata = []byte{}
		// If someone makes a directory or file via SMB/FUSE and then
		// HEADs it via HTTP, we'll see this error. We treat it as
		// though there is no metadata. The middleware is equipped to
		// handle this case.
		if blunder.Is(err, blunder.StreamNotFound) {
			err = nil
		}
		return
	}
	return
}

func (mS *mountStruct) MiddlewarePost(parentDir string, baseName string, newMetaData []byte, oldMetaData []byte) (err error) {

	startTime := time.Now()
	defer func() {
		globals.MiddlewarePostUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.MiddlewarePostBytes.Add(uint64(len(newMetaData)))
		if err != nil {
			globals.MiddlewarePostErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	// Find inode for container or object
	fullPathName := parentDir + "/" + baseName
	baseNameInodeNumber, _, baseInodeLock, err := mS.resolvePathForWrite(fullPathName, nil)
	if err != nil {
		return err
	}
	defer baseInodeLock.Unlock()

	// Compare oldMetaData to existing existingStreamData to make sure that the HTTP metadata has not changed.
	// If it has changed, then return an error since middleware has to handle it.
	existingStreamData, err := mS.volStruct.inodeVolumeHandle.GetStream(baseNameInodeNumber, MiddlewareStream)

	// GetStream() will return an error if there is no "middleware" stream
	if err != nil && blunder.IsNot(err, blunder.StreamNotFound) {
		return err
	}

	// Verify that the oldMetaData is the same as the one we think we are changing.
	if err == nil && !bytes.Equal(existingStreamData, oldMetaData) {
		return blunder.NewError(blunder.TryAgainError, "%s: MetaData different - existingStreamData: %v OldMetaData: %v.", utils.GetFnName(), existingStreamData, oldMetaData)
	}

	// Change looks okay so make it.
	err = mS.volStruct.inodeVolumeHandle.PutStream(baseNameInodeNumber, MiddlewareStream, newMetaData)
	mS.volStruct.untrackInFlightFileInodeData(baseNameInodeNumber, false)

	return err
}

func putObjectHelper(mS *mountStruct, vContainerName string, vObjectPath string, makeInodeFunc func() (inode.InodeNumber, error)) (mtime uint64, ctime uint64, fileInodeNumber inode.InodeNumber, numWrites uint64, err error) {

	// Find the inode of the directory corresponding to the container
	dirInodeNumber, err := mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, vContainerName)
	if err != nil {
		return
	}

	vObjectPathSegments := revSplitPath(vObjectPath)
	vObjectBaseName := vObjectPathSegments[0]
	dirs := vObjectPathSegments[1:]

	// Find all existing directories, taking locks as we go.
	//
	// We want to find the lowest existing subdirectory for this PUT
	// request. For example, if the FS tree has container/d1/d2, and a
	// PUT request comes in for container/d1/d2/d3/d4/file.bin, then
	// this loop will walk down to d2.
	//
	// We take locks as we go so that nobody can sneak d3 in ahead of
	// us.
	var (
		dirEntInodeLock *dlm.RWLockStruct
		dirInodeLock    *dlm.RWLockStruct
	)
	callerID := dlm.GenerateCallerID()
	dirInodeLock, err = mS.volStruct.inodeVolumeHandle.GetWriteLock(dirInodeNumber, callerID)
	if err != nil {
		return
	}
	defer func() {
		if dirInodeLock != nil {
			dirInodeLock.Unlock()
		}
		if dirEntInodeLock != nil {
			dirEntInodeLock.Unlock()
		}
	}()

	for len(dirs) > 0 {
		thisDir := dirs[len(dirs)-1]
		if thisDir == "." {
			// Skip this early so we don't end up trying to write-lock
			// it and deadlocking with ourselves.
			dirs = dirs[0 : len(dirs)-1]
			continue
		}

		dirEntInodeNumber, err1 := mS.volStruct.inodeVolumeHandle.Lookup(dirInodeNumber, thisDir)
		if err1 != nil && blunder.Errno(err1) == int(blunder.NotFoundError) {
			// NotFoundError just means that it's time to start making
			// directories. We deliberately do not unlock dirInodeLock
			// here.
			err = nil
			break
		}
		if err1 != nil {
			// Mysterious error; just bail
			err = err1
			return
		}

		// There's a directory entry there; let's go see what it is
		dirs = dirs[0 : len(dirs)-1]
		dirEntInodeLock, err1 = mS.volStruct.inodeVolumeHandle.GetWriteLock(dirEntInodeNumber, callerID)
		if err1 != nil {
			err = err1
			dirEntInodeLock = nil
			return
		}

		dirEntInodeType, err1 := mS.volStruct.inodeVolumeHandle.GetType(dirEntInodeNumber)
		if err1 != nil {
			err = err1
			return
		}

		switch dirEntInodeType {

		case inode.FileType:
			// We're processing the directory portion of the
			// object path, so if we run into a file, that's an
			// error
			err = blunder.NewError(blunder.NotDirError, "%s is a file, not a directory", thisDir)
			return

		case inode.SymlinkType:
			target, err1 := mS.volStruct.inodeVolumeHandle.GetSymlink(dirEntInodeNumber)
			dirEntInodeLock.Unlock()
			dirEntInodeLock = nil
			if err1 != nil {
				err = err1
				return
			}

			if strings.HasPrefix(target, "/") {
				// Absolute symlink: restart traversal from the
				// root directory.
				dirInodeLock.Unlock()
				dirInodeNumber = inode.RootDirInodeNumber
				dirInodeLock, err1 = mS.volStruct.inodeVolumeHandle.GetWriteLock(inode.RootDirInodeNumber, nil)
				if err1 != nil {
					err = err1
					dirInodeLock = nil
					return
				}
			}
			dirs = append(dirs, revSplitPath(target)...)

		case inode.DirType:
			// There's actually a subdirectory here. Lock it before
			// unlocking the current directory(???) so there's no
			// window for anyone else to sneak into.
			dirInodeLock.Unlock()

			dirInodeNumber = dirEntInodeNumber
			dirInodeLock = dirEntInodeLock
			dirEntInodeLock = nil // avoid double-cleanup in defer

		default:
			// We're processing the directory portion of the
			// object path, so if its something else, that's an
			// error
			err = blunder.NewError(blunder.NotDirError, "%s is a '%v', not a directory",
				thisDir, dirEntInodeType)
			return
		}
	}

	// Now, dirInodeNumber is the inode of the lowest existing
	// directory. Anything else is created by us and isn't part of the
	// filesystem tree until we Link() it in, so we only need to hold
	// DirInodeLock on the directory.  Call the inode-creator function and
	// start linking stuff together.
	fileInodeNumber, err = makeInodeFunc()
	if err != nil {
		return
	}

	highestUnlinkedInodeNumber := fileInodeNumber
	highestUnlinkedName := vObjectBaseName
	for i := 0; i < len(dirs); i++ {
		newDirInodeNumber, err1 := mS.volStruct.inodeVolumeHandle.CreateDir(inode.PosixModePerm, 0, 0)
		if err1 != nil {
			logger.DebugfIDWithError(internalDebug, err1, "mount.CreateDir(): failed!")
			err = err1
			return
		}

		err = mS.volStruct.inodeVolumeHandle.Link(newDirInodeNumber, highestUnlinkedName, highestUnlinkedInodeNumber, false)
		if err != nil {
			logger.DebugfIDWithError(internalDebug, err, "mount.Link(%v, %v, %v) failed",
				newDirInodeNumber, highestUnlinkedName, highestUnlinkedInodeNumber)
			return
		}
		mS.volStruct.untrackInFlightFileInodeData(highestUnlinkedInodeNumber, false)

		highestUnlinkedInodeNumber = newDirInodeNumber
		highestUnlinkedName = dirs[i]
	}

	// Now we've got a pre-existing directory inode in dirInodeNumber,
	// and highestUnlinked(Name,InodeNumber) indicate a thing we need
	// to link into place. The last step is to make sure there's no
	// obstacle to us doing that. Note that this is only required when
	// all the necessary directories already exist; if we had to
	// create any directories, then the bottom directory is empty
	// because we just created it.
	haveObstacle := false
	var obstacleInodeNumber inode.InodeNumber
	if 0 == len(dirs) {
		var err1 error
		obstacleInodeNumber, err1 = mS.volStruct.inodeVolumeHandle.Lookup(dirInodeNumber, vObjectBaseName)
		if err1 != nil && blunder.Errno(err1) == int(blunder.NotFoundError) {
			// File not found? Good!
		} else if err1 != nil {
			err = err1
			return
		} else {
			haveObstacle = true
			// Grab our own lock and call .getstatHelper() instead of
			// letting Getstat() do it for us;
			obstacleInodeLock, err1 := mS.volStruct.inodeVolumeHandle.GetWriteLock(obstacleInodeNumber, callerID)
			if err1 != nil {
				err = err1
				return
			}
			defer obstacleInodeLock.Unlock()

			err = mS.removeObstacleToObjectPut(callerID, dirInodeNumber, vObjectBaseName, obstacleInodeNumber)
			if err != nil {
				return
			}
			// We're now responsible for destroying obstacleInode, but
			// we're not going to do it yet. We'll wait to actually
			// destroy the data until after we've linked in its
			// replacement.
		}
	}

	// If we got here, then there's no obstacle (any more). Link the thing into place.
	//
	// Note that we don't have a lock on highestUnlinkedInodeNumber.
	// That's because this inode was created in this function, so
	// nobody else knows it exists, so we don't have to worry about
	// anyone else accessing it (except for NFS lookup-by-handle
	// which we'll ignore).
	err = mS.volStruct.inodeVolumeHandle.Link(dirInodeNumber, highestUnlinkedName, highestUnlinkedInodeNumber, false)
	if err != nil {
		logger.ErrorfWithError(err, "MiddlewarePutComplete: failed final Link(%v, %v, %v)", dirInodeNumber, highestUnlinkedName, highestUnlinkedInodeNumber)

		// We can try to recover from a Link() failure here by putting
		// the old thing back. We're still holding locks, so it's safe
		// to try.
		if haveObstacle {
			relinkErr := mS.volStruct.inodeVolumeHandle.Link(dirInodeNumber, vObjectBaseName,
				obstacleInodeNumber, false)
			if relinkErr != nil {
				// the rest of the relevant variables were logged in the previous error-logging call
				logger.ErrorfWithError(relinkErr,
					"MiddlewarePutComplete: relink failed for inode=%v name=%v",
					obstacleInodeNumber, vObjectBaseName)
			}
		}
		return
	}

	// Log errors from inode destruction, but don't let them cause the
	// RPC call to fail. As far as this function's caller is
	// concerned, everything worked as intended.
	if haveObstacle {
		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(obstacleInodeNumber)
		if destroyErr != nil {
			logger.ErrorfWithError(destroyErr, "MiddlewarePutComplete: error destroying inode %v", obstacleInodeNumber)
		}
	}

	metadata, err := mS.volStruct.inodeVolumeHandle.GetMetadata(fileInodeNumber) // not getstat() since we're already holding a lock on this inode
	if err != nil {
		return
	}

	mtime = uint64(metadata.ModificationTime.UnixNano())
	ctime = uint64(metadata.AttrChangeTime.UnixNano())
	// fileInodeNumber set above
	numWrites = metadata.NumWrites
	return
}

func (mS *mountStruct) MiddlewarePutComplete(vContainerName string, vObjectPath string, pObjectPaths []string, pObjectLengths []uint64, pObjectMetadata []byte) (mtime uint64, ctime uint64, fileInodeNumber inode.InodeNumber, numWrites uint64, err error) {

	startTime := time.Now()
	defer func() {
		var totalPutBytes uint64
		for _, length := range pObjectLengths {
			totalPutBytes += length
		}

		globals.MiddlewarePutCompleteUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.MiddlewarePutCompleteBytes.Add(uint64(totalPutBytes))
		if err != nil {
			globals.MiddlewarePutCompleteErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	reifyTheFile := func() (fileInodeNumber inode.InodeNumber, err error) {
		// Reify the Swift object into a ProxyFS file by making a new,
		// empty inode and then associating it with the log segment
		// written by the middleware.
		fileInodeNumber, err = mS.volStruct.inodeVolumeHandle.CreateFile(inode.PosixModePerm, 0, 0)
		if err != nil {
			logger.DebugfIDWithError(internalDebug, err, "fs.CreateFile(): vContainerName: %v failed!", vContainerName)
			return
		}

		// Associate fileInodeNumber with log segments written by Swift
		fileOffset := uint64(0) // Swift only writes whole files
		pObjectOffset := uint64(0)
		for i := 0; i < len(pObjectPaths); i++ {
			err = mS.volStruct.inodeVolumeHandle.Wrote(fileInodeNumber, fileOffset, pObjectPaths[i], pObjectOffset, pObjectLengths[i], i > 0)
			if err != nil {
				logger.DebugfIDWithError(internalDebug, err, "mount.Wrote() fileInodeNumber: %v fileOffset: %v pOjectPaths: %v pObjectOffset: %v pObjectLengths: %v i: %v failed!",
					fileInodeNumber, fileOffset, pObjectPaths, pObjectOffset, pObjectLengths, i)
				return
			}
			fileOffset += pObjectLengths[i]
		}

		// Set the metadata on the file
		err = mS.volStruct.inodeVolumeHandle.PutStream(fileInodeNumber, MiddlewareStream, pObjectMetadata)
		if err != nil {
			logger.DebugfIDWithError(internalDebug, err, "mount.PutStream fileInodeNumber: %v metadata: %v failed",
				fileInodeNumber, pObjectMetadata)
			return
		}
		return
	}

	return putObjectHelper(mS, vContainerName, vObjectPath, reifyTheFile)
}

func (mS *mountStruct) MiddlewareMkdir(vContainerName string, vObjectPath string, metadata []byte) (mtime uint64, ctime uint64, inodeNumber inode.InodeNumber, numWrites uint64, err error) {

	startTime := time.Now()
	defer func() {
		globals.MiddlewareMkdirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareMkdirErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	createTheDirectory := func() (dirInodeNumber inode.InodeNumber, err error) {
		dirInodeNumber, err = mS.volStruct.inodeVolumeHandle.CreateDir(inode.PosixModePerm, 0, 0)
		if err != nil {
			logger.ErrorWithError(err)
			return
		}

		if len(metadata) > 0 {
			err = mS.volStruct.inodeVolumeHandle.PutStream(dirInodeNumber, MiddlewareStream, metadata)
			if err != nil {
				logger.DebugfIDWithError(internalDebug, err, "mount.PutStream fileInodeNumber: %v metadata: %v failed",
					dirInodeNumber, metadata)
				return
			}
		}
		return
	}

	return putObjectHelper(mS, vContainerName, vObjectPath, createTheDirectory)
}

func (mS *mountStruct) MiddlewarePutContainer(containerName string, oldMetadata []byte, newMetadata []byte) (err error) {
	var (
		containerInodeLock   *dlm.RWLockStruct
		containerInodeNumber inode.InodeNumber
		existingMetadata     []byte
		newDirInodeLock      *dlm.RWLockStruct
		newDirInodeNumber    inode.InodeNumber
	)

	startTime := time.Now()
	defer func() {
		globals.MiddlewarePutContainerUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.MiddlewarePutContainerBytes.Add(uint64(len(newMetadata)))
		if err != nil {
			globals.MiddlewarePutContainerErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	// Yes, it's a heavy lock to hold on the root inode. However, we
	// might need to add a new directory entry there, so there's not
	// much else we can do.
	rootInodeLock, err := mS.volStruct.inodeVolumeHandle.GetWriteLock(inode.RootDirInodeNumber, nil)
	if nil != err {
		return
	}
	defer rootInodeLock.Unlock()

	containerInodeNumber, err = mS.volStruct.inodeVolumeHandle.Lookup(inode.RootDirInodeNumber, containerName)
	if err != nil && blunder.IsNot(err, blunder.NotFoundError) {
		return
	} else if err != nil {
		// No such container, so we create it
		err = validateBaseName(containerName)
		if err != nil {
			return
		}

		newDirInodeNumber, err = mS.volStruct.inodeVolumeHandle.CreateDir(inode.PosixModePerm, 0, 0)
		if err != nil {
			logger.ErrorWithError(err)
			return
		}

		newDirInodeLock, err = mS.volStruct.inodeVolumeHandle.GetWriteLock(newDirInodeNumber, nil)
		defer newDirInodeLock.Unlock()

		err = mS.volStruct.inodeVolumeHandle.PutStream(newDirInodeNumber, MiddlewareStream, newMetadata)
		if err != nil {
			logger.ErrorWithError(err)
			return
		}

		err = mS.volStruct.inodeVolumeHandle.Link(inode.RootDirInodeNumber, containerName, newDirInodeNumber, false)

		return
	}

	containerInodeLock, err = mS.volStruct.inodeVolumeHandle.GetWriteLock(containerInodeNumber, nil)
	if err != nil {
		return
	}
	defer containerInodeLock.Unlock()

	// Existing container: just update the metadata
	existingMetadata, err = mS.volStruct.inodeVolumeHandle.GetStream(containerInodeNumber, MiddlewareStream)

	// GetStream() will return an error if there is no "middleware" stream
	if err != nil && blunder.IsNot(err, blunder.StreamNotFound) {
		return
	} else if err != nil {
		existingMetadata = []byte{}
	}

	// Only change it if the caller sent the current value
	if !bytes.Equal(existingMetadata, oldMetadata) {
		err = blunder.NewError(blunder.TryAgainError, "Metadata differs - actual: %v request: %v", existingMetadata, oldMetadata)
		return
	}
	err = mS.volStruct.inodeVolumeHandle.PutStream(containerInodeNumber, MiddlewareStream, newMetadata)

	return
}

func (mS *mountStruct) Mkdir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string, filePerm inode.InodeMode) (newDirInodeNumber inode.InodeNumber, err error) {

	startTime := time.Now()
	defer func() {
		globals.MkdirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MkdirErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	// Make sure the file basename is not too long
	err = validateBaseName(basename)
	if err != nil {
		return 0, err
	}

	newDirInodeNumber, err = mS.volStruct.inodeVolumeHandle.CreateDir(filePerm, userID, groupID)
	if err != nil {
		logger.ErrorWithError(err)
		return 0, err
	}

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {

		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(newDirInodeNumber)
		if destroyErr != nil {
			logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Access(F_OK) in fs.Mkdir", newDirInodeNumber)
		}
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return 0, err
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK,
		inode.NoOverride) {

		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(newDirInodeNumber)
		if destroyErr != nil {
			logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Access(W_OK|X_OK) in fs.Mkdir", newDirInodeNumber)
		}
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return 0, err
	}

	err = mS.volStruct.inodeVolumeHandle.Link(inodeNumber, basename, newDirInodeNumber, false)
	if err != nil {
		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(newDirInodeNumber)
		if destroyErr != nil {
			logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Link() in fs.Mkdir", newDirInodeNumber)
		}
		return 0, err
	}

	return newDirInodeNumber, nil
}

func (mS *mountStruct) RemoveXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, streamName string) (err error) {

	startTime := time.Now()
	defer func() {
		globals.RemoveXAttrUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.RemoveXAttrErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	err = mS.volStruct.inodeVolumeHandle.DeleteStream(inodeNumber, streamName)
	if err != nil {
		logger.ErrorfWithError(err, "Failed to delete XAttr %v of inode %v", streamName, inodeNumber)
	}

	mS.volStruct.untrackInFlightFileInodeData(inodeNumber, false)

	return
}

func (mS *mountStruct) Rename(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, srcDirInodeNumber inode.InodeNumber, srcBasename string, dstDirInodeNumber inode.InodeNumber, dstBasename string) (err error) {

	startTime := time.Now()
	defer func() {
		globals.RenameUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.RenameErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	err = validateBaseName(srcBasename)
	if err != nil {
		return
	}

	err = validateBaseName(dstBasename)
	if err != nil {
		return
	}

	// Flag to tell us if there's only one directory to be locked
	srcAndDestDirsAreSame := srcDirInodeNumber == dstDirInodeNumber

	// Generate our calling context ID, so that the locks will have the same callerID
	callerID := dlm.GenerateCallerID()

	// Allocate the source dir lock
	srcDirLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(srcDirInodeNumber, callerID)
	if err != nil {
		return
	}

	// Allocate the dest dir lock
	dstDirLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(dstDirInodeNumber, callerID)
	if err != nil {
		return err
	}

retryLock:
	// Get the source directory's lock
	err = srcDirLock.WriteLock()
	if err != nil {
		return
	}

	if !mS.volStruct.inodeVolumeHandle.Access(srcDirInodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {

		srcDirLock.Unlock()
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(srcDirInodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK,
		inode.NoOverride) {

		srcDirLock.Unlock()
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	// Try to get the destination directory's lock. If we can't get it, drop the
	// source dir lock and try the whole thing again.
	if !srcAndDestDirsAreSame {
		err = dstDirLock.TryWriteLock()
		if blunder.Is(err, blunder.TryAgainError) {
			srcDirLock.Unlock()
			goto retryLock
		} else if blunder.IsNotSuccess(err) {
			// This shouldn't happen...
			srcDirLock.Unlock()
			return err
		}

		if !mS.volStruct.inodeVolumeHandle.Access(dstDirInodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
			inode.NoOverride) {

			srcDirLock.Unlock()
			dstDirLock.Unlock()
			err = blunder.NewError(blunder.NotFoundError, "ENOENT")
			return
		}
		if !mS.volStruct.inodeVolumeHandle.Access(dstDirInodeNumber, userID, groupID, otherGroupIDs,
			inode.W_OK|inode.X_OK, inode.NoOverride) {

			srcDirLock.Unlock()
			dstDirLock.Unlock()
			err = blunder.NewError(blunder.PermDeniedError, "EACCES")
			return
		}
	}

	// Now we have the locks for both directories; we can do the move
	err = mS.volStruct.inodeVolumeHandle.Move(srcDirInodeNumber, srcBasename, dstDirInodeNumber, dstBasename)

	// Release our locks and return
	if !srcAndDestDirsAreSame {
		dstDirLock.Unlock()
	}
	srcDirLock.Unlock()

	// TODO: Where is the lock on the potentially removed fileInode overwritten by the Move() call?

	return err
}

func (mS *mountStruct) Read(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, offset uint64, length uint64, profiler *utils.Profiler) (buf []byte, err error) {

	startTime := time.Now()
	defer func() {
		globals.ReadUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.ReadBytes.Add(uint64(len(buf)))
		if err != nil {
			globals.ReadErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	inodeType, err := mS.volStruct.inodeVolumeHandle.GetType(inodeNumber)
	if err != nil {
		logger.ErrorfWithError(err, "couldn't get type for inode %v", inodeNumber)
		return buf, err
	}
	// Make sure the inode number is for a file inode
	if inodeType != inode.FileType {
		err = fmt.Errorf("%s: expected inode %v to be a file inode, got %v", utils.GetFnName(), inodeNumber, inodeType)
		logger.ErrorWithError(err)
		return buf, blunder.AddError(err, blunder.NotFileError)
	}

	profiler.AddEventNow("before inode.Read()")
	buf, err = mS.volStruct.inodeVolumeHandle.Read(inodeNumber, offset, length, profiler)
	profiler.AddEventNow("after inode.Read()")
	if uint64(len(buf)) > length {
		err = fmt.Errorf("%s: Buf length %v is greater than supplied length %v", utils.GetFnName(), uint64(len(buf)), length)
		logger.ErrorWithError(err)
		return buf, blunder.AddError(err, blunder.IOError)
	}

	return buf, err
}

func (mS *mountStruct) Readdir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, prevBasenameReturned string, maxEntries uint64, maxBufSize uint64) (entries []inode.DirEntry, numEntries uint64, areMoreEntries bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.ReaddirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.ReaddirEntries.Add(uint64(len(entries)))
		if err != nil {
			globals.ReaddirErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	// Call readdir helper function to do the work
	return mS.readdirHelper(inodeNumber, prevBasenameReturned, maxEntries, maxBufSize, inodeLock.GetCallerID())
}

func (mS *mountStruct) ReaddirOne(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, prevDirMarker interface{}) (entries []inode.DirEntry, err error) {

	startTime := time.Now()
	defer func() {
		globals.ReaddirOneUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.ReaddirOneErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return entries, err
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return entries, err
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {

		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.OwnerOverride) {

		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	// Call readdirOne helper function to do the work
	entries, err = mS.readdirOneHelper(inodeNumber, prevDirMarker, inodeLock.GetCallerID())
	if err != nil {
		// When the client uses location-based readdir, it knows it is done when it reads beyond
		// the last entry and gets a not found error. Because of this, we don't log not found as an error.
		if blunder.IsNot(err, blunder.NotFoundError) {
			logger.ErrorWithError(err)
		}
	}
	return entries, err
}

func (mS *mountStruct) ReaddirPlus(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, prevBasenameReturned string, maxEntries uint64, maxBufSize uint64) (dirEntries []inode.DirEntry, statEntries []Stat, numEntries uint64, areMoreEntries bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.ReaddirPlusUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.ReaddirPlusBytes.Add(uint64(len(dirEntries)))
		if err != nil {
			globals.ReaddirPlusErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		inodeLock.Unlock()
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK|inode.X_OK,
		inode.OwnerOverride) {
		inodeLock.Unlock()
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	// Get dir entries; Call readdir helper function to do the work
	dirEntries, numEntries, areMoreEntries, err = mS.readdirHelper(inodeNumber, prevBasenameReturned, maxEntries, maxBufSize, inodeLock.GetCallerID())
	inodeLock.Unlock()

	if err != nil {
		// Not logging here, since Readdir will have done that for us already.
		return dirEntries, statEntries, numEntries, areMoreEntries, err
	}

	// Get stats
	statEntries = make([]Stat, numEntries)
	for i := range dirEntries {
		entryInodeLock, err1 := mS.volStruct.inodeVolumeHandle.InitInodeLock(dirEntries[i].InodeNumber, nil)
		if err = err1; err != nil {
			return
		}
		err = entryInodeLock.ReadLock()
		if err != nil {
			return
		}

		// Fill in stats, calling getstat helper function to do the work
		statEntries[i], err = mS.getstatHelper(dirEntries[i].InodeNumber, entryInodeLock.GetCallerID())
		entryInodeLock.Unlock()

		if err != nil {
			logger.ErrorWithError(err)
			return dirEntries, statEntries, numEntries, areMoreEntries, err
		}
	}

	return dirEntries, statEntries, numEntries, areMoreEntries, err
}

func (mS *mountStruct) ReaddirOnePlus(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, prevDirMarker interface{}) (dirEntries []inode.DirEntry, statEntries []Stat, err error) {

	startTime := time.Now()
	defer func() {
		globals.ReaddirOnePlusUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.ReaddirOnePlusErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		inodeLock.Unlock()
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK|inode.X_OK,
		inode.OwnerOverride) {
		inodeLock.Unlock()
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	// Get dir entries; Call readdirOne helper function to do the work
	dirEntries, err = mS.readdirOneHelper(inodeNumber, prevDirMarker, inodeLock.GetCallerID())
	inodeLock.Unlock()

	if err != nil {
		// When the client uses location-based readdir, it knows it is done when it reads beyond
		// the last entry and gets a not found error. Because of this, we don't log not found as an error.
		if blunder.IsNot(err, blunder.NotFoundError) {
			logger.ErrorWithError(err)
		}
		return dirEntries, statEntries, err
	}

	// Always only one entry
	numEntries := 1

	// Get stats
	statEntries = make([]Stat, numEntries)
	for i := range dirEntries {
		entryInodeLock, err1 := mS.volStruct.inodeVolumeHandle.InitInodeLock(dirEntries[i].InodeNumber, nil)
		if err = err1; err != nil {
			return
		}
		err = entryInodeLock.ReadLock()
		if err != nil {
			return
		}

		// Fill in stats, calling getstat helper function to do the work
		statEntries[i], err = mS.getstatHelper(dirEntries[i].InodeNumber, entryInodeLock.GetCallerID())
		entryInodeLock.Unlock()

		if err != nil {
			logger.ErrorWithError(err)
			return dirEntries, statEntries, err
		}
	}

	return dirEntries, statEntries, err
}

func (mS *mountStruct) Readsymlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (target string, err error) {

	startTime := time.Now()
	defer func() {
		globals.ReadsymlinkUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.ReadsymlinkErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.ReadLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {

		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.NoOverride) {

		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	target, err = mS.volStruct.inodeVolumeHandle.GetSymlink(inodeNumber)

	return target, err
}

func (mS *mountStruct) Resize(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, newSize uint64) (err error) {

	startTime := time.Now()
	defer func() {
		globals.ResizeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.ResizeErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {

		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK,
		inode.OwnerOverride) {

		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	err = mS.volStruct.inodeVolumeHandle.SetSize(inodeNumber, newSize)
	mS.volStruct.untrackInFlightFileInodeData(inodeNumber, false)

	return err
}

func (mS *mountStruct) Rmdir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string) (err error) {

	startTime := time.Now()
	defer func() {
		globals.RmdirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.RmdirErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	callerID := dlm.GenerateCallerID()
	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	basenameInodeNumber, err := mS.volStruct.inodeVolumeHandle.Lookup(inodeNumber, basename)
	if nil != err {
		return
	}

	basenameInodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(basenameInodeNumber, callerID)
	if err != nil {
		return
	}
	err = basenameInodeLock.WriteLock()
	if err != nil {
		return
	}
	defer basenameInodeLock.Unlock()

	// no permissions are required on the target directory

	basenameInodeType, err := mS.volStruct.inodeVolumeHandle.GetType(basenameInodeNumber)
	if nil != err {
		return
	}

	if inode.DirType != basenameInodeType {
		err = fmt.Errorf("Rmdir() called on non-Directory")
		err = blunder.AddError(err, blunder.NotDirError)
		return
	}

	dirEntries, err := mS.volStruct.inodeVolumeHandle.NumDirEntries(basenameInodeNumber)
	if nil != err {
		return
	}

	if 2 != dirEntries {
		err = fmt.Errorf("Directory not empty")
		err = blunder.AddError(err, blunder.NotEmptyError)
		return
	}

	err = mS.volStruct.inodeVolumeHandle.Unlink(inodeNumber, basename, false)
	if nil != err {
		return
	}

	err = mS.volStruct.inodeVolumeHandle.Destroy(basenameInodeNumber)
	if nil != err {
		return
	}

	return
}

func (mS *mountStruct) Setstat(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, stat Stat) (err error) {

	startTime := time.Now()
	defer func() {
		globals.SetstatUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.SetstatErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.P_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotPermError, "EPERM")
		return
	}

	// perform all permissions checks before making any changes
	//
	// changing the filesize requires write permission
	_, ok := stat[StatSize]
	if ok {
		if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK,
			inode.OwnerOverride) {
			err = blunder.NewError(blunder.NotPermError, "EPERM")
			return
		}
	}

	// most other attributes can only be changed by the owner of the file
	ownerOnly := []StatKey{StatCTime, StatCRTime, StatMTime, StatATime, StatMode, StatUserID, StatGroupID}
	for _, key := range ownerOnly {
		_, ok := stat[key]
		if ok {
			if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.P_OK,
				inode.NoOverride) {
				err = blunder.NewError(blunder.NotPermError, "EPERM")
				return
			}
			break
		}
	}

	// the superuser (root) is the only one that can change the owner of the file to a
	// different user, but the owner of the file can perform a no-op "change" in
	// ownership
	newUserID, settingUserID := stat[StatUserID]
	if settingUserID && userID != inode.InodeRootUserID {
		if userID != inode.InodeUserID(newUserID) {
			err = blunder.NewError(blunder.NotPermError, "EPERM")
			return
		}
	}

	// the group can only be changed to the current group or another group the owner
	// is in (unless its the superuser asking)
	newGroupID, settingGroupID := stat[StatGroupID]
	if settingGroupID && groupID != inode.InodeGroupID(newGroupID) && userID != inode.InodeRootUserID {

		err = blunder.NewError(blunder.NotPermError, "EPERM")
		for _, otherID := range otherGroupIDs {
			if inode.InodeGroupID(newGroupID) == otherID {
				err = nil
				break
			}
		}
		if err != nil {
			return
		}
	}

	// sanity checks for invalid/illegal values
	if settingUserID {
		// Since we are using a uint64 to convey a uint32 value, make sure we didn't get something too big
		if newUserID > math.MaxUint32 {
			err = fmt.Errorf("%s: userID is too large - value is %d, max is %d.", utils.GetFnName(), newUserID, math.MaxUint32)
			err = blunder.AddError(err, blunder.InvalidUserIDError)
			return
		}
	}

	if settingGroupID {
		// Since we are using a uint64 to convey a uint32 value, make sure we didn't get something too big
		if newGroupID > math.MaxUint32 {
			err = fmt.Errorf("%s: groupID is too large - value is %d, max is %d.", utils.GetFnName(), newGroupID, math.MaxUint32)
			err = blunder.AddError(err, blunder.InvalidGroupIDError)
			return
		}
	}

	filePerm, settingFilePerm := stat[StatMode]
	if settingFilePerm {
		// Since we are using a uint64 to convey a 12 bit value, make sure we didn't get something too big
		if filePerm >= 1<<12 {
			err = fmt.Errorf("%s: filePerm is too large - value is %d, max is %d.", utils.GetFnName(),
				filePerm, 1<<12)
			err = blunder.AddError(err, blunder.InvalidFileModeError)
			return
		}
	}

	// get to work setting things
	//
	// Set permissions, if present in the map
	if settingFilePerm {
		err = mS.volStruct.inodeVolumeHandle.SetPermMode(inodeNumber, inode.InodeMode(filePerm))
		if err != nil {
			logger.ErrorWithError(err)
			return err
		}
	}

	// set owner and/or group owner, if present in the map
	err = nil
	if settingUserID && settingGroupID {
		err = mS.volStruct.inodeVolumeHandle.SetOwnerUserIDGroupID(inodeNumber, inode.InodeUserID(newUserID),
			inode.InodeGroupID(newGroupID))
	} else if settingUserID {
		err = mS.volStruct.inodeVolumeHandle.SetOwnerUserID(inodeNumber, inode.InodeUserID(newUserID))
	} else if settingGroupID {
		err = mS.volStruct.inodeVolumeHandle.SetOwnerGroupID(inodeNumber, inode.InodeGroupID(newGroupID))
	}
	if err != nil {
		logger.ErrorWithError(err)
		return
	}

	// Set crtime, if present in the map
	crtime, ok := stat[StatCRTime]
	if ok {
		newCreationTime := time.Unix(0, int64(crtime))
		err = mS.volStruct.inodeVolumeHandle.SetCreationTime(inodeNumber, newCreationTime)
		if err != nil {
			logger.ErrorWithError(err)
			return err
		}
	}

	// Set mtime, if present in the map
	mtime, ok := stat[StatMTime]
	if ok {
		newModificationTime := time.Unix(0, int64(mtime))
		err = mS.volStruct.inodeVolumeHandle.SetModificationTime(inodeNumber, newModificationTime)
		if err != nil {
			logger.ErrorWithError(err)
			return err
		}
	}

	// Set atime, if present in the map
	atime, ok := stat[StatATime]
	if ok {
		newAccessTime := time.Unix(0, int64(atime))
		err = mS.volStruct.inodeVolumeHandle.SetAccessTime(inodeNumber, newAccessTime)
		if err != nil {
			logger.ErrorWithError(err)
			return err
		}
	}

	// ctime is used to reliably determine whether the contents of a file
	// have changed so it cannot be altered by a client (some security
	// software depends on this)
	ctime, ok := stat[StatCTime]
	if ok {
		newAccessTime := time.Unix(0, int64(ctime))
		logger.Infof("%s: ignoring attempt to change ctime to %v on volume '%s' inode %v",
			utils.GetFnName(), newAccessTime, mS.volStruct.volumeName, inodeNumber)
	}

	// Set size, if present in the map
	size, ok := stat[StatSize]
	if ok {
		err = mS.volStruct.inodeVolumeHandle.SetSize(inodeNumber, size)
		if err != nil {
			logger.ErrorWithError(err)
			return err
		}
	}

	return
}

// TODO: XATTR_* values are obtained from </usr/include/attr/xattr.h>, remove constants with go equivalent.
const (
	xattr_create  = 1
	xattr_replace = 2
)

func (mS *mountStruct) SetXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, streamName string, value []byte, flags int) (err error) {

	startTime := time.Now()
	defer func() {
		globals.SetXAttrUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.SetXAttrErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	switch flags {
	case 0:
		break
	case xattr_create:
		_, err = mS.GetXAttr(userID, groupID, otherGroupIDs, inodeNumber, streamName)
		if err == nil {
			return blunder.AddError(err, blunder.FileExistsError)
		}
	case xattr_replace:
		_, err = mS.GetXAttr(userID, groupID, otherGroupIDs, inodeNumber, streamName)
		if err != nil {
			return blunder.AddError(err, blunder.StreamNotFound)
		}
	default:
		return blunder.AddError(err, blunder.InvalidArgError)
	}

	err = mS.volStruct.inodeVolumeHandle.PutStream(inodeNumber, streamName, value)
	if err != nil {
		logger.ErrorfWithError(err, "Failed to set XAttr %v to inode %v", streamName, inodeNumber)
	}

	mS.volStruct.untrackInFlightFileInodeData(inodeNumber, false)

	return
}

func (mS *mountStruct) StatVfs() (statVFS StatVFS, err error) {

	startTime := time.Now()
	defer func() {
		globals.StatVfsUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.StatVfsErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	statVFS = make(map[StatVFSKey]uint64)

	statVFS[StatVFSFilesystemID] = mS.volStruct.inodeVolumeHandle.GetFSID()
	statVFS[StatVFSBlockSize] = mS.volStruct.reportedBlockSize
	statVFS[StatVFSFragmentSize] = mS.volStruct.reportedFragmentSize
	statVFS[StatVFSTotalBlocks] = mS.volStruct.reportedNumBlocks
	statVFS[StatVFSFreeBlocks] = mS.volStruct.reportedNumBlocks
	statVFS[StatVFSAvailBlocks] = mS.volStruct.reportedNumBlocks
	statVFS[StatVFSTotalInodes] = mS.volStruct.reportedNumInodes
	statVFS[StatVFSFreeInodes] = mS.volStruct.reportedNumInodes
	statVFS[StatVFSAvailInodes] = mS.volStruct.reportedNumInodes
	statVFS[StatVFSMountFlags] = 0
	statVFS[StatVFSMaxFilenameLen] = FileNameMax

	return statVFS, nil
}

func (mS *mountStruct) Symlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string, target string) (symlinkInodeNumber inode.InodeNumber, err error) {

	startTime := time.Now()
	defer func() {
		globals.SymlinkUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.SymlinkErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	err = validateBaseName(basename)
	if err != nil {
		return
	}

	err = validateFullPath(target)
	if err != nil {
		return
	}

	// Mode for symlinks defaults to rwxrwxrwx, i.e. inode.PosixModePerm
	symlinkInodeNumber, err = mS.volStruct.inodeVolumeHandle.CreateSymlink(target, inode.PosixModePerm, userID, groupID)
	if err != nil {
		return
	}

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {

		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(symlinkInodeNumber)
		if destroyErr != nil {
			logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Access(F_OK) in fs.Symlink", symlinkInodeNumber)
		}
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK,
		inode.NoOverride) {

		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(symlinkInodeNumber)
		if destroyErr != nil {
			logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Access(W_OK|X_OK) in fs.Symlink", symlinkInodeNumber)
		}
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	err = mS.volStruct.inodeVolumeHandle.Link(inodeNumber, basename, symlinkInodeNumber, false)
	if err != nil {
		destroyErr := mS.volStruct.inodeVolumeHandle.Destroy(symlinkInodeNumber)
		if destroyErr != nil {
			logger.WarnfWithError(destroyErr, "couldn't destroy inode %v after failed Link() in fs.Symlink", symlinkInodeNumber)
		}
		return
	}

	return
}

func (mS *mountStruct) Unlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string) (err error) {

	startTime := time.Now()
	defer func() {
		globals.UnlinkUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.UnlinkErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	callerID := dlm.GenerateCallerID()
	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	basenameInodeNumber, err := mS.volStruct.inodeVolumeHandle.Lookup(inodeNumber, basename)
	if nil != err {
		return
	}

	basenameInodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(basenameInodeNumber, callerID)
	if err != nil {
		return
	}
	err = basenameInodeLock.WriteLock()
	if err != nil {
		return
	}
	defer basenameInodeLock.Unlock()

	basenameInodeType, err := mS.volStruct.inodeVolumeHandle.GetType(basenameInodeNumber)
	if nil != err {
		return
	}

	if inode.DirType == basenameInodeType {
		err = fmt.Errorf("Unlink() called on a Directory")
		err = blunder.AddError(err, blunder.IsDirError)
		return
	}

	err = mS.volStruct.inodeVolumeHandle.Unlink(inodeNumber, basename, false)
	if nil != err {
		return
	}

	basenameLinkCount, err := mS.volStruct.inodeVolumeHandle.GetLinkCount(basenameInodeNumber)
	if nil != err {
		return
	}

	if 0 == basenameLinkCount {
		mS.volStruct.untrackInFlightFileInodeData(basenameInodeNumber, false)
		err = mS.volStruct.inodeVolumeHandle.Destroy(basenameInodeNumber)
		if nil != err {
			return
		}
	}

	return
}

func (mS *mountStruct) VolumeName() (volumeName string) {

	startTime := time.Now()

	volumeName = mS.volStruct.volumeName
	globals.VolumeNameUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	return
}

func (mS *mountStruct) Write(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, offset uint64, buf []byte, profiler *utils.Profiler) (size uint64, err error) {

	startTime := time.Now()
	defer func() {
		globals.WriteUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.WriteBytes.Add(size)
		if err != nil {
			globals.WriteErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	logger.Tracef("fs.Write(): starting volume '%s' inode %d offset %d len %d",
		mS.volStruct.volumeName, inodeNumber, offset, len(buf))

	inodeLock, err := mS.volStruct.inodeVolumeHandle.InitInodeLock(inodeNumber, nil)
	if err != nil {
		return
	}
	err = inodeLock.WriteLock()
	if err != nil {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.W_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	profiler.AddEventNow("before inode.Write()")
	err = mS.volStruct.inodeVolumeHandle.Write(inodeNumber, offset, buf, profiler)
	profiler.AddEventNow("after inode.Write()")
	// write to Swift presumably succeeds or fails as a whole
	if err != nil {
		return 0, err
	}

	logger.Tracef("fs.Write(): tracking write volume '%s' inode %d", mS.volStruct.volumeName, inodeNumber)
	mS.volStruct.trackInFlightFileInodeData(inodeNumber)
	size = uint64(len(buf))

	return
}

func validateBaseName(baseName string) (err error) {
	// Make sure the file baseName is not too long
	baseLen := len(baseName)
	if baseLen > FileNameMax {
		err = fmt.Errorf("%s: basename is too long. Length %v, max %v", utils.GetFnName(), baseLen, FileNameMax)
		logger.ErrorWithError(err)
		return blunder.AddError(err, blunder.NameTooLongError)
	}
	return
}

func validateFullPath(fullPath string) (err error) {
	pathLen := len(fullPath)
	if pathLen > FilePathMax {
		err = fmt.Errorf("%s: fullpath is too long. Length %v, max %v", utils.GetFnName(), pathLen, FilePathMax)
		logger.ErrorWithError(err)
		return blunder.AddError(err, blunder.NameTooLongError)
	}
	return
}

func revSplitPath(fullpath string) []string {
	// TrimPrefix avoids empty [0] element in pathSegments
	trimmed := strings.TrimPrefix(fullpath, "/")
	if trimmed == "" {
		// path.Clean("") = ".", which is not useful
		return []string{}
	}

	segments := strings.Split(path.Clean(trimmed), "/")
	slen := len(segments)
	for i := 0; i < slen/2; i++ {
		segments[i], segments[slen-i-1] = segments[slen-i-1], segments[i]
	}
	return segments
}

// Helper function to look up a path and return its inode, inode type,
// and a (locked) read lock for that inode.
//
// When the returned err is nil, the caller is responsible for
// unlocking. If err is non-nil, this function will handle any
// unlocking. This lets the caller return immediately on error, as Go
// code likes to do.
//
// If the referenced entity is a symlink, then it will be followed.
// Subsequent symlinks will also be followed until a terminal
// non-symlink is reached, up to a limit of MaxSymlinks. A terminal
// non-symlink may be a directory, a file, or something that does not
// exist.
func (mS *mountStruct) resolvePathForRead(fullpath string, callerID dlm.CallerID) (inodeNumber inode.InodeNumber, inodeType inode.InodeType, inodeLock *dlm.RWLockStruct, err error) {
	return mS.resolvePath(fullpath, callerID, inode.RootDirInodeNumber, mS.volStruct.inodeVolumeHandle.EnsureReadLock)
}

func (mS *mountStruct) resolvePathForWrite(fullpath string, callerID dlm.CallerID) (inodeNumber inode.InodeNumber, inodeType inode.InodeType, inodeLock *dlm.RWLockStruct, err error) {
	return mS.resolvePath(fullpath, callerID, inode.RootDirInodeNumber, mS.volStruct.inodeVolumeHandle.EnsureWriteLock)
}

func (mS *mountStruct) resolvePath(fullpath string, callerID dlm.CallerID, startingInode inode.InodeNumber, getLock func(inode.InodeNumber, dlm.CallerID) (*dlm.RWLockStruct, error)) (inodeNumber inode.InodeNumber, inodeType inode.InodeType, inodeLock *dlm.RWLockStruct, err error) {
	// pathSegments is the reversed split path. For example, if
	// fullpath is "/etc/thing/default.conf", then pathSegments is
	// ["default.conf", "thing", "etc"].
	//
	// The reversal is just because Go gives us append() but no
	// prepend() for slices.
	pathSegments := revSplitPath(fullpath)

	// Our protection against symlink loops is a limit on the number
	// of symlinks that we will follow.
	followsRemaining := MaxSymlinks

	var cursorInodeNumber inode.InodeNumber
	var cursorInodeType inode.InodeType
	var cursorInodeLock *dlm.RWLockStruct
	dirInodeNumber := startingInode
	dirInodeLock, err := getLock(dirInodeNumber, callerID)

	// Use defer for cleanup so that we don't have to think as hard
	// about every if-error-return block.
	defer func() {
		if dirInodeLock != nil {
			dirInodeLock.Unlock()
		}
	}()
	defer func() {
		if cursorInodeLock != nil {
			cursorInodeLock.Unlock()
		}
	}()

	for len(pathSegments) > 0 {
		segment := pathSegments[len(pathSegments)-1]
		pathSegments = pathSegments[:len(pathSegments)-1]

		if segment == "." {
			continue
		}

		// Look up the entry in the directory.
		//
		// If we find a relative symlink (does not start with "/"),
		// then we'll need to keep this lock around for our next pass
		// through the loop.
		cursorInodeNumber, err = mS.volStruct.inodeVolumeHandle.Lookup(dirInodeNumber, segment)
		if err != nil {
			return
		}

		cursorInodeLock, err = getLock(cursorInodeNumber, callerID)
		if err != nil {
			return
		}
		cursorInodeType, err = mS.volStruct.inodeVolumeHandle.GetType(cursorInodeNumber)
		if err != nil {
			return
		}

		if cursorInodeType == inode.SymlinkType {
			// Dereference the symlink and continue path traversal
			// from the appropriate location.
			if followsRemaining == 0 {
				err = blunder.NewError(blunder.TooManySymlinksError, "Too many symlinks while resolving %s", fullpath)
				return
			} else {
				followsRemaining -= 1
			}

			target, err1 := mS.volStruct.inodeVolumeHandle.GetSymlink(cursorInodeNumber)
			if cursorInodeLock != nil {
				cursorInodeLock.Unlock() // done with this symlink, error or not
				cursorInodeLock = nil
			}
			if err1 != nil {
				err = err1
				return
			}

			if strings.HasPrefix(target, "/") {
				// Absolute symlink; we don't keep track of the
				// current directory any more, but restart traversal
				// from the root directory.
				if dirInodeLock != nil {
					dirInodeLock.Unlock()
					dirInodeLock = nil
				}
				dirInodeNumber = inode.RootDirInodeNumber
				dirInodeLock, err = getLock(inode.RootDirInodeNumber, callerID)
				if err != nil {
					return
				}
			}
			newSegments := revSplitPath(target)
			pathSegments = append(pathSegments, newSegments...)
		} else if len(pathSegments) == 0 {
			// This was the final path segment (and not a symlink), so
			// return what we've found. File? Directory? Something
			// else entirely? Doesn't matter; it's the caller's
			// problem now.
			inodeNumber = cursorInodeNumber
			inodeType = cursorInodeType
			// We're returning a held lock. This is intentional.
			inodeLock = cursorInodeLock
			cursorInodeLock = nil // prevent deferred cleanup
			return
		} else if cursorInodeType == inode.FileType {
			// If we hit a file but there's still path segments
			// left, then the path is invalid , e.g.
			// "/stuff/so-far-so-good/kitten.png/you-cannot-have-this-part"
			err = blunder.NewError(blunder.NotDirError, "%s is a file, not a directory", segment)
			return
		} else {
			// Found a directory; continue traversal from therein
			if dirInodeLock != nil {
				dirInodeLock.Unlock()
			}
			dirInodeNumber = cursorInodeNumber
			dirInodeLock = cursorInodeLock
			cursorInodeLock = nil
		}
	}

	// If we ever had any pathSegments at all, we exited via one of
	// the return statements in the above loop. The only way to get
	// here should be to have pathSegments be empty, which means the
	// caller is resolving the path "/".
	inodeNumber = dirInodeNumber
	inodeType = inode.DirType
	inodeLock = dirInodeLock
	dirInodeLock = nil // prevent deferred cleanup
	return
}

// Utility function to unlink, but not destroy, a particular file or empty subdirectory.
//
// This function checks that the directory is empty.
//
// The caller of this function must hold appropriate locks.
//
// obstacleInodeNumber must refer to an existing file or directory
// that is (a) already part of the directory tree and (b) not the root
// directory.
func (mS *mountStruct) removeObstacleToObjectPut(callerID dlm.CallerID, dirInodeNumber inode.InodeNumber, obstacleName string, obstacleInodeNumber inode.InodeNumber) error {
	statResult, err := mS.getstatHelper(obstacleInodeNumber, callerID)
	if err != nil {
		return err
	}

	fileType := inode.InodeType(statResult[StatFType])
	if fileType == inode.FileType || fileType == inode.SymlinkType {
		// Files and symlinks can always, barring errors, be unlinked
		err = mS.volStruct.inodeVolumeHandle.Unlink(dirInodeNumber, obstacleName, false)
		if err != nil {
			return err
		}
	} else if fileType == inode.DirType {
		numEntries, err := mS.volStruct.inodeVolumeHandle.NumDirEntries(obstacleInodeNumber)
		if err != nil {
			return err
		}
		if numEntries >= 3 {
			// We're looking at a pre-existing, user-visible directory
			// that's linked into the directory structure, so we've
			// got at least two entries, namely "." and ".."
			//
			// If there's a third, then the directory is non-empty.
			return blunder.NewError(blunder.IsDirError, "%s is a non-empty directory", obstacleName)
		} else {
			// We don't want to call Rmdir() here since
			// that function (a) grabs locks, (b) checks
			// that it's a directory and is empty, then
			// (c) calls Unlink() and Destroy().
			//
			// We already have the locks and we've already
			// checked that it's empty, so let's just get
			// down to it.
			err = mS.volStruct.inodeVolumeHandle.Unlink(dirInodeNumber, obstacleName, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Utility function to unlink, but not destroy, a particular file or empty subdirectory.
//
// This function checks that the directory is empty.
//
// The caller of this function must hold appropriate locks.
//
// obstacleInodeNumber must refer to an existing file or directory
// that is (a) already part of the directory tree and (b) not the root
// directory.
func removeObstacleToObjectPut(mount *mountStruct, callerID dlm.CallerID, dirInodeNumber inode.InodeNumber, obstacleName string, obstacleInodeNumber inode.InodeNumber) error {
	statResult, err := mount.getstatHelper(obstacleInodeNumber, callerID)
	if err != nil {
		return err
	}

	fileType := inode.InodeType(statResult[StatFType])
	if fileType == inode.FileType || fileType == inode.SymlinkType {
		// Files and symlinks can always, barring errors, be unlinked
		err = mount.volStruct.inodeVolumeHandle.Unlink(dirInodeNumber, obstacleName, false)
		if err != nil {
			return err
		}
	} else if fileType == inode.DirType {
		numEntries, err := mount.volStruct.inodeVolumeHandle.NumDirEntries(obstacleInodeNumber)
		if err != nil {
			return err
		}
		if numEntries >= 3 {
			// We're looking at a pre-existing, user-visible directory
			// that's linked into the directory structure, so we've
			// got at least two entries, namely "." and ".."
			//
			// If there's a third, then the directory is non-empty.
			return blunder.NewError(blunder.IsDirError, "%s is a non-empty directory", obstacleName)
		} else {
			// We don't want to call Rmdir() here since
			// that function (a) grabs locks, (b) checks
			// that it's a directory and is empty, then
			// (c) calls Unlink() and Destroy().
			//
			// We already have the locks and we've already
			// checked that it's empty, so let's just get
			// down to it.
			err = mount.volStruct.inodeVolumeHandle.Unlink(dirInodeNumber, obstacleName, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Utility function to append entries to reply
func appendReadPlanEntries(readPlan []inode.ReadPlanStep, readRangeOut *[]inode.ReadPlanStep) (numEntries uint64) {
	for i := range readPlan {
		entry := inode.ReadPlanStep{ObjectPath: readPlan[i].ObjectPath, Offset: readPlan[i].Offset, Length: readPlan[i].Length}
		*readRangeOut = append(*readRangeOut, entry)
		numEntries++
	}
	return
}

type dirToDescend struct {
	name string
	path string
	ino  inode.InodeNumber
}

// readdir is a helper function to do the work of Readdir once we hold the lock.
func (mS *mountStruct) readdirHelper(inodeNumber inode.InodeNumber, prevBasenameReturned string, maxEntries uint64, maxBufSize uint64, callerID dlm.CallerID) (entries []inode.DirEntry, numEntries uint64, areMoreEntries bool, err error) {
	lockID, err := mS.volStruct.inodeVolumeHandle.MakeLockID(inodeNumber)
	if err != nil {
		return
	}
	if !dlm.IsLockHeld(lockID, callerID, dlm.ANYLOCK) {
		err = fmt.Errorf("%s: inode %v lock must be held before calling.", utils.GetFnName(), inodeNumber)
		return nil, 0, false, blunder.AddError(err, blunder.NotFoundError)
	}

	entries, areMoreEntries, err = mS.volStruct.inodeVolumeHandle.ReadDir(inodeNumber, maxEntries, maxBufSize, prevBasenameReturned)
	if err != nil {
		return entries, numEntries, areMoreEntries, err
	}
	numEntries = uint64(len(entries))

	// Tracker: 129872175: Directory entry must have the type, we should not be getting from inode, due to potential lock order issues.
	for i := range entries {
		if inodeNumber == entries[i].InodeNumber {
			entries[i].Type, _ = mS.getTypeHelper(entries[i].InodeNumber, callerID) // in case of "."
		} else {
			entryInodeLock, err1 := mS.volStruct.inodeVolumeHandle.InitInodeLock(entries[i].InodeNumber, callerID)
			if err = err1; err != nil {
				return
			}
			err = entryInodeLock.ReadLock()
			if err != nil {
				return
			}
			entries[i].Type, _ = mS.getTypeHelper(entries[i].InodeNumber, entryInodeLock.GetCallerID())
			entryInodeLock.Unlock()
		}
	}
	return entries, numEntries, areMoreEntries, err
}

// readdirOne is a helper function to do the work of ReaddirOne once we hold the lock.
func (mS *mountStruct) readdirOneHelper(inodeNumber inode.InodeNumber, prevDirMarker interface{}, callerID dlm.CallerID) (entries []inode.DirEntry, err error) {
	lockID, err := mS.volStruct.inodeVolumeHandle.MakeLockID(inodeNumber)
	if err != nil {
		return
	}
	if !dlm.IsLockHeld(lockID, callerID, dlm.ANYLOCK) {
		err = fmt.Errorf("%s: inode %v lock must be held before calling.", utils.GetFnName(), inodeNumber)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	entries, _, err = mS.volStruct.inodeVolumeHandle.ReadDir(inodeNumber, 1, 0, prevDirMarker)
	if err != nil {
		// Note: by convention, we don't log errors in helper functions; the caller should
		//       be the one to log or not given its use case.
		return entries, err
	}

	// Tracker: 129872175: Directory entry must have the type, we should not be getting from inode, due to potential lock order issues.
	for i := range entries {
		if inodeNumber == entries[i].InodeNumber {
			entries[i].Type, _ = mS.getTypeHelper(entries[i].InodeNumber, callerID) // in case of "."
		} else {
			entryInodeLock, err1 := mS.volStruct.inodeVolumeHandle.InitInodeLock(entries[i].InodeNumber, callerID)
			if err = err1; err != nil {
				return
			}
			err = entryInodeLock.ReadLock()
			if err != nil {
				return
			}
			entries[i].Type, _ = mS.getTypeHelper(entries[i].InodeNumber, callerID)
			entryInodeLock.Unlock()
		}
	}

	return entries, err
}
