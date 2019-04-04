// Package fs, sitting on top of the inode manager, defines the filesystem exposed by ProxyFS.
package fs

import (
	"bytes"
	"container/list"
	"fmt"
	"math"
	"path"
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
		logger.PanicfWithError(err, "InitInodeLock() for volume '%s' inode %v failed", vS.volumeName, inodeNumber)
	}
	err = inodeLock.WriteLock()
	if nil != err {
		logger.PanicfWithError(err, "dlm.Writelock() for volume '%s' inode %v failed", vS.volumeName, inodeNumber)
	}

	stillExists = vS.inodeVolumeHandle.Access(inodeNumber, inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.F_OK,
		inode.NoOverride)
	if stillExists {
		err = vS.inodeVolumeHandle.Flush(inodeNumber, false)
		if nil == err {
			vS.untrackInFlightFileInodeData(inodeNumber, false)
		} else {
			logger.ErrorfWithError(err, "Flush of file data failed on volume '%s' inode %v", vS.volumeName, inodeNumber)
		}
	}

	err = inodeLock.Unlock()
	if nil != err {
		logger.PanicfWithError(err, "dlm.Unlock() for volume '%s' inode %v failed", vS.volumeName, inodeNumber)
	}
}

func (inFlightFileInodeData *inFlightFileInodeDataStruct) inFlightFileInodeDataTracker() {
	var (
		flushFirst bool
	)

	logger.Tracef("fs.inFlightFileInodeDataTracker(): waiting to flush volume '%s' inode %v",
		inFlightFileInodeData.volStruct.volumeName, inFlightFileInodeData.InodeNumber)

	select {
	case flushFirst = <-inFlightFileInodeData.control:
		// All we needed was the value of flushFirst from control chan
	case <-time.After(inFlightFileInodeData.volStruct.maxFlushTime):
		flushFirst = true
	}

	logger.Tracef("fs.inFlightFileInodeDataTracker(): flush starting for volume '%s' inode %v flushfirst %t",
		inFlightFileInodeData.volStruct.volumeName, inFlightFileInodeData.InodeNumber, flushFirst)

	if flushFirst {
		inFlightFileInodeData.volStruct.inFlightFileInodeDataFlusher(inFlightFileInodeData.InodeNumber)
	}

	inFlightFileInodeData.wg.Done()
}

func mountByAccountName(accountName string, mountOptions MountOptions) (mountHandle MountHandle, err error) {
	var (
		mS         *mountStruct
		ok         bool
		volStruct  *volumeStruct
		volumeName string
	)

	startTime := time.Now()
	defer func() {
		globals.MountUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MountErrors.Add(1)
		}
	}()

	globals.Lock()

	volumeName, ok = inode.AccountNameToVolumeName(accountName)
	if !ok {
		err = fmt.Errorf("Unknown accountName passed to mountByAccountName(): \"%s\"", accountName)
		err = blunder.AddError(err, blunder.NotFoundError)
		globals.Unlock()
		return
	}

	volStruct, ok = globals.volumeMap[volumeName]
	if !ok {
		err = fmt.Errorf("Unknown volumeName computed by mountByAccountName(): \"%s\"", volumeName)
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

func mountByVolumeName(volumeName string, mountOptions MountOptions) (mountHandle MountHandle, err error) {
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
		err = fmt.Errorf("Unknown volumeName passed to mountByVolumeName(): \"%s\"", volumeName)
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

func (mS *mountStruct) FetchExtentMapChunk(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, fileInodeNumber inode.InodeNumber, fileOffset uint64, maxEntriesFromFileOffset int64, maxEntriesBeforeFileOffset int64) (extentMapChunk *inode.ExtentMapChunkStruct, err error) {
	var (
		inodeLock *dlm.RWLockStruct
		inodeType inode.InodeType
	)

	startTime := time.Now()
	defer func() {
		globals.FetchExtentMapChunkUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.FetchExtentMapChunkErrors.Add(1)
		}
	}()

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	inodeLock, err = mS.volStruct.inodeVolumeHandle.InitInodeLock(fileInodeNumber, nil)
	if nil != err {
		return
	}
	err = inodeLock.ReadLock()
	if nil != err {
		return
	}
	defer inodeLock.Unlock()

	if !mS.volStruct.inodeVolumeHandle.Access(fileInodeNumber, userID, groupID, otherGroupIDs, inode.F_OK,
		inode.NoOverride) {
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !mS.volStruct.inodeVolumeHandle.Access(fileInodeNumber, userID, groupID, otherGroupIDs, inode.R_OK,
		inode.OwnerOverride) {
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	inodeType, err = mS.volStruct.inodeVolumeHandle.GetType(fileInodeNumber)
	if nil != err {
		logger.ErrorfWithError(err, "couldn't get type for inode %v", fileInodeNumber)
		return
	}
	// Make sure the inode number is for a file inode
	if inodeType != inode.FileType {
		err = fmt.Errorf("%s: expected inode %v to be a file inode, got %v", utils.GetFnName(), fileInodeNumber, inodeType)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFileError)
		return
	}

	extentMapChunk, err = mS.volStruct.inodeVolumeHandle.FetchExtentMapChunk(fileInodeNumber, fileOffset, maxEntriesFromFileOffset, maxEntriesBeforeFileOffset)

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

// Unlock a given range. All locks held in this range by the process (identified by Pid) are removed.
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

	stat, err = mS.getstatHelperWhileLocked(inodeNumber)

	return
}

func (mS *mountStruct) getstatHelperWhileLocked(inodeNumber inode.InodeNumber) (stat Stat, err error) {
	var (
		metadata *inode.MetadataStruct
	)

	metadata, err = mS.volStruct.inodeVolumeHandle.GetMetadata(inodeNumber)
	if nil != err {
		return
	}

	stat = make(map[StatKey]uint64)

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

	return
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
	var (
		coalesceElementList      []*inode.CoalesceElement
		coalesceElementListIndex int
		coalesceSize             uint64
		coalesceTime             time.Time
		destFileInodeNumber      inode.InodeNumber
		dirEntryBasename         string
		dirEntryInodeNumber      inode.InodeNumber
		dirInodeNumber           inode.InodeNumber
		elementPath              string
		heldLocks                *heldLocksStruct
		restartBackoff           time.Duration
		retryRequired            bool
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

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareCoalesce(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	// Assemble WriteLock on each FileInode and their containing DirInode in elementPaths

	coalesceElementList = make([]*inode.CoalesceElement, len(elementPaths))

	for coalesceElementListIndex, elementPath = range elementPaths {
		dirInodeNumber, dirEntryInodeNumber, dirEntryBasename, _, retryRequired, err =
			mS.resolvePath(
				inode.RootDirInodeNumber,
				elementPath,
				heldLocks,
				resolvePathFollowDirSymlinks|
					resolvePathRequireExclusiveLockOnDirEntryInode|
					resolvePathRequireExclusiveLockOnDirInode)

		if nil != err {
			heldLocks.free()
			return
		}

		if retryRequired {
			heldLocks.free()
			goto Restart
		}

		// Record dirInode & dirEntryInode (fileInode) in elementList

		coalesceElementList[coalesceElementListIndex] = &inode.CoalesceElement{
			ContainingDirectoryInodeNumber: dirInodeNumber,
			ElementInodeNumber:             dirEntryInodeNumber,
			ElementName:                    dirEntryBasename,
		}
	}

	_, dirEntryInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			destPath,
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks|
				resolvePathCreateMissingPathElements|
				resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	// Invoke package inode to actually perform the Coalesce operation

	destFileInodeNumber = dirEntryInodeNumber
	coalesceTime, numWrites, _, err = mS.volStruct.inodeVolumeHandle.Coalesce(destFileInodeNumber, coalesceElementList)

	// We can now release all the WriteLocks we are currently holding

	heldLocks.free()

	// Regardless of err return, fill in other return values

	ino = uint64(destFileInodeNumber)
	modificationTime = uint64(coalesceTime.UnixNano())

	return
}

func (mS *mountStruct) MiddlewareDelete(parentDir string, basename string) (err error) {
	var (
		dirEntryBasename    string
		dirEntryInodeNumber inode.InodeNumber
		dirInodeNumber      inode.InodeNumber
		doDestroy           bool
		heldLocks           *heldLocksStruct
		inodeType           inode.InodeType
		inodeVolumeHandle   inode.VolumeHandle
		linkCount           uint64
		numDirEntries       uint64
		restartBackoff      time.Duration
		retryRequired       bool
	)

	startTime := time.Now()
	defer func() {
		globals.MiddlewareDeleteUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareDeleteErrors.Add(1)
		}
	}()

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareDelete(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	dirInodeNumber, dirEntryInodeNumber, dirEntryBasename, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			parentDir+"/"+basename,
			heldLocks,
			resolvePathFollowDirSymlinks|
				resolvePathRequireExclusiveLockOnDirEntryInode|
				resolvePathRequireExclusiveLockOnDirInode)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	// Check if Unlink() and Destroy() are doable

	inodeVolumeHandle = mS.volStruct.inodeVolumeHandle

	inodeType, err = inodeVolumeHandle.GetType(dirEntryInodeNumber)
	if nil != err {
		heldLocks.free()
		return
	}

	if inode.DirType == inodeType {
		numDirEntries, err = inodeVolumeHandle.NumDirEntries(dirEntryInodeNumber)
		if nil != err {
			heldLocks.free()
			return
		}

		if 2 != numDirEntries {
			heldLocks.free()
			err = blunder.NewError(blunder.NotEmptyError, "%s/%s not empty", parentDir, basename)
			return
		}

		doDestroy = true
	} else {
		linkCount, err = inodeVolumeHandle.GetLinkCount(dirEntryInodeNumber)
		if nil != err {
			heldLocks.free()
			return
		}

		doDestroy = (1 == linkCount)
	}

	// Now perform the Unlink() and (potentially) Destroy()

	err = inodeVolumeHandle.Unlink(dirInodeNumber, dirEntryBasename, false)
	if nil != err {
		heldLocks.free()
		return
	}

	if doDestroy {
		err = inodeVolumeHandle.Destroy(dirEntryInodeNumber)
		if nil != err {
			logger.Errorf("fs.MiddlewareDelete() failed to Destroy dirEntryInodeNumber 0x%016X: %v", dirEntryInodeNumber, err)
		}
	}

	// Release heldLocks and exit with success (even if Destroy() failed earlier)

	heldLocks.free()

	err = nil
	return
}

func (mS *mountStruct) middlewareReadDirHelper(path string, maxEntries uint64, prevBasename string) (pathDirInodeNumber inode.InodeNumber, dirEntrySlice []inode.DirEntry, moreEntries bool, err error) {
	var (
		dirEntrySliceElement  inode.DirEntry
		heldLocks             *heldLocksStruct
		internalDirEntrySlice []inode.DirEntry
		restartBackoff        time.Duration
		retryRequired         bool
	)

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareGetObject(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	_, pathDirInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			path,
			heldLocks,
			resolvePathFollowDirSymlinks)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	// Now assemble response

	internalDirEntrySlice, moreEntries, err = mS.volStruct.inodeVolumeHandle.ReadDir(pathDirInodeNumber, maxEntries, 0, prevBasename)
	if nil != err {
		heldLocks.free()
		return
	}

	// No need to hold any locks now... directory contents should be allowed to change while enumerating
	heldLocks.free()

	dirEntrySlice = make([]inode.DirEntry, 0, len(internalDirEntrySlice))

	for _, dirEntrySliceElement = range internalDirEntrySlice {
		if ("." == dirEntrySliceElement.Basename) || (".." == dirEntrySliceElement.Basename) {
			dirEntrySliceElement.Type = inode.DirType
		} else {
			dirEntrySliceElement.Type, err = mS.GetType(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirEntrySliceElement.InodeNumber)
			if nil != err {
				// It's ok to have an error here... it just means the directory we are iterating is changing
				continue
			}
		}
		dirEntrySlice = append(dirEntrySlice, dirEntrySliceElement)
	}

	dirEntrySlice = dirEntrySlice[:len(dirEntrySlice)]

	err = nil
	return
}

func (mS *mountStruct) MiddlewareGetAccount(maxEntries uint64, marker string, endmarker string) (accountEnts []AccountEntry, mtime uint64, ctime uint64, err error) {
	var (
		dirEntrySlice        []inode.DirEntry
		dirEntrySliceElement inode.DirEntry
		remainingMaxEntries  uint64
		moreEntries          bool
		statResult           Stat
	)

	statResult, err = mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber)
	if nil != err {
		return
	}
	mtime = statResult[StatMTime]
	ctime = statResult[StatCTime]

	if 0 != maxEntries {
		// Hard limit to number of DirInode Basenames to return
		accountEnts = make([]AccountEntry, 0, maxEntries)
	}

	remainingMaxEntries = maxEntries

	moreEntries = true

	for moreEntries {
		_, dirEntrySlice, moreEntries, err = mS.middlewareReadDirHelper("/", remainingMaxEntries, marker)
		if nil != err {
			return
		}

		if 0 == maxEntries {
			// No limit to number of DirInode Basenames to return... so it must be <= len(dirEntrySlice)
			accountEnts = make([]AccountEntry, 0, len(dirEntrySlice))
			// Note: moreEntries should be false so the "for moreEntries" loop should exit after 1st iteration
		}

		for _, dirEntrySliceElement = range dirEntrySlice {
			if ("" != endmarker) && (0 <= strings.Compare(dirEntrySliceElement.Basename, endmarker)) {
				moreEntries = false
				break
			}
			if ("." != dirEntrySliceElement.Basename) && (".." != dirEntrySliceElement.Basename) {
				// So we've skipped "." & ".." - now also skip non-DirInodes
				if inode.DirType == dirEntrySliceElement.Type {
					statResult, err = mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirEntrySliceElement.InodeNumber)
					if nil != err {
						return
					}
					accountEnts = append(accountEnts, AccountEntry{
						Basename:         dirEntrySliceElement.Basename,
						ModificationTime: statResult[StatMTime],
						AttrChangeTime:   statResult[StatCTime],
					})
				}
			}
		}

		if moreEntries && (0 != maxEntries) {
			remainingMaxEntries = maxEntries - uint64(len(accountEnts))
			if 0 == remainingMaxEntries {
				moreEntries = false
			}
		}

		if moreEntries {
			// Adjust marker to fetch next dirEntrySlice
			marker = dirEntrySlice[len(dirEntrySlice)-1].Basename
		}
	}

	accountEnts = accountEnts[:len(accountEnts)]

	return
}

type dirEntrySliceStackElementStruct struct {
	dirPath       string
	dirEntrySlice []inode.DirEntry
	numConsumed   int
	moreEntries   bool
}

func (mS *mountStruct) MiddlewareGetContainer(vContainerName string, maxEntries uint64, marker string, endmarker string, prefix string, delimiter string) (containerEnts []ContainerEntry, err error) {
	var (
		containerEntry                ContainerEntry
		containerEntryBasename        string // Misnamed... this is actually everything after ContainerName
		containerEntryPath            string
		containerEntryPathSplit       []string // Split on only the first '/' (to remove ContainerName from it)
		doSingleDirectory             bool
		dirEntryInodeLock             *dlm.RWLockStruct
		dirEntryInodeNumber           inode.InodeNumber
		dirEntryInodeType             inode.InodeType
		dirEntryMetadata              *inode.MetadataStruct
		dirEntryPath                  string
		dirEntrySlice                 []inode.DirEntry
		dirEntrySliceElement          inode.DirEntry
		dirEntrySliceElementIndex     int
		dirEntrySliceElementToPrepend *inode.DirEntry
		dirEntrySliceStack            []*dirEntrySliceStackElementStruct
		dirEntrySliceStackElement     *dirEntrySliceStackElementStruct
		dirEntrySliceToAppend         []inode.DirEntry
		dirInodeNumber                inode.InodeNumber
		dirPath                       string
		dirPathSplit                  []string
		dlmCallerID                   dlm.CallerID
		endmarkerCanonicalized        string
		endmarkerPath                 []string
		heldLocks                     *heldLocksStruct
		initialDirEntryToMatch        string // == "" if no initial path should be returned (i.e. in marker starting point case)
		inodeVolumeHandle             inode.VolumeHandle
		markerCanonicalized           string
		markerPath                    []string
		markerPathDirInodeIndex       int
		moreEntries                   bool
		pathIndex                     int
		prefixCanonicalized           string
		prefixPath                    []string
		prefixPathDirInodeIndex       int
		prevReturned                  string
		remainingMaxEntries           uint64
		restartBackoff                time.Duration
		retryRequired                 bool
	)

	// Validate marker, endmarker, and prefix

	if "" == marker {
		markerPath = []string{}
		markerPathDirInodeIndex = -1 // Must be special cased below to ensure we don't look in markerPath
		markerCanonicalized = ""     // Actually never accessed
	} else {
		markerPath, markerPathDirInodeIndex, err = mS.canonicalizePathAndLocateLeafDirInode(vContainerName + "/" + marker)
		if nil != err {
			err = blunder.AddError(err, blunder.InvalidArgError)
			return
		}

		markerCanonicalized = strings.Join(markerPath, "/")
		if strings.HasSuffix(marker, "/") {
			markerCanonicalized += "/"
		}

		if vContainerName+"/"+marker != markerCanonicalized {
			err = blunder.NewError(blunder.InvalidArgError, "MiddlewareGetContainer() only supports a canonicalized marker")
			return
		}
	}

	if "" == endmarker {
		endmarkerPath = []string{}
		endmarkerCanonicalized = "" // Actually never accessed
	} else {
		endmarkerPath, _, err = mS.canonicalizePathAndLocateLeafDirInode(vContainerName + "/" + endmarker)
		if nil != err {
			err = blunder.AddError(err, blunder.InvalidArgError)
			return
		}

		endmarkerCanonicalized = strings.Join(endmarkerPath, "/")
		if strings.HasSuffix(endmarker, "/") {
			endmarkerCanonicalized += "/"
		}

		if vContainerName+"/"+endmarker != endmarkerCanonicalized {
			err = blunder.NewError(blunder.InvalidArgError, "MiddlewareGetContainer() only supports a canonicalized endmarker")
			return
		}
	}

	prefixPath, prefixPathDirInodeIndex, err = mS.canonicalizePathAndLocateLeafDirInode(vContainerName + "/" + prefix)
	if nil != err {
		err = blunder.AddError(err, blunder.InvalidArgError)
		return
	}
	if prefixPathDirInodeIndex < 0 {
		err = blunder.NewError(blunder.NotFoundError, "MiddlewareGetContainer() only supports querying an existing Container")
		return
	}

	prefixCanonicalized = strings.Join(prefixPath, "/")
	if strings.HasSuffix(prefix, "/") {
		prefixCanonicalized += "/"
	}

	if (prefix != "") && (vContainerName+"/"+prefix != prefixCanonicalized) {
		err = blunder.NewError(blunder.InvalidArgError, "MiddlewareGetContainer() only supports a canonicalized prefix")
		return
	}

	// Validate delimiter

	switch delimiter {
	case "":
		doSingleDirectory = false
	case "/":
		doSingleDirectory = true
	default:
		err = blunder.NewError(blunder.InvalidArgError, "MiddlewareGetContainer() only supports a delimiter of \"/\"")
		return
	}

	// Determine what DirInode from which to begin our enumeration

	pathIndex = 0

	for {
		if (pathIndex > markerPathDirInodeIndex) && (pathIndex > prefixPathDirInodeIndex) {
			// Special (though probably typical) case where marker lands in prefix-indicated directory

			dirPath = strings.Join(prefixPath[:prefixPathDirInodeIndex+1], "/")

			if (1 == len(prefixPath)) || strings.HasSuffix(prefix, "/") {
				if (markerPathDirInodeIndex + 1) == len(markerPath) {
					prevReturned = ""
				} else {
					prevReturned = markerPath[markerPathDirInodeIndex+1]
				}
			} else {
				// Handle four remaining cases:
				//   marker & prefix both specified directories
				//   marker specified a directory, prefix did not
				//   prefix specified a directory, marker did not
				//   neither marker nor prefix specified a directory

				if (markerPathDirInodeIndex + 1) == len(markerPath) {
					if (prefixPathDirInodeIndex + 1) == len(prefixPath) {
						// Case where marker & prefix both specified directories

						prevReturned = ""
					} else {
						// Case where marker specified a directory, prefix did not

						prevReturned = prefixPath[prefixPathDirInodeIndex+1]
					}
				} else { // (markerPathDirInodeIndex + 1) != len(markerPath)
					if (prefixPathDirInodeIndex + 1) == len(prefixPath) {
						// Case where prefix specified a directory, marker did not

						prevReturned = markerPath[markerPathDirInodeIndex+1]
					} else {
						// Case where neither marker nor prefix specified a directory

						if strings.Compare(prefixPath[prefixPathDirInodeIndex+1], markerPath[markerPathDirInodeIndex+1]) <= 0 {
							prevReturned = markerPath[markerPathDirInodeIndex+1]
						} else {
							prevReturned = prefixPath[prefixPathDirInodeIndex+1]
						}
					}
				}
			}
			initialDirEntryToMatch = prevReturned
			break
		}

		if pathIndex > markerPathDirInodeIndex {
			// Handle case where prefix is more constraining than marker

			if prefixPathDirInodeIndex == (len(prefixPath) - 1) {
				if (1 == len(prefixPath)) || strings.HasSuffix(prefix, "/") {
					dirPath = strings.Join(prefixPath[:prefixPathDirInodeIndex+1], "/")
					prevReturned = ""
				} else {
					dirPath = strings.Join(prefixPath[:prefixPathDirInodeIndex], "/")
					prevReturned = prefixPath[len(prefixPath)-1]
				}
			} else {
				dirPath = strings.Join(prefixPath[:prefixPathDirInodeIndex+1], "/")
				prevReturned = prefixPath[len(prefixPath)-1]
			}
			initialDirEntryToMatch = prevReturned
			break
		}

		if pathIndex > prefixPathDirInodeIndex {
			// Handle case where marker is more constraining than prefix

			dirPath = strings.Join(markerPath[:markerPathDirInodeIndex+1], "/")
			if markerPathDirInodeIndex == (len(markerPath) - 1) {
				prevReturned = ""
			} else {
				prevReturned = markerPath[len(markerPath)-1]
			}
			initialDirEntryToMatch = ""
			break
		}

		switch strings.Compare(prefixPath[pathIndex], markerPath[pathIndex]) {
		case -1:
			dirPath = strings.Join(markerPath[:markerPathDirInodeIndex+1], "/")
			if markerPathDirInodeIndex == (len(markerPath) - 1) {
				prevReturned = ""
			} else {
				prevReturned = markerPath[len(markerPath)-1]
			}
			initialDirEntryToMatch = ""
			break
		case 0:
			pathIndex++
		case 1:
			if prefixPathDirInodeIndex == (len(prefixPath) - 1) {
				if (1 == len(prefixPath)) || strings.HasSuffix(prefix, "/") {
					dirPath = strings.Join(prefixPath[:prefixPathDirInodeIndex+1], "/")
					prevReturned = ""
				} else {
					dirPath = strings.Join(prefixPath[:prefixPathDirInodeIndex], "/")
					prevReturned = prefixPath[len(prefixPath)-1]
				}
			} else {
				dirPath = strings.Join(prefixPath[:prefixPathDirInodeIndex+1], "/")
				prevReturned = prefixPath[len(prefixPath)-1]
			}
			initialDirEntryToMatch = prevReturned
			break
		}
	}

	// Setup shortcuts/contants

	dlmCallerID = dlm.GenerateCallerID()
	inodeVolumeHandle = mS.volStruct.inodeVolumeHandle

	// Compute initial response

	restartBackoff = time.Duration(0)

Restart:

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareGetContainer(): failed in restartBackoff: %v", err)
	}

	heldLocks = newHeldLocks()

	_, dirInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			dirPath,
			heldLocks,
			resolvePathDirEntryInodeMustBeDirectory)
	if nil != err {
		heldLocks.free()
		return
	}
	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	containerEnts = make([]ContainerEntry, 0, maxEntries)

	if 0 == maxEntries {
		heldLocks.free()
		err = nil
		return
	}

	if "" == initialDirEntryToMatch {
		dirEntrySliceElementToPrepend = nil
	} else {
		if "" == dirPath {
			dirEntryPath = initialDirEntryToMatch
		} else {
			dirEntryPath = dirPath + "/" + initialDirEntryToMatch
		}
		if ("" != endmarker) && (strings.Compare(dirEntryPath, endmarkerCanonicalized) >= 0) {
			heldLocks.free()
			err = nil
			return
		}
		dirEntryInodeNumber, err = inodeVolumeHandle.Lookup(dirInodeNumber, initialDirEntryToMatch)
		if nil == err {
			retryRequired = heldLocks.attemptSharedLock(inodeVolumeHandle, dlmCallerID, dirEntryInodeNumber)
			if retryRequired {
				heldLocks.free()
				goto Restart
			}
			dirEntryInodeType, err = inodeVolumeHandle.GetType(dirEntryInodeNumber)
			if nil == err {
				dirEntrySliceElementToPrepend = &inode.DirEntry{
					InodeNumber: dirEntryInodeNumber,
					Basename:    initialDirEntryToMatch,
					Type:        dirEntryInodeType,
				}
			} else {
				dirEntrySliceElementToPrepend = nil
			}
			heldLocks.unlock(dirEntryInodeNumber)
		} else {
			dirEntrySliceElementToPrepend = nil
		}
	}

	heldLocks.free()

	if 0 == maxEntries {
		remainingMaxEntries = 0
	} else {
		if nil == dirEntrySliceElementToPrepend {
			remainingMaxEntries = maxEntries
		} else {
			remainingMaxEntries = maxEntries - 1
		}
	}

	// At this point:
	//   no heldLocks
	//   containerEnts has been declared
	//   doSingleDirectory is set based on supplied delimiter
	//   if {marker,endmarker,prefix} asked to include an exact matched path that existed, it's in dirEntrySliceElementToPrepend
	//   prefixCanonicalized & endmarkerCanonicalized are set to terminate the ensuing treewalk
	//   remainingMaxEntries indicates how many more DirEntry's will fit in containerEnts (if capped)
	//   dirPath is pointing to the initial DirInode to read
	//   prevReturned indicates from where in the DirInode to start reading

	// Perform initial ReadDir and place in dirEntrySliceStack

	if nil == dirEntrySliceElementToPrepend {
		_, dirEntrySlice, moreEntries, err = mS.middlewareReadDirHelper(dirPath, remainingMaxEntries, prevReturned)
		if nil != err {
			return
		}
	} else {
		if 0 == remainingMaxEntries {
			dirEntrySlice = []inode.DirEntry{*dirEntrySliceElementToPrepend}
			moreEntries = false
		} else {
			_, dirEntrySliceToAppend, moreEntries, err = mS.middlewareReadDirHelper(dirPath, remainingMaxEntries, prevReturned)
			if nil == err {
				dirEntrySlice = make([]inode.DirEntry, 1, 1+len(dirEntrySliceToAppend))
				dirEntrySlice[0] = *dirEntrySliceElementToPrepend
				dirEntrySlice = append(dirEntrySlice, dirEntrySliceToAppend...)
			} else {
				return
			}
		}
	}

	dirEntrySliceStackElement = &dirEntrySliceStackElementStruct{
		dirPath:       dirPath,
		dirEntrySlice: dirEntrySlice,
		numConsumed:   0,
		moreEntries:   moreEntries,
	}

	dirEntrySliceStack = []*dirEntrySliceStackElementStruct{dirEntrySliceStackElement}

	containerEnts = make([]ContainerEntry, 0, len(dirEntrySlice))

	// Now append appropriate ContainerEntry's until exit criteria is reached

	for uint64(len(containerEnts)) < maxEntries {
		dirEntrySliceStackElement = dirEntrySliceStack[len(dirEntrySliceStack)-1]

		if dirEntrySliceStackElement.numConsumed == len(dirEntrySliceStackElement.dirEntrySlice) {
			if dirEntrySliceStackElement.moreEntries {
				dirPath = dirEntrySliceStackElement.dirPath
				dirEntrySlice = dirEntrySliceStackElement.dirEntrySlice
				dirEntrySliceElementIndex = len(dirEntrySlice) - 1
				dirEntrySliceElement = dirEntrySlice[dirEntrySliceElementIndex]
				prevReturned = dirEntrySliceElement.Basename

				_, dirEntrySlice, moreEntries, err = mS.middlewareReadDirHelper(dirPath, remainingMaxEntries, prevReturned)
				if (nil != err) || (0 == len(dirEntrySlice)) {
					// Even though we thought there were moreEntries, there now are not for some reason

					if doSingleDirectory {
						// Regardless of remaining contents of dirEntrySliceStack, we must be done

						err = nil
						return
					}

					// Navigate to parent directory

					dirEntrySliceStack = dirEntrySliceStack[:len(dirEntrySliceStack)-1]
					continue
				}

				// Restart this loop on current dirEntrySliceStackElement with new middlewareReadDirHelper() results

				dirEntrySliceStackElement.dirEntrySlice = dirEntrySlice
				dirEntrySliceStackElement.numConsumed = 0
				dirEntrySliceStackElement.moreEntries = moreEntries

				continue
			} else {
				// We've reached the end of this DirInode

				if doSingleDirectory {
					// Regardless of remaining contents of dirEntrySliceStack, we must be done

					err = nil
					return
				}

				// Navigate to parent directory (staying within this Container)

				if 1 == len(dirEntrySliceStack) {
					// We are at the starting directory

					dirPathSplit = strings.Split(dirEntrySliceStackElement.dirPath, "/")

					if 1 == len(dirPathSplit) {
						// We just finished Container-level directory, so we are done

						err = nil
						return
					}

					// Modify dirEntrySliceStackElement to point to parent directory as if we'd just processed the dirEntry of this directory

					dirPath = strings.Join(dirPathSplit[:len(dirPathSplit)-1], "/")

					if 0 == maxEntries {
						remainingMaxEntries = 0
					} else {
						remainingMaxEntries = maxEntries - uint64(len(containerEnts))
					}

					prevReturned = dirPathSplit[len(dirPathSplit)-1]

					_, dirEntrySlice, moreEntries, err = mS.middlewareReadDirHelper(dirPath, remainingMaxEntries, prevReturned)
					if nil != err {
						return
					}

					dirEntrySliceStackElement.dirPath = dirPath
					dirEntrySliceStackElement.dirEntrySlice = dirEntrySlice
					dirEntrySliceStackElement.numConsumed = 0
					dirEntrySliceStackElement.moreEntries = moreEntries
				} else {
					// Parent directory already in dirEntrySliceStack... so just pop current ...Element

					dirEntrySliceStack = dirEntrySliceStack[:len(dirEntrySliceStack)-1]
				}

				continue
			}
		}

		// Consume next dirEntrySliceElement
		// ...skipping "." and ".."
		// ...skipping if <dirPath>/<Basename> <= marker
		// ...recursing when encountering DirInode's if !doSingleDirectory
		// ...terminating early if either:
		//      len(*containerEnts) reaches maxEntries
		//      <dirPath>/<Basename> >= endmarker
		//      <dirPath>/<Basename> does not start with prefix

		dirEntrySlice = dirEntrySliceStackElement.dirEntrySlice
		dirEntrySliceElementIndex = dirEntrySliceStackElement.numConsumed
		dirEntrySliceElement = dirEntrySlice[dirEntrySliceElementIndex]

		dirEntrySliceStackElement.numConsumed++

		if ("." == dirEntrySliceElement.Basename) || (".." == dirEntrySliceElement.Basename) {
			continue
		}

		containerEntryPath = dirEntrySliceStackElement.dirPath + "/" + dirEntrySliceElement.Basename

		if ("" != marker) && (strings.Compare(containerEntryPath, markerCanonicalized) <= 0) {
			err = nil
			return
		}
		if ("" != endmarker) && (strings.Compare(containerEntryPath, endmarkerCanonicalized) >= 0) {
			err = nil
			return
		}
		if ("" != prefix) && !strings.HasPrefix(containerEntryPath, prefixCanonicalized) {
			err = nil
			return
		}

		// Ok... so we actually want to append this entry to containerEnts

		restartBackoff = time.Duration(0)

	Retry:

		restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
		if nil != err {
			logger.Fatalf("MiddlewareGetContainer(): failed in restartBackoff: %v", err)
		}

		dirEntryInodeLock, err = inodeVolumeHandle.AttemptReadLock(dirEntrySliceElement.InodeNumber, dlmCallerID)
		if nil != err {
			goto Retry
		}

		dirEntryMetadata, err = inodeVolumeHandle.GetMetadata(dirEntrySliceElement.InodeNumber)
		if nil != err {
			// Ok... so it must have disappeared... just skip it

			err = dirEntryInodeLock.Unlock()
			if nil != err {
				logger.Fatalf("Failure unlocking a held LockID %s: %v", dirEntryInodeLock.LockID, err)
			}

			continue
		}

		containerEntryPathSplit = strings.SplitN(containerEntryPath, "/", 2)
		containerEntryBasename = containerEntryPathSplit[1]

		containerEntry = ContainerEntry{
			Basename:         containerEntryBasename,
			FileSize:         dirEntryMetadata.Size,
			ModificationTime: uint64(dirEntryMetadata.ModificationTime.UnixNano()),
			AttrChangeTime:   uint64(dirEntryMetadata.AttrChangeTime.UnixNano()),
			IsDir:            (dirEntrySliceElement.Type == inode.DirType),
			NumWrites:        dirEntryMetadata.NumWrites,
			InodeNumber:      uint64(dirEntrySliceElement.InodeNumber),
		}

		containerEntry.Metadata, err = inodeVolumeHandle.GetStream(dirEntrySliceElement.InodeNumber, MiddlewareStream)
		if nil != err {
			if blunder.Is(err, blunder.StreamNotFound) {
				// No MiddlewareStream... just make it appear empty

				containerEntry.Metadata = []byte{}
				err = nil
			} else {
				// Ok... so it must have disappeared... just skip it

				err = dirEntryInodeLock.Unlock()
				if nil != err {
					logger.Fatalf("Failure unlocking a held LockID %s: %v", dirEntryInodeLock.LockID, err)
				}

				continue
			}
		}

		// We can finally Unlock() this dirEntryInodeLock

		err = dirEntryInodeLock.Unlock()
		if nil != err {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", dirEntryInodeLock.LockID, err)
		}

		// If we reach here, we get to append this containerEntry to containerEnts

		containerEnts = append(containerEnts, containerEntry)

		// We must now descend into dirEntryInode descend into it if it's a DirInode and !doSingleDirectory

		if !doSingleDirectory && (dirEntrySliceElement.Type == inode.DirType) {
			dirPath = dirEntrySliceStackElement.dirPath + "/" + dirEntrySliceElement.Basename

			if 0 == maxEntries {
				remainingMaxEntries = 0
			} else {
				remainingMaxEntries = maxEntries - uint64(len(containerEnts))
			}

			prevReturned = ""

			_, dirEntrySlice, moreEntries, err = mS.middlewareReadDirHelper(dirPath, remainingMaxEntries, prevReturned)
			if nil != err {
				return
			}

			dirEntrySliceStackElement = &dirEntrySliceStackElementStruct{
				dirPath:       dirPath,
				dirEntrySlice: dirEntrySlice,
				numConsumed:   0,
				moreEntries:   moreEntries,
			}

			dirEntrySliceStack = append(dirEntrySliceStack, dirEntrySliceStackElement)
		}
	}

	// We will only reach here if we exhausted maxEntries before exhausing the tree/list of containerEntry's to append

	err = nil
	return
}

func (mS *mountStruct) MiddlewareGetObject(containerObjectPath string, readRangeIn []ReadRangeIn, readRangeOut *[]inode.ReadPlanStep) (fileSize uint64, lastModified uint64, lastChanged uint64, ino uint64, numWrites uint64, serializedMetadata []byte, err error) {
	var (
		dirEntryInodeNumber inode.InodeNumber
		fileOffset          uint64
		heldLocks           *heldLocksStruct
		inodeVolumeHandle   inode.VolumeHandle
		readPlan            []inode.ReadPlanStep
		readRangeInIndex    int
		restartBackoff      time.Duration
		retryRequired       bool
		stat                Stat
	)

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

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareGetObject(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	_, dirEntryInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			containerObjectPath,
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	// Now assemble response

	stat, err = mS.getstatHelperWhileLocked(dirEntryInodeNumber)
	if nil != err {
		heldLocks.free()
		return
	}

	fileSize = stat[StatSize]
	lastModified = stat[StatMTime]
	lastChanged = stat[StatCTime]
	ino = uint64(dirEntryInodeNumber)
	numWrites = stat[StatNumWrites]

	serializedMetadata, err = mS.volStruct.inodeVolumeHandle.GetStream(dirEntryInodeNumber, MiddlewareStream)
	if nil != err {
		if blunder.Is(err, blunder.StreamNotFound) {
			serializedMetadata = []byte{}
			err = nil
		} else {
			heldLocks.free()
			return
		}
	}

	inodeVolumeHandle = mS.volStruct.inodeVolumeHandle

	if len(readRangeIn) == 0 {
		// Get ReadPlan for entire file

		fileOffset = 0

		readPlan, err = inodeVolumeHandle.GetReadPlan(dirEntryInodeNumber, &fileOffset, &fileSize)
		if nil != err {
			heldLocks.free()
			return
		}

		_ = appendReadPlanEntries(readPlan, readRangeOut)
	} else { // len(readRangeIn) > 0
		// Append each computed range

		for readRangeInIndex = range readRangeIn {
			readPlan, err = inodeVolumeHandle.GetReadPlan(dirEntryInodeNumber, readRangeIn[readRangeInIndex].Offset, readRangeIn[readRangeInIndex].Len)
			if nil != err {
				heldLocks.free()
				return
			}

			_ = appendReadPlanEntries(readPlan, readRangeOut)
		}
	}

	heldLocks.free()

	err = nil
	return
}

func (mS *mountStruct) MiddlewareHeadResponse(entityPath string) (response HeadResponse, err error) {
	var (
		dirEntryInodeNumber inode.InodeNumber
		heldLocks           *heldLocksStruct
		restartBackoff      time.Duration
		retryRequired       bool
		stat                Stat
	)

	startTime := time.Now()
	defer func() {
		globals.MiddlewareHeadResponseUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareHeadResponseErrors.Add(1)
		}
	}()

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareHeadResponse(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	_, dirEntryInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			entityPath,
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	// Now assemble response

	stat, err = mS.getstatHelperWhileLocked(dirEntryInodeNumber)
	if nil != err {
		heldLocks.free()
		return
	}

	response.ModificationTime = stat[StatMTime]
	response.AttrChangeTime = stat[StatCTime]
	response.FileSize = stat[StatSize]
	response.IsDir = (stat[StatFType] == uint64(inode.DirType))
	response.InodeNumber = dirEntryInodeNumber
	response.NumWrites = stat[StatNumWrites]

	response.Metadata, err = mS.volStruct.inodeVolumeHandle.GetStream(dirEntryInodeNumber, MiddlewareStream)
	if nil != err {
		heldLocks.free()
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

	heldLocks.free()
	return
}

func (mS *mountStruct) MiddlewarePost(parentDir string, baseName string, newMetaData []byte, oldMetaData []byte) (err error) {
	var (
		dirEntryInodeNumber inode.InodeNumber
		existingStreamData  []byte
		heldLocks           *heldLocksStruct
		restartBackoff      time.Duration
		retryRequired       bool
	)

	startTime := time.Now()
	defer func() {
		globals.MiddlewarePostUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.MiddlewarePostBytes.Add(uint64(len(newMetaData)))
		if err != nil {
			globals.MiddlewarePostErrors.Add(1)
		}
	}()

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareHeadResponse(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	_, dirEntryInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			parentDir+"/"+baseName,
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks|
				resolvePathCreateMissingPathElements|
				resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	// Now apply MiddlewareStream update

	// Compare oldMetaData to existing existingStreamData to make sure that the HTTP metadata has not changed.
	// If it has changed, then return an error since middleware has to handle it.

	existingStreamData, err = mS.volStruct.inodeVolumeHandle.GetStream(dirEntryInodeNumber, MiddlewareStream)
	if nil != err {
		if blunder.Is(err, blunder.StreamNotFound) {
			err = nil
			existingStreamData = make([]byte, 0)
		} else {
			heldLocks.free()
			return
		}
	}

	// Verify that the oldMetaData is the same as the one we think we are changing.

	if !bytes.Equal(existingStreamData, oldMetaData) {
		heldLocks.free()
		err = blunder.NewError(blunder.TryAgainError, "MiddlewarePost(): MetaData different - existingStreamData: %v OldMetaData: %v", existingStreamData, oldMetaData)
		return
	}

	// Change looks okay so make it.

	err = mS.volStruct.inodeVolumeHandle.PutStream(dirEntryInodeNumber, MiddlewareStream, newMetaData)
	if nil != err {
		heldLocks.free()
		return
	}

	// PutStream() implicitly flushed... so, if it was a FileInode, we don't need to track it anymore

	mS.volStruct.untrackInFlightFileInodeData(dirEntryInodeNumber, false)

	heldLocks.free()
	return
}

func (mS *mountStruct) MiddlewarePutComplete(vContainerName string, vObjectPath string, pObjectPaths []string, pObjectLengths []uint64, pObjectMetadata []byte) (mtime uint64, ctime uint64, fileInodeNumber inode.InodeNumber, numWrites uint64, err error) {
	var (
		dirEntryInodeNumber inode.InodeNumber
		fileOffset          uint64
		heldLocks           *heldLocksStruct
		inodeVolumeHandle   inode.VolumeHandle
		numPObjects         int
		pObjectIndex        int
		restartBackoff      time.Duration
		retryRequired       bool
		stat                Stat
	)

	startTime := time.Now()
	defer func() {
		globals.MiddlewareMkdirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareMkdirErrors.Add(1)
		}
	}()

	// Validate (pObjectPaths,pObjectLengths) args

	numPObjects = len(pObjectPaths)

	if numPObjects != len(pObjectLengths) {
		blunder.NewError(blunder.InvalidArgError, "MiddlewarePutComplete() expects len(pObjectPaths) == len(pObjectLengths)")
		return
	}

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareHeadResponse(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	_, dirEntryInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			vContainerName+"/"+vObjectPath,
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks|
				resolvePathCreateMissingPathElements|
				resolvePathDirEntryInodeMustBeFile|
				resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	// Apply (pObjectPaths,pObjectLengths) to (erased) FileInode

	inodeVolumeHandle = mS.volStruct.inodeVolumeHandle

	fileOffset = 0

	for pObjectIndex = 0; pObjectIndex < numPObjects; pObjectIndex++ {
		err = inodeVolumeHandle.Wrote(
			dirEntryInodeNumber,
			pObjectPaths[pObjectIndex],
			[]uint64{fileOffset},
			[]uint64{0},
			[]uint64{pObjectLengths[pObjectIndex]},
			pObjectIndex > 0) // Initial pObjectIndex == 0 case will implicitly SetSize(,0)
		if nil != err {
			heldLocks.free()
			logger.DebugfIDWithError(internalDebug, err, "MiddlewarePutComplete(): failed Wrote() for dirEntryInodeNumber 0x%016X", dirEntryInodeNumber)
			return
		}

		fileOffset += pObjectLengths[pObjectIndex]
	}

	// Apply pObjectMetadata to FileInode (this will flush it as well)

	err = inodeVolumeHandle.PutStream(dirEntryInodeNumber, MiddlewareStream, pObjectMetadata)
	if err != nil {
		heldLocks.free()
		logger.DebugfIDWithError(internalDebug, err, "MiddlewarePutComplete(): failed PutStream() for dirEntryInodeNumber 0x%016X (pObjectMetadata: %v)", dirEntryInodeNumber, pObjectMetadata)
		return
	}

	stat, err = mS.getstatHelperWhileLocked(dirEntryInodeNumber)
	if nil != err {
		heldLocks.free()
		return
	}

	mtime = stat[StatMTime]
	ctime = stat[StatCTime]
	fileInodeNumber = dirEntryInodeNumber
	numWrites = stat[StatNumWrites]

	heldLocks.free()
	return
}

func (mS *mountStruct) MiddlewareMkdir(vContainerName string, vObjectPath string, metadata []byte) (mtime uint64, ctime uint64, inodeNumber inode.InodeNumber, numWrites uint64, err error) {
	var (
		dirEntryInodeNumber inode.InodeNumber
		heldLocks           *heldLocksStruct
		restartBackoff      time.Duration
		retryRequired       bool
		stat                Stat
	)

	startTime := time.Now()
	defer func() {
		globals.MiddlewareMkdirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.MiddlewareMkdirErrors.Add(1)
		}
	}()

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareHeadResponse(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	_, dirEntryInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			inode.RootDirInodeNumber,
			vContainerName+"/"+vObjectPath,
			heldLocks,
			resolvePathFollowDirSymlinks|
				resolvePathCreateMissingPathElements|
				resolvePathDirEntryInodeMustBeDirectory|
				resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil != err {
		heldLocks.free()
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	if len(metadata) > 0 {
		err = mS.volStruct.inodeVolumeHandle.PutStream(dirEntryInodeNumber, MiddlewareStream, metadata)
		if err != nil {
			heldLocks.free()
			logger.DebugfIDWithError(internalDebug, err, "MiddlewareHeadResponse(): failed PutStream() for for dirEntryInodeNumber 0x%016X (pObjectMetadata: %v)", dirEntryInodeNumber, metadata)
			return
		}
	}

	stat, err = mS.getstatHelperWhileLocked(dirEntryInodeNumber)
	if nil != err {
		heldLocks.free()
		return
	}

	mtime = stat[StatMTime]
	ctime = stat[StatCTime]
	inodeNumber = dirEntryInodeNumber
	numWrites = stat[StatNumWrites]

	heldLocks.free()
	return
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
	var (
		dirEntryBasename    string
		dirEntryInodeNumber inode.InodeNumber
		dirInodeNumber      inode.InodeNumber
		heldLocks           *heldLocksStruct
		restartBackoff      time.Duration
		retryRequired       bool
	)

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
	if nil != err {
		return
	}

	err = validateBaseName(dstBasename)
	if nil != err {
		return
	}

	// Retry until done or failure (starting with ZERO backoff)

	restartBackoff = time.Duration(0)

Restart:

	// Perform backoff and update for each restart (starting with ZERO backoff of course)

	restartBackoff, err = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)
	if nil != err {
		logger.Fatalf("MiddlewareCoalesce(): failed in restartBackoff: %v", err)
	}

	// Construct fresh heldLocks for this restart

	heldLocks = newHeldLocks()

	// Acquire WriteLock on {srcDirInodeNumber,srcBasename} & perform Access Check

	dirInodeNumber, _, dirEntryBasename, _, retryRequired, err =
		mS.resolvePath(
			srcDirInodeNumber,
			srcBasename,
			heldLocks,
			resolvePathRequireExclusiveLockOnDirEntryInode|
				resolvePathRequireExclusiveLockOnDirInode)

	if nil != err {
		heldLocks.free()
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	if (dirInodeNumber != srcDirInodeNumber) || (dirEntryBasename != srcBasename) {
		heldLocks.free()
		err = blunder.NewError(blunder.InvalidArgError, "EINVAL")
		return
	}

	if !mS.volStruct.inodeVolumeHandle.Access(srcDirInodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK, inode.NoOverride) {
		heldLocks.free()
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}
	// Acquire WriteLock on dstDirInodeNumber & perform Access Check

	_, dirEntryInodeNumber, _, _, retryRequired, err =
		mS.resolvePath(
			dstDirInodeNumber,
			".",
			heldLocks,
			resolvePathDirEntryInodeMustBeDirectory|
				resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil != err {
		heldLocks.free()
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	if retryRequired {
		heldLocks.free()
		goto Restart
	}

	if dirEntryInodeNumber != dstDirInodeNumber {
		heldLocks.free()
		err = blunder.NewError(blunder.InvalidArgError, "EINVAL")
		return
	}

	if !mS.volStruct.inodeVolumeHandle.Access(dstDirInodeNumber, userID, groupID, otherGroupIDs, inode.W_OK|inode.X_OK, inode.NoOverride) {
		heldLocks.free()
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	// Acquire WriteLock on dstBasename if it exists

	dirInodeNumber, _, dirEntryBasename, _, retryRequired, err =
		mS.resolvePath(
			dstDirInodeNumber,
			dstBasename,
			heldLocks,
			resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil == err {
		if retryRequired {
			heldLocks.free()
			goto Restart
		}

		if (dirInodeNumber != dstDirInodeNumber) || (dirEntryBasename != dstBasename) {
			heldLocks.free()
			err = blunder.NewError(blunder.InvalidArgError, "EINVAL")
			return
		}
	} else {
		// This is actually OK... it means the target path of the Rename() isn't being potentially replaced
	}

	// Locks held & Access Checks succeeded... time to do the Move

	err = mS.volStruct.inodeVolumeHandle.Move(srcDirInodeNumber, srcBasename, dstDirInodeNumber, dstBasename)

	heldLocks.free()

	return // err returned from inode.Move() suffices here
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

func (mS *mountStruct) readdirHelper(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, maxEntries uint64, prevReturned ...interface{}) (dirEntries []inode.DirEntry, statEntries []Stat, numEntries uint64, areMoreEntries bool, err error) {
	var (
		dirEntryIndex     uint64
		dlmCallerID       dlm.CallerID
		inodeLock         *dlm.RWLockStruct
		inodeVolumeHandle inode.VolumeHandle
		internalErr       error
		restartBackoff    time.Duration
	)

	mS.volStruct.jobRWMutex.RLock()
	defer mS.volStruct.jobRWMutex.RUnlock()

	dlmCallerID = dlm.GenerateCallerID()
	inodeVolumeHandle = mS.volStruct.inodeVolumeHandle

	restartBackoff = time.Duration(0)

Restart:

	restartBackoff, _ = utils.PerformDelayAndComputeNextDelay(restartBackoff, globals.tryLockBackoffMin, globals.tryLockBackoffMax)

	inodeLock, err = inodeVolumeHandle.AttemptReadLock(inodeNumber, dlmCallerID)
	if nil != err {
		goto Restart
	}

	if !inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.F_OK, inode.NoOverride) {
		internalErr = inodeLock.Unlock()
		if nil != internalErr {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", inodeLock.LockID, internalErr)
		}
		err = blunder.NewError(blunder.NotFoundError, "ENOENT")
		return
	}
	if !inodeVolumeHandle.Access(inodeNumber, userID, groupID, otherGroupIDs, inode.R_OK, inode.OwnerOverride) {
		internalErr = inodeLock.Unlock()
		if nil != internalErr {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", inodeLock.LockID, internalErr)
		}
		err = blunder.NewError(blunder.PermDeniedError, "EACCES")
		return
	}

	dirEntries, areMoreEntries, err = inodeVolumeHandle.ReadDir(inodeNumber, maxEntries, 0, prevReturned...)

	internalErr = inodeLock.Unlock()
	if nil != internalErr {
		logger.Fatalf("Failure unlocking a held LockID %s: %v", inodeLock.LockID, internalErr)
	}

	if nil != err {
		return
	}

	// Now go back and fill in (dirEntries.Type and) statEntries

	numEntries = uint64(len(dirEntries))

	statEntries = make([]Stat, numEntries, numEntries)

	for dirEntryIndex = 0; dirEntryIndex < numEntries; dirEntryIndex++ {
		inodeLock, err = inodeVolumeHandle.AttemptReadLock(dirEntries[dirEntryIndex].InodeNumber, dlmCallerID)
		if nil != err {
			goto Restart
		}

		statEntries[dirEntryIndex], err = mS.getstatHelperWhileLocked(dirEntries[dirEntryIndex].InodeNumber)
		if nil != err {
			internalErr = inodeLock.Unlock()
			if nil != internalErr {
				logger.Fatalf("Failure unlocking a held LockID %s: %v", inodeLock.LockID, internalErr)
			}
			return
		}

		dirEntries[dirEntryIndex].Type = inode.InodeType(statEntries[dirEntryIndex][StatFType])

		internalErr = inodeLock.Unlock()
		if nil != internalErr {
			logger.Fatalf("Failure unlocking a held LockID %s: %v", inodeLock.LockID, internalErr)
		}
	}

	return
}

func (mS *mountStruct) Readdir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, maxEntries uint64, prevReturned ...interface{}) (entries []inode.DirEntry, numEntries uint64, areMoreEntries bool, err error) {
	startTime := time.Now()
	defer func() {
		globals.ReaddirUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.ReaddirEntries.Add(uint64(len(entries)))
		if err != nil {
			globals.ReaddirErrors.Add(1)
		}
	}()

	entries, _, numEntries, areMoreEntries, err = mS.readdirHelper(userID, groupID, otherGroupIDs, inodeNumber, maxEntries, prevReturned...)

	return
}

func (mS *mountStruct) ReaddirPlus(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, maxEntries uint64, prevReturned ...interface{}) (dirEntries []inode.DirEntry, statEntries []Stat, numEntries uint64, areMoreEntries bool, err error) {
	startTime := time.Now()
	defer func() {
		globals.ReaddirPlusUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.ReaddirPlusBytes.Add(uint64(len(dirEntries)))
		if err != nil {
			globals.ReaddirPlusErrors.Add(1)
		}
	}()

	dirEntries, statEntries, numEntries, areMoreEntries, err = mS.readdirHelper(userID, groupID, otherGroupIDs, inodeNumber, maxEntries, prevReturned...)

	return
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
		if newUserID > uint64(math.MaxUint32) {
			err = fmt.Errorf("%s: userID is too large - value is %v, max is %v.", utils.GetFnName(), newUserID, uint64(math.MaxUint32))
			err = blunder.AddError(err, blunder.InvalidUserIDError)
			return
		}
	}

	if settingGroupID {
		// Since we are using a uint64 to convey a uint32 value, make sure we didn't get something too big
		if newGroupID > uint64(math.MaxUint32) {
			err = fmt.Errorf("%s: groupID is too large - value is %v, max is %v.", utils.GetFnName(), newGroupID, uint64(math.MaxUint32))
			err = blunder.AddError(err, blunder.InvalidGroupIDError)
			return
		}
	}

	filePerm, settingFilePerm := stat[StatMode]
	if settingFilePerm {
		// Since we are using a uint64 to convey a 12 bit value, make sure we didn't get something too big
		if filePerm >= 1<<12 {
			err = fmt.Errorf("%s: filePerm is too large - value is %v, max is %v.", utils.GetFnName(),
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

	logger.Tracef("fs.Write(): starting volume '%s' inode %v offset %v len %v",
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

	logger.Tracef("fs.Write(): tracking write volume '%s' inode %v", mS.volStruct.volumeName, inodeNumber)
	mS.volStruct.trackInFlightFileInodeData(inodeNumber)
	size = uint64(len(buf))

	return
}

func (mS *mountStruct) Wrote(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, objectPath string, fileOffset []uint64, objectOffset []uint64, length []uint64) (err error) {
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

	err = mS.volStruct.inodeVolumeHandle.Flush(inodeNumber, false)
	mS.volStruct.untrackInFlightFileInodeData(inodeNumber, false)
	mS.doInlineCheckpointIfEnabled()

	err = mS.volStruct.inodeVolumeHandle.Wrote(inodeNumber, objectPath, fileOffset, objectOffset, length, false)

	return // err, as set by inode.Wrote(), is sufficient
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

// Utility function to append entries to reply
func appendReadPlanEntries(readPlan []inode.ReadPlanStep, readRangeOut *[]inode.ReadPlanStep) (numEntries uint64) {
	for i := range readPlan {
		entry := inode.ReadPlanStep{ObjectPath: readPlan[i].ObjectPath, Offset: readPlan[i].Offset, Length: readPlan[i].Length}
		*readRangeOut = append(*readRangeOut, entry)
		numEntries++
	}
	return
}
