package headhunter

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

func (volume *volumeStruct) RegisterForEvents(listener VolumeEventListener) {
	var (
		ok bool
	)

	volume.Lock()

	_, ok = volume.eventListeners[listener]
	if ok {
		logger.Fatalf("headhunter.RegisterForEvents() called for volume %v listener %v already active", volume.volumeName)
	}

	volume.eventListeners[listener] = struct{}{}

	volume.Unlock()
}

func (volume *volumeStruct) UnregisterForEvents(listener VolumeEventListener) {
	var (
		ok bool
	)

	volume.Lock()

	_, ok = volume.eventListeners[listener]
	if !ok {
		logger.Fatalf("headhunter.UnregisterForEvents() called for volume %v listener %v not active", volume.volumeName)
	}

	delete(volume.eventListeners, listener)

	volume.Unlock()
}

func (volume *volumeStruct) FetchAccountAndCheckpointContainerNames() (accountName string, checkpointContainerName string) {
	accountName = volume.accountName
	checkpointContainerName = volume.checkpointContainerName
	return
}

func (volume *volumeStruct) fetchNextCheckPointDoneWaitGroupWhileLocked() (wg *sync.WaitGroup) {
	if nil == volume.checkpointDoneWaitGroup {
		volume.checkpointDoneWaitGroup = &sync.WaitGroup{}
		volume.checkpointDoneWaitGroup.Add(1)
	}
	wg = volume.checkpointDoneWaitGroup
	return
}

func (volume *volumeStruct) fetchNonceWhileLocked() (nonce uint64, err error) {
	var (
		checkpointContainerHeaders map[string][]string
		checkpointHeaderValue      string
		checkpointHeaderValues     []string
		newReservedToNonce         uint64
	)

	if volume.nextNonce >= volume.maxNonce {
		logger.Fatalf("Nonces have been exhausted !!!")
	}

	if volume.nextNonce == volume.checkpointHeader.ReservedToNonce {
		newReservedToNonce = volume.checkpointHeader.ReservedToNonce + uint64(volume.nonceValuesToReserve)

		// TODO: Move this inside recordTransaction() once it is a, uh, transaction :-)
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionNonceRangeReserve, volume.volumeName, volume.nextNonce, newReservedToNonce-1)

		// checkpointHeaderVersion2 == volume.checkpointHeaderVersion

		checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
			checkpointHeaderVersion2,
			volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber,
			volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectLength,
			newReservedToNonce,
		)

		checkpointHeaderValues = []string{checkpointHeaderValue}

		checkpointContainerHeaders = make(map[string][]string)

		checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues

		err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
		if nil != err {
			return
		}

		volume.checkpointHeader.ReservedToNonce = newReservedToNonce
	}

	nonce = volume.nextNonce
	volume.nextNonce++

	err = nil
	return
}

func (volume *volumeStruct) FetchNonce() (nonce uint64, err error) {
	volume.Lock()
	nonce, err = volume.fetchNonceWhileLocked()
	volume.Unlock()
	return
}

func (volume *volumeStruct) GetInodeRec(inodeNumber uint64) (value []byte, ok bool, err error) {
	volume.Lock()

	valueAsValue, ok, err := volume.liveView.inodeRecWrapper.bPlusTree.GetByKey(inodeNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		evtlog.Record(evtlog.FormatHeadhunterMissingInodeRec, volume.volumeName, inodeNumber)
		err = fmt.Errorf("inodeNumber 0x%016X not found in volume \"%v\" inodeRecWrapper.bPlusTree", inodeNumber, volume.volumeName)
		return
	}
	valueFromTree := valueAsValue.([]byte)
	value = make([]byte, len(valueFromTree))
	copy(value, valueFromTree)

	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutInodeRec(inodeNumber uint64, value []byte) (err error) {
	valueToTree := make([]byte, len(value))
	copy(valueToTree, value)

	volume.Lock()

	ok, err := volume.liveView.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.liveView.inodeRecWrapper.bPlusTree.Put(inodeNumber, valueToTree)
		if nil != err {
			volume.Unlock()
			return
		}
	}

	volume.recordTransaction(transactionPutInodeRec, inodeNumber, value)

	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutInodeRecs(inodeNumbers []uint64, values [][]byte) (err error) {
	if len(inodeNumbers) != len(values) {
		err = fmt.Errorf("InodeNumber and Values array don't match")
		return
	}

	valuesToTree := make([][]byte, len(inodeNumbers))

	for i := range inodeNumbers {
		valuesToTree[i] = make([]byte, len(values[i]))
		copy(valuesToTree[i], values[i])
	}

	volume.Lock()

	for i, inodeNumber := range inodeNumbers {
		ok, nonShadowingErr := volume.liveView.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, valuesToTree[i])
		if nil != nonShadowingErr {
			volume.Unlock()
			err = nonShadowingErr
			return
		}
		if !ok {
			_, err = volume.liveView.inodeRecWrapper.bPlusTree.Put(inodeNumber, valuesToTree[i])
			if nil != err {
				volume.Unlock()
				return
			}
		}
	}

	volume.recordTransaction(transactionPutInodeRecs, inodeNumbers, values)

	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) DeleteInodeRec(inodeNumber uint64) (err error) {
	volume.Lock()

	_, err = volume.liveView.inodeRecWrapper.bPlusTree.DeleteByKey(inodeNumber)

	volume.recordTransaction(transactionDeleteInodeRec, inodeNumber, nil)

	volume.Unlock()

	return
}

func (volume *volumeStruct) IndexedInodeNumber(index uint64) (inodeNumber uint64, ok bool, err error) {
	volume.Lock()
	key, _, ok, err := volume.liveView.inodeRecWrapper.bPlusTree.GetByIndex(int(index))
	if nil != err {
		volume.Unlock()
		return
	}
	volume.Unlock()

	if !ok {
		return
	}

	inodeNumber = key.(uint64)

	return
}

func (volume *volumeStruct) GetLogSegmentRec(logSegmentNumber uint64) (value []byte, err error) {
	volume.Lock()

	valueAsValue, ok, err := volume.liveView.logSegmentRecWrapper.bPlusTree.GetByKey(logSegmentNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		evtlog.Record(evtlog.FormatHeadhunterMissingLogSegmentRec, volume.volumeName, logSegmentNumber)
		err = fmt.Errorf("logSegmentNumber 0x%016X not found in volume \"%v\" logSegmentRecWrapper.bPlusTree", logSegmentNumber, volume.volumeName)
		return
	}
	valueFromTree := valueAsValue.([]byte)
	value = make([]byte, len(valueFromTree))
	copy(value, valueFromTree)

	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutLogSegmentRec(logSegmentNumber uint64, value []byte) (err error) {
	valueToTree := make([]byte, len(value))
	copy(valueToTree, value)

	volume.Lock()

	ok, err := volume.liveView.logSegmentRecWrapper.bPlusTree.PatchByKey(logSegmentNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.liveView.logSegmentRecWrapper.bPlusTree.Put(logSegmentNumber, valueToTree)
		if nil != err {
			volume.Unlock()
			return
		}
	}

	volume.recordTransaction(transactionPutLogSegmentRec, logSegmentNumber, value)

	volume.Unlock()

	err = nil
	return
}

/*
	containerNameAsByteSlice := utils.StringToByteSlice(containerName)
	err = vS.headhunterVolumeHandle.PutLogSegmentRec(logSegmentNumber, containerNameAsByteSlice)
*/
func (volume *volumeStruct) DeleteLogSegmentRec(logSegmentNumber uint64) (err error) {
	var (
		containerNameAsValue      sortedmap.Value
		delayedObjectDeleteSSTODO delayedObjectDeleteSSTODOStruct
		ok                        bool
	)

	volume.Lock()
	defer volume.Unlock()

	containerNameAsValue, ok, err = volume.liveView.logSegmentRecWrapper.bPlusTree.GetByKey(logSegmentNumber)
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("Missing logSegmentNumber (0x%016X) in volume %v LogSegmentRec B+Tree", logSegmentNumber, volume.volumeName)
		return
	}

	_, err = volume.liveView.logSegmentRecWrapper.bPlusTree.DeleteByKey(logSegmentNumber)
	if nil != err {
		return
	}

	volume.recordTransaction(transactionDeleteLogSegmentRec, logSegmentNumber, nil)

	delayedObjectDeleteSSTODO.containerName = utils.ByteSliceToString(containerNameAsValue.([]byte))
	delayedObjectDeleteSSTODO.objectNumber = logSegmentNumber

	volume.delayedObjectDeleteSSTODOList = append(volume.delayedObjectDeleteSSTODOList, delayedObjectDeleteSSTODO)

	return
}

func (volume *volumeStruct) IndexedLogSegmentNumber(index uint64) (logSegmentNumber uint64, ok bool, err error) {
	volume.Lock()
	key, _, ok, err := volume.liveView.logSegmentRecWrapper.bPlusTree.GetByIndex(int(index))
	if nil != err {
		volume.Unlock()
		return
	}
	volume.Unlock()

	if !ok {
		return
	}

	logSegmentNumber = key.(uint64)

	return
}

func (volume *volumeStruct) GetBPlusTreeObject(objectNumber uint64) (value []byte, err error) {
	volume.Lock()

	valueAsValue, ok, err := volume.liveView.bPlusTreeObjectWrapper.bPlusTree.GetByKey(objectNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		evtlog.Record(evtlog.FormatHeadhunterMissingBPlusTreeObject, volume.volumeName, objectNumber)
		err = fmt.Errorf("objectNumber 0x%016X not found in volume \"%v\" bPlusTreeObjectWrapper.bPlusTree", objectNumber, volume.volumeName)
		return
	}
	valueFromTree := valueAsValue.([]byte)
	value = make([]byte, len(valueFromTree))
	copy(value, valueFromTree)

	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutBPlusTreeObject(objectNumber uint64, value []byte) (err error) {
	valueToTree := make([]byte, len(value))
	copy(valueToTree, value)

	volume.Lock()

	ok, err := volume.liveView.bPlusTreeObjectWrapper.bPlusTree.PatchByKey(objectNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.Put(objectNumber, valueToTree)
		if nil != err {
			volume.Unlock()
			return
		}
	}

	volume.recordTransaction(transactionPutBPlusTreeObject, objectNumber, value)

	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) DeleteBPlusTreeObject(objectNumber uint64) (err error) {
	volume.Lock()

	_, err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.DeleteByKey(objectNumber)

	volume.recordTransaction(transactionDeleteBPlusTreeObject, objectNumber, nil)

	volume.Unlock()

	return
}

func (volume *volumeStruct) IndexedBPlusTreeObjectNumber(index uint64) (objectNumber uint64, ok bool, err error) {
	volume.Lock()
	key, _, ok, err := volume.liveView.bPlusTreeObjectWrapper.bPlusTree.GetByIndex(int(index))
	if nil != err {
		volume.Unlock()
		return
	}
	volume.Unlock()

	if !ok {
		return
	}

	objectNumber = key.(uint64)

	return
}

func (volume *volumeStruct) DoCheckpoint() (err error) {
	var (
		checkpointRequest checkpointRequestStruct
	)

	checkpointRequest.exitOnCompletion = false

	checkpointRequest.waitGroup.Add(1)
	volume.checkpointRequestChan <- &checkpointRequest
	checkpointRequest.waitGroup.Wait()

	err = checkpointRequest.err

	return
}

func (volume *volumeStruct) fetchLayoutReport(treeType BPlusTreeType) (layoutReport sortedmap.LayoutReport, discrepencies uint64, err error) {
	var (
		measuredLayoutReport sortedmap.LayoutReport
		objectBytesMeasured  uint64
		objectBytesTracked   uint64
		objectNumber         uint64
		ok                   bool
		treeName             string
		treeWrapper          *bPlusTreeWrapperStruct
	)

	switch treeType {
	case InodeRecBPlusTree:
		treeName = "InodeRec"
		treeWrapper = volume.liveView.inodeRecWrapper
	case LogSegmentRecBPlusTree:
		treeName = "LogSegmentRec"
		treeWrapper = volume.liveView.logSegmentRecWrapper
	case BPlusTreeObjectBPlusTree:
		treeName = "BPlusTreeObject"
		treeWrapper = volume.liveView.bPlusTreeObjectWrapper
	case CreatedObjectsBPlusTree:
		treeName = "CreatedObjectObject"
		treeWrapper = volume.liveView.createdObjectsWrapper
	case DeletedObjectsBPlusTree:
		treeName = "DeletedObjectObject"
		treeWrapper = volume.liveView.deletedObjectsWrapper
	default:
		err = fmt.Errorf("fetchLayoutReport(treeType %d): bad tree type", treeType)
		logger.ErrorfWithError(err, "volume '%s'", volume.volumeName)
		return
	}

	measuredLayoutReport, err = treeWrapper.bPlusTree.FetchLayoutReport()
	if nil != err {
		logger.ErrorfWithError(err, "FetchLayoutReport() volume '%s' tree '%s'", volume.volumeName, treeName)
		return
	}

	// Compare measuredLayoutReport & trackingLayoutReport computing discrepencies

	discrepencies = 0

	for objectNumber, objectBytesMeasured = range measuredLayoutReport {
		objectBytesTracked, ok = treeWrapper.trackingBPlusTreeLayout[objectNumber]
		if ok {
			if objectBytesMeasured != objectBytesTracked {
				discrepencies++
				logger.Errorf("headhunter.fetchLayoutReport(%v) for volume %v found objectBytes mismatch between measuredLayoutReport & trackingLayoutReport for objectNumber 0x%016X", treeName, volume.volumeName, objectNumber)
			}
		} else {
			discrepencies++
			logger.Errorf("headhunter.fetchLayoutReport(%v) for volume %v found objectBytes in measuredLayoutReport but missing from trackingLayoutReport for objectNumber 0x%016X", treeName, volume.volumeName, objectNumber)
		}
	}

	for objectNumber, objectBytesTracked = range treeWrapper.trackingBPlusTreeLayout {
		objectBytesMeasured, ok = measuredLayoutReport[objectNumber]
		if ok {
			// Already handled above
		} else {
			discrepencies++
			logger.Errorf("headhunter.fetchLayoutReport(%v) for volume %v found objectBytes in trackingLayoutReport but missing from measuredLayoutReport for objectNumber 0x%016X", treeName, volume.volumeName, objectNumber)
		}
	}

	// In the case that they differ, return measuredLayoutReport rather than trackingLayoutReport

	layoutReport = measuredLayoutReport

	err = nil

	return
}

// FetchLayoutReport returns the B+Tree sortedmap.LayoutReport for one or all
// of the HeadHunter tables. In the case of requesting "all", the checkpoint
// overhead will also be included.
//
// Note that only the "live" view information is reported. If prior SnapShot's
// exist, these will not be reported (even t for the "all" case). This is
// primarily due to the fact that SnapShot's inherently overlap in the objects
// they reference. In the case where one or more SnapShot's exist, the
// CreatedObjectsBPlusTree should be expected to be empty.
func (volume *volumeStruct) FetchLayoutReport(treeType BPlusTreeType) (layoutReport sortedmap.LayoutReport, err error) {
	var (
		checkpointLayoutReport sortedmap.LayoutReport
		checkpointObjectBytes  uint64
		objectBytes            uint64
		objectNumber           uint64
		ok                     bool
		perTreeLayoutReport    sortedmap.LayoutReport
		perTreeObjectBytes     uint64
	)

	volume.Lock()
	defer volume.Unlock()

	if MergedBPlusTree == treeType {
		// First, accumulate the 5 B+Tree sortedmap.LayoutReport's

		layoutReport, _, err = volume.fetchLayoutReport(InodeRecBPlusTree)
		if nil != err {
			return
		}
		perTreeLayoutReport, _, err = volume.fetchLayoutReport(LogSegmentRecBPlusTree)
		if nil != err {
			return
		}
		for objectNumber, perTreeObjectBytes = range perTreeLayoutReport {
			objectBytes, ok = layoutReport[objectNumber]
			if ok {
				layoutReport[objectNumber] = objectBytes + perTreeObjectBytes
			} else {
				layoutReport[objectNumber] = perTreeObjectBytes
			}
		}
		perTreeLayoutReport, _, err = volume.fetchLayoutReport(BPlusTreeObjectBPlusTree)
		if nil != err {
			return
		}
		for objectNumber, perTreeObjectBytes = range perTreeLayoutReport {
			objectBytes, ok = layoutReport[objectNumber]
			if ok {
				layoutReport[objectNumber] = objectBytes + perTreeObjectBytes
			} else {
				layoutReport[objectNumber] = perTreeObjectBytes
			}
		}
		perTreeLayoutReport, _, err = volume.fetchLayoutReport(CreatedObjectsBPlusTree)
		if nil != err {
			return
		}
		for objectNumber, perTreeObjectBytes = range perTreeLayoutReport {
			objectBytes, ok = layoutReport[objectNumber]
			if ok {
				layoutReport[objectNumber] = objectBytes + perTreeObjectBytes
			} else {
				layoutReport[objectNumber] = perTreeObjectBytes
			}
		}
		perTreeLayoutReport, _, err = volume.fetchLayoutReport(DeletedObjectsBPlusTree)
		if nil != err {
			return
		}
		for objectNumber, perTreeObjectBytes = range perTreeLayoutReport {
			objectBytes, ok = layoutReport[objectNumber]
			if ok {
				layoutReport[objectNumber] = objectBytes + perTreeObjectBytes
			} else {
				layoutReport[objectNumber] = perTreeObjectBytes
			}
		}

		// Now, add in the checkpointLayoutReport

		checkpointLayoutReport, err = volume.fetchCheckpointLayoutReport()
		if nil != err {
			return
		}
		for objectNumber, checkpointObjectBytes = range checkpointLayoutReport {
			objectBytes, ok = layoutReport[objectNumber]
			if ok {
				layoutReport[objectNumber] = objectBytes + checkpointObjectBytes
			} else {
				layoutReport[objectNumber] = perTreeObjectBytes
			}
		}
	} else {
		layoutReport, _, err = volume.fetchLayoutReport(treeType)
	}

	return
}

func (volume *volumeStruct) SnapShotCreateByInodeLayer(name string) (id uint64, err error) {
	var (
		availableSnapShotIDListElement *list.Element
		ok                             bool
		snapShotNonce                  uint64
		snapShotTime                   time.Time
		volumeView                     *volumeViewStruct
	)

	volume.Lock()

	_, ok, err = volume.viewTreeByName.GetByKey(name)
	if nil != err {
		volume.Unlock()
		return
	}
	if ok {
		volume.Unlock()
		err = fmt.Errorf("SnapShot Name \"%v\" already in use", name)
		return
	}

	ok = true

	for ok {
		snapShotTime = time.Now()
		_, ok, err = volume.viewTreeByTime.GetByKey(snapShotTime)
		if nil != err {
			volume.Unlock()
			return
		}
	}

	if 0 == volume.availableSnapShotIDList.Len() {
		volume.Unlock()
		err = fmt.Errorf("No SnapShot IDs available")
		return
	}

	snapShotNonce, err = volume.fetchNonceWhileLocked()
	if nil != err {
		volume.Unlock()
		return
	}

	availableSnapShotIDListElement = volume.availableSnapShotIDList.Front()
	id, ok = availableSnapShotIDListElement.Value.(uint64)
	if !ok {
		logger.Fatalf("Logic error - volume %v has non-uint64 element at Front() of availableSnapShotIDList", volume.volumeName)
	}
	_ = volume.availableSnapShotIDList.Remove(availableSnapShotIDListElement)

	volumeView = &volumeViewStruct{
		volume:       volume,
		nonce:        snapShotNonce,
		snapShotID:   id,
		snapShotTime: snapShotTime,
		snapShotName: name,
	}

	ok, err = volume.viewTreeByNonce.Put(snapShotNonce, volumeView)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByNonce.Put() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByNonce.Put() returned ok == false")
	}

	ok, err = volume.viewTreeByID.Put(id, volumeView)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByID.Put() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByID.Put() returned ok == false")
	}

	ok, err = volume.viewTreeByTime.Put(snapShotTime, volumeView)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByTime.Put() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByTime.Put() returned ok == false")
	}

	ok, err = volume.viewTreeByName.Put(name, volumeView)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByName.Put() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByName.Put() returned ok == false")
	}

	volume.Unlock()

	return // TODO - there's more to do here...
}

func (volume *volumeStruct) SnapShotDeleteByInodeLayer(id uint64) (err error) {
	var (
		ok         bool
		value      sortedmap.Value
		volumeView *volumeViewStruct
	)

	volume.Lock()

	value, ok, err = volume.viewTreeByID.GetByKey(id)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		err = fmt.Errorf("viewTreeByID.GetByKey(0x%016X) not found", id)
		return
	}

	volumeView, ok = value.(*volumeViewStruct)
	if !ok {
		logger.Fatalf("viewTreeByID.GetByKey(0x%016X) returned something other than a volumeView", id)
	}

	ok, err = volume.viewTreeByNonce.DeleteByKey(volumeView.nonce)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByNonce.DeleteByKey() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByNonce.DeleteByKey() returned ok == false")
	}

	ok, err = volume.viewTreeByID.DeleteByKey(id)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByID.DeleteByKey() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByID.DeleteByKey() returned ok == false")
	}

	ok, err = volume.viewTreeByTime.DeleteByKey(volumeView.snapShotTime)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByTime.DeleteByKey() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByTime.DeleteByKey() returned ok == false")
	}

	ok, err = volume.viewTreeByName.DeleteByKey(volumeView.snapShotName)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByName.DeleteByKey() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByName.DeleteByKey() returned ok == false")
	}

	volume.availableSnapShotIDList.PushBack(id)

	volume.Unlock()

	return // TODO - there's more to do here...
}

func (volume *volumeStruct) snapShotList(viewTree sortedmap.LLRBTree, reversed bool) (list []SnapShotStruct) {
	var (
		err        error
		len        int
		listIndex  int
		ok         bool
		treeIndex  int
		value      sortedmap.Value
		volumeView *volumeViewStruct
	)

	len, err = viewTree.Len()
	if nil != err {
		logger.Fatalf("headhunter.snapShotList() for volume %v hit viewTree.Len() error: %v", volume.volumeName, err)
	}

	list = make([]SnapShotStruct, len, len)

	for treeIndex = 0; treeIndex < len; treeIndex++ {
		if reversed {
			listIndex = len - 1 - treeIndex
		} else {
			listIndex = treeIndex
		}

		_, value, ok, err = viewTree.GetByIndex(treeIndex)
		if nil != err {
			logger.Fatalf("headhunter.snapShotList() for volume %v hit viewTree.GetByIndex() error: %v", volume.volumeName, err)
		}
		if !ok {
			logger.Fatalf("headhunter.snapShotList() for volume %v hit viewTree.GetByIndex() !ok", volume.volumeName)
		}

		volumeView, ok = value.(*volumeViewStruct)
		if !ok {
			logger.Fatalf("headhunter.snapShotList() for volume %v hit value.(*volumeViewStruct) !ok", volume.volumeName)
		}

		list[listIndex].ID = volumeView.snapShotID
		list[listIndex].Time = volumeView.snapShotTime
		list[listIndex].Name = volumeView.snapShotName
	}

	return
}

func (volume *volumeStruct) SnapShotListByID(reversed bool) (list []SnapShotStruct) {
	volume.Lock()
	list = volume.snapShotList(volume.viewTreeByID, reversed)
	volume.Unlock()

	return
}

func (volume *volumeStruct) SnapShotListByTime(reversed bool) (list []SnapShotStruct) {
	volume.Lock()
	list = volume.snapShotList(volume.viewTreeByTime, reversed)
	volume.Unlock()

	return
}

func (volume *volumeStruct) SnapShotListByName(reversed bool) (list []SnapShotStruct) {
	volume.Lock()
	list = volume.snapShotList(volume.viewTreeByName, reversed)
	volume.Unlock()

	return
}
