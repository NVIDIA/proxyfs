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
)

func (volume *volumeStruct) RegisterForEvents(listener VolumeEventListener) {
	var (
		ok bool
	)

	volume.Lock()

	_, ok = volume.eventListeners[listener]
	if ok {
		logger.Fatalf("headhunter.RegisterForEvents() called for volume %v listener %p already active", volume.volumeName, listener)
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
		logger.Fatalf("headhunter.UnregisterForEvents() called for volume %v listener %p not active", volume.volumeName, listener)
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

	if volume.nextNonce == volume.checkpointHeader.reservedToNonce {
		newReservedToNonce = volume.checkpointHeader.reservedToNonce + uint64(volume.nonceValuesToReserve)

		// TODO: Move this inside recordTransaction() once it is a, uh, transaction :-)
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionNonceRangeReserve, volume.volumeName, volume.nextNonce, newReservedToNonce-1)

		checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
			volume.checkpointHeader.checkpointVersion,
			volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber,
			volume.checkpointHeader.checkpointObjectTrailerStructObjectLength,
			newReservedToNonce,
		)

		checkpointHeaderValues = []string{checkpointHeaderValue}

		checkpointContainerHeaders = make(map[string][]string)

		checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues

		err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
		if nil != err {
			return
		}

		volume.checkpointHeader.reservedToNonce = newReservedToNonce
	}

	nonce = volume.nextNonce
	volume.nextNonce++

	err = nil
	return
}

func (volume *volumeStruct) FetchNonce() (nonce uint64, err error) {

	startTime := time.Now()
	defer func() {
		globals.FetchNonceUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.FetchNonceErrors.Add(1)
		}
	}()

	volume.Lock()
	nonce, err = volume.fetchNonceWhileLocked()
	volume.Unlock()

	return
}

func (volume *volumeStruct) findVolumeViewAndNonceWhileLocked(key uint64) (volumeView *volumeViewStruct, nonce uint64, ok bool, err error) {
	var (
		snapShotID     uint64
		snapShotIDType SnapShotIDType
		value          sortedmap.Value
	)

	snapShotIDType, snapShotID, nonce = volume.SnapShotU64Decode(key)

	if SnapShotIDTypeDotSnapShot == snapShotIDType {
		err = fmt.Errorf("findVolumeViewAndNonceWhileLocked() called for SnapShotIDTypeDotSnapShot key")
		return
	}

	if SnapShotIDTypeLive == snapShotIDType {
		volumeView = volume.liveView
		ok = true
		err = nil
		return
	}

	value, ok, err = volume.viewTreeByID.GetByKey(snapShotID)
	if (nil != err) || !ok {
		return
	}

	volumeView, ok = value.(*volumeViewStruct)

	return
}

func (volume *volumeStruct) GetInodeRec(inodeNumber uint64) (value []byte, ok bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.GetInodeRecUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.GetInodeRecBytes.Add(uint64(len(value)))
		if err != nil {
			globals.GetInodeRecErrors.Add(1)
		}
	}()

	volume.Lock()

	volumeView, nonce, ok, err := volume.findVolumeViewAndNonceWhileLocked(inodeNumber)
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

	valueAsValue, ok, err := volumeView.inodeRecWrapper.bPlusTree.GetByKey(nonce)
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

	startTime := time.Now()
	defer func() {
		globals.PutInodeRecUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.PutInodeRecBytes.Add(uint64(len(value)))
		if err != nil {
			globals.PutInodeRecErrors.Add(1)
		}
	}()

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

	startTime := time.Now()
	defer func() {
		var totalBytes int
		for _, inodeValue := range values {
			totalBytes += len(inodeValue)
		}

		globals.PutInodeRecsUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.PutInodeRecsBytes.Add(uint64(totalBytes))
		if err != nil {
			globals.PutInodeRecsErrors.Add(1)
		}
	}()

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

	startTime := time.Now()
	defer func() {
		globals.DeleteInodeRecUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.DeleteInodeRecErrors.Add(1)
		}
	}()

	volume.Lock()

	_, err = volume.liveView.inodeRecWrapper.bPlusTree.DeleteByKey(inodeNumber)

	volume.recordTransaction(transactionDeleteInodeRec, inodeNumber, nil)

	volume.Unlock()

	return
}

func (volume *volumeStruct) IndexedInodeNumber(index uint64) (inodeNumber uint64, ok bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.IndexedInodeNumberUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.IndexedInodeNumberErrors.Add(1)
		}
	}()

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

	startTime := time.Now()
	defer func() {
		globals.GetLogSegmentRecUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.GetLogSegmentRecErrors.Add(1)
		}
	}()

	volume.Lock()

	volumeView, nonce, ok, err := volume.findVolumeViewAndNonceWhileLocked(logSegmentNumber)
	if (nil != err) || !ok {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		evtlog.Record(evtlog.FormatHeadhunterMissingLogSegmentRec, volume.volumeName, logSegmentNumber)
		err = fmt.Errorf("logSegmentNumber 0x%016X not found in volume \"%v\" logSegmentRecWrapper.bPlusTree", logSegmentNumber, volume.volumeName)
		return
	}

	valueAsValue, ok, err := volumeView.logSegmentRecWrapper.bPlusTree.GetByKey(nonce)
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

	startTime := time.Now()
	defer func() {
		globals.PutLogSegmentRecUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.PutLogSegmentRecErrors.Add(1)
		}
	}()

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

	if nil != volume.priorView {
		_, err = volume.priorView.createdObjectsWrapper.bPlusTree.Put(logSegmentNumber, valueToTree)
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

func (volume *volumeStruct) DeleteLogSegmentRec(logSegmentNumber uint64) (err error) {
	var (
		containerNameAsValue sortedmap.Value
		ok                   bool
	)

	startTime := time.Now()
	defer func() {
		globals.DeleteLogSegmentRecUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.DeleteLogSegmentRecErrors.Add(1)
		}
	}()

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

	if nil == volume.priorView {
		_, err = volume.liveView.deletedObjectsWrapper.bPlusTree.Put(logSegmentNumber, containerNameAsValue)
		if nil != err {
			return
		}
	} else {
		ok, err = volume.priorView.createdObjectsWrapper.bPlusTree.DeleteByKey(logSegmentNumber)
		if nil != err {
			return
		}
		if ok {
			_, err = volume.liveView.deletedObjectsWrapper.bPlusTree.Put(logSegmentNumber, containerNameAsValue)
			if nil != err {
				return
			}
		} else {
			_, err = volume.priorView.deletedObjectsWrapper.bPlusTree.Put(logSegmentNumber, containerNameAsValue)
			if nil != err {
				return
			}
		}
	}

	volume.recordTransaction(transactionDeleteLogSegmentRec, logSegmentNumber, nil)

	return
}

func (volume *volumeStruct) IndexedLogSegmentNumber(index uint64) (logSegmentNumber uint64, ok bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.IndexedLogSegmentNumberUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.IndexedLogSegmentNumberErrors.Add(1)
		}
	}()

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

	startTime := time.Now()
	defer func() {
		globals.GetBPlusTreeObjectUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.GetBPlusTreeObjectBytes.Add(uint64(len(value)))
		if err != nil {
			globals.GetBPlusTreeObjectErrors.Add(1)
		}
	}()

	volume.Lock()

	volumeView, nonce, ok, err := volume.findVolumeViewAndNonceWhileLocked(objectNumber)
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

	valueAsValue, ok, err := volumeView.bPlusTreeObjectWrapper.bPlusTree.GetByKey(nonce)
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

	startTime := time.Now()
	defer func() {
		globals.PutBPlusTreeObjectUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		globals.PutBPlusTreeObjectBytes.Add(uint64(len(value)))
		if err != nil {
			globals.PutBPlusTreeObjectErrors.Add(1)
		}
	}()

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

	startTime := time.Now()
	defer func() {
		globals.DeleteBPlusTreeObjectUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.DeleteBPlusTreeObjectErrors.Add(1)
		}
	}()

	volume.Lock()

	_, err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.DeleteByKey(objectNumber)

	volume.recordTransaction(transactionDeleteBPlusTreeObject, objectNumber, nil)

	volume.Unlock()

	return
}

func (volume *volumeStruct) IndexedBPlusTreeObjectNumber(index uint64) (objectNumber uint64, ok bool, err error) {

	startTime := time.Now()
	defer func() {
		globals.IndexedBPlusTreeObjectNumberUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.IndexedBPlusTreeObjectNumberErrors.Add(1)
		}
	}()

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

	startTime := time.Now()
	defer func() {
		globals.DoCheckpointUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.DoCheckpointErrors.Add(1)
		}
	}()

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
		objectBytesTracked, ok = treeWrapper.bPlusTreeTracker.bPlusTreeLayout[objectNumber]
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

	for objectNumber, objectBytesTracked = range treeWrapper.bPlusTreeTracker.bPlusTreeLayout {
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

	startTime := time.Now()
	defer func() {
		globals.FetchLayoutReportUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.FetchLayoutReportErrors.Add(1)
		}
	}()

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
		availableSnapShotIDListElement           *list.Element
		bPlusTreeObjectBPlusTreeRootObjectLength uint64
		bPlusTreeObjectBPlusTreeRootObjectNumber uint64
		bPlusTreeObjectBPlusTreeRootObjectOffset uint64
		inodeRecBPlusTreeRootObjectLength        uint64
		inodeRecBPlusTreeRootObjectNumber        uint64
		inodeRecBPlusTreeRootObjectOffset        uint64
		logSegmentRecBPlusTreeRootObjectLength   uint64
		logSegmentRecBPlusTreeRootObjectNumber   uint64
		logSegmentRecBPlusTreeRootObjectOffset   uint64
		ok                                       bool
		snapShotNonce                            uint64
		snapShotTime                             time.Time
		volumeView                               *volumeViewStruct
	)

	startTime := time.Now()
	defer func() {
		globals.SnapShotCreateByInodeLayerUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.SnapShotCreateByInodeLayerErrors.Add(1)
		}
	}()

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

	evtlog.Record(evtlog.FormatHeadhunterCheckpointStart, volume.volumeName)
	err = volume.putCheckpoint()
	if nil != err {
		evtlog.Record(evtlog.FormatHeadhunterCheckpointEndFailure, volume.volumeName, err.Error())
		logger.FatalfWithError(err, "Shutting down to prevent subsequent checkpoints from corrupting Swift")
	}
	evtlog.Record(evtlog.FormatHeadhunterCheckpointEndSuccess, volume.volumeName)

	volumeView = &volumeViewStruct{
		volume:       volume,
		nonce:        snapShotNonce,
		snapShotID:   id,
		snapShotTime: snapShotTime,
		snapShotName: name,
	}

	volumeView.inodeRecWrapper = &bPlusTreeWrapperStruct{
		volumeView:       volumeView,
		bPlusTreeTracker: nil,
	}

	inodeRecBPlusTreeRootObjectNumber,
		inodeRecBPlusTreeRootObjectOffset,
		inodeRecBPlusTreeRootObjectLength = volume.liveView.inodeRecWrapper.bPlusTree.FetchLocation()

	if 0 == inodeRecBPlusTreeRootObjectNumber {
		volumeView.inodeRecWrapper.bPlusTree =
			sortedmap.NewBPlusTree(
				volumeView.volume.maxInodesPerMetadataNode,
				sortedmap.CompareUint64,
				volumeView.inodeRecWrapper,
				globals.inodeRecCache)
	} else {
		volumeView.inodeRecWrapper.bPlusTree, err =
			sortedmap.OldBPlusTree(
				inodeRecBPlusTreeRootObjectNumber,
				inodeRecBPlusTreeRootObjectOffset,
				inodeRecBPlusTreeRootObjectLength,
				sortedmap.CompareUint64,
				volumeView.inodeRecWrapper,
				globals.inodeRecCache)
		if nil != err {
			logger.Fatalf("Logic error - sortedmap.OldBPlusTree(<InodeRecBPlusTree>) failed with error: %v", err)
		}
	}

	volumeView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
		volumeView:       volumeView,
		bPlusTreeTracker: nil,
	}

	logSegmentRecBPlusTreeRootObjectNumber,
		logSegmentRecBPlusTreeRootObjectOffset,
		logSegmentRecBPlusTreeRootObjectLength = volume.liveView.logSegmentRecWrapper.bPlusTree.FetchLocation()

	if 0 == logSegmentRecBPlusTreeRootObjectNumber {
		volumeView.logSegmentRecWrapper.bPlusTree =
			sortedmap.NewBPlusTree(
				volumeView.volume.maxLogSegmentsPerMetadataNode,
				sortedmap.CompareUint64,
				volumeView.logSegmentRecWrapper,
				globals.logSegmentRecCache)
	} else {
		volumeView.logSegmentRecWrapper.bPlusTree, err =
			sortedmap.OldBPlusTree(
				logSegmentRecBPlusTreeRootObjectNumber,
				logSegmentRecBPlusTreeRootObjectOffset,
				logSegmentRecBPlusTreeRootObjectLength,
				sortedmap.CompareUint64,
				volumeView.logSegmentRecWrapper,
				globals.logSegmentRecCache)
		if nil != err {
			logger.Fatalf("Logic error - sortedmap.OldBPlusTree(<LogSegmentRecBPlusTree>) failed with error: %v", err)
		}
	}

	volumeView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
		volumeView:       volumeView,
		bPlusTreeTracker: nil,
	}

	bPlusTreeObjectBPlusTreeRootObjectNumber,
		bPlusTreeObjectBPlusTreeRootObjectOffset,
		bPlusTreeObjectBPlusTreeRootObjectLength = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.FetchLocation()

	if 0 == bPlusTreeObjectBPlusTreeRootObjectNumber {
		volumeView.bPlusTreeObjectWrapper.bPlusTree =
			sortedmap.NewBPlusTree(
				volumeView.volume.maxDirFileNodesPerMetadataNode,
				sortedmap.CompareUint64,
				volumeView.bPlusTreeObjectWrapper,
				globals.bPlusTreeObjectCache)
	} else {
		volumeView.bPlusTreeObjectWrapper.bPlusTree, err =
			sortedmap.OldBPlusTree(
				bPlusTreeObjectBPlusTreeRootObjectNumber,
				bPlusTreeObjectBPlusTreeRootObjectOffset,
				bPlusTreeObjectBPlusTreeRootObjectLength,
				sortedmap.CompareUint64,
				volumeView.bPlusTreeObjectWrapper,
				globals.bPlusTreeObjectCache)
		if nil != err {
			logger.Fatalf("Logic error - sortedmap.OldBPlusTree(<BPlusTreeObjectBPlusTree>) failed with error: %v", err)
		}
	}

	volumeView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
		volumeView:       volumeView,
		bPlusTreeTracker: volumeView.volume.liveView.createdObjectsWrapper.bPlusTreeTracker,
	}

	volumeView.createdObjectsWrapper.bPlusTree =
		sortedmap.NewBPlusTree(
			volumeView.volume.maxCreatedDeletedObjectsPerMetadataNode,
			sortedmap.CompareUint64,
			volumeView.volume.liveView.createdObjectsWrapper,
			globals.createdDeletedObjectsCache)

	volumeView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
		volumeView:       volumeView,
		bPlusTreeTracker: volumeView.volume.liveView.deletedObjectsWrapper.bPlusTreeTracker,
	}

	volumeView.deletedObjectsWrapper.bPlusTree =
		sortedmap.NewBPlusTree(
			volumeView.volume.maxCreatedDeletedObjectsPerMetadataNode,
			sortedmap.CompareUint64,
			volumeView.volume.liveView.deletedObjectsWrapper,
			globals.createdDeletedObjectsCache)

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

	volume.priorView = volumeView

	evtlog.Record(evtlog.FormatHeadhunterCheckpointStart, volume.volumeName)
	err = volume.putCheckpoint()
	if nil != err {
		evtlog.Record(evtlog.FormatHeadhunterCheckpointEndFailure, volume.volumeName, err.Error())
		logger.FatalfWithError(err, "Shutting down to prevent subsequent checkpoints from corrupting Swift")
	}
	evtlog.Record(evtlog.FormatHeadhunterCheckpointEndSuccess, volume.volumeName)

	volume.Unlock()

	return
}

func (volume *volumeStruct) SnapShotDeleteByInodeLayer(id uint64) (err error) {
	var (
		deletedVolumeView              *volumeViewStruct
		deletedVolumeViewIndex         int
		found                          bool
		key                            sortedmap.Key
		newPriorVolumeView             *volumeViewStruct
		ok                             bool
		predecessorToDeletedVolumeView *volumeViewStruct
		remainingSnapShotCount         int
		value                          sortedmap.Value
	)

	startTime := time.Now()
	defer func() {
		globals.SnapShotDeleteByInodeLayerUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.SnapShotDeleteByInodeLayerErrors.Add(1)
		}
	}()

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

	deletedVolumeView, ok = value.(*volumeViewStruct)
	if !ok {
		logger.Fatalf("viewTreeByID.GetByKey(0x%016X) returned something other than a volumeView", id)
	}
	deletedVolumeViewIndex, found, err = volume.viewTreeByNonce.BisectLeft(deletedVolumeView.nonce)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByNonce.BisectLeft() failed with error: %v", err)
	}
	if !found {
		logger.Fatalf("Logic error - viewTreeByNonce.BisectLeft() returned found == false")
	}

	if 0 == deletedVolumeViewIndex {
		ok = true
		for ok {
			ok, err = deletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByIndex(0)
			if nil != err {
				logger.Fatalf("Logic error - deletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByIndex(0) failed with error: %v", err)
			}
		}
		ok = true
		for ok {
			key, value, ok, err = deletedVolumeView.deletedObjectsWrapper.bPlusTree.GetByIndex(0)
			if nil != err {
				logger.Fatalf("Logic error - deletedVolumeView.deletedObjectsWrapper.bPlusTree.GetByIndex(0) failed with error: %v", err)
			}
			if ok {
				ok, err = deletedVolumeView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0)
				if nil != err {
					logger.Fatalf("Logic error - deletedVolumeView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0) failed with error: %v", err)
				}
				if !ok {
					logger.Fatalf("Logic error - deletedVolumeView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0) returned ok == false")
				}
				ok, err = volume.liveView.deletedObjectsWrapper.bPlusTree.Put(key, value)
				if nil != err {
					logger.Fatalf("Logic error - liveView.deletedObjectsWrapper.bPlusTree.Put() failed with error: %v", err)
				}
				if !ok {
					logger.Fatalf("Logic error - liveView.deletedObjectsWrapper.bPlusTree.Put() returned ok == false")
				}
			}
		}
	} else {
		_, value, ok, err = volume.viewTreeByNonce.GetByIndex(deletedVolumeViewIndex - 1)
		if nil != err {
			logger.Fatalf("Logic error - viewTreeByNonce.GetByIndex() failed with error: %v", err)
		}
		if !ok {
			logger.Fatalf("Logic error - viewTreeByNonce.GetByIndex() returned ok == false")
		}
		predecessorToDeletedVolumeView, ok = value.(*volumeViewStruct)
		if !ok {
			logger.Fatalf("Logic error - viewTreeByNonce.GetByIndex() returned something other than a volumeView")
		}

		ok = true
		for ok {
			key, value, ok, err = deletedVolumeView.createdObjectsWrapper.bPlusTree.GetByIndex(0)
			if nil != err {
				logger.Fatalf("Logic error - deletedVolumeView.createdObjectsWrapper.bPlusTree.GetByIndex(0) failed with error: %v", err)
			}
			if ok {
				ok, err = deletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByIndex(0)
				if nil != err {
					logger.Fatalf("Logic error - deletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByIndex(0) failed with error: %v", err)
				}
				if !ok {
					logger.Fatalf("Logic error - deletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByIndex(0) returned ok == false")
				}
				ok, err = predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.Put(key, value)
				if nil != err {
					logger.Fatalf("Logic error - predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.Put() failed with error: %v", err)
				}
				if !ok {
					logger.Fatalf("Logic error - predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.Put() returned ok == false")
				}
			}
		}
		ok = true
		for ok {
			key, value, ok, err = deletedVolumeView.deletedObjectsWrapper.bPlusTree.GetByIndex(0)
			if nil != err {
				logger.Fatalf("Logic error - deletedVolumeView.deletedObjectsWrapper.bPlusTree.GetByIndex(0) failed with error: %v", err)
			}
			if ok {
				ok, err = deletedVolumeView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0)
				if nil != err {
					logger.Fatalf("Logic error - deletedVolumeView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0) failed with error: %v", err)
				}
				_, found, err = predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.BisectLeft(key)
				if nil != err {
					logger.Fatalf("Logic error - predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.BisectLeft(key) failed with error: %v", err)
				}
				if found {
					ok, err = predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByKey(key)
					if nil != err {
						logger.Fatalf("Logic error - predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByKey(key) failed with error: %v", err)
					}
					if !ok {
						logger.Fatalf("Logic error - predecessorToDeletedVolumeView.createdObjectsWrapper.bPlusTree.DeleteByKey(key) returned ok == false")
					}
					ok, err = volume.liveView.deletedObjectsWrapper.bPlusTree.Put(key, value)
					if nil != err {
						logger.Fatalf("Logic error - liveView.deletedObjectsWrapper.bPlusTree.Put() failed with error: %v", err)
					}
					if !ok {
						logger.Fatalf("Logic error - liveView.deletedObjectsWrapper.bPlusTree.Put() returned ok == false")
					}
				} else {
					ok, err = predecessorToDeletedVolumeView.deletedObjectsWrapper.bPlusTree.Put(key, value)
					if nil != err {
						logger.Fatalf("Logic error - predecessorToDeletedVolumeView.deletedObjectsWrapper.bPlusTree.Put() failed with error: %v", err)
					}
					if !ok {
						logger.Fatalf("Logic error - predecessorToDeletedVolumeView.deletedObjectsWrapper.bPlusTree.Put() returned ok == false")
					}
				}
			}
		}
	}

	evtlog.Record(evtlog.FormatHeadhunterCheckpointStart, volume.volumeName)
	err = volume.putCheckpoint()
	if nil != err {
		evtlog.Record(evtlog.FormatHeadhunterCheckpointEndFailure, volume.volumeName, err.Error())
		logger.FatalfWithError(err, "Shutting down to prevent subsequent checkpoints from corrupting Swift")
	}
	evtlog.Record(evtlog.FormatHeadhunterCheckpointEndSuccess, volume.volumeName)

	ok, err = volume.viewTreeByNonce.DeleteByKey(deletedVolumeView.nonce)
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

	ok, err = volume.viewTreeByTime.DeleteByKey(deletedVolumeView.snapShotTime)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByTime.DeleteByKey() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByTime.DeleteByKey() returned ok == false")
	}

	ok, err = volume.viewTreeByName.DeleteByKey(deletedVolumeView.snapShotName)
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByName.DeleteByKey() failed with error: %v", err)
	}
	if !ok {
		logger.Fatalf("Logic error - viewTreeByName.DeleteByKey() returned ok == false")
	}

	remainingSnapShotCount, err = volume.viewTreeByNonce.Len()
	if nil != err {
		logger.Fatalf("Logic error - viewTreeByNonce.Len() failed with error: %v", err)
	}

	if 0 == remainingSnapShotCount {
		volume.priorView = nil
	} else {
		_, value, ok, err = volume.viewTreeByNonce.GetByIndex(remainingSnapShotCount - 1)
		if nil != err {
			logger.Fatalf("Logic error - viewTreeByNonce.GetByIndex(<last>) failed with error: %v", err)
		}
		if !ok {
			logger.Fatalf("Logic error - viewTreeByNonce.GetByIndex(<last>) returned ok == false")
		}
		newPriorVolumeView, ok = value.(*volumeViewStruct)
		if !ok {
			logger.Fatalf("Logic error - viewTreeByNonce.GetByIndex(<last>) returned something other than a volumeView")
		}
		volume.priorView = newPriorVolumeView
	}

	volume.availableSnapShotIDList.PushBack(id)

	evtlog.Record(evtlog.FormatHeadhunterCheckpointStart, volume.volumeName)
	err = volume.putCheckpoint()
	if nil != err {
		evtlog.Record(evtlog.FormatHeadhunterCheckpointEndFailure, volume.volumeName, err.Error())
		logger.FatalfWithError(err, "Shutting down to prevent subsequent checkpoints from corrupting Swift")
	}
	evtlog.Record(evtlog.FormatHeadhunterCheckpointEndSuccess, volume.volumeName)

	err = deletedVolumeView.inodeRecWrapper.bPlusTree.Prune()
	if nil != err {
		logger.Fatalf("Logic error - deletedVolumeView.inodeRecWrapper.bPlusTree.Prune() failed with error: %v", err)
	}

	err = deletedVolumeView.logSegmentRecWrapper.bPlusTree.Prune()
	if nil != err {
		logger.Fatalf("Logic error - deletedVolumeView.logSegmentRecWrapper.bPlusTree.Prune() failed with error: %v", err)
	}

	err = deletedVolumeView.bPlusTreeObjectWrapper.bPlusTree.Prune()
	if nil != err {
		logger.Fatalf("Logic error - deletedVolumeView.bPlusTreeObjectWrapper.bPlusTree.Prune() failed with error: %v", err)
	}

	err = deletedVolumeView.createdObjectsWrapper.bPlusTree.Prune()
	if nil != err {
		logger.Fatalf("Logic error - deletedVolumeView.createdObjectsWrapper.bPlusTree.Prune() failed with error: %v", err)
	}

	err = deletedVolumeView.deletedObjectsWrapper.bPlusTree.Prune()
	if nil != err {
		logger.Fatalf("Logic error - deletedVolumeView.deletedObjectsWrapper.bPlusTree.Prune() failed with error: %v", err)
	}

	volume.Unlock()

	return
}

func (volume *volumeStruct) SnapShotCount() (snapShotCount uint64) {
	var (
		err error
		len int
	)

	startTime := time.Now()
	defer func() {
		globals.SnapShotCountUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.SnapShotCountErrors.Add(1)
		}
	}()

	len, err = volume.viewTreeByID.Len()
	if nil != err {
		logger.Fatalf("headhunter.SnapShotCount() for volume %v hit viewTreeByID.Len() error: %v", volume.volumeName, err)
	}
	snapShotCount = uint64(len)
	return
}

func (volume *volumeStruct) SnapShotLookupByName(name string) (snapShot SnapShotStruct, ok bool) {
	var (
		err        error
		value      sortedmap.Value
		volumeView *volumeViewStruct
	)

	startTime := time.Now()
	defer func() {
		globals.SnapShotLookupByNameUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.SnapShotLookupByNameErrors.Add(1)
		}
	}()

	value, ok, err = volume.viewTreeByName.GetByKey(name)
	if nil != err {
		logger.Fatalf("headhunter.SnapShotLookupByName() for volume %v hit viewTreeByName.GetByKey(\"%v\") error: %v", volume.volumeName, name, err)
	}
	if !ok {
		return
	}

	volumeView, ok = value.(*volumeViewStruct)
	if !ok {
		logger.Fatalf("headhunter.SnapShotLookupByName() for volume %v hit value.(*volumeViewStruct) !ok", volume.volumeName)
	}

	snapShot = SnapShotStruct{
		ID:   volumeView.snapShotID,
		Time: volumeView.snapShotTime,
		Name: volumeView.snapShotName, // == name
	}

	return
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

	startTime := time.Now()
	defer func() {
		globals.SnapShotListByIDUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	volume.Lock()
	list = volume.snapShotList(volume.viewTreeByID, reversed)
	volume.Unlock()

	return
}

func (volume *volumeStruct) SnapShotListByTime(reversed bool) (list []SnapShotStruct) {

	startTime := time.Now()
	defer func() {
		globals.SnapShotListByTimeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	volume.Lock()
	list = volume.snapShotList(volume.viewTreeByTime, reversed)
	volume.Unlock()

	return
}

func (volume *volumeStruct) SnapShotListByName(reversed bool) (list []SnapShotStruct) {

	startTime := time.Now()
	defer func() {
		globals.SnapShotListByNameUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	volume.Lock()
	list = volume.snapShotList(volume.viewTreeByName, reversed)
	volume.Unlock()

	return
}

func (volume *volumeStruct) SnapShotU64Decode(snapShotU64 uint64) (snapShotIDType SnapShotIDType, snapShotID uint64, nonce uint64) {

	startTime := time.Now()
	defer func() {
		globals.SnapShotU64DecodeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	snapShotID = snapShotU64 >> volume.snapShotIDShift
	if 0 == snapShotID {
		snapShotIDType = SnapShotIDTypeLive
	} else if volume.dotSnapShotDirSnapShotID == snapShotID {
		snapShotIDType = SnapShotIDTypeDotSnapShot
	} else {
		snapShotIDType = SnapShotIDTypeSnapShot
	}

	nonce = snapShotU64 & volume.snapShotU64NonceMask

	return
}

func (volume *volumeStruct) SnapShotIDAndNonceEncode(snapShotID uint64, nonce uint64) (snapShotU64 uint64) {

	startTime := time.Now()
	defer func() {
		globals.SnapShotIDAndNonceEncodeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	snapShotU64 = snapShotID
	snapShotU64 = snapShotU64 << volume.snapShotIDShift
	snapShotU64 = snapShotU64 | nonce

	return
}

func (volume *volumeStruct) SnapShotTypeDotSnapShotAndNonceEncode(nonce uint64) (snapShotU64 uint64) {

	startTime := time.Now()
	defer func() {
		globals.SnapShotTypeDotSnapShotAndNonceEncodeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	snapShotU64 = volume.dotSnapShotDirSnapShotID
	snapShotU64 = snapShotU64 << volume.snapShotIDShift
	snapShotU64 = snapShotU64 | nonce

	return
}
