package headhunter

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

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

	valueAsValue, ok, err := volume.inodeRecWrapper.bPlusTree.GetByKey(inodeNumber)
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

	ok, err := volume.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.inodeRecWrapper.bPlusTree.Put(inodeNumber, valueToTree)
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
		ok, nonShadowingErr := volume.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, valuesToTree[i])
		if nil != nonShadowingErr {
			volume.Unlock()
			err = nonShadowingErr
			return
		}
		if !ok {
			_, err = volume.inodeRecWrapper.bPlusTree.Put(inodeNumber, valuesToTree[i])
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

	_, err = volume.inodeRecWrapper.bPlusTree.DeleteByKey(inodeNumber)

	volume.recordTransaction(transactionDeleteInodeRec, inodeNumber, nil)

	volume.Unlock()

	return
}

func (volume *volumeStruct) IndexedInodeNumber(index uint64) (inodeNumber uint64, ok bool, err error) {
	volume.Lock()
	key, _, ok, err := volume.inodeRecWrapper.bPlusTree.GetByIndex(int(index))
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

	valueAsValue, ok, err := volume.logSegmentRecWrapper.bPlusTree.GetByKey(logSegmentNumber)
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

	ok, err := volume.logSegmentRecWrapper.bPlusTree.PatchByKey(logSegmentNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.logSegmentRecWrapper.bPlusTree.Put(logSegmentNumber, valueToTree)
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
	volume.Lock()

	_, err = volume.logSegmentRecWrapper.bPlusTree.DeleteByKey(logSegmentNumber)

	volume.recordTransaction(transactionDeleteLogSegmentRec, logSegmentNumber, nil)

	// SSTODO: need to record this DELETE for later removal

	volume.Unlock()

	return
}

func (volume *volumeStruct) IndexedLogSegmentNumber(index uint64) (logSegmentNumber uint64, ok bool, err error) {
	volume.Lock()
	key, _, ok, err := volume.logSegmentRecWrapper.bPlusTree.GetByIndex(int(index))
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

	valueAsValue, ok, err := volume.bPlusTreeObjectWrapper.bPlusTree.GetByKey(objectNumber)
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

	ok, err := volume.bPlusTreeObjectWrapper.bPlusTree.PatchByKey(objectNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.bPlusTreeObjectWrapper.bPlusTree.Put(objectNumber, valueToTree)
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

	_, err = volume.bPlusTreeObjectWrapper.bPlusTree.DeleteByKey(objectNumber)

	volume.recordTransaction(transactionDeleteBPlusTreeObject, objectNumber, nil)

	volume.Unlock()

	return
}

func (volume *volumeStruct) IndexedBPlusTreeObjectNumber(index uint64) (objectNumber uint64, ok bool, err error) {
	volume.Lock()
	key, _, ok, err := volume.bPlusTreeObjectWrapper.bPlusTree.GetByIndex(int(index))
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
		trackingLayoutReport sortedmap.LayoutReport
		treeName             string
		treeWrapper          *bPlusTreeWrapperStruct
	)

	switch treeType {
	case InodeRecBPlusTree:
		treeName = "InodeRec"
		treeWrapper = volume.inodeRecWrapper
		trackingLayoutReport = volume.inodeRecBPlusTreeLayout
	case LogSegmentRecBPlusTree:
		treeName = "LogSegmentRec"
		treeWrapper = volume.logSegmentRecWrapper
		trackingLayoutReport = volume.logSegmentRecBPlusTreeLayout
	case BPlusTreeObjectBPlusTree:
		treeName = "BPlusTreeObject"
		treeWrapper = volume.bPlusTreeObjectWrapper
		trackingLayoutReport = volume.bPlusTreeObjectBPlusTreeLayout
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
		objectBytesTracked, ok = trackingLayoutReport[objectNumber]
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

	for objectNumber, objectBytesTracked = range trackingLayoutReport {
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
		// First, accumulate the 3 B+Tree sortedmap.LayoutReport's

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

func (volume *volumeStruct) CreateSnapShot(name string) (id uint64, err error) {
	var (
		snapShot *snapShotStruct
	)

	volume.Lock()
	defer volume.Unlock()

	id, err = volume.fetchNonceWhileLocked()
	if nil != err {
		return
	}

	snapShot = &snapShotStruct{
		id:        id,
		timeStamp: time.Now(),
		name:      name,
	}

	volume.snapShotMap[id] = snapShot // TODO: Need to actually create it

	err = nil

	return
}

func (volume *volumeStruct) DeleteSnapShot(id uint64) (err error) {
	var (
		ok bool
	)

	volume.Lock()
	defer volume.Unlock()

	_, ok = volume.snapShotMap[id]
	if !ok {
		err = fmt.Errorf("SnapShot ID == 0x%016X not found", id)
		return
	}

	delete(volume.snapShotMap, id) // TODO: Need to actually delete it

	err = nil

	return
}

// FetchSnapShotList returns list (most recent first) of available SnapShot's.
func (volume *volumeStruct) FetchSnapShotList() (list []SnapShotStruct) {
	var (
		snapShot *snapShotStruct
	)

	volume.Lock()
	defer volume.Unlock()

	list = make([]SnapShotStruct, 0, len(volume.snapShotMap))

	for _, snapShot = range volume.snapShotMap {
		list = append(list, SnapShotStruct{ID: snapShot.id, TimeStamp: snapShot.timeStamp, Name: snapShot.name})
	}

	sort.Slice(list, func(i int, j int) bool { return list[i].ID > list[j].ID })

	return
}
