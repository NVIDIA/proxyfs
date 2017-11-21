package headhunter

import (
	"fmt"
	"sync"

	"github.com/swiftstack/ProxyFS/swiftclient"
)

func (volume *volumeStruct) fetchNextCheckPointDoneWaitGroupWhileLocked() (wg *sync.WaitGroup) {
	if nil == volume.checkpointDoneWaitGroup {
		volume.checkpointDoneWaitGroup = &sync.WaitGroup{}
		volume.checkpointDoneWaitGroup.Add(1)
	}
	wg = volume.checkpointDoneWaitGroup
	return
}

func (volume *volumeStruct) FetchNextCheckPointDoneWaitGroup() (wg *sync.WaitGroup) {
	volume.Lock()
	wg = volume.fetchNextCheckPointDoneWaitGroupWhileLocked()
	volume.Unlock()
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

	logger.Tracef("headhunger.GetInodeRec() entry: volume '%s' inode %d", volume.volumeName, inodeNumber)
	defer logger.Tracef("headhunger.GetInodeRec() return: volume '%s' inode %d", volume.volumeName, inodeNumber)

	volume.Lock()

	valueAsValue, ok, err := volume.inodeRecWrapper.bPlusTree.GetByKey(inodeNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
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

	logger.Tracef("headhunger.PutInodeRec() entry: volume '%s' inode %d", volume.volumeName, inodeNumber)
	defer logger.Tracef("headhunger.PutInodeRec() return: volume '%s' inode %d", volume.volumeName, inodeNumber)

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

	logger.Tracef("headhunter.PutInodeRecs() entry: volume '%s' inodes %v", volume.volumeName, inodeNumbers)
	defer logger.Tracef("headhunter.PutInodeRecs() return: volume '%s' inodes %v", volume.volumeName, inodeNumbers)

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

	volume.recordTransaction(transactionDeleteInodeRec, transactionDeleteInodeRec, nil)

	volume.Unlock()

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

	logger.Tracef("headhunter.PutLogSegmentRec() entry: volume '%s' Segment %v",
		volume.volumeName, logSegmentNumber)
	defer logger.Tracef("headhunter.PutLogSegmentRec() return: volume '%s' Segment %v",
		volume.volumeName, logSegmentNumber)

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

	volume.Unlock()

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
