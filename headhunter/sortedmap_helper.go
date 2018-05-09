package headhunter

import (
	"fmt"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("headhunter.bPlusTreeWrapper.DumpKey() could not parse key as a uint64")
		return
	}

	keyAsString = fmt.Sprintf("0x%016X", keyAsUint64)

	err = nil
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = fmt.Errorf("headhunter.bPlusTreeWrapper.DumpValue() count not parse value as []byte")
		return
	}

	if 0 == len(valueAsByteSlice) {
		valueAsString = ""
		err = nil
		return
	}

	valueAsHexDigitByteSlice := make([]byte, 2+(3*(len(valueAsByteSlice)-1)))

	for i, u8 := range valueAsByteSlice {
		if i == 0 {
			valueAsHexDigitByteSlice[0] = utils.ByteToHexDigit(u8 >> 4)
			valueAsHexDigitByteSlice[1] = utils.ByteToHexDigit(u8 & 0x0F)
		} else {
			valueAsHexDigitByteSlice[(3*i)-1] = ' '
			valueAsHexDigitByteSlice[(3*i)+0] = utils.ByteToHexDigit(u8 >> 4)
			valueAsHexDigitByteSlice[(3*i)+1] = utils.ByteToHexDigit(u8 & 0x0F)
		}
	}

	valueAsString = string(valueAsHexDigitByteSlice[:])

	err = nil
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	stats.IncrementOperations(&stats.HeadhunterBPlusTreeNodeFaults)
	evtlog.Record(evtlog.FormatHeadhunterBPlusTreeNodeFault, bPlusTreeWrapper.volumeView.volume.volumeName, objectNumber, objectOffset, objectLength)

	nodeByteSlice, err =
		swiftclient.ObjectGet(
			bPlusTreeWrapper.volumeView.volume.accountName,
			bPlusTreeWrapper.volumeView.volume.checkpointContainerName,
			utils.Uint64ToHexStr(objectNumber),
			objectOffset,
			objectLength)

	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	var (
		bytesUsed uint64
		ok        bool
	)

	err = bPlusTreeWrapper.volumeView.volume.openCheckpointChunkedPutContextIfNecessary()
	if nil != err {
		return
	}

	objectNumber = bPlusTreeWrapper.volumeView.volume.checkpointChunkedPutContextObjectNumber

	objectOffset, err = bPlusTreeWrapper.volumeView.volume.bytesPutToCheckpointChunkedPutContext()
	if nil != err {
		return
	}

	err = bPlusTreeWrapper.volumeView.volume.sendChunkToCheckpointChunkedPutContext(nodeByteSlice)
	if nil != err {
		return
	}

	if nil != bPlusTreeWrapper.bPlusTreeTracker {
		bytesUsed, ok = bPlusTreeWrapper.bPlusTreeTracker.bPlusTreeLayout[objectNumber]

		if ok {
			bytesUsed += uint64(len(nodeByteSlice))
		} else {
			bytesUsed = uint64(len(nodeByteSlice))
		}

		bPlusTreeWrapper.bPlusTreeTracker.bPlusTreeLayout[objectNumber] = bytesUsed
	}

	err = bPlusTreeWrapper.volumeView.volume.closeCheckpointChunkedPutContextIfNecessary()

	return // err set as appropriate
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	var (
		bytesUsed uint64
		ok        bool
	)

	if nil != bPlusTreeWrapper.bPlusTreeTracker {
		bytesUsed, ok = bPlusTreeWrapper.bPlusTreeTracker.bPlusTreeLayout[objectNumber]

		if ok {
			if objectLength > bytesUsed {
				err = fmt.Errorf("Logic error: bPlusTreeWrapper.DiscardNode() called referencing too many bytes (0x%016X) in objectNumber 0x%016X", objectLength, objectNumber)
				logger.ErrorfWithError(err, "disk corruption or logic error [case 1]")
			} else {
				bytesUsed -= objectLength
				bPlusTreeWrapper.bPlusTreeTracker.bPlusTreeLayout[objectNumber] = bytesUsed
				err = nil
			}
		} else {
			err = fmt.Errorf("Logic error: bPlusTreeWrapper.DiscardNode() called referencing bytes (0x%016X) in unreferenced objectNumber 0x%016X", objectLength, objectNumber)
			logger.ErrorfWithError(err, "disk corruption or logic error [case 2]")
		}
	}

	return // err set as appropriate regardless of path
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.PackKey(key == %v) failed to convert key to uint64", key)
		return
	}
	packedKey = utils.Uint64ToByteSlice(keyAsUint64)
	err = nil
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackKey(payloadData) failed - len(payloadData) must be atleast 8 (was %v)", len(payloadData))
		return
	}
	keyAsUint64, ok := utils.ByteSliceToUint64(payloadData[:8])
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackKey(payloadData) failed in call to utils.ByteSliceToUint64()")
		return
	}
	key = keyAsUint64
	bytesConsumed = 8
	err = nil
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.PackValue() failed - value isn't a []byte")
		return
	}
	packedValue = utils.Uint64ToByteSlice(uint64(len(valueAsByteSlice)))
	packedValue = append(packedValue, valueAsByteSlice...)
	err = nil
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackValue(payloadData) failed - len(payloadData) must be atleast 8 (was %v)", len(payloadData))
		return
	}
	valueSize, ok := utils.ByteSliceToUint64(payloadData[:8])
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackValue(payloadData) failed in call to utils.ByteSliceToUint64()")
		return
	}
	valueAsByteSlice := make([]byte, valueSize)
	if 0 < valueSize {
		copy(valueAsByteSlice, payloadData[8:(8+valueSize)])
	}
	value = valueAsByteSlice
	bytesConsumed = 8 + valueSize
	err = nil
	return
}
