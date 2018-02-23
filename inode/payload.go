package inode

import (
	"bytes"
	"fmt"

	"github.com/swiftstack/ProxyFS/logger"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"
)

// For DirType inodes, our payload tree-map is from basenames to inode numbers.
// For FileType inodes, our payload tree-map is from file offsets to file extents.

// The signature of the pack/unpack methods says that failure is not an option
// (which is OK because it's reasonable to expect a well-defined data structure to
// have a well-defined serialization/deserialization), which is why our unpack
// methods have so many panic codepaths (that we expect to never run).

type treeNodeLoadable struct {
	inode *inMemoryInodeStruct
}

func (tnl *treeNodeLoadable) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	if 0 != objectOffset {
		err = fmt.Errorf("*treeNodeLoadable.GetNode(): Unexpected (non-zero) objectOffset (%v)", objectOffset)
		return
	}

	nodeByteSlice, err = tnl.inode.volume.headhunterVolumeHandle.GetBPlusTreeObject(objectNumber)

	if (nil == err) && (uint64(len(nodeByteSlice)) != objectLength) {
		err = fmt.Errorf("*treeNodeLoadable.GetNode(): Requested objectLength (%v) != Actual objectLength (%v)", objectLength, len(nodeByteSlice))
		return
	}

	return
}

func (tnl *treeNodeLoadable) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber, err = tnl.inode.volume.headhunterVolumeHandle.FetchNonce()
	if nil != err {
		return
	}

	objectOffset = 0

	err = tnl.inode.volume.headhunterVolumeHandle.PutBPlusTreeObject(objectNumber, nodeByteSlice)

	return
}

func (tnl *treeNodeLoadable) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	logger.Tracef("inode.Discardnode(): volume '%s' inode %d: "+
		"root Object %016X  discarding Object %016X  len %d",
		tnl.inode.volume.volumeName, tnl.inode.onDiskInodeV1Struct.InodeNumber,
		tnl.inode.onDiskInodeV1Struct.PayloadObjectNumber,
		objectNumber, objectLength)

	if 0 != objectOffset {
		err = fmt.Errorf("*treeNodeLoadable.DiscardNode(): Unexpected (non-zero) objectOffset (%v)", objectOffset)
		return
	}

	// Tell Headhunter we are done with this persisted node

	err = tnl.inode.volume.headhunterVolumeHandle.DeleteBPlusTreeObject(objectNumber)

	return
}

type fileInodeCallbacks struct {
	treeNodeLoadable
}

func (c *fileInodeCallbacks) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("fileInodeCallbacks.DumpKey() could not parse key as a uint64")
		return
	}

	keyAsString = fmt.Sprintf("0x%016X", keyAsUint64)

	err = nil
	return
}

func (c *fileInodeCallbacks) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsFileExtentPtr, ok := value.(*fileExtentStruct)
	if !ok {
		err = fmt.Errorf("fileInodeCallbacks.DumpValue() could not parse key as a *fileExtent")
		return
	}

	valueAsString = fmt.Sprintf("@%p: %#v", valueAsFileExtentPtr, valueAsFileExtentPtr)

	err = nil
	return
}

func (c *fileInodeCallbacks) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	packedKey, err = cstruct.Pack(key, sortedmap.OnDiskByteOrder)
	return
}

func (c *fileInodeCallbacks) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	fileExtent, ok := value.(*fileExtentStruct)
	if !ok {
		err = fmt.Errorf("PackValue() arg is not a *fileExtentStruct")
		return
	}
	packedValue, err = cstruct.Pack(fileExtent, sortedmap.OnDiskByteOrder)
	if nil != err {
		return
	}
	if uint64(len(packedValue)) != globals.fileExtentStructSize {
		err = fmt.Errorf("PackValue() should have produced len(packedValue) == %v", globals.fileExtentStructSize)
	}
	return
}

func (c *fileInodeCallbacks) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var fileOffset uint64
	bytesConsumed, err = cstruct.Unpack(payloadData, &fileOffset, sortedmap.OnDiskByteOrder)
	if nil != err {
		return
	}
	key = fileOffset
	return
}

func (c *fileInodeCallbacks) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if uint64(len(payloadData)) < globals.fileExtentStructSize {
		err = fmt.Errorf("UnpackValue() arg not big enough to encode fileExtentStruct")
		return
	}
	valueAsFileExtentPtr := &fileExtentStruct{}
	_, err = cstruct.Unpack(payloadData, valueAsFileExtentPtr, sortedmap.OnDiskByteOrder)
	if nil != err {
		return
	}
	value = valueAsFileExtentPtr
	bytesConsumed = globals.fileExtentStructSize
	err = nil
	return
}

type dirInodeCallbacks struct {
	treeNodeLoadable
}

func (c *dirInodeCallbacks) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString, ok := key.(string)

	if ok {
		err = nil
	} else {
		err = fmt.Errorf("dirInodeCallbacks.DumpKey() could not parse key as a string")
	}

	return
}

func (c *dirInodeCallbacks) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsUint64, ok := value.(InodeNumber)
	if !ok {
		err = fmt.Errorf("dirInodeCallbacks.DumpValue() could not parse value as a uint64")
		return
	}

	valueAsString = fmt.Sprintf("0x%016X", valueAsUint64)

	err = nil
	return
}

func (c *dirInodeCallbacks) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	basename, ok := key.(string)
	if !ok {
		err = fmt.Errorf("PackKey() arg not a string")
		return
	}
	packedKey = []byte(basename)
	packedKey = append(packedKey, 0) // null terminator
	err = nil
	return
}

func (c *dirInodeCallbacks) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	valueAsInodeNumber, ok := value.(InodeNumber)
	if !ok {
		err = fmt.Errorf("PackValue() arg is not an InodeNumber")
		return
	}
	valueAsUint64 := uint64(valueAsInodeNumber)
	packedValue, err = cstruct.Pack(valueAsUint64, sortedmap.OnDiskByteOrder)
	return
}

func (c *dirInodeCallbacks) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	basenameAndRemainderBytes := bytes.SplitN(payloadData, []byte{0}, 2)
	basenameBytes := basenameAndRemainderBytes[0]
	basename := string(basenameBytes)
	key = basename
	bytesConsumed = uint64(len(basenameBytes) + 1)
	err = nil
	return
}

func (c *dirInodeCallbacks) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var valueAsUint64 uint64
	bytesConsumed, err = cstruct.Unpack(payloadData, &valueAsUint64, sortedmap.OnDiskByteOrder)
	if nil != err {
		return
	}
	value = InodeNumber(valueAsUint64)
	bytesConsumed = 8
	return
}
