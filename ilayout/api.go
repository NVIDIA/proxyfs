// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package ilayout specifies the "on disk" format of the file system metadata.
//
// The file system heavily leverages package sortedmap to provide a pageable
// B+Tree metadata system capable of very large inode counts and sizes. Several
// data structures are also "versioned" to enable gradual modification.
//
// The structure of the file system assumes an underlying object store system
// (e.g. an S3 Bucket or OpenStack Swift Container) that may or may not support
// consistency. As such, no Object (or any other numerically identified entity
// in the file system) is ever written twice. This means that the file system
// operates in a write journaled mode.
//
// At the top level, the file system periodically checkpoints. This enables the
// file system to be complemented by the extreme scale of such Object Store
// systems despite such often suffering long latencies and coarse granularity
// of writes. The CheckPointHeader is the changing indication of the most
// recent checkpoint performed.
//
// From the latest checkpoint, the file system's SuperBlock is located. This
// SuperBlock is responsible for tracking the location, in the Object Store,
// of the Inodes (identified by InodeNumber's). This is the first use case
// for the pageable B+Tree mechanism.
//
// The file system's Inodes are stored in unique Objects. The "tail" of the
// most recent Object written for the Inode contains the Inode's state
// (e.g. Mode, UserID, CreationTime, etc...). The Inode's "type" will be
// either a Directory, a normal File, or a Symbolic Link.
//
// For a Directory Inodes, large directory entry counts call for a second
// use case for the pageable B+Tree mechanism. Each entry represents a
// mapping from a "basename" to another Inode in the file system. As an
// optimization, this mapping also records the InodeType of the refernced
// Inode. This Directory B+Tree is stored in the same sequence of Objects
// as that holding the Directory Inode's state.
//
// For normal File Inodes, large amounts of data must be managed. File data
// is represented as a sequence of "extents" that may be individually modest
// in size but may be very large in number. This requirement presents the
// third use case for the pageable B+Tree mechanism. This time, the entries
// in the ExtentMap B+Tree are mappings from File "offsets" and "extent lengths"
// to elsewhere in the Object Store objects where the actual File data is held.
// Both the ExtentMap B+Tree and the File data extents are stored in the same
// sequence of Objects that hold the File Inode's state.
//
// For Symbolic Link Inodes, there is no need for anything more than the
// Inode's "state". Here, the "state" adds in the SymLinkTarget.
//
// In addition to structures and constants laying out the file system's "on-disk"
// format, several marshaling func's are provided to convert between this
// "on disk" format and an "in memory" equivalent. These func's are both high
// level (e.g. "superblock") and low level (e.g. uint64) to assist in managing
// the "on disk" representation of the file system.
//
package ilayout

import (
	"time"
)

// CheckPointHeaderName specifies the name of the CheckPointHeader applied
// to the Container to identify the SuperBlock.
//
const (
	CheckPointHeaderName = "X-Container-Meta-Check-Point"
)

// CheckpointHeaderVersionV* specifies the format of the CheckPointHeader.
// The CheckPointHeaderVersion must always be fetched by scanning the entire
// CheckPointHeader using a %016X format specifier. This value will then be
// used to interpret the remaining characters of the CheckPointHeader string.
//
const (
	CheckPointHeaderVersionV1 uint64 = 1
)

// UnmarshalCheckPointHeaderVersion extracts checkPointHeaderVersion from checkpointHeaderString.
//
func UnmarshalCheckPointHeaderVersion(checkpointHeaderString string) (checkPointHeaderVersion uint64, err error) {
	checkPointHeaderVersion, err = unmarshalCheckPointHeaderVersion(checkpointHeaderString)
	return
}

// CheckPointHeaderV1Struct specifies the format of the CheckPointHeader
// as of V1.
//
// The contents of the struct are serialized as space separated fields formatted
// via %016X numbers.
//
type CheckPointHeaderV1Struct struct {
	Version                uint64 // == CheckPointHeaderVersionV1
	SuperBlockObjectNumber uint64 // Identifies the Object containing the SuperBlock at the end
	SuperBlockLength       uint64 // Total length of the SuperBlock found at the end of the Object indicated by SuperBlockObjectNumber
	ReservedToNonce        uint64 // Ensures all numbers requiring uniqueness (e.g. Object numbers, Inode numbers) are never reused
}

// MarshalCheckPointHeaderV1 encodes checkPointHeaderV1 to checkpointHeaderString.
//
func (checkPointHeaderV1 *CheckPointHeaderV1Struct) MarshalCheckPointHeaderV1() (checkPointHeaderV1String string, err error) {
	checkPointHeaderV1String, err = checkPointHeaderV1.marshalCheckPointHeaderV1()
	return
}

// UnmarshalCheckPointHeaderV1 decodes checkPointHeaderV1 from checkpointHeaderString.
//
func UnmarshalCheckPointHeaderV1(checkPointHeaderV1String string) (checkPointHeaderV1 *CheckPointHeaderV1Struct, err error) {
	checkPointHeaderV1, err = unmarshalCheckPointHeaderV1(checkPointHeaderV1String)
	return
}

// ObjectTrailerStruct specifies the layout of a trailer found in each Object
// that identifies the objType, version, and size of a structure immediately
// proceeding it.
//
// The struct is serialized as a sequence of LittleEndian formatted fields.
//
type ObjectTrailerStruct struct {
	ObjType uint16
	Version uint16
	Length  uint32
}

// MarshalObjectTrailer encodes objectTrailer to objectTrailerBuf.
//
func (objectTrailer *ObjectTrailerStruct) MarshalObjectTrailer() (objectTrailerBuf []byte, err error) {
	objectTrailerBuf, err = objectTrailer.marshalObjectTrailer()
	return
}

// UnmarshalObjectTrailer decodes objectTrailer from objectTrailerBuf.
//
// Note that the last 8 bytes of objectTrailerBuf are decoded. The entire
// objectTrailerBuf is expected to contain precisely objectTrailer.Length
// bytes before the ObjectTrailerStruct.
//
func UnmarshalObjectTrailer(objectTrailerBuf []byte) (objectTrailer *ObjectTrailerStruct, err error) {
	objectTrailer, err = unmarshalObjectTrailer(objectTrailerBuf)
	return
}

// SuperBlockType specifies that this ObjectTrailerStruct refers to
// a SuperBlockV*Struct immediately preceeding it.
//
const (
	SuperBlockType uint16 = 0x5342 // 'S' 'B'
)

// SuperBlockVersionV* specifies, for an ObjectTrailerStruct of Type SuperBlockType,
// the Version of the SuperBlockV*Struct immediately preceeding the ObjectTrailerStruct.
//
const (
	SuperBlockVersionV1 uint16 = 1
)

// InodeTableLayoutEntryV1Struct specifies the layout of the InodeTable B+Tree in Objects.
// Since any modification of the Volume will result in a fresh SuperBlockV1Struct
// being written to a new Object, when BytesReferenced drops to zero, the Object
// may be deleted.
//
// The struct is serialized as a sequence of LittleEndian formatted fields.
//
type InodeTableLayoutEntryV1Struct struct {
	ObjectNumber    uint64 // Identifies the Object containing the page(s) of the InodeTable B+Tree
	ObjectSize      uint64 // Number of bytes written to the Object
	BytesReferenced uint64 // Number of bytes currently referenced in the Object
}

// SuperBlockStruct specifies the format of the SuperBlock found at
// the CheckPointHeaderV1Struct.SuperBlockLength trailing bytes of
// the Object indicated by CheckPointHeaderV1Struct.SuperBlockObjectNumber.
//
// The InodeTable is a B+Tree where the Key is the uint64 InodeNumber. The Value
// is a InodeTableEntryValueV1Struct.
//
// The struct is serialized as a sequence of LittleEndian formatted fields.
// The InodeTableLayout slice is serialized by a preceeding LittleEndian
// count of the number of InodeTableLayoutEntryV1Struct's followed by the
// serialization of each one.
//
// Note that the CheckPointHeaderV1Struct.SuperBlockLength also includes the bytes for
// holding the ObjectTrailerStruct{ObjType: SuperBlockType, Version: SuperBlockVersionV1}
// that is appended.
//
type SuperBlockV1Struct struct {
	InodeTableRootObjectNumber uint64                          // Identifies the Object containing the root of the InodeTable
	InodeTableRootObjectOffset uint64                          // Starting offset in the Object of the root of the InodeTable
	InodeTableRootObjectLength uint64                          // Number of bytes in the Object of the root of the InodeTable
	InodeTableLayout           []InodeTableLayoutEntryV1Struct // Describes the data and space occupied by the the InodeTable
	InodeObjectCount           uint64                          // Number of Objects holding Inodes
	InodeObjectSize            uint64                          // Sum of sizes of all Objects holding Inodes
	InodeBytesReferenced       uint64                          // Sum of bytes referenced in all Objects holding Inodes
}

// MarshalSuperBlockV1 encodes superBlockV1 to superBlockV1Buf.
//
func (superBlockV1 *SuperBlockV1Struct) MarshalSuperBlockV1() (superBlockV1Buf []byte, err error) {
	superBlockV1Buf, err = superBlockV1.marshalSuperBlockV1()
	return
}

// UnmarshalSuperBlockV1 decodes superBlockV1 from superBlockV1Buf.
//
func UnmarshalSuperBlockV1(superBlockV1Buf []byte) (superBlockV1 *SuperBlockV1Struct, err error) {
	superBlockV1, err = unmarshalSuperBlockV1(superBlockV1Buf)
	return
}

// InodeTableEntryValueVersionV* specifies the format of all following bytes
// in an InodeTable entry's Value InodeTableEntryStruct.
//
// The value is stored in LittleEndian format.
//
const (
	InodeTableEntryValueVersionV1 uint64 = 1
)

// UnmarshalInodeTableEntryValueVersion extracts inodeTableEntryValueVersion from inodeTableEntryValueBuf.
//
func UnmarshalInodeTableEntryValueVersion(inodeTableEntryValueBuf []byte) (inodeTableEntryValueVersion uint64, err error) {
	inodeTableEntryValueVersion, err = unmarshalInodeTableEntryValueVersion(inodeTableEntryValueBuf)
	return
}

// InodeTableEntryValueV1Struct specifies the format of the bytes in the InodeTable entry's
// Value following InodeTableEntryValueVersionV1.
//
// The struct is serialized as a sequence of LittleEndian formatted fields.
//
// Note that there is no InodeTableEntryKeyV1Struct as it is simply a uint64 InodeNumber.
//
type InodeTableEntryValueV1Struct struct {
	InodeHeadObjectNumber uint64 // Identifies the Object containing InodeHeadV*Struct
	InodeHeadLength       uint64 // Total length of the InodeHead found at the end of the Object indicated by InodeHeadObjectNumber
}

// MarshalInodeTableEntryValueV1 encodes inodeTableEntryValueV1 to inodeTableEntryValueV1Buf.
//
func (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct) MarshalInodeTableEntryValueV1() (inodeTableEntryValueV1Buf []byte, err error) {
	inodeTableEntryValueV1Buf, err = inodeTableEntryValueV1.marshalInodeTableEntryValueV1()
	return
}

// UnmarshalInodeTableEntryValueV1 decodes inodeTableEntryValueV1 from inodeTableEntryValueV1Buf.
//
func UnmarshalInodeTableEntryValueV1(inodeTableEntryValueV1Buf []byte) (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct, err error) {
	inodeTableEntryValueV1, err = unmarshalInodeTableEntryValueV1(inodeTableEntryValueV1Buf)
	return
}

// InodeHeadType specifies that this ObjectTrailerStruct refers to
// a InodeHeadV*Struct immediately preceeding it.
//
const (
	InodeHeadType uint16 = 0x4948 // 'I' 'H'
)

// InodeHeadVersionV* specifies, for an ObjectTrailerStruct of Type InodeHeadType,
// the Version of InodeHeadV*Struct immediately preceeding the ObjectTrailerStruct.
//
const (
	InodeHeadVersionV1 uint16 = 1
)

// RootDirInodeNumber is the InodeNumber for the directory at the root of the file system.
//
const (
	RootDirInodeNumber uint64 = 1
)

// InodeType* specifies the type of Inode.
//
const (
	InodeTypeDir     uint8 = 0
	InodeTypeFile    uint8 = 1
	InodeTypeSymLink uint8 = 2
)

// InodeLinkTableEntryStruct specifies the layout of an InodeHeadV1Struct.LinkTable's entry.
//
// The struct's uint64 field is serialized in LittleEndian format followed by
// the struct's string field serialized as a LittleEndian length followed by
// the bytes of the string.
//
type InodeLinkTableEntryStruct struct {
	ParentDirInodeNumber uint64
	ParentDirEntryName   string
}

// InodeModeMask provides a bound on the acceptable values of an Inode's Mode field's
// protection bits (i.e. rwx bits for each of user, group, and other).
//
// The value is stored in LittleEndian format.
//
const (
	InodeModeMask uint16 = 0o777
)

// InodeStreamTableEntryStruct specifies the layout of an InodeHeadV1Struct.StreamTable's entry.
//
// The struct is serialized be treating both fields as an array of bytes preceeded
// by a LittleEndian length.
//
type InodeStreamTableEntryStruct struct {
	Name  string
	Value []byte
}

// InodeHeadLayoutEntryV1Struct is utilized in both DirInode's and FileInode's. For DirInode's,
// it specifies the layout of the Directory B+Tree in Objects. For FileInode's, it specifies
// the layout of the ExtentMap B+Tree as well as the File's contents in Objects. Since any
// modification of the Inode will result in a fresh InodeHeadStruct being written to a new
// Object, when BytesReferenced drops to zero, the Object may be deleted.
//
// The struct is serialized as a sequence of LittleEndian formatted fields.
//
type InodeHeadLayoutEntryV1Struct struct {
	ObjectNumber uint64 //    For DirInode's:
	//                          Identifies the Object containing the page(s) of the Directory B+Tree
	//                        For FileInode's:
	//                          Identifies the Object containing the page(s) of the ExtentMap B+Tree
	//                          as well as the bytes of the File's contents
	ObjectSize      uint64 // Number of bytes written to the Object
	BytesReferenced uint64 // Number of bytes currently referenced in the Object
}

// InodeHeadV1Struct specifies the layout of an Inode.
//
// The struct is serializes as a sequence of fields:
//   For uint* fields, LittleEndian format is used.
//   For table fields, a uint64 length in LittleEndian format is followed by the serialization
//     specified in the table entry struct.
//   For time.Time fields, a uint64 in LittleEndian is used to hold the UnixNano() equivalent.
//
// Note that the InodeTableEntryValueV1Struct.InodeHeadLength also includes the bytes for
// holding the ObjectTrailerStruct{ObjType: InodeHeadType, Version: InodeHeadVersionV1}
// that is appended.
//
type InodeHeadV1Struct struct {
	InodeNumber         uint64
	InodeType           uint8                       //    One of InodeType*
	LinkTable           []InodeLinkTableEntryStruct //    List of Directory Entry references to this Inode
	Size                uint64                      //    Only applicable to File Inodes
	CreationTime        time.Time
	ModificationTime    time.Time
	AccessTime          time.Time
	AttrChangeTime      time.Time
	Mode                uint16 //                         Must be <= InodeModeMask (Note: does not include InodeType encoding)
	UserID              uint64
	GroupID             uint64
	StreamTable         []InodeStreamTableEntryStruct  // List of Alternate Data Streams for this Inode
	PayloadObjectNumber uint64                         // For Dir & File Inodes, identifies the Object containing the root of the Directory or ExtentMap B+Tree
	PayloadObjectOffset uint64                         // For Dir & File Inodes, starting offset in the Object of the root of the Directory or ExtentMap B+Tree
	PayloadObjectLength uint64                         // For Dir & File Inodes, number of bytes in the Object of the root of the Directory or ExtentMap B+Tree
	SymLinkTarget       string                         // For SymLink Inodes, the target of the link
	Layout              []InodeHeadLayoutEntryV1Struct // Describes the data and space occupied by the the InodeTable
}

// MarshalInodeHeadV1 encodes inodeHeadV1 to inodeHeadV1Buf.
//
func (inodeHeadV1 *InodeHeadV1Struct) MarshalInodeHeadV1() (inodeHeadV1Buf []byte, err error) {
	inodeHeadV1Buf, err = inodeHeadV1.marshalInodeHeadV1()
	return
}

// UnmarshalInodeHeadV1 decodes inodeHeadV1 from inodeHeadV1Buf.
//
func UnmarshalInodeHeadV1(inodeHeadV1Buf []byte) (inodeHeadV1 *InodeHeadV1Struct, err error) {
	inodeHeadV1, err = unmarshalInodeHeadV1(inodeHeadV1Buf)
	return
}

// DirectoryEntryValueV1Struct specifies the format, for an Inode of type InodeTypeDir,
// of the bytes in a .Payload-identified B+Tree's Value.
//
// The struct is serialized as a sequence of uint* fields in LittleEndian format.
//
// Note that there is no DirectoryEntryKeyV1Struct as it is simply a string serialized
// by a uint64 length in LittleEndian format followed by the bytes of the string.
//
type DirectoryEntryValueV1Struct struct {
	InodeNumber uint64
	InodeType   uint8
}

// MarshalDirectoryEntryValueV1 encodes directoryEntryValueV1 to directoryEntryValueV1Buf.
//
func (directoryEntryValueV1 *DirectoryEntryValueV1Struct) MarshalDirectoryEntryValueV1() (directoryEntryValueV1Buf []byte, err error) {
	directoryEntryValueV1Buf, err = directoryEntryValueV1.marshalDirectoryEntryValueV1()
	return
}

// UnmarshalDirectoryEntryValueV1 decodes directoryEntryValueV1 from directoryEntryValueV1Buf.
//
func UnmarshalDirectoryEntryValueV1(directoryEntryValueV1Buf []byte) (directoryEntryValueV1 *DirectoryEntryValueV1Struct, err error) {
	directoryEntryValueV1, err = unmarshalDirectoryEntryValueV1(directoryEntryValueV1Buf)
	return
}

// ExtentMapEntryValueV1Struct specifies the format, for an Inode of type InodeTypeFile,
// of the bytes in a .Payload-identified B+Tree's Value.
//
// The struct is serialized as a sequence of uint64 fields in LittleEndian format.
//
// Note that there is no ExtentMapEntryKeyV1Struct as it is simply a uint64 serialized
// in LittleEndian format.
//
type ExtentMapEntryValueV1Struct struct {
	FileOffset   uint64 // Offset from the start of the File
	Length       uint64 // Length of this extent (both in the File and in the Object)
	ObjectNumber uint64 // Identifies the Object containing this extent's data
	ObjectOffset uint64 // Starting offset in the Object of this extent's data
}

// MarshalExtentMapEntryValueV1 encodes directoryEntryValueV1 to directoryEntryValueV1Buf.
//
func (extentMapEntryValueV1 *ExtentMapEntryValueV1Struct) MarshalExtentMapEntryValueV1() (extentMapEntryValueV1Buf []byte, err error) {
	extentMapEntryValueV1Buf, err = extentMapEntryValueV1.marshalExtentMapEntryValueV1()
	return
}

// UnmarshalExtentMapEntryValueV1 decodes directoryEntryValueV1 from directoryEntryValueV1Buf.
//
func UnmarshalExtentMapEntryValueV1(extentMapEntryValueV1Buf []byte) (extentMapEntryValueV1 *ExtentMapEntryValueV1Struct, err error) {
	extentMapEntryValueV1, err = unmarshalExtentMapEntryValueV1(extentMapEntryValueV1Buf)
	return
}

// GetLEUint8FromBuf fetches a uint8 from buf starting at curPos.
//
// The returned nextPos indicates where the next field (if any) should be read from.
//
func GetLEUint8FromBuf(buf []byte, curPos int) (u8 uint8, nextPos int, err error) {
	u8, nextPos, err = getLEUint8FromBuf(buf, curPos)
	return
}

// PutLEUint8ToBuf writes a uint8 to buf starting at curPos.
//
// The returned nextPost indicates where the next field (if any) should be written.
//
func PutLEUint8ToBuf(buf []byte, curPos int, u8 uint8) (nextPos int, err error) {
	nextPos, err = putLEUint8ToBuf(buf, curPos, u8)
	return
}

// GetLEUint16FromBuf fetches a uint16 from buf starting at curPos.
//
// The uint16 is assumed to have been written in LittleEndian byte order.
// The returned nextPos indicates where the next field (if any) should be read from.
//
func GetLEUint16FromBuf(buf []byte, curPos int) (u16 uint16, nextPos int, err error) {
	u16, nextPos, err = getLEUint16FromBuf(buf, curPos)
	return
}

// PutLEUint16ToBuf writes a uint16 to buf starting at curPos.
//
// The uint16 is written in LittleEndian byte order.
// The returned nextPost indicates where the next field (if any) should be written.
//
func PutLEUint16ToBuf(buf []byte, curPos int, u16 uint16) (nextPos int, err error) {
	nextPos, err = putLEUint16ToBuf(buf, curPos, u16)
	return
}

// GetLEUint32FromBuf fetches a uint32 from buf starting at curPos.
//
// The uint32 is assumed to have been written in LittleEndian byte order.
// The returned nextPos indicates where the next field (if any) should be read from.
//
func GetLEUint32FromBuf(buf []byte, curPos int) (u32 uint32, nextPos int, err error) {
	u32, nextPos, err = getLEUint32FromBuf(buf, curPos)
	return
}

// PutLEUint32ToBuf writes a uint32 to buf starting at curPos.
//
// The uint32 is written in LittleEndian byte order.
// The returned nextPost indicates where the next field (if any) should be written.
//
func PutLEUint32ToBuf(buf []byte, curPos int, u32 uint32) (nextPos int, err error) {
	nextPos, err = putLEUint32ToBuf(buf, curPos, u32)
	return
}

// GetLEUint64FromBuf fetches a uint64 from buf starting at curPos.
//
// The uint64 is assumed to have been written in LittleEndian byte order.
// The returned nextPos indicates where the next field (if any) should be read from.
//
func GetLEUint64FromBuf(buf []byte, curPos int) (u64 uint64, nextPos int, err error) {
	u64, nextPos, err = getLEUint64FromBuf(buf, curPos)
	return
}

// PutLEUint64ToBuf writes a uint64 to buf starting at curPos.
//
// The uint64 is written in LittleEndian byte order.
// The returned nextPost indicates where the next field (if any) should be written.
//
func PutLEUint64ToBuf(buf []byte, curPos int, u64 uint64) (nextPos int, err error) {
	nextPos, err = putLEUint64ToBuf(buf, curPos, u64)
	return
}

// GetLEStringFromBuf fetches a string from buf starting at curPos.
//
// The string is assumed to have been written as a LittleEndian byte order uint64
// length followed by the bytes that make up the string. The returned nextPos
// indicates where the next field (if any) should be read from.
//
func GetLEStringFromBuf(buf []byte, curPos int) (str string, nextPos int, err error) {
	str, nextPos, err = getLEStringFromBuf(buf, curPos)
	return
}

// PutLEStringToBuf writes a string to buf starting at curPos.
//
// The string is written as a LittleEndian byte order uint64 length followed by the
// bytes that make up the string. The returned nextPost indicates where the next field
// (if any) should be written.
//
func PutLEStringToBuf(buf []byte, curPos int, str string) (nextPos int, err error) {
	nextPos, err = putLEStringToBuf(buf, curPos, str)
	return
}

// GetLEByteSliceFromBuf fetches a []byte from buf starting at curPos.
//
// The []byte is assumed to have been written as a LittleEndian byte order uint64
// length followed by the bytes that make up the []byte. The returned nextPos
// indicates where the next field (if any) should be read from.
//
func GetLEByteSliceFromBuf(buf []byte, curPos int) (byteSlice []byte, nextPos int, err error) {
	byteSlice, nextPos, err = getLEByteSliceFromBuf(buf, curPos)
	return
}

// PutLEByteSliceToBuf writes a []byte to buf starting at curPos.
//
// The []byte is written as a LittleEndian byte order uint64 length followed by the
// bytes that make up the []byte. The returned nextPost indicates where the next field
// (if any) should be written.
//
func PutLEByteSliceToBuf(buf []byte, curPos int, byteSlice []byte) (nextPos int, err error) {
	nextPos, err = putLEByteSliceToBuf(buf, curPos, byteSlice)
	return
}

// GetFixedByteSliceFromBuf fetches a []byte from buf starting at curPos.
//
// The []byte is assumed to have been written with the same length as byteSlice.
// The returned nextPos indicates where the next field (if any) should be read from.
//
func GetFixedByteSliceFromBuf(buf []byte, curPos int, byteSlice []byte) (nextPos int, err error) {
	nextPos, err = getFixedByteSliceFromBuf(buf, curPos, byteSlice)
	return
}

// PutFixedByteSliceToBuf writes a []byte to buf starting at curPos.
//
// The returned nextPost indicates where the next field (if any) should be written.
//
func PutFixedByteSliceToBuf(buf []byte, curPos int, byteSlice []byte) (nextPos int, err error) {
	nextPos, err = putFixedByteSliceToBuf(buf, curPos, byteSlice)
	return
}
