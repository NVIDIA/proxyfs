// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ilayout

import "time"

// CheckpointHeaderVersionV* specifies the format of the CheckPointHeader.
// The CheckPointHeaderVersion must always be fetched by scanning the entire
// CheckPointHeader using a %016X format specifier. This value will then be
// used to interpret the remaining characters of the CheckPointHeader string.
//
const (
	CheckPointHeaderVersionV1 uint64 = 1
)

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

// SuperBlockVersionV* specifies the format of all preceeding bytes
// referenced by CheckPointHeaderV1Struct.SuperBlock{ObjectNumber|Length}.
//
// The value is stored in BigEndian (binary uint64) format.
//
const (
	SuperBlockVersionV1 uint64 = 1
)

// InodeTableLayoutEntryV1Struct specifies the layout of the InodeTable B+Tree in Objects.
// Since any modification of the Volume will result in a fresh SuperBlockV1Struct
// being written to a new Object, when BytesReferenced drops to zero, the Object
// may be deleted.
//
// The struct is serialized as a sequence of BigEndian (binary uint64) formatted fields.
//
type InodeTableLayoutEntryV1Struct struct {
	ObjectNumber    uint64 // Identifies the Object containing the page(s) of the InodeTable B+Tree
	ObjectSize      uint64 // Number of bytes written to the Object
	BytesReferenced uint64 // Number of bytes currently referenced in the Object
}

// SuperBlockStruct specifies the format of the SuperBlock found at
// the CheckPointHeaderV1Struct.SuperBlockLength trailing bytes of
// the Object indicated by CheckPointHeaderV1Struct.SuperBlockObjectNumber.
// Note that the ...Length includes the SuperBlockVersionV1 value appended
// to it.
//
// The InodeTable is a B+Tree where the Key is the uint64 InodeNumber. The Value
// is
//
// The struct is serialized as a sequence of BigEndian (binary uint64) formatted fields.
// The InodeTableLayout slice is serialized by a preceeding BigEndian (binary uint64)
// count of the number of InodeTableLayoutEntryV1Struct's followed by the serialization
// of each one.
//
// Note that the CheckPointHeaderV1Struct.SuperBlockLength also includes the bytes for
// holding SuperBlockVersionV1 that is appended.
//
type SuperBlockV1Struct struct {
	InodeTableRootObjectNumber uint64                          // Identifies the Object containing the root of the InodeTable
	InodeTableRootObjectOffset uint64                          // Starting offset in the Object of the root of the InodeTable
	InodeTableRootObjectLength uint64                          // Number of bytes in the Object of the root of the InodeTable
	InodeTableLayout           []InodeTableLayoutEntryV1Struct // Describes the data and space occupied by the the InodeTable
}

// InodeTableEntryVersionV* specifies the format of all preceeding bytes
// in an InodeTable entry's Value InodeTableEntryStruct.
//
// The value is stored in BigEndian (binary uint64) format.
//
const (
	InodeTableEntryVersionV1 uint64 = 1
)

// InodeTableEntryValueV1Struct specifies the format of the bytes in the InodeTable entry's
// Value preceeding InodeTableEntryVersionV1.
//
// The struct is serialized as a sequence of BigEndian (binary uint64) formatted fields.
//
// Note that there is no InodeTableEntryKeyV1Struct as it is simply a uint64 InodeNumber.
//
type InodeTableEntryValueV1Struct struct {
	InodeHeadObjectNumber uint64 // Identifies the Object containing InodeHeadV*Struct
	InodeHeadLength       uint64 // Total length of the InodeHead found at the end of the Object indicated by InodeHeadObjectNumber
}

// InodeHeadVersionV* specifies the format of all preceeding bytes referenced
// by InodeTableEntryValueV1Struct.InodeHead{ObjectNumber|Length}.
//
// The value is stored in BigEndian (binary uint64) format.
//
const (
	InodeHeadVersionV1 uint64 = 1
)

// InodeType* specifies the type of Inode.
//
const (
	InodeTypeDir     uint8 = 0
	InodeTypeFile    uint8 = 1
	InodeTypeSymlink uint8 = 2
)

// InodeLinkTableEntryStruct specifies the layout of an InodeHeadV1Struct.LinkTable's entry.
//
// The struct's uint64 field is serialized in BigEndian (binary uint64) format followed by
// the struct's string field serialized as a BigEndian (binary uint64) length followed by
// the bytes of the string.
//
type InodeLinkTableEntryStruct struct {
	ParentDirInodeNumber uint64
	ParentDirEntryName   string
}

// InodeModeMask provides a bound on the acceptable values of an Inode's Mode field's
// protection bits (i.e. rwx bits for each of user, group, and other).
//
// The value is stored in BigEndian (binary uint16) format.
//
const (
	InodeModeMask uint16 = 0o777
)

// InodeStreamTableEntryStruct specifies the layout of an InodeHeadV1Struct.StreamTable's entry.
//
// The struct is serialized be treating both fields as an array of bytes preceeded
// by a BigEndian (binary uint64) length.
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
// The struct is serialized as a sequence of BigEndian (binary uint64) formatted fields.
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
// The struct is serializes as a sequence of fields.
//   For uint* fields, BigEndian format is used.
//   For table fields, a uint64 length in BigEndian format is followed by the serialization specified in
//     the table entry struct.
//   For time.Time fields, a string (i.e. uint64 length followed by that number of bytes) is used.
//     The string is itself time.RFC3339 formatted
//
// Note that the InodeTableEntryValueV1Struct.InodeHeadLength also includes the bytes for
// holding InodeHeadVersionV1 that is appended.
//
type InodeHeadV1Struct struct {
	InodeNumber         uint64
	InodeType           uint8                       //    One of InodeType*
	LinkTable           []InodeLinkTableEntryStruct //    List of Directory Entry references to this Inode
	Size                uint64
	CreationTime        time.Time
	ModificationTime    time.Time
	AccessTimeTime      time.Time
	AttrChangeTimeTime  time.Time
	Mode                uint16 //                         Must be <= InodeModeMask (Note: does not include InodeType encoding)
	UserID              uint64
	GroupID             uint64
	StreamTable         []InodeStreamTableEntryStruct  // List of Alternate Data Streams for this Inode
	PayloadObjectNumber uint64                         // For Dir & File Inodes, identifies the Object containing the root of the Directory or ExtentMap B+Tree
	PayloadObjectOffset uint64                         // For Dir & File Inodes, starting offset in the Object of the root of the Directory or ExtentMap B+Tree
	PayloadObjectLength uint64                         // For Dir & File Inodes, number of bytes in the Object of the root of the Directory or ExtentMap B+Tree
	SymlinkTarget       string                         // For Symlink Inodes, the target of the link
	Layout              []InodeHeadLayoutEntryV1Struct // Describes the data and space occupied by the the InodeTable
}

// DirectoryEntryValueV1Struct specifies the format, for an Inode of type InodeTypeDir,
// of the bytes in a .Payload-identified B+Tree's Value.
//
// The struct is serialized as a sequence of uint* fields in BigEndian (binary uint64) format.
//
// Note that there is no DirectoryEntryKeyV1Struct as it is simply a string serialized
// by a uint64 length in BigEndian format followed by the bytes of the string.
//
type DirectoryEntryValueV1Struct struct {
	InodeNumber uint64
	InodeType   uint8
}

// ExtentMapEntryValueV1Struct specifies the format, for an Inode of type InodeTypeFile,
// of the bytes in a .Payload-identified B+Tree's Value.
//
// The struct is serialized as a sequence of uint64 fields in BigEndian (binary uint64) format.
//
// Note that there is no ExtentMapEntryKeyV1Struct as it is simply a uint64 serialized
// in BigEndian format.
//
type ExtentMapEntryValueV1Struct struct {
	FileOffset   uint64 // Offset from the start of the File
	Length       uint64 // Length of this extent (both in the File and in the Object)
	ObjectNumber uint64 // Identifies the Object containing this extent's data
	ObjectOffset uint64 // Starting offset in the Object of this extent's data
}
