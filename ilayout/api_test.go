// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ilayout

import (
	"bytes"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	var (
		err error

		testCheckPointV1 = &CheckPointV1Struct{
			Version:                CheckPointVersionV1,
			SuperBlockObjectNumber: 2,
			SuperBlockLength:       3,
			ReservedToNonce:        4,
		}

		marshaledCheckPointV1        string
		unmarshaledCheckPointVersion uint64
		unmarshaledCheckPointV1      *CheckPointV1Struct

		testObjectTrailer = &ObjectTrailerStruct{
			ObjType: 1,
			Version: 2,
			Length:  0, // Appropriate if marshaledObjectTrailer is used to unmarshal (with no prepended payload)
		}

		marshaledObjectTrailer   []byte
		unmarshaledObjectTrailer *ObjectTrailerStruct

		testSuperBlockV1 = &SuperBlockV1Struct{
			InodeTableRootObjectNumber: 1,
			InodeTableRootObjectOffset: 2,
			InodeTableRootObjectLength: 3,
			InodeTableLayout: []InodeTableLayoutEntryV1Struct{
				{
					ObjectNumber:    11,
					ObjectSize:      12,
					BytesReferenced: 13,
				},
				{
					ObjectNumber:    21,
					ObjectSize:      22,
					BytesReferenced: 23,
				},
			},
			InodeObjectCount:     4,
			InodeObjectSize:      5,
			InodeBytesReferenced: 6,
			PendingDeleteObjectNumberArray: []uint64{
				7,
				8,
				9,
			},
		}

		marshaledSuperBlockV1   []byte
		unmarshaledSuperBlockV1 *SuperBlockV1Struct

		inodeTableLayoutIndex               int
		pendingDeleteObjectNumberArrayIndex int

		testInodeTableEntryValueV1 = &InodeTableEntryValueV1Struct{
			InodeHeadObjectNumber: 2,
			InodeHeadLength:       3,
		}

		marshaledInodeTableEntryValueV1                        []byte
		unmarshaledInodeTableEntryValueVersion                 uint64
		unmarshaledInodeTableEntryValueV1                      *InodeTableEntryValueV1Struct
		unmarshaledInodeTableEntryValueV1BytesConsumed         int
		unmarshaledInodeTableEntryValueV1BytesConsumedExpected = int(8 + 8 + 8)

		testStartTime = time.Now().Truncate(time.Second)

		testInodeHeadV1 = &InodeHeadV1Struct{
			InodeNumber: 1,
			InodeType:   InodeTypeSymLink,
			LinkTable: []InodeLinkTableEntryStruct{
				{
					ParentDirInodeNumber: 101,
					ParentDirEntryName:   "101",
				},
				{
					ParentDirInodeNumber: 102,
					ParentDirEntryName:   "102",
				},
			},
			Size:             3,
			ModificationTime: testStartTime.AddDate(0, 0, -2),
			StatusChangeTime: testStartTime.AddDate(0, 0, -1),
			Mode:             0o456,
			UserID:           7,
			GroupID:          8,
			StreamTable: []InodeStreamTableEntryStruct{
				{
					Name:  "201",
					Value: []byte{2, 0, 1},
				},
				{
					Name:  "202",
					Value: []byte{2, 0, 2},
				},
			},
			PayloadObjectNumber: 11,
			PayloadObjectOffset: 12,
			PayloadObjectLength: 13,
			SymLinkTarget:       "sym-link-target",
			Layout: []InodeHeadLayoutEntryV1Struct{
				{
					ObjectNumber:    311,
					ObjectSize:      312,
					BytesReferenced: 313,
				},
				{
					ObjectNumber:    321,
					ObjectSize:      322,
					BytesReferenced: 323,
				},
			},
		}

		marshaledInodeHeadV1   []byte
		unmarshaledInodeHeadV1 *InodeHeadV1Struct

		linkTableIndex   int
		streamTableIndex int
		layoutIndex      int

		testDirectoryEntryValueV1 = &DirectoryEntryValueV1Struct{
			InodeNumber: 1,
			InodeType:   2,
		}

		marshaledDirectoryEntryValueV1                        []byte
		unmarshaledDirectoryEntryValueV1                      *DirectoryEntryValueV1Struct
		unmarshaledDirectoryEntryValueV1BytesConsumed         int
		unmarshaledDirectoryEntryValueV1BytesConsumedExpected = int(8 + 1)

		testExtentMapEntryValueV1 = &ExtentMapEntryValueV1Struct{
			FileOffset:   1,
			Length:       2,
			ObjectNumber: 3,
			ObjectOffset: 4,
		}

		marshaledExtentMapEntryValueV1                        []byte
		unmarshaledExtentMapEntryValueV1                      *ExtentMapEntryValueV1Struct
		unmarshaledExtentMapEntryValueV1BytesConsumed         int
		unmarshaledExtentMapEntryValueV1BytesConsumedExpected = int(8 + 8 + 8 + 8)
	)

	marshaledCheckPointV1, err = testCheckPointV1.MarshalCheckPointV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledCheckPointVersion, err = UnmarshalCheckPointVersion(marshaledCheckPointV1)
	if nil != err {
		t.Fatal(err)
	}
	if CheckPointVersionV1 != unmarshaledCheckPointVersion {
		t.Fatalf("Bad unmarshaledCheckPointVersion (%016X) - expected CheckPointVersionV1 (%016X)", unmarshaledCheckPointVersion, CheckPointVersionV1)
	}

	unmarshaledCheckPointV1, err = UnmarshalCheckPointV1(marshaledCheckPointV1)
	if nil != err {
		t.Fatal(err)
	}
	if *testCheckPointV1 != *unmarshaledCheckPointV1 {
		t.Fatalf("Bad unmarshaledCheckPointV1 (%+v) - expected testCheckPointV1 (%+v)", unmarshaledCheckPointV1, testCheckPointV1)
	}

	marshaledObjectTrailer, err = testObjectTrailer.marshalObjectTrailer()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledObjectTrailer, err = UnmarshalObjectTrailer(marshaledObjectTrailer)
	if nil != err {
		t.Fatal(err)
	}
	if (testObjectTrailer.ObjType != unmarshaledObjectTrailer.ObjType) ||
		(testObjectTrailer.Version != unmarshaledObjectTrailer.Version) ||
		(testObjectTrailer.Length != unmarshaledObjectTrailer.Length) {
		t.Fatalf("Bad unmarshaledObjectTrailer (%+v) - expected testObjectTrailer (%+v)", unmarshaledObjectTrailer, testObjectTrailer)
	}

	marshaledSuperBlockV1, err = testSuperBlockV1.MarshalSuperBlockV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledSuperBlockV1, err = UnmarshalSuperBlockV1(marshaledSuperBlockV1)
	if nil != err {
		t.Fatal(err)
	}
	if (testSuperBlockV1.InodeTableRootObjectNumber != unmarshaledSuperBlockV1.InodeTableRootObjectNumber) ||
		(testSuperBlockV1.InodeTableRootObjectOffset != unmarshaledSuperBlockV1.InodeTableRootObjectOffset) ||
		(testSuperBlockV1.InodeTableRootObjectLength != unmarshaledSuperBlockV1.InodeTableRootObjectLength) ||
		(len(testSuperBlockV1.InodeTableLayout) != len(unmarshaledSuperBlockV1.InodeTableLayout)) ||
		(len(testSuperBlockV1.PendingDeleteObjectNumberArray) != len(unmarshaledSuperBlockV1.PendingDeleteObjectNumberArray)) {
		t.Fatalf("Bad unmarshaledSuperBlockV1 (%+v) - expected testSuperBlockV1 (%+v) [Case 1]", unmarshaledSuperBlockV1, testSuperBlockV1)
	}
	for inodeTableLayoutIndex = range testSuperBlockV1.InodeTableLayout {
		if testSuperBlockV1.InodeTableLayout[inodeTableLayoutIndex] != unmarshaledSuperBlockV1.InodeTableLayout[inodeTableLayoutIndex] {
			t.Fatalf("Bad unmarshaledSuperBlockV1 (%+v) - expected testSuperBlockV1 (%+v) [Case 2]", unmarshaledSuperBlockV1, testSuperBlockV1)
		}
	}
	for pendingDeleteObjectNumberArrayIndex = range testSuperBlockV1.PendingDeleteObjectNumberArray {
		if testSuperBlockV1.PendingDeleteObjectNumberArray[pendingDeleteObjectNumberArrayIndex] != unmarshaledSuperBlockV1.PendingDeleteObjectNumberArray[pendingDeleteObjectNumberArrayIndex] {
			t.Fatalf("Bad unmarshaledSuperBlockV1 (%+v) - expected testSuperBlockV1 (%+v) [Case 3]", unmarshaledSuperBlockV1, testSuperBlockV1)
		}
	}

	marshaledInodeTableEntryValueV1, err = testInodeTableEntryValueV1.MarshalInodeTableEntryValueV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledInodeTableEntryValueVersion, err = UnmarshalInodeTableEntryValueVersion(marshaledInodeTableEntryValueV1)
	if nil != err {
		t.Fatal(err)
	}
	if InodeTableEntryValueVersionV1 != unmarshaledInodeTableEntryValueVersion {
		t.Fatalf("Bad unmarshaledInodeTableEntryValueVersion (%016X) - expected InodeTableEntryValueVersionV1 (%016X)", unmarshaledInodeTableEntryValueVersion, InodeTableEntryValueVersionV1)
	}

	unmarshaledInodeTableEntryValueV1, unmarshaledInodeTableEntryValueV1BytesConsumed, err = UnmarshalInodeTableEntryValueV1(marshaledInodeTableEntryValueV1)
	if nil != err {
		t.Fatal(err)
	}
	if (testInodeTableEntryValueV1.InodeHeadObjectNumber != unmarshaledInodeTableEntryValueV1.InodeHeadObjectNumber) ||
		(testInodeTableEntryValueV1.InodeHeadLength != unmarshaledInodeTableEntryValueV1.InodeHeadLength) {
		t.Fatalf("Bad unmarshaledInodeTableEntryValueV1 (%+v) - expected testInodeTableEntryValueV1 (%+v)", unmarshaledInodeTableEntryValueV1, testInodeTableEntryValueV1)
	}
	if unmarshaledInodeTableEntryValueV1BytesConsumed != unmarshaledInodeTableEntryValueV1BytesConsumedExpected {
		t.Fatalf("Bad unmarshaledInodeTableEntryValueV1BytesConsumed (%v) - expected %v", unmarshaledInodeTableEntryValueV1BytesConsumed, unmarshaledInodeTableEntryValueV1BytesConsumedExpected)
	}

	marshaledInodeHeadV1, err = testInodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledInodeHeadV1, err = UnmarshalInodeHeadV1(marshaledInodeHeadV1)
	if nil != err {
		t.Fatal(err)
	}
	if (testInodeHeadV1.InodeNumber != unmarshaledInodeHeadV1.InodeNumber) ||
		(testInodeHeadV1.InodeType != unmarshaledInodeHeadV1.InodeType) ||
		(len(testInodeHeadV1.LinkTable) != len(unmarshaledInodeHeadV1.LinkTable)) ||
		(testInodeHeadV1.Size != unmarshaledInodeHeadV1.Size) ||
		(testInodeHeadV1.ModificationTime != unmarshaledInodeHeadV1.ModificationTime) ||
		(testInodeHeadV1.StatusChangeTime != unmarshaledInodeHeadV1.StatusChangeTime) ||
		(testInodeHeadV1.Mode != unmarshaledInodeHeadV1.Mode) ||
		(testInodeHeadV1.UserID != unmarshaledInodeHeadV1.UserID) ||
		(testInodeHeadV1.GroupID != unmarshaledInodeHeadV1.GroupID) ||
		(len(testInodeHeadV1.StreamTable) != len(unmarshaledInodeHeadV1.StreamTable)) ||
		(testInodeHeadV1.PayloadObjectNumber != unmarshaledInodeHeadV1.PayloadObjectNumber) ||
		(testInodeHeadV1.PayloadObjectOffset != unmarshaledInodeHeadV1.PayloadObjectOffset) ||
		(testInodeHeadV1.PayloadObjectLength != unmarshaledInodeHeadV1.PayloadObjectLength) ||
		(testInodeHeadV1.SymLinkTarget != unmarshaledInodeHeadV1.SymLinkTarget) ||
		(len(testInodeHeadV1.Layout) != len(unmarshaledInodeHeadV1.Layout)) {
		t.Fatalf("Bad unmarshaledInodeHeadV1 (%+v) - expected testInodeHeadV1 (%+v) [Case 1]", unmarshaledInodeHeadV1, testInodeHeadV1)
	}
	for linkTableIndex = range testInodeHeadV1.LinkTable {
		if testInodeHeadV1.LinkTable[linkTableIndex] != unmarshaledInodeHeadV1.LinkTable[linkTableIndex] {
			t.Fatalf("Bad unmarshaledInodeHeadV1 (%+v) - expected testInodeHeadV1 (%+v) [Case 2]", unmarshaledInodeHeadV1, testInodeHeadV1)
		}
	}
	for streamTableIndex = range testInodeHeadV1.StreamTable {
		if (testInodeHeadV1.StreamTable[streamTableIndex].Name != unmarshaledInodeHeadV1.StreamTable[streamTableIndex].Name) ||
			!bytes.Equal(testInodeHeadV1.StreamTable[streamTableIndex].Value, unmarshaledInodeHeadV1.StreamTable[streamTableIndex].Value) {
			t.Fatalf("Bad unmarshaledInodeHeadV1 (%+v) - expected testInodeHeadV1 (%+v) [Case 3]", unmarshaledInodeHeadV1, testInodeHeadV1)
		}
	}
	for layoutIndex = range testInodeHeadV1.Layout {
		if testInodeHeadV1.Layout[layoutIndex] != unmarshaledInodeHeadV1.Layout[layoutIndex] {
			t.Fatalf("Bad unmarshaledInodeHeadV1 (%+v) - expected testInodeHeadV1 (%+v) [Case 4]", unmarshaledInodeHeadV1, testInodeHeadV1)
		}
	}

	marshaledDirectoryEntryValueV1, err = testDirectoryEntryValueV1.MarshalDirectoryEntryValueV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledDirectoryEntryValueV1, unmarshaledDirectoryEntryValueV1BytesConsumed, err = UnmarshalDirectoryEntryValueV1(marshaledDirectoryEntryValueV1)
	if nil != err {
		t.Fatal(err)
	}
	if *testDirectoryEntryValueV1 != *unmarshaledDirectoryEntryValueV1 {
		t.Fatalf("Bad unmarshaledDirectoryEntryValueV1 (%+v) - expected testDirectoryEntryValueV1 (%+v)", unmarshaledDirectoryEntryValueV1, testDirectoryEntryValueV1)
	}
	if unmarshaledDirectoryEntryValueV1BytesConsumed != unmarshaledDirectoryEntryValueV1BytesConsumedExpected {
		t.Fatalf("Bad unmarshaledDirectoryEntryValueV1BytesConsumed (%v) - expected %v", unmarshaledDirectoryEntryValueV1BytesConsumed, unmarshaledDirectoryEntryValueV1BytesConsumedExpected)
	}

	marshaledExtentMapEntryValueV1, err = testExtentMapEntryValueV1.MarshalExtentMapEntryValueV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledExtentMapEntryValueV1, unmarshaledExtentMapEntryValueV1BytesConsumed, err = UnmarshalExtentMapEntryValueV1(marshaledExtentMapEntryValueV1)
	if nil != err {
		t.Fatal(err)
	}
	if *testExtentMapEntryValueV1 != *unmarshaledExtentMapEntryValueV1 {
		t.Fatalf("Bad unmarshaledExtentMapEntryValueV1 (%+v) - expected testExtentMapEntryValueV1 (%+v)", unmarshaledExtentMapEntryValueV1, testExtentMapEntryValueV1)
	}
	if unmarshaledExtentMapEntryValueV1BytesConsumed != unmarshaledExtentMapEntryValueV1BytesConsumedExpected {
		t.Fatalf("Bad unmarshaledExtentMapEntryValueV1BytesConsumed (%v) - expected %v", unmarshaledExtentMapEntryValueV1BytesConsumed, unmarshaledExtentMapEntryValueV1BytesConsumedExpected)
	}
}
