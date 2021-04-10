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

		testCheckPointHeaderV1 = &CheckPointHeaderV1Struct{
			Version:                CheckPointHeaderVersionV1,
			SuperBlockObjectNumber: 2,
			SuperBlockLength:       3,
			ReservedToNonce:        4,
		}

		marshaledCheckPointHeaderV1        string
		unmarshaledCheckPointHeaderVersion uint64
		unmarshaledCheckPointHeaderV1      *CheckPointHeaderV1Struct

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
		}

		marshaledSuperBlockV1        []byte
		unmarshaledSuperBlockVersion uint64
		unmarshaledSuperBlockV1      *SuperBlockV1Struct

		inodeTableLayoutIndex int

		testInodeTableEntryValueV1 = &InodeTableEntryValueV1Struct{
			InodeHeadObjectNumber: 2,
			InodeHeadLength:       3,
		}

		marshaledInodeTableEntryValueV1        []byte
		unmarshaledInodeTableEntryValueVersion uint64
		unmarshaledInodeTableEntryValueV1      *InodeTableEntryValueV1Struct

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
			CreationTime:     testStartTime.AddDate(0, 0, -4),
			ModificationTime: testStartTime.AddDate(0, 0, -3),
			AccessTime:       testStartTime.AddDate(0, 0, -1),
			AttrChangeTime:   testStartTime.AddDate(0, 0, -2),
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

		marshaledInodeHeadV1        []byte
		unmarshaledInodeHeadVersion uint64
		unmarshaledInodeHeadV1      *InodeHeadV1Struct

		linkTableIndex   int
		streamTableIndex int
		layoutIndex      int

		testDirectoryEntryValueV1 = &DirectoryEntryValueV1Struct{
			InodeNumber: 1,
			InodeType:   2,
		}

		marshaledDirectoryEntryValueV1   []byte
		unmarshaledDirectoryEntryValueV1 *DirectoryEntryValueV1Struct

		testExtentMapEntryValueV1 = &ExtentMapEntryValueV1Struct{
			FileOffset:   1,
			Length:       2,
			ObjectNumber: 3,
			ObjectOffset: 4,
		}

		marshaledExtentMapEntryValueV1   []byte
		unmarshaledExtentMapEntryValueV1 *ExtentMapEntryValueV1Struct
	)

	marshaledCheckPointHeaderV1, err = testCheckPointHeaderV1.MarshalCheckPointHeaderV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledCheckPointHeaderVersion, err = UnmarshalCheckPointHeaderVersion(marshaledCheckPointHeaderV1)
	if nil != err {
		t.Fatal(err)
	}
	if CheckPointHeaderVersionV1 != unmarshaledCheckPointHeaderVersion {
		t.Fatalf("Bad unmarshaledCheckPointHeaderVersion (%016X) - expected CheckPointHeaderVersionV1 (%016X)", unmarshaledCheckPointHeaderVersion, CheckPointHeaderVersionV1)
	}

	unmarshaledCheckPointHeaderV1, err = UnmarshalCheckPointHeaderV1(marshaledCheckPointHeaderV1)
	if nil != err {
		t.Fatal(err)
	}
	if *testCheckPointHeaderV1 != *unmarshaledCheckPointHeaderV1 {
		t.Fatalf("Bad unmarshaledCheckPointHeaderV1 (%+v) - expected testCheckPointHeaderV1 (%+v)", unmarshaledCheckPointHeaderV1, testCheckPointHeaderV1)
	}

	marshaledSuperBlockV1, err = testSuperBlockV1.MarshalSuperBlockV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledSuperBlockVersion, err = UnmarshalSuperBlockVersion(marshaledSuperBlockV1)
	if nil != err {
		t.Fatal(err)
	}
	if SuperBlockVersionV1 != unmarshaledSuperBlockVersion {
		t.Fatalf("Bad unmarshaledSuperBlockVersion (%016X) - expected SuperBlockVersionV1 (%016X)", unmarshaledSuperBlockVersion, SuperBlockVersionV1)
	}

	unmarshaledSuperBlockV1, err = UnmarshalSuperBlockV1(marshaledSuperBlockV1)
	if nil != err {
		t.Fatal(err)
	}
	if (testSuperBlockV1.InodeTableRootObjectNumber != unmarshaledSuperBlockV1.InodeTableRootObjectNumber) ||
		(testSuperBlockV1.InodeTableRootObjectOffset != unmarshaledSuperBlockV1.InodeTableRootObjectOffset) ||
		(testSuperBlockV1.InodeTableRootObjectLength != unmarshaledSuperBlockV1.InodeTableRootObjectLength) ||
		(len(testSuperBlockV1.InodeTableLayout) != len(unmarshaledSuperBlockV1.InodeTableLayout)) {
		t.Fatalf("Bad unmarshaledSuperBlockV1 (%+v) - expected testSuperBlockV1 (%+v) [Case 1]", unmarshaledSuperBlockV1, testSuperBlockV1)
	}
	for inodeTableLayoutIndex = range testSuperBlockV1.InodeTableLayout {
		if testSuperBlockV1.InodeTableLayout[inodeTableLayoutIndex] != unmarshaledSuperBlockV1.InodeTableLayout[inodeTableLayoutIndex] {
			t.Fatalf("Bad unmarshaledSuperBlockV1 (%+v) - expected testSuperBlockV1 (%+v) [Case 2]", unmarshaledSuperBlockV1, testSuperBlockV1)
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
		t.Fatalf("Bad unmarshaledSuperBlockVersion (%016X) - expected SuperBlockVersionV1 (%016X)", unmarshaledSuperBlockVersion, SuperBlockVersionV1)
	}

	unmarshaledInodeTableEntryValueV1, err = UnmarshalInodeTableEntryValueV1(marshaledInodeTableEntryValueV1)
	if nil != err {
		t.Fatal(err)
	}
	if (testInodeTableEntryValueV1.InodeHeadObjectNumber != unmarshaledInodeTableEntryValueV1.InodeHeadObjectNumber) ||
		(testInodeTableEntryValueV1.InodeHeadLength != unmarshaledInodeTableEntryValueV1.InodeHeadLength) {
		t.Fatalf("Bad unmarshaledInodeTableEntryValueV1 (%+v) - expected testInodeTableEntryValueV1 (%+v)", unmarshaledInodeTableEntryValueV1, testInodeTableEntryValueV1)
	}

	marshaledInodeHeadV1, err = testInodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledInodeHeadVersion, err = UnmarshalInodeHeadVersion(marshaledInodeHeadV1)
	if nil != err {
		t.Fatal(err)
	}
	if InodeHeadVersionV1 != unmarshaledInodeHeadVersion {
		t.Fatalf("Bad unmarshaledInodeHeadVersion (%016X) - expected InodeHeadVersionV1 (%016X)", unmarshaledInodeHeadVersion, InodeHeadVersionV1)
	}

	unmarshaledInodeHeadV1, err = UnmarshalInodeHeadV1(marshaledInodeHeadV1)
	if nil != err {
		t.Fatal(err)
	}
	if (testInodeHeadV1.InodeNumber != unmarshaledInodeHeadV1.InodeNumber) ||
		(testInodeHeadV1.InodeType != unmarshaledInodeHeadV1.InodeType) ||
		(len(testInodeHeadV1.LinkTable) != len(unmarshaledInodeHeadV1.LinkTable)) ||
		(testInodeHeadV1.Size != unmarshaledInodeHeadV1.Size) ||
		(testInodeHeadV1.CreationTime != unmarshaledInodeHeadV1.CreationTime) ||
		(testInodeHeadV1.ModificationTime != unmarshaledInodeHeadV1.ModificationTime) ||
		(testInodeHeadV1.AccessTime != unmarshaledInodeHeadV1.AccessTime) ||
		(testInodeHeadV1.AttrChangeTime != unmarshaledInodeHeadV1.AttrChangeTime) ||
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
			(0 != bytes.Compare(testInodeHeadV1.StreamTable[streamTableIndex].Value, unmarshaledInodeHeadV1.StreamTable[streamTableIndex].Value)) {
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

	unmarshaledDirectoryEntryValueV1, err = UnmarshalDirectoryEntryValueV1(marshaledDirectoryEntryValueV1)
	if nil != err {
		t.Fatal(err)
	}
	if *testDirectoryEntryValueV1 != *unmarshaledDirectoryEntryValueV1 {
		t.Fatalf("Bad unmarshaledDirectoryEntryValueV1 (%+v) - expected testDirectoryEntryValueV1 (%+v)", unmarshaledDirectoryEntryValueV1, testDirectoryEntryValueV1)
	}

	marshaledExtentMapEntryValueV1, err = testExtentMapEntryValueV1.MarshalExtentMapEntryValueV1()
	if nil != err {
		t.Fatal(err)
	}

	unmarshaledExtentMapEntryValueV1, err = UnmarshalExtentMapEntryValueV1(marshaledExtentMapEntryValueV1)
	if nil != err {
		t.Fatal(err)
	}
	if *testExtentMapEntryValueV1 != *unmarshaledExtentMapEntryValueV1 {
		t.Fatalf("Bad unmarshaledExtentMapEntryValueV1 (%+v) - expected testExtentMapEntryValueV1 (%+v)", unmarshaledExtentMapEntryValueV1, testExtentMapEntryValueV1)
	}
}
