// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ilayout

import (
	"testing"
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
					ObjectNumber:    4,
					ObjectSize:      5,
					BytesReferenced: 6,
				},
				{
					ObjectNumber:    7,
					ObjectSize:      8,
					BytesReferenced: 9,
				},
			},
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
		t.Fatalf("Bad unmarshaledCheckPointHeaderV1 (%#v) - expected testCheckPointHeaderV1 (%#v)", unmarshaledCheckPointHeaderV1, testCheckPointHeaderV1)
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
		t.Fatalf("Bad unmarshaledSuperBlockV1 (%#v) - expected testSuperBlockV1 (%#v) [Case 1]", unmarshaledSuperBlockV1, testSuperBlockV1)
	}
	for inodeTableLayoutIndex = range testSuperBlockV1.InodeTableLayout {
		if testSuperBlockV1.InodeTableLayout[inodeTableLayoutIndex] != unmarshaledSuperBlockV1.InodeTableLayout[inodeTableLayoutIndex] {
			t.Fatalf("Bad unmarshaledSuperBlockV1 (%#v) - expected testSuperBlockV1 (%#v) [Case 2]", unmarshaledSuperBlockV1, testSuperBlockV1)
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
		t.Fatalf("Bad unmarshaledInodeTableEntryValueV1 (%#v) - expected testInodeTableEntryValueV1 (%#v)", unmarshaledInodeTableEntryValueV1, testInodeTableEntryValueV1)
	}

	t.Logf("TODO: test MarshalInodeHeadV1()")
	t.Logf("TODO: test UnmarshalInodeHeadVersion()")
	t.Logf("TODO: test UnmarshalInodeHeadV1()")
}
