// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ilayout

import (
	"fmt"
)

func unmarshalCheckPointHeaderVersion(checkpointHeaderString string) (checkPointHeaderVersion uint64, err error) {
	return 0, fmt.Errorf("TODO")
}

func (checkPointHeaderV1 *CheckPointHeaderV1Struct) marshalCheckPointHeaderV1() (checkPointHeaderString string, err error) {
	return "", fmt.Errorf("TODO")
}

func unmarshalCheckPointHeaderV1(checkPointHeaderString string) (checkPointHeaderV1 *CheckPointHeaderV1Struct, err error) {
	return nil, fmt.Errorf("TODO")
}

func unmarshalSuperBlockVersion(superBlockBuf []byte) (superBlockVersion uint64, err error) {
	return 0, fmt.Errorf("TODO")
}

func (superBlockV1 *SuperBlockV1Struct) marshalSuperBlockV1() (superBlockV1Buf []byte, err error) {
	return nil, fmt.Errorf("TODO")
}

func unmarshalSuperBlockV1(superBlockV1Buf []byte) (superBlockV1 *SuperBlockV1Struct, err error) {
	return nil, fmt.Errorf("TODO")
}

func unmarshalInodeTableEntryValueVersion(inodeTableEntryValueBuf []byte) (inodeTableEntryValueVersion uint64, err error) {
	return 0, fmt.Errorf("TODO")
}

func (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct) marshalInodeTableEntryValueV1() (inodeTableEntryValueV1Buf []byte, err error) {
	return nil, fmt.Errorf("TODO")
}

func unmarshalInodeTableEntryValueV1(inodeTableEntryValueV1Buf []byte) (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct, err error) {
	return nil, fmt.Errorf("TODO")
}

func unmarshalInodeHeadVersion(inodeHeadBuf []byte) (inodeHeadVersion uint64, err error) {
	return 0, fmt.Errorf("TODO")
}

func (inodeHeadV1 *InodeHeadV1Struct) marshalInodeHeadV1() (inodeHeadV1Buf []byte, err error) {
	return nil, fmt.Errorf("TODO")
}

func unmarshalInodeHeadV1(inodeHeadV1Buf []byte) (inodeHeadV1 *InodeHeadV1Struct, err error) {
	return nil, fmt.Errorf("TODO")
}
