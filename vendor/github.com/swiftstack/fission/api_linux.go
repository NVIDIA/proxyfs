// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package fission

const AttrSize = 88

type Attr struct {
	Ino       uint64
	Size      uint64
	Blocks    uint64
	ATimeSec  uint64
	MTimeSec  uint64
	CTimeSec  uint64
	ATimeNSec uint32
	MTimeNSec uint32
	CTimeNSec uint32
	Mode      uint32
	NLink     uint32
	UID       uint32
	GID       uint32
	RDev      uint32
	BlkSize   uint32
	Padding   uint32
}

const SetAttrInSize = 88

type SetAttrIn struct {
	Valid     uint32 // mask of const SetAttrInValid* bits
	Padding   uint32
	FH        uint64
	Size      uint64
	LockOwner uint64
	ATimeSec  uint64
	MTimeSec  uint64
	Unused2   uint64
	ATimeNSec uint32
	MTimeNSec uint32
	Unused3   uint32
	Mode      uint32
	Unused4   uint32
	UID       uint32
	GID       uint32
	Unused5   uint32
}

const SetXAttrInFixedPortionSize = 8 // + len(Name) + 1 + len(Data)

const GetXAttrInFixedPortionSize = 8 // + len(Name)

const ListXAttrInSize = 8
