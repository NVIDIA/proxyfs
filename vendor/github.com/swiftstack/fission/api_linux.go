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
