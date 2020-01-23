package fission

const AttrSize = 104

type Attr struct { // Note: Must be kept in sync with Attr struct in api_darwin.go
	Ino        uint64
	Size       uint64
	Blocks     uint64
	ATimeSec   uint64
	MTimeSec   uint64
	CTimeSec   uint64
	CrTimeSec  uint64
	ATimeNSec  uint32
	MTimeNSec  uint32
	CTimeNSec  uint32
	CrTimeNSec uint32
	Mode       uint32
	NLink      uint32
	UID        uint32
	GID        uint32
	RDev       uint32
	BlkSize    uint32
	Padding    uint32
	Flags      uint32
}

const SetAttrInSize = 128

type SetAttrIn struct {
	Valid        uint32 // mask of const SetAttrInValid* bits
	Padding      uint32
	FH           uint64
	Size         uint64
	LockOwner    uint64
	ATimeSec     uint64
	MTimeSec     uint64
	Unused2      uint64
	ATimeNSec    uint32
	MTimeNSec    uint32
	Unused3      uint32
	Mode         uint32
	Unused4      uint32
	UID          uint32
	GID          uint32
	Unused5      uint32
	BkupTimeSec  uint64
	ChgTimeSec   uint64
	CrTimeSec    uint64
	BkupTimeNSec uint32
	ChgTimeNSec  uint32
	CrTimeNSec   uint32
	Flags        uint32
}

const SetXAttrInFixedPortionSize = 16 // + len(Name) + 1 + len(Data)

const GetXAttrInFixedPortionSize = 16 // + len(Name)

const ListXAttrInSize = 16
