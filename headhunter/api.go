// Package headhunter manages the headhunter databases, which keep track of which log segments correspond to the current revision of a given inode.
package headhunter

import (
	"fmt"
	"time"

	"github.com/swiftstack/sortedmap"
)

type BPlusTreeType uint32

const (
	MergedBPlusTree BPlusTreeType = iota // Used only for FetchLayoutReport when a merged result is desired
	InodeRecBPlusTree
	LogSegmentRecBPlusTree
	BPlusTreeObjectBPlusTree
)

type SnapShotStruct struct {
	ID        uint64
	TimeStamp time.Time
	Name      string
}

// VolumeHandle is used to operate on a given volume's database
type VolumeHandle interface {
	FetchAccountAndCheckpointContainerNames() (accountName string, checkpointContainerName string)
	FetchNonce() (nonce uint64, err error)
	GetInodeRec(inodeNumber uint64) (value []byte, ok bool, err error)
	PutInodeRec(inodeNumber uint64, value []byte) (err error)
	PutInodeRecs(inodeNumbers []uint64, values [][]byte) (err error)
	DeleteInodeRec(inodeNumber uint64) (err error)
	IndexedInodeNumber(index uint64) (inodeNumber uint64, ok bool, err error)
	GetLogSegmentRec(logSegmentNumber uint64) (value []byte, err error)
	PutLogSegmentRec(logSegmentNumber uint64, value []byte) (err error)
	DeleteLogSegmentRec(logSegmentNumber uint64) (err error)
	IndexedLogSegmentNumber(index uint64) (logSegmentNumber uint64, ok bool, err error)
	GetBPlusTreeObject(objectNumber uint64) (value []byte, err error)
	PutBPlusTreeObject(objectNumber uint64, value []byte) (err error)
	DeleteBPlusTreeObject(objectNumber uint64) (err error)
	IndexedBPlusTreeObjectNumber(index uint64) (objectNumber uint64, ok bool, err error)
	DoCheckpoint() (err error)
	FetchLayoutReport(treeType BPlusTreeType) (layoutReport sortedmap.LayoutReport, err error)
	CreateSnapShot(name string) (id uint64, err error)
	DeleteSnapShot(id uint64) (err error)
	FetchSnapShotList() (list []SnapShotStruct)
}

// FetchVolumeHandle is used to fetch a VolumeHandle to use when operating on a given volume's database
func FetchVolumeHandle(volumeName string) (volumeHandle VolumeHandle, err error) {
	volume, ok := globals.volumeMap[volumeName]
	if !ok {
		err = fmt.Errorf("FetchVolumeHandle(\"%v\") unable to find volume", volumeName)
		return
	}

	volumeHandle = volume
	err = nil

	return
}
