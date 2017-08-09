// Package headhunter manages the headhunter databases, which keep track of which log segments correspond to the current revision of a given inode.
package headhunter

import (
	"fmt"
	"sync"
)

// VolumeHandle is used to operate on a given volume's database
type VolumeHandle interface {
	FetchNextCheckPointDoneWaitGroup() (wg *sync.WaitGroup)
	FetchNonce() (nonce uint64, err error)
	GetInodeRec(inodeNumber uint64) (value []byte, err error)
	PutInodeRec(inodeNumber uint64, value []byte) (err error)
	PutInodeRecs(inodeNumbers []uint64, values [][]byte) (err error)
	DeleteInodeRec(inodeNumber uint64) (err error)
	GetLogSegmentRec(logSegmentNumber uint64) (value []byte, err error)
	PutLogSegmentRec(logSegmentNumber uint64, value []byte) (err error)
	DeleteLogSegmentRec(logSegmentNumber uint64) (err error)
	GetBPlusTreeObject(objectNumber uint64) (value []byte, err error)
	PutBPlusTreeObject(objectNumber uint64, value []byte) (err error)
	DeleteBPlusTreeObject(objectNumber uint64) (err error)
	DoCheckpoint() (err error)
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
