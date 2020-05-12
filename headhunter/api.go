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
	CreatedObjectsBPlusTree
	DeletedObjectsBPlusTree
)

type SnapShotIDType uint8

const (
	SnapShotIDTypeLive SnapShotIDType = iota
	SnapShotIDTypeSnapShot
	SnapShotIDTypeDotSnapShot
)

type SnapShotStruct struct {
	ID   uint64
	Time time.Time
	Name string
}

type VolumeEventListener interface {
	CheckpointCompleted()
}

// VolumeHandle is used to operate on a given volume's database
type VolumeHandle interface {
	RegisterForEvents(listener VolumeEventListener)
	UnregisterForEvents(listener VolumeEventListener)
	FetchAccountAndCheckpointContainerNames() (accountName string, checkpointContainerName string)
	FetchNonce() (nonce uint64)
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
	FetchLayoutReport(treeType BPlusTreeType, validate bool) (layoutReport sortedmap.LayoutReport, discrepencies uint64, err error)
	DefragmentMetadata(treeType BPlusTreeType, thisStartPercentage uint8, thisStopPercentage uint8) (nextStartPercentage uint8, err error)
	SnapShotCreateByInodeLayer(name string) (id uint64, err error)
	SnapShotDeleteByInodeLayer(id uint64) (err error)
	SnapShotCount() (snapShotCount uint64)
	SnapShotLookupByName(name string) (snapShot SnapShotStruct, ok bool)
	SnapShotListByID(reversed bool) (list []SnapShotStruct)
	SnapShotListByTime(reversed bool) (list []SnapShotStruct)
	SnapShotListByName(reversed bool) (list []SnapShotStruct)
	SnapShotU64Decode(snapShotU64 uint64) (snapShotIDType SnapShotIDType, snapShotID uint64, nonce uint64)
	SnapShotIDAndNonceEncode(snapShotID uint64, nonce uint64) (snapShotU64 uint64)
	SnapShotTypeDotSnapShotAndNonceEncode(nonce uint64) (snapShotU64 uint64)
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

// DisableObjectDeletions prevents objects from being deleted until EnableObjectDeletions() is called
func DisableObjectDeletions() {
	globals.backgroundObjectDeleteRWMutex.Lock()

	if !globals.backgroundObjectDeleteEnabled {
		// Already disabled... just exit
		globals.backgroundObjectDeleteRWMutex.Unlock()
		return
	}

	globals.backgroundObjectDeleteEnabled = false

	globals.backgroundObjectDeleteEnabledWG.Add(1)

	globals.backgroundObjectDeleteRWMutex.Unlock()

	globals.backgroundObjectDeleteActiveWG.Wait()
}

// EnableObjectDeletions resumes background object deletion blocked by a prior call to DisableObjectDeletions()
func EnableObjectDeletions() {
	globals.backgroundObjectDeleteRWMutex.Lock()

	if globals.backgroundObjectDeleteEnabled {
		// Already enabled... just exit
		globals.backgroundObjectDeleteRWMutex.Unlock()
		return
	}

	globals.backgroundObjectDeleteEnabled = true

	globals.backgroundObjectDeleteEnabledWG.Done()

	globals.backgroundObjectDeleteRWMutex.Unlock()
}
