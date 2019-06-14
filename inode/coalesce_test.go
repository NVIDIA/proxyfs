package inode

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/blunder"
)

// NB: test setup and such is in api_test.go (look for TestMain function)

func TestCoalesce(t *testing.T) {
	testSetup(t, false)

	// We're going to take some files:
	//
	// d1/file1a   (contents "abcd")
	// d1/file1b   (contents "efgh")
	// d2/file2a   (contents "ijkl")
	// d2/file2b   (contents "mnop")
	// d2/file2c   (contents "\0\0st\0\0")
	//
	// and coalesce them into a single file:
	//
	// d1/combined (contents "abcdefghijklmnop\0\0st\0\0")
	//
	// This will also unlink the constituent files from their directories.

	assert := assert.New(t)
	vh, err := FetchVolumeHandle("TestVolume")
	if !assert.Nil(err) {
		return
	}

	d1InodeNumber, err := vh.CreateDir(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(RootDirInodeNumber, "d1", d1InodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	d2InodeNumber, err := vh.CreateDir(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(RootDirInodeNumber, "d2", d2InodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1aInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file1aInodeNumber, 0, []byte("abcd"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "file1a", file1aInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1bInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file1bInodeNumber, 0, []byte("efgh"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "file1b", file1bInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file2aInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file2aInodeNumber, 0, []byte("ijkl"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d2InodeNumber, "file2a", file2aInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file2bInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file2bInodeNumber, 0, []byte("mnop"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d2InodeNumber, "file2b", file2bInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	// Note that this one is sparse: the first 2 bytes are 0, then we have "st", then 2 more 0s
	file2cInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file2cInodeNumber, 2, []byte("st"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d2InodeNumber, "file2c", file2cInodeNumber, false)
	if !assert.Nil(err) {
		return
	}
	err = vh.SetSize(file2cInodeNumber, 6)
	if !assert.Nil(err) {
		return
	}

	// Now create destination file
	combinedInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "combined", combinedInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	// test setup's done; now we can coalesce things
	elements := make([]*CoalesceElement, 0, 5)
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1aInodeNumber,
		ElementName:                    "file1a"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1bInodeNumber,
		ElementName:                    "file1b"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d2InodeNumber,
		ElementInodeNumber:             file2aInodeNumber,
		ElementName:                    "file2a"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d2InodeNumber,
		ElementInodeNumber:             file2bInodeNumber,
		ElementName:                    "file2b"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d2InodeNumber,
		ElementInodeNumber:             file2cInodeNumber,
		ElementName:                    "file2c"})

	newMetaData := []byte("The quick brown fox jumped over the lazy dog.")

	// Coalesce the above 5 files and metadata into d1/combined
	startTime := time.Now()
	attrChangeTime, modificationTime, numWrites, fileSize, err := vh.Coalesce(
		combinedInodeNumber, "MetaDataStream", newMetaData, elements)
	if !assert.Nil(err) {
		return
	}
	assert.Equal(uint64(22), fileSize)
	assert.Equal(uint64(5), numWrites)
	assert.Equal(attrChangeTime, modificationTime)
	assert.True(attrChangeTime.After(startTime))

	// The new file has the contents of the old files combined
	contents, err := vh.Read(combinedInodeNumber, 0, 22, nil)
	if !assert.Nil(err) {
		return
	}
	assert.Equal([]byte("abcdefghijklmnop\x00\x00st\x00\x00"), contents)

	// The old files have ceased to be
	_, err = vh.Lookup(d1InodeNumber, "file1a")
	assert.True(blunder.Is(err, blunder.NotFoundError))
	_, err = vh.Lookup(d1InodeNumber, "file1b")
	assert.True(blunder.Is(err, blunder.NotFoundError))
	_, err = vh.Lookup(d2InodeNumber, "file2a")
	assert.True(blunder.Is(err, blunder.NotFoundError))
	_, err = vh.Lookup(d2InodeNumber, "file2b")
	assert.True(blunder.Is(err, blunder.NotFoundError))
	_, err = vh.Lookup(d2InodeNumber, "file2c")
	assert.True(blunder.Is(err, blunder.NotFoundError))

	// The new file is linked in at the right spot
	foundInodeNumber, err := vh.Lookup(d1InodeNumber, "combined")
	if !assert.Nil(err) {
		return
	}
	assert.Equal(combinedInodeNumber, foundInodeNumber)

	// Verify the new file has the new metadata
	buf, err := vh.GetStream(combinedInodeNumber, "MetaDataStream")
	if assert.Nil(err) {
		assert.Equal(buf, newMetaData)
	}

	testTeardown(t)
}

func TestCoalesceDir(t *testing.T) {
	testSetup(t, false)

	// We're going to take some files:
	//
	// d1/file1a   (contents "abcd")
	// d1/file1b   (contents "efgh")
	//
	// and attempt to coalesce them into a directory:
	//
	// d1

	assert := assert.New(t)
	vh, err := FetchVolumeHandle("TestVolume")
	if !assert.Nil(err) {
		return
	}

	d1InodeNumber, err := vh.CreateDir(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(RootDirInodeNumber, "d1", d1InodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1aInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file1aInodeNumber, 0, []byte("abcd"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "file1a", file1aInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1bInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file1bInodeNumber, 0, []byte("efgh"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "file1b", file1bInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	// test setup's done; now we can coalesce things
	elements := make([]*CoalesceElement, 0, 2)
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1aInodeNumber,
		ElementName:                    "file1a"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1bInodeNumber,
		ElementName:                    "file1b"})

	// Coalesce the above 2 files into d1
	_, _, _, _, err = vh.Coalesce(d1InodeNumber, "MetaDataStream", nil, elements)
	assert.NotNil(err)
	assert.True(blunder.Is(err, blunder.PermDeniedError))

	testTeardown(t)
}

func TestCoalesceMultipleLinks(t *testing.T) {
	testSetup(t, false)

	// We're going to take hard-linked files:
	//
	// d1/file1a   (contents "abcd")
	// d1/file1b   (hard-linked to d1/file1a)
	//
	// and attempt to coalesce them into a single file:
	//
	// d1/combined

	assert := assert.New(t)
	vh, err := FetchVolumeHandle("TestVolume")
	if !assert.Nil(err) {
		return
	}

	d1InodeNumber, err := vh.CreateDir(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(RootDirInodeNumber, "d1", d1InodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1aInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file1aInodeNumber, 0, []byte("abcd"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "file1a", file1aInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1bInodeNumber := file1aInodeNumber
	err = vh.Link(d1InodeNumber, "file1b", file1bInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	// Now create destination file
	combinedInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "combined", combinedInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	// test setup's done; now we can coalesce things
	elements := make([]*CoalesceElement, 0, 2)
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1aInodeNumber,
		ElementName:                    "file1a"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1bInodeNumber,
		ElementName:                    "file1b"})

	// Coalesce the above 2 files into d1/combined
	_, _, _, _, err = vh.Coalesce(combinedInodeNumber, "MetaDataStream", nil, elements)
	assert.NotNil(err)
	assert.True(blunder.Is(err, blunder.TooManyLinksError))

	testTeardown(t)
}

func TestCoalesceDuplicates(t *testing.T) {
	testSetup(t, false)

	// We're going to take hard-linked files:
	//
	// d1/file1a   (contents "abcd")
	// d1/file1b   (contents "efgh")
	// d1/file1a   (again)
	//
	// and attempt to coalesce them into a single file:
	//
	// d1/combined

	assert := assert.New(t)
	vh, err := FetchVolumeHandle("TestVolume")
	if !assert.Nil(err) {
		return
	}

	d1InodeNumber, err := vh.CreateDir(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(RootDirInodeNumber, "d1", d1InodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1aInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file1aInodeNumber, 0, []byte("abcd"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "file1a", file1aInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	file1bInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Write(file1bInodeNumber, 0, []byte("efgh"), nil)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "file1b", file1bInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	// Now create destination file
	combinedInodeNumber, err := vh.CreateFile(PosixModePerm, 0, 0)
	if !assert.Nil(err) {
		return
	}
	err = vh.Link(d1InodeNumber, "combined", combinedInodeNumber, false)
	if !assert.Nil(err) {
		return
	}

	// test setup's done; now we can coalesce things
	elements := make([]*CoalesceElement, 0, 3)
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1aInodeNumber,
		ElementName:                    "file1a"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1bInodeNumber,
		ElementName:                    "file1b"})
	elements = append(elements, &CoalesceElement{
		ContainingDirectoryInodeNumber: d1InodeNumber,
		ElementInodeNumber:             file1aInodeNumber,
		ElementName:                    "file1a"})

	// Coalesce the above 3 files into d1/combined
	_, _, _, _, err = vh.Coalesce(combinedInodeNumber, "MetaDataStream", nil, elements)
	assert.NotNil(err)
	assert.True(blunder.Is(err, blunder.InvalidArgError))

	testTeardown(t)
}
