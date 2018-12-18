package inode

import (
	"strconv"
	"testing"

	"github.com/swiftstack/ProxyFS/utils"
)

func writeBenchmarkHelper(b *testing.B, byteSize uint64) {
	testVolumeHandle, _ := FetchVolumeHandle("TestVolume")
	fileInodeNumber, _ := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	buffer := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testVolumeHandle.Write(fileInodeNumber, 0, buffer, nil)
	}
}

func Benchmark4KiBWrite(b *testing.B) {
	writeBenchmarkHelper(b, 4*1024)
}

func Benchmark8KiBWrite(b *testing.B) {
	writeBenchmarkHelper(b, 8*1024)
}

func Benchmark16KiBWrite(b *testing.B) {
	writeBenchmarkHelper(b, 16*1024)
}

func Benchmark32KiBWrite(b *testing.B) {
	writeBenchmarkHelper(b, 32*1024)
}

func Benchmark64KiBWrite(b *testing.B) {
	writeBenchmarkHelper(b, 64*1024)
}

func readBenchmarkHelper(b *testing.B, byteSize uint64) {
	testVolumeHandle, _ := FetchVolumeHandle("TestVolume")
	fileInodeNumber, _ := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	buffer := make([]byte, byteSize)
	testVolumeHandle.Write(fileInodeNumber, 0, buffer, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testVolumeHandle.Read(fileInodeNumber, 0, byteSize, nil)
	}
}

func BenchmarkRead4KiB(b *testing.B) {
	readBenchmarkHelper(b, 4*1024)
}

func BenchmarkRead8KiB(b *testing.B) {
	readBenchmarkHelper(b, 8*1024)
}

func BenchmarkRead16KiB(b *testing.B) {
	readBenchmarkHelper(b, 16*1024)
}

func BenchmarkRead32KiB(b *testing.B) {
	readBenchmarkHelper(b, 32*1024)
}

func BenchmarkRead64KiB(b *testing.B) {
	readBenchmarkHelper(b, 64*1024)
}

func getReadPlanBenchmarkHelper(b *testing.B, byteSize uint64) {
	testVolumeHandle, _ := FetchVolumeHandle("TestVolume")
	fileInodeNumber, _ := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	buffer := make([]byte, byteSize)
	testVolumeHandle.Write(fileInodeNumber, 0, buffer, nil)
	var zero uint64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testVolumeHandle.GetReadPlan(fileInodeNumber, &zero, &byteSize)
	}
}

func BenchmarkGetReadPlan4KiB(b *testing.B) {
	getReadPlanBenchmarkHelper(b, 4*1024)
}

func BenchmarkGetReadPlan8KiB(b *testing.B) {
	getReadPlanBenchmarkHelper(b, 8*1024)
}

func BenchmarkGetReadPlan16KiB(b *testing.B) {
	getReadPlanBenchmarkHelper(b, 16*1024)
}

func BenchmarkGetReadPlan32KiB(b *testing.B) {
	getReadPlanBenchmarkHelper(b, 32*1024)
}

func BenchmarkGetReadPlan64KiB(b *testing.B) {
	getReadPlanBenchmarkHelper(b, 64*1024)
}

func readCacheBenchmarkHelper(b *testing.B, byteSize uint64) {
	testVolumeHandle, _ := FetchVolumeHandle("TestVolume")
	fileInodeNumber, _ := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	buffer := make([]byte, byteSize)
	testVolumeHandle.Write(fileInodeNumber, 0, buffer, nil)
	testVolumeHandle.Flush(fileInodeNumber, false)
	var zero uint64
	zero = 0
	readPlan, _ := testVolumeHandle.GetReadPlan(fileInodeNumber, &zero, &byteSize)
	testVolumeHandle.Read(fileInodeNumber, 0, byteSize, nil)
	// at this point, the read cache should be populated

	// let's get the log segment number
	_, _, objectName, _ := utils.PathToAcctContObj(readPlan[0].ObjectPath)
	logSegmentNumber, _ := strconv.ParseUint(objectName, 16, 64)

	volume := testVolumeHandle.(*volumeStruct)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := []byte{}
		readCacheKey := readCacheKeyStruct{volumeName: "TestVolume", logSegmentNumber: logSegmentNumber, cacheLineTag: 0}
		volume.volumeGroup.Lock()
		readCacheElement, _ := volume.volumeGroup.readCache[readCacheKey]
		cacheLine := readCacheElement.cacheLine
		buf = append(buf, cacheLine[:byteSize]...)
		volume.volumeGroup.Unlock()
	}
}

func BenchmarkReadCache4KiB(b *testing.B) {
	readCacheBenchmarkHelper(b, 4*1024)
}

func BenchmarkReadCache8KiB(b *testing.B) {
	readCacheBenchmarkHelper(b, 8*1024)
}

func BenchmarkReadCache16KiB(b *testing.B) {
	readCacheBenchmarkHelper(b, 16*1024)
}

func BenchmarkReadCache32KiB(b *testing.B) {
	readCacheBenchmarkHelper(b, 32*1024)
}

func BenchmarkReadCache64KiB(b *testing.B) {
	readCacheBenchmarkHelper(b, 64*1024)
}
