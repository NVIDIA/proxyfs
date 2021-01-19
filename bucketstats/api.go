// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package bucketstats implements easy to use statistics collection and
// reporting, including bucketized statistics.  Statistics start at zero and
// grow as they are added to.
//
// The statistics provided include totaler (with the Totaler interface), average
// (with the Averager interface), and distributions (with the Bucketer
// interface).
//
// Each statistic must have a unique name, "Name".  One or more statistics is
// placed in a structure and registered, with a name, via a call to Register()
// before being used.  The set of the statistics registered can be queried using
// the registered name or individually.
//
package bucketstats

import (
	"math/bits"
	"sync/atomic"
)

type StatStringFormat int

const (
	StatFormatParsable1 StatStringFormat = iota
)

// A Totaler can be incremented, or added to, and tracks the total value of all
// values added.
//
// Adding a negative value is not supported.
//
type Totaler interface {
	Increment()
	Add(value uint64)
	TotalGet() (total uint64)
	Sprint(stringFmt StatStringFormat, pkgName string, statsGroupName string) (values string)
}

// An Averager is a Totaler with a average (mean) function added.
//
// This adds a CountGet() function that returns the number of values added as
// well as an AverageGet() method that returns the average.
//
type Averager interface {
	Totaler
	CountGet() (count uint64)
	AverageGet() (avg uint64)
}

// Holds information for an individual statistics bucket, consisting of:
//
// Count the number of values added to the bucket
// RangeLow the smallest value mapped to the bucket
// RangeHigh the largest value mapped to the bucket
// NominalVal the nominal value of the bucket (sqrt(2)^n or 2^n)
// MeanVal the mean value of values added to the bucket, assuming
//         a uniform distribution
//
// When performing math on these statistics be careful of overflowing a uint64.
// It may be a good idea to use the "math/big" package.
//
type BucketInfo struct {
	Count      uint64
	NominalVal uint64
	MeanVal    uint64
	RangeLow   uint64
	RangeHigh  uint64
}

// A Bucketer is a Averager which also tracks the distribution of values.
//
// The number of buckets and the range of values mapped to a bucket depends on
// the bucket type used.
//
// DistGet() returns the distribution of values across the buckets as an array
// of BucketInfo.
//
type Bucketer interface {
	Averager
	DistGet() []BucketInfo
}

// Register and initialize a set of statistics.
//
// statsStruct is a pointer to a structure which has one or more fields holding
// statistics.  It may also contain other fields that are not bucketstats types.
//
// The combination of pkgName and statsGroupName must be unique.  pkgName is
// typically the name of a pacakge and statsGroupName is the name for the group
// of stats.  One or the other, but not both, can be the empty string.
// Whitespace characters, '"' (double quote), '*' (asterik), and ':' (colon) are
// not allowed in either name.
//
func Register(pkgName string, statsGroupName string, statsStruct interface{}) {
	register(pkgName, statsGroupName, statsStruct)
}

// UnRegister a set of statistics.
//
// Once unregistered, the same or a different set of statistics can be
// registered using the same name.
//
func UnRegister(pkgName string, statsGroupName string) {
	unRegister(pkgName, statsGroupName)
}

// Print one or more groups of statistics.
//
// The value of all statistics associated with pkgName and statsGroupName are
// returned as a string, with one statistic per line, according to the specified
// format.
//
// Use "*" to select all package names with a given group name, all
// groups with a given package name, or all groups.
//
func SprintStats(stringFmt StatStringFormat, pkgName string, statsGroupName string) (values string) {
	return sprintStats(stringFmt, pkgName, statsGroupName)
}

// Total is a simple totaler. It supports the Totaler interface.
//
// Name must be unique within statistics in the structure.  If it is "" then
// Register() will assign a name based on the name of the field.
//
type Total struct {
	total uint64 // Ensure 64-bit alignment
	Name  string
}

func (this *Total) Add(value uint64) {
	atomicAddUint64(&this.total, value)
}

func (this *Total) Increment() {
	atomicAddUint64(&this.total, 1)
}

func (this *Total) TotalGet() uint64 {
	return this.total
}

// Return a string with the statistic's value in the specified format.
//
func (this *Total) Sprint(stringFmt StatStringFormat, pkgName string, statsGroupName string) string {
	return this.sprint(stringFmt, pkgName, statsGroupName)
}

// Average counts a number of items and their average size. It supports the
// Averager interface.
//
// Name must be unique within statistics in the structure.  If it is "" then
// Register() will assign a name based on the name of the field.
//
type Average struct {
	count uint64 // Ensure 64-bit alignment
	total uint64 // Ensure 64-bit alignment
	Name  string
}

// Add a value to the mean statistics.
//
func (this *Average) Add(value uint64) {
	atomicAddUint64(&this.total, value)
	atomicAddUint64(&this.count, 1)
}

// Add a value of 1 to the mean statistics.
//
func (this *Average) Increment() {
	this.Add(1)
}

func (this *Average) CountGet() uint64 {
	return atomicLoadUint64(&this.count)
}

func (this *Average) TotalGet() uint64 {
	return atomicLoadUint64(&this.total)
}

func (this *Average) AverageGet() uint64 {
	return atomicLoadUint64(&this.total) / atomicLoadUint64(&this.count)
}

// Return a string with the statistic's value in the specified format.
//
func (this *Average) Sprint(stringFmt StatStringFormat, pkgName string, statsGroupName string) string {
	return this.sprint(stringFmt, pkgName, statsGroupName)
}

// BucketLog2Round holds bucketized statistics where the stats value is placed in
// bucket N, determined by round(log2(value) + 1), where round() rounds to the
// nearest integar and value 0 goes in bucket 0 instead of negative infinity.
//
// NBucket determines the number of buckets and has a maximum value of 65 and a
// minimum value that is implementation defined (currently 9).  If it is less
// then the minimum it will be changed to the minimum.  If NBucket is not set it
// defaults to 65. It must be set before the statistic is registered and cannot
// be changed afterward.
//
// Name must be unique within statistics in the structure.  If it is "" then
// Register() will assign a name based on the name of the field.
//
// Example mappings of values to buckets:
//
//  Values  Bucket
//       0       0
//       1       1
//       2       2
//   3 - 5       3
//  6 - 11       4
// 12 - 22       5
//     etc.
//
// Note that value 2^n increments the count in bucket n + 1, but the average of
// values in bucket n is very slightly larger than 2^n.
//
type BucketLog2Round struct {
	Name        string
	NBucket     uint
	statBuckets [65]uint32
}

func (this *BucketLog2Round) Add(value uint64) {
	if value < 256 {
		idx := log2RoundIdxTable[value]
		atomic.AddUint32(&this.statBuckets[idx], 1)
		return
	}

	bits := uint(bits.Len64(value))
	baseIdx := uint(log2RoundIdxTable[value>>(bits-8)])
	idx := baseIdx + bits - 8
	if idx > this.NBucket-1 {
		idx = this.NBucket - 1
	}

	atomic.AddUint32(&this.statBuckets[idx], 1)
	return
}

// Add a value of 1 to the bucketized statistics.
//
func (this *BucketLog2Round) Increment() {
	this.Add(1)
}

func (this *BucketLog2Round) CountGet() uint64 {
	_, _, count, _, _ := bucketCalcStat(this.DistGet())
	return count
}

func (this *BucketLog2Round) TotalGet() uint64 {
	_, _, _, total, _ := bucketCalcStat(this.DistGet())
	return total
}

func (this *BucketLog2Round) AverageGet() uint64 {
	_, _, _, _, mean := bucketCalcStat(this.DistGet())
	return mean
}

// Return BucketInfo information for all the buckets.
//
func (this *BucketLog2Round) DistGet() []BucketInfo {
	return bucketDistMake(this.NBucket, this.statBuckets[:], log2RoundBucketTable[:])
}

// Return a string with the statistic's value in the specified format.
//
func (this *BucketLog2Round) Sprint(stringFmt StatStringFormat, pkgName string, statsGroupName string) string {
	return bucketSprint(stringFmt, pkgName, statsGroupName, this.Name, this.DistGet())
}

// BucketLogRoot2Round holds bucketized statistics where the stats value is
// placed bucket N determined by round(logRoot(2)(value), except that:
//
// value 0 goes in bucket 0 (instead of negative infinity)
// value 1 goes in bucket 1 (instead of 0)
// value 2 goes in bucket 2 (instead of 1)
//
// NBucket determines the number of buckets and has a maximum value of 128 and a
// minimum value that is implementation defined (currently 9).  If it is less
// then the minimum it will be changed to the minimum.  If NBucket is not set it
// defaults to 128. It must be set before the statistic is registered and cannot
// be changed afterward.
//
// Name must be unique within statistics in the structure.  If it is "" then
// Register() will assign a name based on the name of the field.
//
// Example mappings of values to buckets:
//
//  Values  Bucket
//       0       0
//       1       1
//       2       2
//       3       3
//       4       4
//   5 - 6       5
//   7 - 9       6
// 10 - 13       7
//     etc.
//
// Note that a value sqrt(2)^n increments the count in bucket 2 * n, but the
// average of values in bucket n is slightly larger than sqrt(2)^n.
//
type BucketLogRoot2Round struct {
	Name        string
	NBucket     uint
	statBuckets [128]uint32
}

func (this *BucketLogRoot2Round) Add(value uint64) {
	if value < 256 {
		idx := logRoot2RoundIdxTable[value]
		atomic.AddUint32(&this.statBuckets[idx], 1)
		return
	}

	bits := uint(bits.Len64(value))
	baseIdx := uint(logRoot2RoundIdxTable[value>>(bits-8)])
	idx := baseIdx + (bits-8)*2
	if idx > this.NBucket-1 {
		idx = this.NBucket - 1
	}

	atomic.AddUint32(&this.statBuckets[idx], 1)
	return
}

// Add a value of 1 to the bucketized statistics.
//
func (this *BucketLogRoot2Round) Increment() {
	this.Add(1)
}

func (this *BucketLogRoot2Round) CountGet() uint64 {
	_, _, count, _, _ := bucketCalcStat(this.DistGet())
	return count
}

func (this *BucketLogRoot2Round) TotalGet() uint64 {
	_, _, _, total, _ := bucketCalcStat(this.DistGet())
	return total
}

func (this *BucketLogRoot2Round) AverageGet() uint64 {
	_, _, _, _, mean := bucketCalcStat(this.DistGet())
	return mean
}

// Return BucketInfo information for all the buckets.
//
func (this *BucketLogRoot2Round) DistGet() []BucketInfo {
	return bucketDistMake(this.NBucket, this.statBuckets[:], logRoot2RoundBucketTable[:])
}

// Return a string with the statistic's value in the specified format.
//
func (this *BucketLogRoot2Round) Sprint(stringFmt StatStringFormat, pkgName string, statsGroupName string) string {
	return bucketSprint(stringFmt, pkgName, statsGroupName, this.Name, this.DistGet())
}
