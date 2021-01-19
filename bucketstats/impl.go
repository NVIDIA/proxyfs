// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// The bucketstats Package implements convenient, easy to use, bucketized
// statistics.

package bucketstats

import (
	"fmt"
	"math/big"
	"math/bits"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

var (
	pkgNameToGroupName map[string]map[string]interface{}
	statsNameMapLock   sync.Mutex
)

// Register a set of statistics, where the statistics are one or more fields in
// the passed structure.
//
func register(pkgName string, statsGroupName string, statsStruct interface{}) {

	var ok bool

	if pkgName == "" && statsGroupName == "" {
		panic(fmt.Sprintf("statistics group must have non-empty pkgName or statsGroupName"))
	}

	// let us reflect upon any statistics fields in statsStruct ...
	//
	// but first verify this is a pointer to a struct
	if reflect.TypeOf(statsStruct).Kind() != reflect.Ptr ||
		reflect.ValueOf(statsStruct).Elem().Type().Kind() != reflect.Struct {
		panic(fmt.Sprintf("statsStruct for statistics group '%s' is (%s), should be (*struct)",
			statsGroupName, reflect.TypeOf(statsStruct)))
	}

	structAsValue := reflect.ValueOf(statsStruct).Elem()
	structAsType := structAsValue.Type()

	// find all the statistics fields and init them;
	// assign them a name if they don't have one;
	// verify each name is only used once
	names := make(map[string]struct{})

	for i := 0; i < structAsType.NumField(); i++ {
		fieldName := structAsType.Field(i).Name
		fieldAsType := structAsType.Field(i).Type
		fieldAsValue := structAsValue.Field(i)

		// ignore fields that are not a BucketStats type
		var (
			countStat          Total
			averageStat        Average
			bucketLog2Stat     BucketLog2Round
			bucketLogRoot2Stat BucketLogRoot2Round
		)
		if fieldAsType != reflect.TypeOf(countStat) &&
			fieldAsType != reflect.TypeOf(averageStat) &&
			fieldAsType != reflect.TypeOf(bucketLog2Stat) &&
			fieldAsType != reflect.TypeOf(bucketLogRoot2Stat) {
			continue
		}

		// verify BucketStats fields are setable (exported)
		if !fieldAsValue.CanSet() {
			panic(fmt.Sprintf("statistics group '%s' field %s must be exported to be usable by bucketstats",
				statsGroupName, fieldName))
		}

		// get the statistic name and insure its initialized;
		// then verify its unique
		statNameValue := fieldAsValue.FieldByName("Name")
		if !statNameValue.IsValid() {
			panic(fmt.Sprintf("statistics Group '%s' field %s does not does not contain a 'Name' field",
				statsGroupName, fieldName))
		}
		if statNameValue.String() == "" {
			statNameValue.SetString(fieldName)
		} else {
			statNameValue.SetString(statNameValue.String())
		}
		_, ok = names[statNameValue.String()]
		if ok {
			panic(fmt.Sprintf("stats '%s' field %s Name '%s' is already in use",
				statsGroupName, fieldName, statNameValue))
		}
		names[statNameValue.String()] = struct{}{}

		// initialize the statistic (all fields are already zero - unless relaunched in test sequence)
		switch v := (fieldAsValue.Addr().Interface()).(type) {
		case *Total:
		case *Average:
		case *BucketLog2Round:
			if v.NBucket == 0 || v.NBucket > uint(len(v.statBuckets)) {
				v.NBucket = uint(len(v.statBuckets))
			} else if v.NBucket < 10 {
				v.NBucket = 10
			}
		case *BucketLogRoot2Round:
			if v.NBucket == 0 || v.NBucket > uint(len(v.statBuckets)) {
				v.NBucket = uint(len(v.statBuckets))
			} else if v.NBucket < 17 {
				v.NBucket = 17
			}
		default:
			panic(fmt.Sprintf("statistics Group '%s' field %s type '%v' unknown: internal error",
				statsGroupName, fieldName, fieldAsType))
		}

	}

	// add statsGroupName to the list of statistics
	statsNameMapLock.Lock()
	defer statsNameMapLock.Unlock()

	if pkgNameToGroupName == nil {
		pkgNameToGroupName = make(map[string]map[string]interface{})
	}
	if pkgNameToGroupName[pkgName] == nil {
		pkgNameToGroupName[pkgName] = make(map[string]interface{})
	}

	// check for pre-existence
	if pkgNameToGroupName[pkgName][statsGroupName] != nil {
		panic(fmt.Sprintf("pkgName '%s' with statsGroupName '%s' is already registered",
			pkgName, statsGroupName))
	}

	pkgNameToGroupName[pkgName][statsGroupName] = statsStruct

	return
}

func unRegister(pkgName string, statsGroupName string) {

	statsNameMapLock.Lock()
	defer statsNameMapLock.Unlock()

	// remove statsGroupName from the list of statistics (silently ignore it
	// if it doesn't exist)
	if pkgNameToGroupName[pkgName] != nil {
		delete(pkgNameToGroupName[pkgName], statsGroupName)

		if len(pkgNameToGroupName[pkgName]) == 0 {
			delete(pkgNameToGroupName, pkgName)
		}
	}

	return
}

// Return the selected group(s) of statistics as a string.
//
func sprintStats(statFmt StatStringFormat, pkgName string, statsGroupName string) (statValues string) {

	statsNameMapLock.Lock()
	defer statsNameMapLock.Unlock()

	var (
		pkgNameMap   map[string]map[string]interface{}
		groupNameMap map[string]interface{}
	)
	if pkgName == "*" {
		pkgNameMap = pkgNameToGroupName
	} else {
		// make a map with a single entry for the pkgName
		pkgNameMap = map[string]map[string]interface{}{pkgName: nil}
	}

	for pkg := range pkgNameMap {
		if statsGroupName == "*" {
			groupNameMap = pkgNameToGroupName[pkg]
		} else {
			// make a map with a single entry for the statsGroupName
			groupNameMap = map[string]interface{}{statsGroupName: nil}
		}

		for group := range groupNameMap {
			_, ok := pkgNameToGroupName[pkg][group]
			if !ok {
				panic(fmt.Sprintf(
					"bucketstats.sprintStats(): statistics group '%s.%s' is not registered",
					pkg, group))
			}
			statValues += sprintStatsStruct(statFmt, pkg, group, pkgNameToGroupName[pkg][group])
		}
	}
	return
}

func sprintStatsStruct(statFmt StatStringFormat, pkgName string, statsGroupName string,
	statsStruct interface{}) (statValues string) {

	// let us reflect upon any statistic fields in statsStruct ...
	//
	// but first verify this is a pointer to a struct
	if reflect.TypeOf(statsStruct).Kind() != reflect.Ptr ||
		reflect.ValueOf(statsStruct).Elem().Type().Kind() != reflect.Struct {
		panic(fmt.Sprintf("statsStruct for statistics group '%s' is (%s), should be (*struct)",
			statsGroupName, reflect.TypeOf(statsStruct)))
	}

	structAsValue := reflect.ValueOf(statsStruct).Elem()
	structAsType := structAsValue.Type()

	// find all the statistics fields and sprint them
	for i := 0; i < structAsType.NumField(); i++ {
		fieldAsType := structAsType.Field(i).Type
		fieldAsValue := structAsValue.Field(i)

		// ignore fields that are not a BucketStats type
		var (
			countStat          Total
			averageStat        Average
			bucketLog2Stat     BucketLog2Round
			bucketLogRoot2Stat BucketLogRoot2Round
		)
		if fieldAsType != reflect.TypeOf(countStat) &&
			fieldAsType != reflect.TypeOf(averageStat) &&
			fieldAsType != reflect.TypeOf(bucketLog2Stat) &&
			fieldAsType != reflect.TypeOf(bucketLogRoot2Stat) {
			continue
		}

		switch v := (fieldAsValue.Addr().Interface()).(type) {
		case *Total:
			statValues += v.Sprint(statFmt, pkgName, statsGroupName)
		case *Average:
			statValues += v.Sprint(statFmt, pkgName, statsGroupName)
		case *BucketLog2Round:
			statValues += v.Sprint(statFmt, pkgName, statsGroupName)
		case *BucketLogRoot2Round:
			statValues += v.Sprint(statFmt, pkgName, statsGroupName)
		default:
			panic(fmt.Sprintf("Unknown type in struct: %s", fieldAsType.Name()))
		}
	}
	return
}

// Construct and return a statistics name (fully qualified field name) in the specified format.
//
func statisticName(statFmt StatStringFormat, pkgName string, statsGroupName string, fieldName string) string {

	switch statFmt {

	default:
		panic(fmt.Sprintf("Unknown StatStringFormat '%v'", statFmt))

	case StatFormatParsable1:
		pkgName = scrubName(pkgName)
		statsGroupName = scrubName(statsGroupName)
		fieldName = scrubName(fieldName)

		if pkgName == "" && statsGroupName == "" {
			return fieldName
		}
		if pkgName == "" {
			return statsGroupName + "." + fieldName
		}
		if statsGroupName == "" {
			return pkgName + "." + fieldName
		}
		return pkgName + "." + statsGroupName + "." + fieldName
	}
}

// Return the "name" of the bucket that would hold 'n' as the string "2^x".
//
func bucketNameLog2(value uint64) string {

	var idx uint
	if value < 256 {
		idx = uint(log2RoundIdxTable[value])
	} else {
		bits := uint(bits.Len64(value))
		baseIdx := uint(log2RoundIdxTable[value>>(bits-8)])
		idx = baseIdx + bits - 8
	}
	return fmt.Sprintf("2^%d", idx-1)
}

// Return the "name" of the bucket that would hold 'n' as the string "2^x",
// where x can have the suffix ".5" as in "2^7.5".
//
func bucketNameLogRoot2(value uint64) string {

	var idx uint
	if value < 256 {
		idx = uint(log2RoundIdxTable[value])
	} else {
		bits := uint(bits.Len64(value))
		baseIdx := uint(logRoot2RoundIdxTable[value>>(bits-8)])
		idx = baseIdx + (bits-8)*2
	}
	if idx%2 == 1 {
		return fmt.Sprintf("2^%1.0f", float32(idx-1)/2)
	}
	return fmt.Sprintf("2^%1.1f", float32(idx-1)/2)
}

// Return a string with the statistic's value in the specified format.
//
func (this *Total) sprint(statFmt StatStringFormat, pkgName string, statsGroupName string) string {

	statName := statisticName(statFmt, pkgName, statsGroupName, this.Name)

	switch statFmt {
	case StatFormatParsable1:
		return fmt.Sprintf("%s total:%d\n", statName, this.total)
	}

	return fmt.Sprintf("statName '%s': Unknown StatStringFormat: '%v'\n", statName, statFmt)
}

// Return a string with the statistic's value in the specified format.
//
func (this *Average) sprint(statFmt StatStringFormat, pkgName string, statsGroupName string) string {

	statName := statisticName(statFmt, pkgName, statsGroupName, this.Name)
	var avg uint64
	if this.count > 0 {
		avg = this.total / this.count
	}

	switch statFmt {
	case StatFormatParsable1:
		return fmt.Sprintf("%s avg:%d count:%d total:%d\n",
			statName, avg, this.count, this.total)
	}

	return fmt.Sprintf("statName '%s': Unknown StatStringFormat: '%v'\n", statName, statFmt)
}

// The canonical distribution for a bucketized statistic is an array of BucketInfo.
// Create one based on the information for this bucketstat .
//
func bucketDistMake(nBucket uint, statBuckets []uint32, bucketInfoBase []BucketInfo) []BucketInfo {

	// copy the base []BucketInfo before modifying it
	bucketInfo := make([]BucketInfo, nBucket, nBucket)
	copy(bucketInfo, bucketInfoBase[0:nBucket])
	for i := uint(0); i < nBucket; i += 1 {
		bucketInfo[i].Count = uint64(statBuckets[i])
	}

	// if nBucket is less then len(bucketInfo) then update the range and
	// average for the last bucket that's used
	if nBucket < uint(len(bucketInfoBase)) {
		bucketInfo[nBucket-1].RangeHigh = bucketInfo[len(bucketInfo)-1].RangeHigh

		mean := bucketInfo[nBucket-1].RangeLow / 2
		mean += bucketInfo[nBucket-1].RangeHigh / 2
		bothOdd := bucketInfo[nBucket-1].RangeLow & bucketInfo[nBucket-1].RangeHigh & 0x1
		if bothOdd == 1 {
			mean += 1
		}
		bucketInfo[nBucket-1].MeanVal = mean
	}
	return bucketInfo
}

// Given the distribution ([]BucketInfo) for a bucketized statistic, calculate:
//
// o the index of the first entry with a non-zero count
// o the index + 1 of the last entry with a non-zero count, or zero if no such
//   bucket exists
// o the count (number things in buckets)
// o sum of counts * count_meanVal, and
// o mean (average)
//
func bucketCalcStat(bucketInfo []BucketInfo) (firstIdx int, maxIdx int, count uint64, sum uint64, mean uint64) {

	var (
		bigSum     big.Int
		bigMean    big.Int
		bigTmp     big.Int
		bigProduct big.Int
	)

	// firstIdx is the index of the first bucket with a non-zero count
	// maxIdx is the index + 1 of the last bucket with a non-zero count, or zero
	// bigSum is the running total of count * bucket_meanval
	firstIdx = 0
	maxIdx = 0
	for i := 0; i < len(bucketInfo); i += 1 {
		if bucketInfo[i].Count > 0 {
			firstIdx = i
			break
		}
	}
	for i := firstIdx; i < len(bucketInfo); i += 1 {
		count += bucketInfo[i].Count

		bigTmp.SetUint64(bucketInfo[i].Count)
		bigProduct.SetUint64(bucketInfo[i].MeanVal)
		bigProduct.Mul(&bigProduct, &bigTmp)
		bigSum.Add(&bigSum, &bigProduct)

		if bucketInfo[i].Count > 0 {
			maxIdx = i + 1
		}
	}
	if count > 0 {
		bigTmp.SetUint64(count)
		bigMean.Div(&bigSum, &bigTmp)
	}

	// sum will be set to math.MaxUint64 if bigSum overflows
	mean = bigMean.Uint64()
	sum = bigSum.Uint64()

	return
}

// Return a string with the bucketized statistic content in the specified format.
//
func bucketSprint(statFmt StatStringFormat, pkgName string, statsGroupName string, fieldName string,
	bucketInfo []BucketInfo) string {

	var (
		idx        int
		statName   string
		bucketName string
	)
	firstIdx, maxIdx, count, sum, mean := bucketCalcStat(bucketInfo)
	statName = statisticName(statFmt, pkgName, statsGroupName, fieldName)

	switch statFmt {

	case StatFormatParsable1:
		line := fmt.Sprintf("%s avg:%d count:%d total:%d", statName, mean, count, sum)

		// bucket names are printed as a number upto 3 digits long and
		// as a power of 2 after that
		for idx = firstIdx; idx < maxIdx && bucketInfo[idx].NominalVal < 1024; idx += 1 {
			line += fmt.Sprintf(" %d:%d", bucketInfo[idx].NominalVal, bucketInfo[idx].Count)
		}
		for ; idx < maxIdx; idx += 1 {
			// bucketInfo[3] must exist and its value depends on the base
			if bucketInfo[3].NominalVal == 3 {
				bucketName = bucketNameLogRoot2(bucketInfo[idx].NominalVal)
			} else {
				bucketName = bucketNameLog2(bucketInfo[idx].NominalVal)
			}
			line += fmt.Sprintf(" %s:%d", bucketName, bucketInfo[idx].Count)
		}
		return line + "\n"
	}

	return fmt.Sprintf("StatisticName '%s': Unknown StatStringFormat: '%v'\n", statName, statFmt)
}

// Replace illegal characters in names with underbar (`_`)
//
func scrubName(name string) string {

	// Names should include only pritable characters that are not
	// whitespace.  Also disallow splat ('*') (used for wildcard for
	// statistic group names), sharp ('#') (used for comments in output) and
	// colon (':') (used as a delimiter in "key:value" output).
	replaceChar := func(r rune) rune {
		switch {
		case unicode.IsSpace(r):
			return '_'
		case !unicode.IsPrint(r):
			return '_'
		case r == '*':
			return '_'
		case r == ':':
			return '_'
		case r == '#':
			return '_'
		}
		return r
	}

	return strings.Map(replaceChar, name)
}
