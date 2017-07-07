package sortedmap

// Meta Test library for packages implementing SortedMap APIs

// Random (non-repeating) sequence statically  generated via https://www.random.org/sequences/
// Random (non-repeating) sequence dynamically generated via Knuth Shuffle

import (
	cryptoRand "crypto/rand"
	"fmt"
	"math/big"
	mathRand "math/rand"
	"strconv"
	"testing"
)

const (
	testHugeNumKeys = 5000

	pseudoRandom     = false
	pseudoRandomSeed = int64(0)
)

var (
	randSource *mathRand.Rand // A source for pseudo-random numbers (if selected)
)

func metaTestAllButDeleteSimple(t *testing.T, tree SortedMap) {
	var (
		err           error
		found         bool
		index         int
		keyAsInt      int
		keyAsKey      Key
		numberOfItems int
		ok            bool
		valueAsString string
		valueAsValue  Value
	)

	index, found, err = tree.BisectLeft(0)
	if nil != err {
		t.Fatal(err)
	}
	if -1 != index {
		t.Fatalf("BisectLeft(0).index of just initialized LLRB should have been -1... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectLeft(0).found of just initialized LLRB should have been false")
	}

	index, found, err = tree.BisectRight(0)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectRight(0).index of just initialized LLRB should have been 0... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectRight(0).found of just initialized LLRB should have been false")
	}

	_, _, ok, err = tree.GetByIndex(-1)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByIndex(-1).ok should have been false")
	}

	_, _, ok, err = tree.GetByIndex(0)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByIndex(0).ok [Case 1] of just initialized LLRB should have been false")
	}

	_, ok, err = tree.GetByKey(0)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByKey(0).ok of just initialized LLRB should have been false")
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 0 != numberOfItems {
		t.Fatalf("Len() [Case 1] of just initialized LLRB should have been 0... instead it was %v", numberOfItems)
	}

	ok, err = tree.Put(5, "5")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(5, \"5\").ok should have been true")
	}

	index, found, err = tree.BisectLeft(3)
	if nil != err {
		t.Fatal(err)
	}
	if -1 != index {
		t.Fatalf("BisectLeft(3).index [Case 1] should have been -1... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectLeft(3).found [Case 1] should have been false")
	}

	index, found, err = tree.BisectLeft(5)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectLeft(5).index [Case 1] should have been 0... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectLeft(5).found [Case 1] should have been true")
	}

	index, found, err = tree.BisectLeft(7)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectLeft(7).index [Case 1] should have been 0... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectLeft(7).found [Case 1] should have been false")
	}

	index, found, err = tree.BisectRight(3)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectRight(3).index [Case 1] should have been 0... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectRight(3).found [Case 1] should have been false")
	}

	index, found, err = tree.BisectRight(5)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectRight(5).index [Case 1] should have been 0... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectRight(5).found [Case 1] should have been true")
	}

	index, found, err = tree.BisectRight(7)
	if nil != err {
		t.Fatal(err)
	}
	if 1 != index {
		t.Fatalf("BisectRight(7).index [Case 1] should have been 1... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectRight(7).found [Case 1] should have been false")
	}

	keyAsKey, valueAsValue, ok, err = tree.GetByIndex(0)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByIndex(0).ok [Case 2] should have been true")
	}
	keyAsInt = keyAsKey.(int)
	if 5 != keyAsInt {
		t.Fatalf("GetByIndex(0).key [Case 2] should have been 5... instead it was %v", keyAsInt)
	}
	valueAsString = valueAsValue.(string)
	if "5" != valueAsString {
		t.Fatalf("GetByIndex(0).value [Case 2] should have been \"5\"... instead it was \"%v\"", valueAsString)
	}

	_, _, ok, err = tree.GetByIndex(1)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByIndex(1).ok [Case 1] should have been false")
	}

	_, ok, err = tree.GetByKey(3)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByKey(3).ok [Case 1] should have been false")
	}

	valueAsValue, ok, err = tree.GetByKey(5)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByKey(5).ok [Case 1] should have been true")
	}
	valueAsString = valueAsValue.(string)
	if "5" != valueAsString {
		t.Fatalf("GetByKey(5).value [Case 1] should have been \"5\"... instead it was \"%v\"", valueAsString)
	}

	_, ok, err = tree.GetByKey(7)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByKey(7).ok [Case 1] should have been false")
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 1 != numberOfItems {
		t.Fatalf("Len() [Case 2] should have been 1... instead it was %v", numberOfItems)
	}

	ok, err = tree.Put(3, "3")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(3, \"3\").ok should have been true")
	}

	ok, err = tree.Put(7, "7")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(7, \"7\").ok should have been true")
	}

	index, found, err = tree.BisectLeft(2)
	if nil != err {
		t.Fatal(err)
	}
	if -1 != index {
		t.Fatalf("BisectLeft(2).index should have been -1... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectLeft(2).found should have been false")
	}

	index, found, err = tree.BisectLeft(3)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectLeft(3).index [Case 2] should have been 0... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectLeft(3).found [Case 2] should have been true")
	}

	index, found, err = tree.BisectLeft(4)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectLeft(4).index should have been 0... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectLeft(4).found should have been false")
	}

	index, found, err = tree.BisectLeft(5)
	if nil != err {
		t.Fatal(err)
	}
	if 1 != index {
		t.Fatalf("BisectLeft(5).index [Case 2] should have been 1... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectLeft(5).found [Case 2] should have been true")
	}

	index, found, err = tree.BisectLeft(6)
	if nil != err {
		t.Fatal(err)
	}
	if 1 != index {
		t.Fatalf("BisectLeft(5).index should have been 1... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectLeft(6).found should have been false")
	}

	index, found, err = tree.BisectLeft(7)
	if nil != err {
		t.Fatal(err)
	}
	if 2 != index {
		t.Fatalf("BisectLeft(7).index [Case 2] should have been 2... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectLeft(7).found [Case 2] should have been true")
	}

	index, found, err = tree.BisectLeft(8)
	if nil != err {
		t.Fatal(err)
	}
	if 2 != index {
		t.Fatalf("BisectLeft(8).index should have been 2... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectLeft(8).found should have been false")
	}

	index, found, err = tree.BisectRight(2)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectRight(2).index should have been 0... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectRight(2).found should have been false")
	}

	index, found, err = tree.BisectRight(3)
	if nil != err {
		t.Fatal(err)
	}
	if 0 != index {
		t.Fatalf("BisectRight(3).index [Case 2] should have been 0... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectRight(3).found [Case 2] should have been true")
	}

	index, found, err = tree.BisectRight(4)
	if nil != err {
		t.Fatal(err)
	}
	if 1 != index {
		t.Fatalf("BisectRight(4).index should have been 1... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectRight(4).found should have been false")
	}

	index, found, err = tree.BisectRight(5)
	if nil != err {
		t.Fatal(err)
	}
	if 1 != index {
		t.Fatalf("BisectRight(5).index [Case 2] should have been 1... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectRight(5).found [Case 2] should have been true")
	}

	index, found, err = tree.BisectRight(6)
	if nil != err {
		t.Fatal(err)
	}
	if 2 != index {
		t.Fatalf("BisectRight(5).index should have been 2... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectRight(6).found should have been false")
	}

	index, found, err = tree.BisectRight(7)
	if nil != err {
		t.Fatal(err)
	}
	if 2 != index {
		t.Fatalf("BisectRight(7).index [Case 2] should have been 2... instead it was %v", index)
	}
	if !found {
		t.Fatalf("BisectRight(7).found [Case 2] should have been true")
	}

	index, found, err = tree.BisectRight(8)
	if nil != err {
		t.Fatal(err)
	}
	if 3 != index {
		t.Fatalf("BisectRight(8).index should have been 3... instead it was %v", index)
	}
	if found {
		t.Fatalf("BisectRight(8).found should have been false")
	}

	keyAsKey, valueAsValue, ok, err = tree.GetByIndex(0)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByIndex(0).ok [Case 3] should have been true")
	}
	keyAsInt = keyAsKey.(int)
	if 3 != keyAsInt {
		t.Fatalf("GetByIndex(0).key [Case 3] should have been 3... instead it was %v", keyAsInt)
	}
	valueAsString = valueAsValue.(string)
	if "3" != valueAsString {
		t.Fatalf("GetByIndex(0).value [Case 3] should have been \"3\"... instead it was \"%v\"", valueAsString)
	}

	keyAsKey, valueAsValue, ok, err = tree.GetByIndex(1)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByIndex(1).ok [Case 2] should have been true")
	}
	keyAsInt = keyAsKey.(int)
	if 5 != keyAsInt {
		t.Fatalf("GetByIndex(1).key [Case 2] should have been 5... instead it was %v", keyAsInt)
	}
	valueAsString = valueAsValue.(string)
	if "5" != valueAsString {
		t.Fatalf("GetByIndex(1).value [Case 2] should have been \"5\"... instead it was \"%v\"", valueAsString)
	}

	keyAsKey, valueAsValue, ok, err = tree.GetByIndex(2)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByIndex(2).ok [Case 1] should have been true")
	}
	keyAsInt = keyAsKey.(int)
	if 7 != keyAsInt {
		t.Fatalf("GetByIndex(2).key [Case 1] should have been 7... instead it was %v", keyAsInt)
	}
	valueAsString = valueAsValue.(string)
	if "7" != valueAsString {
		t.Fatalf("GetByIndex(2).value [Case 1] should have been \"7\"... instead it was \"%v\"", valueAsString)
	}

	_, _, ok, err = tree.GetByIndex(3)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByIndex(3).ok should have been false")
	}

	_, ok, err = tree.GetByKey(2)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByKey(2).ok should have been false")
	}

	valueAsValue, ok, err = tree.GetByKey(3)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByKey(3).ok [Case 2] should have been true")
	}
	valueAsString = valueAsValue.(string)
	if "3" != valueAsString {
		t.Fatalf("GetByKey(3).value [Case 2] should have been \"3\"... instead it was \"%v\"", valueAsString)
	}

	_, ok, err = tree.GetByKey(4)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByKey(4).ok should have been false")
	}

	valueAsValue, ok, err = tree.GetByKey(5)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByKey(5).ok [Case 2] should have been true")
	}
	valueAsString = valueAsValue.(string)
	if "5" != valueAsString {
		t.Fatalf("GetByKey(5).value [Case 2] should have been \"5\"... instead it was \"%v\"", valueAsString)
	}

	_, ok, err = tree.GetByKey(6)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByKey(6).ok should have been false")
	}

	valueAsValue, ok, err = tree.GetByKey(7)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByKey(7).ok [Case 2] should have been true")
	}
	valueAsString = valueAsValue.(string)
	if "7" != valueAsString {
		t.Fatalf("GetByKey(7).value [Case 2] should have been \"7\"... instead it was \"%v\"", valueAsString)
	}

	_, ok, err = tree.GetByKey(8)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("GetByKey(8).ok should have been false")
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 3 != numberOfItems {
		t.Fatalf("Len() [Case 3] should have been 3... instead it was %v", numberOfItems)
	}

	ok, err = tree.PatchByIndex(-1, "")
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("PatchByIndex(-1, \"\").ok should have been false")
	}

	ok, err = tree.PatchByIndex(3, "")
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("PatchByIndex(3, \"\").ok should have been false")
	}

	ok, err = tree.PatchByKey(1, "")
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("PatchByKey(1, \"\").ok should have been false")
	}

	ok, err = tree.PatchByIndex(0, "T")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("PatchByIndex(0, \"T\").ok should have been true")
	}

	valueAsValue, ok, err = tree.GetByKey(3)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByKey(3).ok [Case 3] should have been true")
	}
	valueAsString = valueAsValue.(string)
	if "T" != valueAsString {
		t.Fatalf("GetByKey(3).value [Case 3] should have been \"3\"... instead it was \"%v\"", valueAsString)
	}

	ok, err = tree.PatchByKey(7, "S")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("PatchByKey(7, \"S\").ok should have been true")
	}

	keyAsKey, valueAsValue, ok, err = tree.GetByIndex(2)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("GetByIndex(2).ok [Case 2] should have been true")
	}
	keyAsInt = keyAsKey.(int)
	if 7 != keyAsInt {
		t.Fatalf("GetByIndex(2).key [Case 2] should have been 7... instead it was %v", keyAsInt)
	}
	valueAsString = valueAsValue.(string)
	if "S" != valueAsString {
		t.Fatalf("GetByIndex(2).value [Case 2] should have been \"S\"... instead it was \"%v\"", valueAsString)
	}
}

func metaTestDeleteByIndexSimple(t *testing.T, tree SortedMap) {
	var (
		err           error
		numberOfItems int
		ok            bool
	)

	ok, err = tree.Put(3, "3")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(3, \"3\").ok should have been true")
	}

	ok, err = tree.Put(5, "5")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(5, \"5\").ok should have been true")
	}

	ok, err = tree.Put(7, "7")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(7, \"7\").ok should have been true")
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 3 != numberOfItems {
		t.Fatalf("Len() [Case 1] should have been 3... instead it was %v", numberOfItems)
	}

	ok, err = tree.DeleteByIndex(-1)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("DeleteByIndex(-1).ok should have been false")
	}

	ok, err = tree.DeleteByIndex(3)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("DeleteByIndex(3).ok should have been false")
	}

	ok, err = tree.DeleteByIndex(2)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("DeleteByIndex(2).ok should have been true")
	}

	ok, err = tree.DeleteByIndex(0)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("DeleteByIndex(0).ok [Case 1] should have been true")
	}

	ok, err = tree.DeleteByIndex(0)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("DeleteByIndex(0).ok [Case 2] should have been true")
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 0 != numberOfItems {
		t.Fatalf("Len() [Case 2] should have been 0... instead it was %v", numberOfItems)
	}
}

func metaTestDeleteByKeySimple(t *testing.T, tree SortedMap) {
	var (
		err           error
		numberOfItems int
		ok            bool
	)

	ok, err = tree.Put(3, "3")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(3, \"3\").ok should have been true")
	}

	ok, err = tree.Put(5, "5")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(5, \"5\").ok should have been true")
	}

	ok, err = tree.Put(7, "7")
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("Put(7, \"7\").ok should have been true")
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 3 != numberOfItems {
		t.Fatalf("Len() [Case 1] should have been 3... instead it was %v", numberOfItems)
	}

	ok, err = tree.DeleteByKey(2)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("DeleteByKey(2).ok should have been false")
	}

	ok, err = tree.DeleteByKey(3)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("DeleteByKey(3).ok should have been true")
	}

	ok, err = tree.DeleteByKey(4)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("DeleteByKey(4).ok should have been false")
	}

	ok, err = tree.DeleteByKey(5)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("DeleteByKey(5).ok should have been true")
	}

	ok, err = tree.DeleteByKey(6)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("DeleteByKey(6).ok should have been false")
	}

	ok, err = tree.DeleteByKey(7)
	if nil != err {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("DeleteByKey(7).ok should have been true")
	}

	ok, err = tree.DeleteByKey(8)
	if nil != err {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("DeleteByKey(8).ok should have been false")
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 0 != numberOfItems {
		t.Fatalf("Len() [Case 2] should have been 0... instead it was %v", numberOfItems)
	}
}

func testKnuthShuffledIntSlice(n int) (intSlice []int, err error) {
	var (
		swapFrom int64
		swapTo   int64
	)
	intSlice = make([]int, n)
	for i := 0; i < n; i++ {
		intSlice[i] = i
	}
	for swapFrom = int64(n - 1); swapFrom > int64(0); swapFrom-- {
		if pseudoRandom {
			if nil == randSource {
				randSource = mathRand.New(mathRand.NewSource(pseudoRandomSeed))
			}

			swapTo = randSource.Int63n(swapFrom + 1)
		} else {
			swapFromPlusOneBigIntPtr := big.NewInt(int64(swapFrom + 1))

			swapToBigIntPtr, nonShadowingErr := cryptoRand.Int(cryptoRand.Reader, swapFromPlusOneBigIntPtr)
			if nil != nonShadowingErr {
				err = fmt.Errorf("cryptoRand.Int(cryptoRand.Reader, swapFromPlusOneBigIntPtr) returned error == \"%v\"", nonShadowingErr)
				return
			}

			swapTo = swapToBigIntPtr.Int64()
		}

		if swapFrom != swapTo {
			intSlice[swapFrom], intSlice[swapTo] = intSlice[swapTo], intSlice[swapFrom]
		}
	}

	err = nil
	return
}

func testFetchIndicesToDeleteNormalized(indicesToDeleteNotNormalized []int) (indicesToDeleteNormalized []int) {
	var (
		elementIndexInner int
		elementIndexOuter int
		elementValueInner int
		elementValueOuter int
	)

	indicesToDeleteNormalized = make([]int, len(indicesToDeleteNotNormalized))

	for elementIndexOuter, elementValueOuter = range indicesToDeleteNotNormalized {
		indicesToDeleteNormalized[elementIndexOuter] = elementValueOuter
	}

	for elementIndexOuter, elementValueOuter = range indicesToDeleteNormalized {
		for elementIndexInner = (elementIndexOuter + 1); elementIndexInner < len(indicesToDeleteNormalized); elementIndexInner++ {
			elementValueInner = indicesToDeleteNormalized[elementIndexInner]
			if elementValueInner > elementValueOuter {
				elementValueInner--
			}
			indicesToDeleteNormalized[elementIndexInner] = elementValueInner
		}
	}

	return
}

func testInsertGetDeleteByIndex(t *testing.T, tree SortedMap, keysToInsert []int, indicesToGet []int, indicesToDeleteNotNormalized []int) {
	var (
		err                       error
		indexToDelete             int
		indexToGet                int
		indicesToDeleteNormalized []int
		keyAsInt                  int
		keyAsKey                  Key
		keyToInsert               int
		numberOfItems             int
		ok                        bool
		valueAsString             string
		valueAsValue              Value
	)

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 0 != numberOfItems {
		t.Fatalf("Len() [Case 1] should have been 0... instead it was %v", numberOfItems)
	}

	for _, keyToInsert = range keysToInsert {
		valueAsString = strconv.Itoa(keyToInsert)
		ok, err = tree.Put(keyToInsert, valueAsString)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Put(%v, \"%v\").ok should have been true", keyToInsert, valueAsString)
		}
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if len(keysToInsert) != numberOfItems {
		t.Fatalf("Len() [Case 2] should have been %v... instead it was %v", len(keysToInsert), numberOfItems)
	}

	for _, indexToGet = range indicesToGet {
		keyAsKey, valueAsValue, ok, err = tree.GetByIndex(indexToGet)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("GetByIndex(%v).ok should have been true", indexToGet)
		}
		keyAsInt = keyAsKey.(int)
		if indexToGet != keyAsInt {
			t.Fatalf("GetByIndex(%v).key should have been %v... instead it was %v", indexToGet, indexToGet, keyAsInt)
		}
		valueAsString = valueAsValue.(string)
		if strconv.Itoa(indexToGet) != valueAsString {
			t.Fatalf("GetByIndex(%v).value should have been \"%v\"... instead it was \"%v\"", indexToGet, strconv.Itoa(indexToGet), valueAsString)
		}
	}

	indicesToDeleteNormalized = testFetchIndicesToDeleteNormalized(indicesToDeleteNotNormalized)

	for _, indexToDelete = range indicesToDeleteNormalized {
		ok, err = tree.DeleteByIndex(indexToDelete)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("DeleteByIndex(%v).ok should have been true", indexToDelete)
		}
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 0 != numberOfItems {
		t.Fatalf("Len() [Case 3] should have been %v... instead it was %v", 0, numberOfItems)
	}
}

func metaTestInsertGetDeleteByIndexTrivial(t *testing.T, tree SortedMap) {
	keysToInsert := []int{
		5, 3, 2, 6, 1, 0, 4,
	}

	indicesToGet := []int{
		0, 5, 6, 1, 2, 4, 3,
	}

	indicesToDeleteNotNormalized := []int{
		3, 2, 5, 4, 0, 6, 1,
	}

	testInsertGetDeleteByIndex(t, tree, keysToInsert, indicesToGet, indicesToDeleteNotNormalized)
}

func metaTestInsertGetDeleteByIndexSmall(t *testing.T, tree SortedMap) {
	keysToInsert := []int{
		2, 0, 5, 31, 1, 12, 30, 13, 21, 37, 17, 22, 24, 11, 35, 6, 36, 15, 23, 19,
		20, 25, 3, 34, 14, 27, 26, 33, 28, 9, 29, 16, 18, 38, 7, 39, 8, 32, 4, 10,
	}

	indicesToGet := []int{
		7, 11, 8, 33, 4, 21, 13, 0, 10, 39, 29, 35, 23, 3, 30, 24, 9, 5, 37, 38,
		17, 14, 25, 1, 16, 36, 12, 31, 2, 27, 19, 15, 6, 20, 22, 18, 34, 28, 26, 32,
	}

	indicesToDeleteNotNormalized := []int{
		8, 18, 24, 12, 6, 28, 34, 25, 29, 38, 13, 9, 32, 7, 26, 39, 17, 37, 2, 3,
		4, 23, 36, 22, 15, 19, 20, 11, 0, 5, 35, 33, 14, 27, 1, 10, 30, 31, 21, 16,
	}

	testInsertGetDeleteByIndex(t, tree, keysToInsert, indicesToGet, indicesToDeleteNotNormalized)
}

func metaTestInsertGetDeleteByIndexLarge(t *testing.T, tree SortedMap) {
	keysToInsert := []int{
		940, 565, 961, 599, 254, 252, 170, 812, 804, 334, 449, 131, 527, 44, 236, 636, 233, 318, 299, 725,
		4, 960, 721, 281, 720, 975, 811, 990, 964, 324, 286, 301, 474, 803, 65, 918, 967, 905, 794, 649,
		935, 560, 261, 306, 980, 797, 680, 309, 788, 62, 135, 845, 229, 581, 57, 459, 687, 361, 477, 968,
		987, 87, 985, 513, 351, 313, 944, 90, 429, 832, 942, 320, 377, 260, 13, 718, 900, 629, 924, 153,
		709, 762, 146, 280, 856, 776, 768, 814, 765, 285, 852, 568, 686, 630, 752, 144, 611, 15, 174, 160,
		375, 37, 618, 770, 657, 147, 829, 781, 743, 899, 86, 843, 866, 64, 192, 128, 914, 534, 659, 16,
		579, 199, 733, 840, 507, 430, 826, 8, 875, 278, 621, 1, 880, 665, 766, 18, 610, 295, 31, 288,
		374, 484, 575, 978, 427, 416, 300, 129, 878, 719, 350, 561, 792, 750, 113, 34, 825, 773, 779, 7,
		55, 80, 958, 970, 60, 494, 929, 727, 78, 983, 902, 103, 437, 139, 972, 933, 909, 248, 913, 185,
		570, 392, 923, 412, 137, 263, 404, 609, 438, 869, 319, 317, 329, 461, 421, 655, 114, 257, 119, 966,
		884, 653, 988, 849, 602, 120, 69, 436, 259, 572, 274, 145, 576, 21, 385, 669, 273, 995, 830, 425,
		562, 731, 388, 724, 470, 540, 97, 231, 623, 100, 457, 934, 608, 154, 965, 672, 251, 772, 499, 323,
		264, 27, 379, 469, 888, 433, 885, 30, 305, 115, 212, 284, 696, 426, 580, 698, 168, 332, 746, 410,
		557, 276, 569, 466, 167, 614, 481, 590, 244, 166, 716, 14, 79, 904, 642, 783, 982, 230, 704, 454,
		0, 444, 920, 321, 3, 539, 993, 39, 683, 771, 181, 693, 328, 999, 628, 127, 930, 956, 367, 232,
		805, 938, 639, 705, 894, 310, 83, 269, 148, 515, 640, 521, 183, 163, 643, 862, 741, 778, 478, 304,
		708, 511, 460, 303, 889, 739, 132, 53, 46, 986, 732, 344, 650, 592, 542, 312, 641, 519, 895, 745,
		679, 777, 979, 827, 336, 43, 180, 222, 109, 991, 343, 859, 819, 887, 423, 453, 19, 818, 806, 451,
		890, 963, 906, 722, 165, 813, 627, 420, 339, 916, 749, 910, 355, 652, 85, 465, 266, 33, 567, 200,
		925, 279, 842, 104, 530, 728, 456, 325, 577, 327, 685, 997, 677, 759, 387, 712, 88, 348, 441, 292,
		322, 293, 815, 178, 555, 214, 563, 213, 59, 807, 337, 715, 791, 376, 399, 977, 954, 586, 197, 510,
		822, 648, 863, 49, 881, 908, 883, 287, 210, 853, 668, 391, 864, 703, 193, 514, 91, 681, 156, 901,
		699, 891, 52, 54, 836, 431, 512, 143, 398, 941, 371, 442, 690, 76, 545, 389, 723, 736, 989, 225,
		971, 841, 205, 701, 82, 345, 400, 331, 571, 142, 241, 443, 882, 717, 11, 600, 613, 40, 757, 589,
		138, 250, 211, 238, 29, 247, 203, 860, 36, 440, 202, 501, 258, 175, 714, 94, 227, 676, 786, 634,
		742, 624, 896, 450, 932, 769, 61, 622, 937, 480, 617, 347, 338, 158, 631, 684, 422, 9, 96, 748,
		546, 607, 141, 184, 223, 38, 190, 378, 226, 81, 730, 173, 10, 744, 73, 664, 820, 475, 795, 502,
		529, 547, 948, 637, 667, 262, 333, 992, 26, 538, 601, 99, 463, 408, 848, 403, 159, 697, 472, 417,
		738, 177, 296, 493, 12, 726, 503, 780, 646, 50, 912, 671, 228, 939, 504, 349, 108, 591, 585, 357,
		314, 854, 487, 130, 596, 734, 949, 121, 785, 936, 823, 877, 625, 283, 204, 962, 419, 467, 928, 390,
		124, 871, 215, 747, 520, 48, 663, 352, 25, 66, 692, 584, 382, 548, 846, 75, 63, 23, 188, 981,
		445, 136, 243, 41, 176, 651, 833, 868, 72, 945, 411, 239, 994, 439, 710, 356, 342, 255, 372, 106,
		386, 473, 162, 974, 381, 898, 110, 767, 735, 536, 315, 605, 761, 140, 362, 353, 216, 809, 816, 660,
		409, 240, 267, 689, 751, 620, 221, 455, 335, 509, 360, 151, 518, 298, 277, 47, 952, 208, 393, 218,
		756, 817, 101, 532, 462, 774, 973, 265, 224, 838, 206, 543, 24, 92, 93, 855, 354, 753, 597, 666,
		675, 186, 775, 587, 415, 688, 531, 464, 297, 58, 535, 976, 525, 397, 492, 850, 67, 405, 793, 541,
		32, 424, 302, 369, 234, 824, 656, 754, 593, 573, 98, 702, 879, 340, 922, 865, 713, 662, 330, 559,
		615, 858, 808, 122, 695, 380, 524, 152, 790, 249, 432, 700, 414, 553, 915, 556, 800, 483, 446, 272,
		291, 674, 164, 682, 220, 196, 897, 35, 758, 516, 566, 544, 588, 635, 552, 760, 290, 947, 707, 373,
		51, 471, 632, 346, 931, 517, 207, 407, 943, 45, 755, 706, 893, 506, 729, 533, 645, 953, 522, 491,
		550, 447, 644, 633, 235, 498, 801, 594, 112, 654, 268, 201, 316, 20, 921, 458, 435, 270, 256, 911,
		564, 496, 134, 107, 927, 311, 876, 526, 294, 917, 448, 56, 957, 574, 523, 359, 799, 366, 187, 802,
		505, 959, 551, 17, 150, 182, 434, 428, 401, 647, 837, 583, 42, 737, 670, 161, 396, 497, 857, 870,
		839, 595, 661, 89, 84, 764, 341, 984, 831, 603, 70, 370, 358, 275, 489, 172, 626, 246, 155, 395,
		368, 892, 872, 95, 485, 179, 969, 782, 998, 479, 384, 116, 326, 5, 71, 22, 919, 694, 907, 612,
		606, 289, 365, 406, 691, 282, 74, 673, 476, 763, 946, 798, 996, 117, 951, 488, 740, 133, 242, 619,
		191, 711, 578, 363, 528, 784, 217, 508, 171, 834, 198, 787, 413, 537, 638, 482, 102, 126, 6, 194,
		271, 678, 549, 245, 950, 598, 926, 402, 418, 495, 28, 658, 861, 835, 237, 123, 307, 554, 873, 490,
		2, 558, 383, 219, 500, 105, 195, 886, 157, 903, 169, 468, 68, 821, 810, 308, 452, 847, 582, 867,
		851, 844, 789, 111, 955, 125, 118, 874, 364, 616, 209, 149, 77, 828, 604, 189, 796, 253, 486, 394,
	}

	indicesToGet := []int{
		92, 628, 523, 154, 46, 111, 212, 385, 342, 705, 265, 543, 281, 999, 951, 307, 565, 252, 638, 352,
		632, 956, 408, 128, 806, 177, 75, 924, 832, 76, 562, 131, 755, 214, 453, 159, 635, 709, 618, 164,
		218, 791, 340, 260, 53, 667, 216, 639, 837, 387, 483, 456, 681, 993, 897, 583, 189, 446, 173, 93,
		361, 417, 463, 469, 2, 642, 511, 190, 809, 659, 754, 70, 636, 490, 81, 441, 354, 979, 871, 333,
		894, 650, 158, 627, 866, 458, 816, 226, 519, 674, 95, 557, 822, 608, 570, 394, 78, 572, 16, 774,
		836, 163, 435, 671, 347, 309, 982, 367, 505, 730, 239, 887, 231, 320, 963, 646, 554, 694, 316, 884,
		867, 606, 677, 213, 6, 914, 663, 852, 749, 311, 73, 559, 66, 700, 990, 985, 911, 548, 360, 957,
		869, 449, 953, 601, 733, 358, 949, 403, 895, 788, 821, 273, 7, 117, 874, 912, 568, 855, 919, 404,
		624, 448, 878, 308, 248, 192, 541, 233, 732, 29, 467, 528, 889, 533, 680, 362, 455, 988, 372, 506,
		958, 556, 936, 170, 811, 729, 687, 222, 844, 351, 699, 860, 492, 20, 517, 977, 59, 413, 862, 376,
		179, 301, 465, 113, 199, 580, 713, 474, 928, 607, 366, 771, 918, 865, 551, 598, 643, 161, 904, 292,
		906, 972, 703, 101, 939, 997, 955, 317, 145, 420, 858, 85, 187, 484, 609, 740, 279, 736, 712, 421,
		960, 534, 396, 807, 141, 532, 900, 881, 33, 561, 142, 348, 379, 577, 792, 782, 653, 863, 954, 769,
		98, 662, 445, 640, 737, 675, 234, 798, 971, 140, 758, 434, 546, 313, 518, 842, 471, 49, 507, 87,
		426, 536, 704, 856, 402, 99, 356, 276, 381, 945, 558, 470, 67, 495, 500, 3, 346, 995, 934, 679,
		97, 817, 980, 497, 773, 84, 138, 57, 489, 902, 259, 167, 989, 229, 32, 591, 848, 651, 327, 440,
		407, 965, 418, 155, 488, 375, 151, 318, 272, 249, 44, 184, 60, 124, 691, 664, 416, 285, 35, 566,
		315, 94, 759, 820, 612, 436, 245, 779, 724, 217, 778, 983, 515, 160, 310, 186, 739, 452, 623, 8,
		975, 880, 631, 13, 330, 63, 126, 586, 253, 115, 885, 64, 374, 925, 485, 162, 931, 412, 146, 4,
		762, 102, 794, 770, 240, 244, 708, 969, 775, 206, 877, 859, 617, 409, 306, 80, 785, 621, 30, 166,
		795, 223, 723, 602, 814, 405, 24, 236, 531, 359, 130, 108, 411, 896, 647, 840, 425, 916, 116, 910,
		986, 38, 753, 395, 132, 457, 386, 725, 304, 196, 293, 873, 363, 898, 781, 808, 51, 267, 104, 110,
		510, 868, 291, 243, 527, 15, 182, 652, 905, 286, 109, 537, 706, 927, 79, 581, 825, 538, 178, 752,
		522, 255, 689, 462, 772, 658, 935, 603, 937, 545, 696, 743, 284, 620, 766, 892, 390, 735, 208, 728,
		202, 682, 879, 805, 171, 974, 941, 761, 526, 295, 432, 410, 731, 365, 793, 328, 251, 200, 136, 289,
		22, 784, 787, 741, 41, 437, 494, 9, 428, 660, 183, 150, 516, 329, 152, 451, 277, 332, 427, 872,
		634, 539, 325, 802, 567, 321, 764, 697, 947, 219, 282, 476, 275, 215, 336, 645, 923, 967, 970, 433,
		595, 23, 478, 139, 876, 673, 334, 438, 237, 574, 19, 530, 571, 665, 930, 353, 502, 297, 661, 886,
		672, 114, 100, 520, 547, 50, 204, 959, 392, 18, 82, 853, 678, 790, 684, 481, 331, 388, 1, 242,
		486, 514, 683, 922, 444, 118, 271, 302, 299, 232, 529, 839, 625, 908, 193, 391, 828, 875, 626, 68,
		619, 198, 27, 235, 891, 742, 907, 893, 657, 803, 439, 780, 812, 296, 278, 763, 127, 77, 504, 587,
		144, 978, 383, 174, 599, 149, 339, 670, 169, 508, 258, 168, 976, 369, 442, 940, 968, 459, 676, 399,
		263, 181, 841, 831, 575, 718, 58, 147, 280, 201, 48, 890, 377, 261, 337, 468, 588, 55, 668, 74,
		641, 604, 472, 69, 83, 350, 686, 789, 584, 726, 56, 826, 622, 42, 371, 129, 843, 711, 648, 824,
		512, 480, 475, 553, 929, 314, 783, 496, 91, 943, 944, 357, 768, 710, 797, 269, 300, 225, 344, 800,
		45, 135, 765, 915, 105, 197, 185, 12, 323, 341, 707, 605, 345, 322, 123, 637, 564, 717, 827, 846,
		594, 71, 227, 86, 148, 854, 228, 257, 268, 849, 888, 933, 834, 847, 373, 942, 423, 573, 207, 857,
		920, 61, 414, 493, 727, 443, 143, 431, 501, 597, 851, 153, 964, 973, 40, 813, 585, 303, 767, 343,
		194, 616, 419, 998, 544, 818, 845, 950, 819, 165, 319, 90, 714, 688, 172, 191, 422, 655, 644, 241,
		535, 563, 719, 525, 175, 569, 615, 294, 290, 0, 690, 962, 582, 961, 324, 125, 72, 747, 701, 590,
		693, 498, 596, 509, 932, 542, 946, 576, 156, 54, 429, 28, 133, 756, 65, 882, 716, 578, 384, 103,
		917, 17, 838, 21, 835, 654, 630, 796, 11, 274, 43, 119, 88, 400, 380, 746, 593, 266, 549, 994,
		685, 122, 106, 810, 799, 734, 830, 589, 903, 722, 750, 899, 984, 264, 479, 112, 987, 305, 26, 176,
		382, 464, 250, 121, 981, 499, 378, 745, 909, 466, 254, 389, 195, 137, 25, 220, 823, 482, 721, 473,
		952, 996, 335, 415, 47, 205, 656, 62, 966, 948, 883, 801, 757, 698, 870, 738, 10, 368, 287, 89,
		777, 614, 649, 992, 938, 600, 283, 180, 921, 555, 14, 107, 450, 312, 454, 513, 397, 210, 503, 424,
		669, 751, 447, 270, 288, 406, 861, 39, 52, 398, 695, 901, 864, 815, 461, 256, 748, 460, 579, 720,
		120, 666, 355, 401, 487, 613, 913, 430, 338, 633, 364, 592, 540, 550, 560, 692, 804, 157, 850, 36,
		610, 991, 349, 491, 744, 611, 326, 211, 477, 776, 224, 134, 833, 702, 521, 209, 247, 552, 760, 926,
		5, 188, 246, 238, 31, 37, 221, 298, 715, 393, 203, 230, 524, 370, 262, 96, 829, 786, 34, 629,
	}

	indicesToDeleteNotNormalized := []int{
		401, 426, 285, 997, 355, 109, 205, 89, 129, 698, 266, 102, 691, 676, 912, 223, 892, 93, 472, 870,
		904, 111, 560, 284, 438, 201, 863, 380, 368, 804, 341, 627, 273, 837, 577, 744, 934, 556, 208, 394,
		622, 121, 452, 773, 721, 287, 354, 981, 607, 349, 546, 467, 827, 670, 491, 221, 785, 407, 767, 794,
		835, 386, 339, 715, 758, 66, 891, 62, 531, 399, 135, 747, 493, 122, 460, 445, 821, 115, 194, 310,
		695, 651, 542, 791, 602, 327, 434, 113, 965, 889, 149, 834, 866, 799, 127, 592, 324, 841, 732, 857,
		280, 489, 190, 663, 389, 55, 131, 914, 911, 992, 453, 249, 473, 126, 532, 883, 40, 660, 902, 949,
		648, 463, 533, 924, 764, 270, 748, 646, 567, 207, 613, 749, 690, 158, 793, 810, 935, 45, 243, 325,
		621, 179, 301, 543, 1, 563, 151, 15, 605, 825, 862, 229, 147, 406, 357, 847, 403, 165, 510, 882,
		206, 888, 124, 983, 261, 508, 417, 59, 554, 218, 653, 669, 673, 334, 236, 860, 38, 788, 81, 629,
		163, 523, 373, 790, 631, 858, 329, 838, 496, 143, 9, 734, 649, 500, 502, 142, 263, 971, 780, 365,
		828, 961, 782, 720, 872, 64, 309, 986, 941, 367, 247, 798, 322, 672, 364, 177, 829, 896, 609, 430,
		719, 958, 960, 947, 561, 645, 382, 604, 976, 182, 569, 29, 275, 282, 343, 288, 279, 683, 134, 574,
		848, 878, 110, 686, 18, 926, 763, 849, 362, 487, 19, 675, 369, 375, 232, 474, 217, 746, 702, 479,
		998, 742, 518, 737, 446, 659, 707, 624, 139, 595, 145, 515, 411, 995, 630, 514, 724, 899, 754, 522,
		548, 776, 335, 240, 634, 765, 880, 8, 433, 571, 257, 503, 704, 191, 224, 449, 0, 685, 429, 710,
		465, 214, 276, 950, 374, 713, 133, 658, 617, 919, 447, 643, 117, 787, 688, 930, 49, 83, 32, 740,
		332, 895, 340, 920, 501, 192, 466, 277, 220, 50, 119, 844, 733, 931, 311, 652, 418, 637, 566, 245,
		39, 726, 91, 86, 456, 778, 678, 226, 730, 23, 781, 450, 408, 222, 267, 783, 802, 819, 869, 956,
		30, 231, 527, 5, 680, 576, 922, 168, 657, 204, 547, 372, 199, 314, 10, 552, 295, 517, 306, 852,
		432, 545, 215, 692, 202, 97, 471, 925, 786, 957, 760, 99, 164, 384, 519, 751, 260, 756, 936, 25,
		771, 485, 570, 770, 11, 632, 120, 94, 761, 216, 865, 409, 606, 65, 937, 709, 457, 13, 60, 455,
		197, 877, 772, 498, 854, 69, 696, 932, 475, 251, 213, 184, 855, 582, 893, 537, 58, 264, 903, 499,
		693, 973, 714, 969, 839, 152, 371, 655, 991, 809, 167, 289, 316, 705, 379, 513, 544, 106, 599, 635,
		584, 415, 644, 146, 846, 753, 796, 52, 393, 731, 128, 979, 815, 913, 587, 940, 7, 588, 398, 271,
		808, 189, 647, 559, 738, 47, 641, 36, 851, 148, 203, 681, 114, 774, 982, 274, 942, 336, 317, 187,
		529, 464, 939, 370, 901, 917, 422, 573, 951, 811, 253, 3, 853, 654, 874, 234, 272, 342, 668, 459,
		308, 56, 505, 520, 541, 777, 727, 431, 150, 12, 591, 188, 616, 20, 974, 43, 153, 454, 557, 24,
		323, 944, 166, 915, 256, 843, 173, 425, 977, 994, 970, 581, 154, 416, 268, 141, 509, 305, 181, 701,
		968, 708, 980, 642, 484, 252, 293, 26, 212, 259, 687, 822, 82, 528, 75, 290, 909, 361, 580, 227,
		183, 27, 535, 37, 536, 640, 414, 694, 112, 2, 755, 871, 486, 469, 550, 16, 41, 511, 564, 359,
		598, 775, 752, 84, 521, 477, 745, 178, 294, 832, 331, 125, 820, 176, 427, 291, 679, 608, 568, 897,
		307, 497, 993, 792, 73, 814, 468, 697, 623, 850, 516, 578, 611, 174, 666, 377, 875, 14, 123, 881,
		948, 144, 378, 779, 262, 723, 155, 320, 989, 964, 625, 583, 70, 71, 650, 241, 244, 302, 248, 990,
		138, 17, 87, 35, 795, 79, 400, 428, 439, 237, 718, 57, 955, 200, 929, 419, 999, 363, 927, 555,
		988, 836, 876, 347, 830, 77, 656, 195, 476, 51, 962, 265, 387, 209, 423, 76, 879, 868, 867, 762,
		945, 985, 633, 421, 908, 90, 717, 255, 228, 711, 789, 196, 330, 344, 558, 906, 298, 619, 171, 590,
		766, 108, 328, 699, 198, 392, 482, 938, 921, 600, 596, 918, 246, 424, 22, 859, 539, 861, 488, 180,
		618, 356, 412, 440, 436, 898, 104, 437, 304, 589, 887, 894, 159, 95, 856, 390, 160, 769, 553, 725,
		628, 565, 864, 46, 492, 481, 286, 483, 413, 716, 299, 996, 638, 254, 943, 884, 296, 338, 105, 100,
		689, 818, 886, 444, 671, 478, 297, 959, 494, 750, 405, 806, 420, 953, 353, 910, 319, 92, 281, 684,
		85, 185, 443, 230, 395, 300, 157, 923, 250, 967, 383, 768, 313, 601, 757, 594, 603, 480, 526, 946,
		283, 549, 451, 636, 842, 845, 53, 258, 80, 639, 495, 612, 524, 6, 840, 817, 729, 101, 562, 972,
		21, 784, 103, 512, 736, 741, 610, 219, 728, 807, 350, 156, 402, 933, 873, 470, 54, 388, 812, 345,
		954, 885, 118, 74, 579, 572, 116, 441, 435, 397, 4, 739, 805, 759, 448, 352, 551, 333, 615, 620,
		905, 797, 278, 161, 900, 42, 907, 211, 44, 677, 28, 137, 385, 391, 186, 233, 661, 321, 34, 540,
		396, 292, 801, 72, 63, 890, 813, 831, 816, 210, 735, 225, 823, 130, 490, 175, 963, 743, 978, 534,
		530, 667, 162, 442, 824, 593, 303, 803, 800, 140, 239, 507, 966, 312, 242, 664, 337, 269, 665, 78,
		366, 381, 67, 61, 132, 462, 315, 461, 506, 700, 326, 916, 107, 706, 674, 504, 98, 525, 172, 928,
		586, 193, 585, 575, 376, 833, 712, 235, 238, 987, 722, 404, 346, 682, 31, 48, 458, 662, 68, 88,
		952, 169, 597, 136, 360, 703, 984, 410, 96, 348, 33, 614, 170, 318, 351, 538, 826, 626, 358, 975,
	}

	testInsertGetDeleteByIndex(t, tree, keysToInsert, indicesToGet, indicesToDeleteNotNormalized)
}

func metaTestInsertGetDeleteByIndexHuge(t *testing.T, tree SortedMap) {
	var (
		err                          error
		indicesToDeleteNotNormalized []int
		indicesToGet                 []int
		keysToInsert                 []int
	)

	keysToInsert, err = testKnuthShuffledIntSlice(testHugeNumKeys)
	if nil != err {
		t.Fatal(err)
	}
	indicesToGet, err = testKnuthShuffledIntSlice(testHugeNumKeys)
	if nil != err {
		t.Fatal(err)
	}
	indicesToDeleteNotNormalized, err = testKnuthShuffledIntSlice(testHugeNumKeys)
	if nil != err {
		t.Fatal(err)
	}

	testInsertGetDeleteByIndex(t, tree, keysToInsert, indicesToGet, indicesToDeleteNotNormalized)
}

func testInsertGetDeleteByKey(t *testing.T, tree SortedMap, keysToInsert []int, keysToGet []int, keysToDelete []int) {
	var (
		err           error
		keyToDelete   int
		keyToGet      int
		keyToInsert   int
		numberOfItems int
		ok            bool
		valueAsString string
		valueAsValue  Value
	)

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 0 != numberOfItems {
		t.Fatalf("Len() [Case 1] should have been 0... instead it was %v", numberOfItems)
	}

	for _, keyToInsert = range keysToInsert {
		valueAsString = strconv.Itoa(keyToInsert)
		ok, err = tree.Put(keyToInsert, valueAsString)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Put(%v, \"%v\").ok should have been true", keyToInsert, valueAsString)
		}
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if len(keysToInsert) != numberOfItems {
		t.Fatalf("Len() [Case 2] should have been %v... instead it was %v", len(keysToInsert), numberOfItems)
	}

	for _, keyToGet = range keysToGet {
		valueAsValue, ok, err = tree.GetByKey(keyToGet)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("GetByKey(%v).ok should have been true", keyToGet)
		}
		valueAsString = valueAsValue.(string)
		if strconv.Itoa(keyToGet) != valueAsString {
			t.Fatalf("GetByKey(%v).value should have been \"%v\"... instead it was \"%v\"", keyToGet, strconv.Itoa(keyToGet), valueAsString)
		}
	}

	for _, keyToDelete = range keysToDelete {
		ok, err = tree.DeleteByKey(keyToDelete)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("DeleteByKey(%v).ok should have been true", keyToDelete)
		}
	}

	numberOfItems, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if 0 != numberOfItems {
		t.Fatalf("Len() [Case 3] should have been %v... instead it was %v", 0, numberOfItems)
	}
}

func metaTestInsertGetDeleteByKeyTrivial(t *testing.T, tree SortedMap) {
	keysToInsert := []int{
		6, 3, 5, 2, 4, 0, 1,
	}

	keysToGet := []int{
		5, 3, 2, 6, 1, 0, 4,
	}

	keysToDelete := []int{
		5, 4, 1, 6, 2, 0, 3,
	}

	testInsertGetDeleteByKey(t, tree, keysToInsert, keysToGet, keysToDelete)
}

func metaTestInsertGetDeleteByKeySmall(t *testing.T, tree SortedMap) {
	keysToInsert := []int{
		4, 25, 38, 14, 36, 8, 6, 37, 23, 13, 24, 22, 5, 18, 39, 3, 7, 19, 26, 21,
		12, 32, 35, 30, 28, 11, 15, 20, 33, 9, 31, 1, 29, 16, 17, 27, 2, 34, 0, 10,
	}

	keysToGet := []int{
		9, 20, 37, 11, 5, 36, 29, 32, 21, 3, 0, 24, 23, 25, 26, 19, 33, 38, 28, 1,
		14, 18, 10, 16, 4, 8, 31, 12, 13, 39, 34, 35, 27, 2, 17, 22, 15, 30, 7, 6,
	}

	keysToDelete := []int{
		37, 29, 21, 38, 10, 16, 22, 33, 5, 31, 23, 1, 8, 36, 0, 17, 24, 35, 3, 34,
		11, 14, 6, 12, 30, 15, 18, 19, 20, 26, 25, 27, 7, 4, 2, 13, 32, 28, 39, 9,
	}

	testInsertGetDeleteByKey(t, tree, keysToInsert, keysToGet, keysToDelete)
}

func metaTestInsertGetDeleteByKeyLarge(t *testing.T, tree SortedMap) {
	keysToInsert := []int{
		360, 500, 214, 811, 371, 388, 174, 143, 643, 39, 402, 121, 926, 565, 516, 324, 693, 486, 591, 45,
		637, 328, 948, 97, 284, 738, 474, 103, 702, 91, 168, 233, 891, 351, 303, 253, 134, 62, 119, 387,
		211, 579, 787, 484, 635, 480, 26, 836, 426, 227, 319, 285, 874, 990, 323, 118, 369, 676, 666, 286,
		512, 445, 869, 208, 341, 536, 147, 246, 821, 255, 482, 419, 372, 716, 293, 646, 457, 60, 32, 683,
		639, 8, 979, 425, 262, 854, 145, 23, 146, 361, 695, 191, 852, 884, 352, 104, 808, 412, 597, 265,
		796, 789, 526, 271, 392, 933, 819, 82, 910, 222, 617, 440, 952, 681, 633, 704, 653, 823, 59, 140,
		87, 337, 217, 655, 871, 909, 699, 764, 511, 545, 66, 346, 533, 469, 289, 30, 473, 332, 729, 428,
		650, 98, 55, 580, 837, 607, 263, 577, 551, 834, 169, 712, 304, 620, 588, 604, 947, 16, 160, 43,
		862, 970, 397, 747, 772, 478, 905, 112, 307, 899, 52, 520, 158, 880, 357, 766, 602, 706, 968, 75,
		374, 89, 701, 609, 574, 898, 186, 636, 6, 612, 578, 395, 476, 94, 14, 730, 592, 441, 902, 252,
		25, 300, 674, 251, 198, 355, 325, 781, 266, 936, 282, 786, 826, 317, 627, 889, 878, 40, 713, 553,
		443, 783, 56, 182, 349, 7, 561, 68, 254, 813, 501, 162, 329, 930, 149, 105, 816, 928, 753, 133,
		861, 761, 490, 645, 931, 971, 760, 237, 629, 988, 224, 677, 111, 276, 774, 108, 172, 468, 399, 539,
		830, 405, 279, 142, 991, 385, 151, 814, 339, 321, 508, 546, 630, 858, 302, 689, 275, 529, 389, 472,
		563, 965, 35, 820, 193, 758, 380, 458, 995, 998, 980, 258, 461, 961, 442, 27, 708, 647, 594, 465,
		471, 280, 295, 434, 453, 801, 163, 567, 648, 914, 771, 36, 175, 542, 756, 964, 489, 483, 364, 743,
		264, 67, 590, 958, 665, 113, 779, 844, 803, 657, 135, 406, 583, 76, 1, 966, 220, 535, 913, 907,
		225, 245, 759, 682, 203, 236, 256, 173, 316, 731, 114, 924, 865, 197, 784, 344, 308, 309, 644, 589,
		736, 954, 109, 851, 515, 155, 918, 872, 305, 138, 96, 800, 390, 628, 850, 873, 290, 573, 959, 330,
		268, 239, 327, 368, 559, 335, 944, 749, 797, 576, 69, 514, 841, 446, 943, 73, 269, 622, 226, 722,
		805, 866, 877, 449, 587, 610, 4, 171, 863, 216, 292, 86, 977, 895, 444, 13, 9, 47, 497, 31,
		433, 624, 812, 11, 987, 270, 843, 616, 88, 903, 656, 462, 185, 950, 177, 394, 503, 775, 117, 54,
		939, 815, 593, 890, 223, 417, 291, 299, 3, 973, 322, 829, 946, 84, 189, 125, 518, 196, 378, 240,
		431, 929, 564, 975, 178, 243, 116, 248, 450, 493, 983, 853, 272, 101, 2, 488, 993, 343, 420, 984,
		170, 126, 562, 822, 557, 703, 123, 919, 167, 632, 297, 601, 157, 626, 780, 424, 527, 818, 810, 259,
		456, 556, 99, 479, 205, 460, 652, 187, 427, 839, 48, 544, 447, 79, 439, 669, 798, 274, 437, 634,
		466, 684, 548, 71, 999, 298, 65, 510, 555, 922, 336, 925, 312, 418, 967, 904, 882, 521, 532, 807,
		375, 762, 213, 37, 107, 238, 896, 288, 61, 651, 696, 917, 832, 688, 502, 435, 600, 584, 77, 78,
		403, 148, 969, 744, 353, 407, 358, 596, 625, 391, 838, 694, 124, 29, 900, 710, 481, 53, 725, 206,
		680, 413, 311, 739, 568, 241, 746, 247, 448, 799, 949, 393, 897, 752, 932, 95, 566, 715, 795, 42,
		921, 997, 506, 129, 957, 188, 570, 912, 221, 719, 938, 230, 164, 927, 467, 523, 868, 18, 331, 642,
		751, 202, 320, 90, 530, 986, 672, 70, 356, 953, 314, 156, 794, 631, 859, 470, 621, 835, 429, 664,
		846, 190, 908, 244, 44, 773, 883, 989, 20, 732, 411, 165, 423, 613, 901, 306, 714, 273, 209, 522,
		141, 916, 345, 934, 709, 534, 857, 718, 864, 338, 379, 598, 498, 74, 137, 847, 242, 575, 430, 791,
		152, 605, 207, 608, 734, 509, 340, 654, 785, 940, 692, 849, 366, 347, 408, 195, 845, 763, 793, 120,
		100, 28, 543, 641, 817, 586, 161, 477, 17, 115, 487, 662, 414, 558, 278, 235, 982, 128, 310, 454,
		277, 136, 945, 260, 24, 606, 809, 686, 782, 742, 720, 199, 881, 102, 618, 400, 802, 459, 212, 313,
		582, 721, 824, 382, 827, 15, 828, 671, 726, 717, 485, 996, 386, 767, 875, 58, 363, 153, 724, 267,
		690, 232, 955, 733, 249, 867, 318, 377, 507, 750, 410, 885, 778, 51, 994, 21, 49, 892, 376, 687,
		768, 792, 416, 334, 513, 92, 234, 455, 735, 915, 661, 976, 150, 740, 963, 770, 700, 370, 12, 599,
		524, 110, 404, 673, 492, 741, 106, 0, 184, 707, 705, 937, 840, 261, 93, 83, 886, 906, 876, 549,
		727, 640, 401, 127, 691, 833, 525, 855, 496, 615, 550, 856, 848, 396, 728, 737, 139, 581, 144, 180,
		697, 757, 537, 315, 132, 257, 19, 491, 519, 350, 72, 769, 679, 294, 667, 499, 860, 281, 5, 870,
		888, 154, 475, 130, 166, 367, 204, 438, 540, 122, 531, 560, 804, 538, 978, 228, 326, 748, 920, 451,
		34, 384, 663, 436, 80, 825, 745, 755, 923, 215, 194, 893, 552, 659, 571, 879, 229, 754, 432, 296,
		678, 33, 974, 505, 675, 595, 495, 159, 517, 981, 831, 452, 250, 287, 183, 192, 46, 85, 985, 619,
		765, 22, 894, 603, 960, 806, 421, 956, 623, 41, 50, 658, 359, 231, 63, 210, 373, 572, 685, 365,
		611, 348, 788, 668, 333, 842, 585, 541, 941, 528, 711, 776, 614, 383, 381, 911, 38, 64, 951, 200,
		887, 398, 354, 81, 649, 10, 494, 463, 660, 790, 179, 569, 219, 504, 176, 283, 181, 972, 201, 777,
		301, 415, 942, 218, 342, 962, 57, 723, 464, 409, 935, 131, 670, 698, 992, 554, 638, 547, 362, 422,
	}

	keysToGet := []int{
		134, 105, 783, 731, 203, 206, 367, 375, 374, 233, 370, 390, 384, 386, 581, 938, 680, 66, 606, 503,
		673, 65, 907, 425, 948, 38, 924, 790, 651, 246, 767, 87, 331, 63, 114, 103, 758, 573, 833, 724,
		592, 723, 761, 264, 985, 966, 1, 16, 825, 822, 746, 502, 937, 43, 57, 515, 996, 401, 662, 191,
		149, 73, 294, 800, 696, 458, 796, 908, 929, 473, 975, 660, 863, 976, 163, 896, 942, 282, 388, 274,
		623, 693, 603, 485, 612, 97, 110, 675, 765, 268, 669, 18, 795, 993, 220, 116, 977, 71, 779, 378,
		809, 142, 476, 855, 711, 186, 946, 72, 815, 210, 395, 793, 85, 974, 479, 340, 145, 702, 755, 70,
		199, 4, 392, 144, 208, 526, 371, 894, 205, 236, 415, 970, 570, 979, 180, 122, 358, 928, 745, 450,
		195, 377, 824, 313, 296, 322, 609, 578, 645, 131, 794, 194, 460, 311, 527, 906, 911, 64, 949, 360,
		154, 380, 598, 40, 667, 127, 368, 640, 898, 757, 548, 287, 281, 681, 62, 915, 193, 29, 469, 437,
		537, 89, 487, 42, 870, 982, 814, 997, 823, 525, 507, 709, 136, 497, 841, 452, 20, 862, 185, 647,
		569, 876, 935, 226, 602, 737, 599, 56, 834, 77, 555, 936, 878, 466, 742, 55, 442, 330, 188, 690,
		983, 943, 309, 817, 319, 786, 963, 334, 931, 871, 840, 644, 955, 664, 575, 543, 506, 829, 336, 495,
		558, 540, 492, 266, 227, 980, 972, 799, 832, 721, 51, 33, 31, 501, 546, 234, 860, 510, 990, 912,
		480, 760, 616, 850, 961, 169, 225, 184, 351, 299, 108, 327, 778, 654, 678, 159, 491, 212, 315, 784,
		797, 706, 560, 918, 653, 308, 686, 272, 725, 133, 633, 818, 280, 302, 774, 753, 682, 455, 712, 436,
		423, 951, 826, 532, 434, 239, 463, 329, 3, 583, 28, 365, 262, 92, 76, 749, 198, 574, 533, 332,
		152, 454, 421, 917, 459, 98, 934, 317, 446, 720, 276, 173, 600, 123, 534, 90, 893, 978, 24, 754,
		32, 192, 559, 632, 7, 362, 944, 615, 626, 182, 801, 889, 872, 60, 216, 547, 947, 729, 636, 300,
		591, 190, 554, 759, 597, 248, 104, 519, 94, 260, 167, 27, 670, 699, 472, 617, 768, 0, 400, 160,
		483, 235, 341, 542, 932, 897, 461, 237, 403, 646, 484, 763, 728, 214, 39, 812, 811, 86, 14, 539,
		95, 451, 722, 921, 482, 504, 914, 125, 756, 930, 572, 905, 953, 223, 880, 324, 164, 427, 2, 414,
		541, 625, 426, 847, 422, 715, 429, 158, 325, 270, 54, 364, 788, 320, 701, 950, 883, 895, 46, 298,
		156, 957, 74, 727, 676, 610, 433, 130, 807, 836, 157, 744, 448, 478, 819, 475, 297, 165, 113, 81,
		453, 346, 68, 293, 595, 565, 659, 207, 168, 716, 576, 391, 762, 810, 456, 691, 88, 431, 582, 366,
		333, 196, 717, 605, 404, 498, 713, 959, 577, 481, 962, 221, 430, 137, 521, 243, 129, 126, 994, 93,
		677, 111, 381, 441, 652, 356, 279, 449, 290, 666, 416, 604, 656, 695, 328, 200, 435, 580, 357, 698,
		747, 355, 508, 359, 326, 536, 283, 694, 842, 224, 621, 998, 764, 751, 493, 301, 269, 649, 769, 967,
		968, 925, 58, 117, 67, 204, 881, 770, 48, 289, 8, 21, 965, 657, 271, 247, 511, 838, 242, 563,
		509, 5, 629, 910, 628, 396, 304, 228, 82, 844, 890, 517, 531, 30, 901, 553, 78, 310, 566, 373,
		353, 514, 10, 488, 187, 879, 887, 102, 44, 868, 162, 846, 124, 471, 608, 464, 909, 791, 885, 750,
		372, 665, 813, 153, 172, 383, 703, 902, 954, 743, 69, 941, 882, 552, 256, 278, 989, 251, 697, 892,
		432, 535, 867, 306, 904, 342, 618, 960, 586, 923, 338, 151, 369, 848, 927, 26, 601, 550, 79, 719,
		345, 143, 291, 50, 500, 229, 843, 112, 831, 101, 588, 178, 622, 614, 474, 658, 856, 420, 584, 835,
		316, 671, 851, 250, 804, 211, 286, 874, 35, 828, 973, 244, 945, 339, 820, 589, 837, 213, 643, 642,
		288, 619, 255, 240, 23, 940, 562, 80, 861, 303, 505, 419, 620, 926, 674, 789, 494, 470, 385, 399,
		261, 873, 209, 805, 347, 900, 952, 991, 752, 875, 138, 335, 179, 45, 986, 964, 556, 418, 551, 594,
		321, 638, 350, 467, 181, 740, 704, 135, 639, 587, 217, 679, 869, 726, 115, 992, 648, 413, 232, 808,
		343, 672, 522, 379, 109, 292, 688, 710, 84, 866, 849, 382, 933, 13, 798, 513, 34, 821, 218, 61,
		408, 411, 999, 428, 913, 865, 512, 139, 780, 277, 516, 650, 524, 83, 305, 17, 438, 468, 852, 59,
		47, 41, 176, 15, 718, 987, 634, 132, 518, 174, 830, 398, 732, 585, 397, 490, 689, 920, 22, 741,
		884, 663, 37, 772, 314, 637, 462, 611, 776, 91, 275, 361, 285, 827, 641, 613, 523, 734, 477, 318,
		969, 781, 201, 668, 736, 36, 349, 995, 528, 147, 624, 254, 687, 457, 984, 888, 417, 363, 96, 189,
		263, 545, 630, 175, 215, 627, 845, 886, 273, 258, 730, 202, 443, 579, 354, 981, 561, 161, 708, 496,
		520, 197, 252, 409, 499, 100, 792, 19, 170, 971, 267, 249, 692, 146, 445, 222, 714, 107, 775, 53,
		568, 171, 700, 253, 12, 307, 141, 988, 6, 11, 891, 782, 52, 106, 344, 567, 857, 312, 593, 120,
		766, 389, 707, 785, 230, 444, 238, 738, 387, 402, 118, 155, 899, 99, 265, 922, 439, 859, 631, 231,
		685, 903, 394, 121, 9, 352, 177, 49, 284, 140, 939, 245, 148, 771, 424, 549, 465, 777, 803, 376,
		348, 337, 150, 544, 683, 733, 655, 916, 259, 407, 447, 393, 596, 538, 219, 25, 787, 661, 295, 773,
		323, 489, 748, 241, 410, 956, 684, 257, 75, 858, 529, 571, 864, 816, 854, 739, 635, 564, 735, 183,
		440, 839, 128, 705, 405, 877, 853, 958, 486, 166, 590, 919, 412, 530, 806, 119, 406, 802, 607, 557,
	}

	keysToDelete := []int{
		910, 109, 39, 984, 833, 364, 905, 90, 236, 226, 27, 262, 10, 292, 16, 40, 918, 516, 737, 654,
		741, 604, 760, 228, 688, 398, 791, 339, 979, 71, 610, 514, 354, 729, 225, 399, 32, 715, 773, 134,
		940, 468, 31, 975, 486, 380, 735, 93, 673, 220, 59, 806, 67, 851, 107, 382, 247, 991, 63, 306,
		204, 70, 436, 389, 561, 764, 942, 4, 782, 593, 877, 565, 158, 903, 885, 985, 796, 283, 229, 552,
		542, 254, 811, 125, 338, 600, 156, 599, 923, 607, 700, 634, 208, 725, 371, 471, 161, 913, 210, 503,
		274, 901, 0, 42, 428, 768, 350, 407, 420, 995, 457, 929, 914, 684, 922, 636, 145, 46, 482, 934,
		149, 319, 794, 506, 863, 320, 470, 619, 697, 34, 438, 381, 100, 151, 529, 936, 761, 459, 854, 852,
		206, 429, 315, 360, 372, 275, 460, 499, 390, 35, 803, 683, 268, 500, 286, 825, 614, 590, 401, 237,
		114, 272, 82, 784, 223, 778, 195, 685, 767, 895, 217, 259, 269, 549, 844, 449, 163, 563, 647, 906,
		157, 489, 451, 800, 703, 576, 886, 678, 992, 188, 164, 391, 505, 621, 810, 301, 993, 246, 944, 871,
		186, 233, 116, 598, 596, 96, 442, 435, 196, 64, 14, 308, 173, 876, 140, 592, 44, 686, 570, 419,
		881, 450, 569, 628, 495, 692, 61, 902, 815, 143, 47, 702, 652, 263, 521, 935, 608, 771, 234, 836,
		425, 238, 579, 279, 256, 2, 564, 805, 517, 744, 841, 508, 240, 343, 74, 129, 13, 527, 494, 483,
		756, 988, 526, 952, 752, 545, 819, 106, 332, 551, 285, 759, 953, 541, 490, 554, 169, 130, 431, 245,
		329, 342, 84, 965, 484, 763, 960, 19, 487, 548, 755, 282, 855, 242, 126, 112, 980, 235, 712, 578,
		799, 75, 613, 37, 353, 884, 253, 243, 101, 640, 996, 967, 890, 369, 830, 566, 102, 696, 518, 141,
		458, 861, 440, 418, 406, 3, 727, 230, 730, 974, 732, 657, 945, 138, 898, 290, 894, 154, 645, 146,
		160, 293, 362, 972, 379, 11, 588, 872, 629, 300, 105, 642, 582, 550, 547, 675, 618, 325, 252, 211,
		99, 785, 172, 655, 726, 997, 433, 51, 153, 973, 417, 994, 239, 312, 18, 232, 954, 783, 258, 774,
		405, 409, 711, 113, 36, 926, 679, 777, 288, 941, 983, 241, 757, 637, 395, 891, 133, 699, 216, 577,
		908, 558, 832, 203, 190, 219, 981, 135, 375, 522, 513, 808, 971, 424, 139, 625, 171, 168, 707, 573,
		672, 412, 746, 314, 840, 348, 747, 244, 804, 257, 427, 920, 387, 769, 617, 530, 739, 817, 580, 858,
		875, 313, 656, 605, 396, 693, 631, 650, 691, 53, 638, 731, 131, 606, 897, 304, 465, 751, 641, 595,
		860, 86, 199, 643, 609, 176, 276, 123, 322, 835, 633, 368, 690, 5, 481, 583, 662, 45, 915, 358,
		152, 260, 887, 668, 192, 964, 850, 477, 868, 365, 207, 128, 795, 649, 469, 148, 108, 862, 480, 214,
		816, 231, 962, 414, 170, 439, 961, 432, 281, 66, 404, 147, 809, 334, 528, 136, 911, 540, 103, 184,
		758, 9, 48, 183, 397, 651, 49, 77, 603, 373, 20, 644, 175, 317, 728, 179, 888, 91, 781, 415,
		813, 770, 689, 97, 62, 21, 879, 277, 52, 79, 670, 969, 81, 553, 639, 509, 218, 488, 430, 434,
		762, 669, 826, 127, 626, 215, 182, 41, 525, 352, 958, 8, 174, 132, 65, 560, 698, 85, 821, 532,
		250, 706, 534, 904, 305, 273, 115, 270, 987, 765, 925, 6, 422, 421, 776, 296, 416, 25, 298, 403,
		197, 366, 648, 775, 165, 278, 568, 845, 680, 660, 591, 912, 507, 622, 321, 955, 848, 284, 831, 710,
		57, 98, 865, 213, 318, 581, 408, 870, 72, 512, 538, 559, 392, 497, 316, 294, 462, 667, 349, 536,
		466, 110, 76, 812, 456, 562, 713, 221, 324, 742, 722, 12, 571, 839, 370, 843, 820, 83, 227, 56,
		892, 681, 271, 177, 478, 287, 120, 515, 772, 351, 295, 87, 630, 411, 661, 426, 191, 947, 705, 907,
		531, 155, 899, 646, 446, 586, 22, 585, 202, 704, 212, 441, 823, 30, 917, 708, 524, 201, 330, 878,
		829, 336, 574, 376, 394, 748, 846, 519, 695, 537, 323, 50, 664, 663, 24, 827, 849, 266, 310, 594,
		472, 932, 999, 801, 327, 724, 124, 866, 714, 344, 556, 209, 454, 535, 198, 144, 834, 80, 137, 786,
		180, 251, 749, 501, 29, 867, 355, 122, 789, 818, 677, 88, 159, 882, 92, 443, 718, 200, 880, 118,
		779, 721, 474, 620, 717, 694, 95, 335, 189, 740, 43, 38, 873, 584, 822, 745, 473, 957, 367, 94,
		793, 939, 491, 959, 384, 671, 111, 356, 69, 989, 951, 224, 597, 511, 736, 950, 437, 963, 341, 187,
		193, 627, 575, 185, 28, 567, 400, 682, 453, 248, 938, 864, 924, 889, 587, 167, 533, 828, 589, 485,
		766, 687, 790, 117, 998, 104, 539, 990, 615, 328, 743, 893, 493, 23, 386, 701, 966, 601, 377, 857,
		26, 546, 309, 289, 255, 734, 787, 956, 792, 333, 249, 267, 738, 883, 331, 916, 150, 676, 659, 302,
		723, 346, 464, 635, 977, 520, 89, 68, 919, 222, 496, 900, 502, 479, 345, 265, 299, 814, 976, 162,
		510, 754, 837, 572, 467, 674, 359, 463, 946, 921, 1, 410, 933, 653, 750, 948, 874, 978, 383, 665,
		181, 297, 970, 303, 363, 602, 264, 194, 982, 476, 423, 807, 445, 543, 666, 927, 753, 73, 909, 611,
		78, 986, 7, 733, 60, 166, 455, 388, 385, 121, 357, 307, 802, 853, 557, 931, 797, 842, 311, 632,
		413, 444, 930, 859, 340, 856, 475, 261, 498, 448, 378, 780, 15, 928, 838, 623, 720, 709, 54, 402,
		326, 612, 788, 616, 798, 492, 943, 58, 178, 142, 624, 33, 824, 17, 452, 337, 968, 205, 544, 847,
		361, 347, 119, 280, 374, 461, 291, 949, 937, 896, 393, 447, 504, 716, 719, 658, 555, 869, 523, 55,
	}

	testInsertGetDeleteByKey(t, tree, keysToInsert, keysToGet, keysToDelete)
}

func metaTestInsertGetDeleteByKeyHuge(t *testing.T, tree SortedMap) {
	var (
		err          error
		keysToDelete []int
		keysToGet    []int
		keysToInsert []int
	)

	keysToInsert, err = testKnuthShuffledIntSlice(testHugeNumKeys)
	if nil != err {
		t.Fatal(err)
	}
	keysToGet, err = testKnuthShuffledIntSlice(testHugeNumKeys)
	if nil != err {
		t.Fatal(err)
	}
	keysToDelete, err = testKnuthShuffledIntSlice(testHugeNumKeys)
	if nil != err {
		t.Fatal(err)
	}

	testInsertGetDeleteByKey(t, tree, keysToInsert, keysToGet, keysToDelete)
}

func metaTestBisect(t *testing.T, tree SortedMap) {
	var (
		bisectIndex int
		bisectKey   int
		err         error
		found       bool
		key         int
		keyAsKey    Key
		keyExpected int
		keyIndex    int
		keyIndices  []int
		ok          bool
		treeLen     int
		value       string
	)

	// Insert testHugeNumKeys keys using ascending odd ints starting at int(1)
	keyIndices, err = testKnuthShuffledIntSlice(testHugeNumKeys)
	if nil != err {
		t.Fatalf("Knuth Shuffle failed: %v", err)
	}

	for _, keyIndex = range keyIndices {
		key = (2 * keyIndex) + 1
		value = strconv.Itoa(key)
		ok, err = tree.Put(key, value)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("Put(%v, \"%v\").ok should have been true", key, value)
		}
	}

	// Verify tree now contains precisely these keys
	treeLen, err = tree.Len()
	if nil != err {
		t.Fatal(err)
	}
	if testHugeNumKeys != treeLen {
		t.Fatalf("Len() returned %v... expected %v", treeLen, testHugeNumKeys)
	}

	for keyIndex = int(0); keyIndex < testHugeNumKeys; keyIndex++ {
		keyExpected = (2 * keyIndex) + 1
		keyAsKey, _, ok, err = tree.GetByIndex(keyIndex)
		if nil != err {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("GetByIndex(%v).ok should have been true", keyIndex)
		}
		key = keyAsKey.(int)
		if key != keyExpected {
			t.Fatalf("GetByIndex(%v).key returned %v... expected %v", keyIndex, key, keyExpected)
		}
	}

	// Verify Bisect{Left|Right}() on each of these keys returns correct index & found == true
	for keyIndex = int(0); keyIndex < testHugeNumKeys; keyIndex++ {
		bisectKey = (2 * keyIndex) + 1

		bisectIndex, found, err = tree.BisectLeft(bisectKey)
		if nil != err {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("BisectLeft(%v).found should have been true", bisectKey)
		}
		if bisectIndex != keyIndex {
			t.Fatalf("BisectLeft(%v).index returned %v... expected %v", bisectKey, bisectIndex, keyIndex)
		}

		bisectIndex, found, err = tree.BisectRight(bisectKey)
		if nil != err {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("BisectRight(%v).found should have been true", bisectKey)
		}
		if bisectIndex != keyIndex {
			t.Fatalf("BisectRight(%v).index returned %v... expected %v", bisectKey, bisectIndex, keyIndex)
		}
	}

	// Verify Bisect{Left|Right}(0) returns correct index & found == false
	bisectIndex, found, err = tree.BisectLeft(0)
	if nil != err {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("BisectLeft(0).found should have been false")
	}
	if bisectIndex != int(-1) {
		t.Fatalf("BisectLeft(0).index returned %v... expected -1", bisectIndex)
	}

	bisectIndex, found, err = tree.BisectRight(0)
	if nil != err {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("BisectRight(0).found should have been false")
	}
	if bisectIndex != 0 {
		t.Fatalf("BisectRight(0).index returned %v... expected 0", bisectIndex)
	}

	// Verify Bisect{Left|Right}() on each of these keys plus 1 (the following even ints) returns correct index & found == false
	for keyIndex = int(0); keyIndex < testHugeNumKeys; keyIndex++ {
		bisectKey = (2 * keyIndex) + 1 + 1

		bisectIndex, found, err = tree.BisectLeft(bisectKey)
		if nil != err {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("BisectLeft(%v).found should have been false", bisectKey)
		}
		if bisectIndex != keyIndex {
			t.Fatalf("BisectLeft(%v).index returned %v... expected %v", bisectKey, bisectIndex, keyIndex)
		}

		bisectIndex, found, err = tree.BisectRight(bisectKey)
		if nil != err {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("BisectRight(%v).found should have been false", bisectKey)
		}
		if bisectIndex != (keyIndex + 1) {
			t.Fatalf("BisectRight(%v).index returned %v... expected %v", bisectKey, bisectIndex, keyIndex)
		}
	}
}

func metaBenchmarkPutStep(b *testing.B, tree SortedMap, keysToPut []int) {
	var (
		err           error
		keyToPut      int
		ok            bool
		valueAsString string
	)

	for _, keyToPut = range keysToPut {
		valueAsString = strconv.Itoa(keyToPut)
		ok, err = tree.Put(keyToPut, valueAsString)
		if nil != err {
			err = fmt.Errorf("Put() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !ok {
			err = fmt.Errorf("Put().ok should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkGetByIndexStep(b *testing.B, tree SortedMap, indicesToGet []int) {
	var (
		err        error
		indexToGet int
		ok         bool
	)

	for _, indexToGet = range indicesToGet {
		_, _, ok, err = tree.GetByIndex(indexToGet)
		if nil != err {
			err = fmt.Errorf("GetByIndex() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !ok {
			err = fmt.Errorf("GetByIndex().ok should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkPatchByIndexStep(b *testing.B, tree SortedMap, indicesToPatch []int) {
	var (
		err           error
		indexToPatch  int
		ok            bool
		valueAsString string
	)

	for _, indexToPatch = range indicesToPatch {
		valueAsString = strconv.Itoa(indexToPatch)
		ok, err = tree.PatchByIndex(indexToPatch, valueAsString)
		if nil != err {
			err = fmt.Errorf("PatchByIndex() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !ok {
			err = fmt.Errorf("PatchByIndex().ok should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkDeleteByIndexStep(b *testing.B, tree SortedMap, indicesToDeleteByIndexNormalized []int) {
	var (
		err           error
		indexToDelete int
		ok            bool
	)

	for _, indexToDelete = range indicesToDeleteByIndexNormalized {
		ok, err = tree.DeleteByIndex(indexToDelete)
		if nil != err {
			err = fmt.Errorf("DeleteByIndex() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !ok {
			err = fmt.Errorf("DeleteByIndex().ok should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkGetByKeyStep(b *testing.B, tree SortedMap, keysToGet []int) {
	var (
		err      error
		keyToGet int
		ok       bool
	)

	for _, keyToGet = range keysToGet {
		_, ok, err = tree.GetByKey(keyToGet)
		if nil != err {
			err = fmt.Errorf("GetByKey() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !ok {
			err = fmt.Errorf("GetByKey().ok should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkBisectLeftStep(b *testing.B, tree SortedMap, keysToBisectLeft []int) {
	var (
		err             error
		found           bool
		keyToBisectLeft int
	)

	for _, keyToBisectLeft = range keysToBisectLeft {
		_, found, err = tree.BisectLeft(keyToBisectLeft)
		if nil != err {
			err = fmt.Errorf("BisectLeft() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !found {
			err = fmt.Errorf("BisectLeft().found should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkBisectRightStep(b *testing.B, tree SortedMap, keysToBisectRight []int) {
	var (
		err              error
		found            bool
		keyToBisectRight int
	)

	for _, keyToBisectRight = range keysToBisectRight {
		_, found, err = tree.BisectRight(keyToBisectRight)
		if nil != err {
			err = fmt.Errorf("BisectRight() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !found {
			err = fmt.Errorf("BisectRight().found should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkPatchByKeyStep(b *testing.B, tree SortedMap, keysToPatch []int) {
	var (
		err           error
		keyToPatch    int
		ok            bool
		valueAsString string
	)

	for _, keyToPatch = range keysToPatch {
		valueAsString = strconv.Itoa(keyToPatch)
		ok, err = tree.PatchByKey(keyToPatch, valueAsString)
		if nil != err {
			err = fmt.Errorf("PatchByKey() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !ok {
			err = fmt.Errorf("PatchByKey().ok should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkDeleteByKeyStep(b *testing.B, tree SortedMap, keysToDelete []int) {
	var (
		err         error
		keyToDelete int
		ok          bool
	)

	for _, keyToDelete = range keysToDelete {
		ok, err = tree.DeleteByKey(keyToDelete)
		if nil != err {
			err = fmt.Errorf("DeleteByKey() returned unexpected error: %v", err)
			b.Fatal(err)
		}
		if !ok {
			err = fmt.Errorf("DeleteByKey().ok should have been true")
			b.Fatal(err)
		}
	}
}

func metaBenchmarkPut(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err        error
		keysToPut  []int
		remainingN int
	)

	keysToPut, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkPutStep(b, tree, keysToPut)
		b.StopTimer()
		remainingN -= numKeys
		metaBenchmarkDeleteByKeyStep(b, tree, keysToPut)
		b.StartTimer()
	}
	metaBenchmarkPutStep(b, tree, keysToPut[:remainingN])
}

func metaBenchmarkGetByIndex(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                     error
		indicesToGet            []int
		keysToPutForByIndexAPIs []int
		remainingN              int
	)

	keysToPutForByIndexAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	indicesToGet, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	metaBenchmarkPutStep(b, tree, keysToPutForByIndexAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkGetByIndexStep(b, tree, indicesToGet)
		remainingN -= numKeys
	}
	metaBenchmarkGetByIndexStep(b, tree, indicesToGet[:remainingN])
}

func metaBenchmarkPatchByIndex(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                     error
		indicesToPatch          []int
		keysToPutForByIndexAPIs []int
		remainingN              int
	)

	keysToPutForByIndexAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	indicesToPatch, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	metaBenchmarkPutStep(b, tree, keysToPutForByIndexAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkPatchByIndexStep(b, tree, indicesToPatch)
		remainingN -= numKeys
	}
	metaBenchmarkPatchByIndexStep(b, tree, indicesToPatch[:remainingN])
}

func metaBenchmarkDeleteByIndex(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                                 error
		indicesToDeleteByIndexNormalized    []int
		indicesToDeleteByIndexNotNormalized []int
		keysToPutForByIndexAPIs             []int
		remainingN                          int
	)

	keysToPutForByIndexAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	indicesToDeleteByIndexNotNormalized, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	indicesToDeleteByIndexNormalized = testFetchIndicesToDeleteNormalized(indicesToDeleteByIndexNotNormalized)

	metaBenchmarkPutStep(b, tree, keysToPutForByIndexAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkDeleteByIndexStep(b, tree, indicesToDeleteByIndexNormalized)
		b.StopTimer()
		remainingN -= numKeys
		metaBenchmarkPutStep(b, tree, keysToPutForByIndexAPIs)
		b.StartTimer()
	}
	metaBenchmarkDeleteByIndexStep(b, tree, indicesToDeleteByIndexNormalized[:remainingN])
}

func metaBenchmarkGetByKey(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                   error
		keysToGet             []int
		keysToPutForByKeyAPIs []int
		remainingN            int
	)

	keysToPutForByKeyAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	keysToGet, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	metaBenchmarkPutStep(b, tree, keysToPutForByKeyAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkGetByKeyStep(b, tree, keysToGet)
		remainingN -= numKeys
	}
	metaBenchmarkGetByKeyStep(b, tree, keysToGet[:remainingN])
}

func metaBenchmarkBisectLeft(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                   error
		keysToBisectLeft      []int
		keysToPutForByKeyAPIs []int
		remainingN            int
	)

	keysToPutForByKeyAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	keysToBisectLeft, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	metaBenchmarkPutStep(b, tree, keysToPutForByKeyAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkBisectLeftStep(b, tree, keysToBisectLeft)
		remainingN -= numKeys
	}
	metaBenchmarkBisectLeftStep(b, tree, keysToBisectLeft[:remainingN])
}

func metaBenchmarkBisectRight(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                   error
		keysToBisectRight     []int
		keysToPutForByKeyAPIs []int
		remainingN            int
	)

	keysToPutForByKeyAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	keysToBisectRight, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	metaBenchmarkPutStep(b, tree, keysToPutForByKeyAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkBisectRightStep(b, tree, keysToBisectRight)
		remainingN -= numKeys
	}
	metaBenchmarkBisectRightStep(b, tree, keysToBisectRight[:remainingN])
}

func metaBenchmarkPatchByKey(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                   error
		keysToPatch           []int
		keysToPutForByKeyAPIs []int
		remainingN            int
	)

	keysToPutForByKeyAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	keysToPatch, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	metaBenchmarkPutStep(b, tree, keysToPutForByKeyAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkPatchByKeyStep(b, tree, keysToPatch)
		remainingN -= numKeys
	}
	metaBenchmarkPatchByKeyStep(b, tree, keysToPatch[:remainingN])
}

func metaBenchmarkDeleteByKey(b *testing.B, tree SortedMap, numKeys int) {
	var (
		err                   error
		keysToDelete          []int
		keysToPutForByKeyAPIs []int
		remainingN            int
	)

	keysToPutForByKeyAPIs, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}
	keysToDelete, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		err = fmt.Errorf("Knuth Shuffle failed: %v", err)
		b.Fatal(err)
	}

	metaBenchmarkPutStep(b, tree, keysToPutForByKeyAPIs)

	remainingN = b.N

	b.ResetTimer()

	for remainingN > numKeys {
		metaBenchmarkDeleteByKeyStep(b, tree, keysToDelete)
		b.StopTimer()
		remainingN -= numKeys
		metaBenchmarkPutStep(b, tree, keysToPutForByKeyAPIs)
		b.StartTimer()
	}
	metaBenchmarkDeleteByKeyStep(b, tree, keysToDelete[:remainingN])
}
