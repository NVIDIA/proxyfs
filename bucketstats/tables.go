// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// The bucketstats Package implements convenient, easy to use, bucketized
// statistics.

package bucketstats

import (
	"fmt"
	"math"
	"math/big"
)

// Tables for bucketized statistics and the code to generate them.  These tables
// map values to bucket indexes and inversely map bucket indexes to the range of
// values they hold.

// generate the tables for BucketStatsLogRoot2
//
func genLogRoot2Table() {

	logRoot2Index := func(val int) (logRoot2_x float64, idx uint) {

		logRoot2_x = math.Log(float64(val)) / math.Log(math.Sqrt(2))
		if logRoot2_x < 0 {
			idx = 0
		} else if logRoot2_x == 0 {
			idx = 1
		} else {
			// other values are rounded to the nearest bucket
			idx = uint(math.Round(logRoot2_x))
		}
		return
	}

	genIdxTable("logRoot2RoundIdxTable", logRoot2Index)
	fmt.Printf("\n")
	genBucketTable("logRoot2RoundBucketTable", logRoot2Index, 128, 2)
	fmt.Printf("\n")
}

// generate the tables for BucketStatsLog2
//
func genLog2Table() {

	log2Index := func(val int) (log2_x float64, idx uint) {

		log2_x = math.Log(float64(val)) / math.Log(2)
		if log2_x < 0 {
			idx = 0
		} else {
			// other values are shifted by 1 bucket
			idx = uint(math.Round(log2_x)) + 1
		}
		return
	}

	genIdxTable("log2RoundIdxTable", log2Index)
	fmt.Printf("\n")
	genBucketTable("log2RoundBucketTable", log2Index, 65, 1)
	fmt.Printf("\n")
}

// Generate go code for an array mapping the integers 0 .. 255 to an bucket
// (index) in a bucketStats array.
//
// indexFunc() is either our tweaked version of log2(x) or logRoot2(x) with
// float64 being the actual value and int being the bucket index its mapped
// to.
//
func genIdxTable(name string, indexFunc func(int) (float64, uint)) {
	var (
		indent  int = 8
		columns int = 16
	)

	fmt.Printf("var %s = [256]uint8{\n", name)
	for i := 0; i < 256; i += columns {

		// print a line with the actual values
		fmt.Printf("%*s//", indent, "")
		for j := 0; j < columns; j += 1 {
			// log_x, idx := indexFunc(i + j)
			fmt.Printf(" %4d", i+j)
		}
		fmt.Printf("\n")

		// print a line with the actual log_2(x) values
		fmt.Printf("%*s//", indent, "")
		for j := 0; j < columns; j += 1 {
			log_x, _ := indexFunc(i + j)
			fmt.Printf(" %4.1f", log_x)
			// fmt.Printf(" %3.0f", i+j)
		}
		fmt.Printf("\n")
		// fmt.Printf("%*s// log_2(%d .. %d)\n", indent, "", int(i), int(i+columns-1))

		// print a line with the corresponding decimal value
		// fmt.Printf("%*s ", indent, "")
		fmt.Printf("%*s ", indent, "")
		for j := 0; j < columns; j += 1 {
			_, idx := indexFunc(i + j)
			fmt.Printf(" %3d,", idx)
		}
		fmt.Printf("\n")
		fmt.Printf("\n")
	}
	fmt.Printf("    }\n")
}

// Generate go code for an array mapping the indexes of a bucketized statistic
// array to the corresponding BucketInfo.
//
// indexFunc() is either our tweaked version of log2(x) or logRoot2(x) for the
// table with float64 being the actual value and int being the bucket index its
// mapped to.
//
func genBucketTable(name string, indexFunc func(int) (float64, uint),
	nBucket uint, bucketsPerBit uint) {

	var (
		indent int = 8
	)

	if bucketsPerBit != 1 && bucketsPerBit != 2 {
		panic(fmt.Sprintf("genBucketTable(): bucketsPerBit must be 1 or 2: bucketsPerBit %d", bucketsPerBit))
	}

	// create the same array that genIdxTable creates, but extend it to
	// 9 bits so we can walk the value of 255 upto the next index change
	var idxTable [512]uint
	for i := 0; i < 256; i += 1 {
		_, idxTable[i] = indexFunc(i)
	}
	for i := 256; i < 512; i += 1 {
		idxTable[i] = idxTable[i>>1] + 1
	}

	fmt.Printf("var %s = [%d]BucketInfo {\n", name, nBucket)
	fmt.Printf("%*s/*0*/ { RangeLow: 0, RangeHigh: 0, NominalVal: 0, MeanVal: 0 },\n",
		indent, "")

	// compute and print BucketInfo for the other buckets
	var rangeHigh uint64 = 0
	for i := uint(1); i < nBucket; i += 1 {

		// start right after the previous entry
		rangeLow := rangeHigh + 1

		// calculate the nominal value of this bucket; use exponent (i - 1)
		// because bucket 0 is used for 0 and that causes subsequent
		// indexes to be offset for all the log base 2 buckets, but with
		// log base sqrt(2) buckets the indexes eventually converge
		var nominal uint64
		if bucketsPerBit == 1 {
			nominal = uint64(1) << (i - 1)
		} else {
			nominal = powRoot2(i)
		}

		// the value for rangeHigh is one less then the value that maps
		// to a new index
		var (
			idxOffset     uint   = 0
			scaledNominal uint64 = nominal
		)
		for scaledNominal >= 256 {
			scaledNominal >>= 1
			idxOffset += bucketsPerBit
		}
		if idxTable[scaledNominal] != i-idxOffset {
			panic(fmt.Sprintf("idxTable[%d] (%d) != i (%d) - idxOffset (%d)",
				scaledNominal, idxTable[scaledNominal], i, idxOffset))
		}

		curIdx := idxTable[scaledNominal]
		nextVal := scaledNominal
		for ; idxTable[nextVal] == curIdx; nextVal += 1 {
		}
		rangeHigh = (nextVal << (idxOffset / bucketsPerBit)) - 1

		// if this is the last bucket then rangeHigh is the end of the
		// range
		if i == nBucket-1 {
			rangeHigh = (1 << 64) - 1
		}

		// calculate meanVal; use big.Int because it may overflow
		var bigMeanVal, tmpInt big.Int
		bigMeanVal.SetUint64(rangeHigh)
		tmpInt.SetUint64(rangeLow)
		bigMeanVal.Add(&bigMeanVal, &tmpInt)
		tmpInt.SetUint64(2)
		bigMeanVal.Div(&bigMeanVal, &tmpInt)
		meanVal := bigMeanVal.Uint64()

		if nominal < 1<<40 {
			fmt.Printf("%*s/*%d*/ { RangeLow: %d, RangeHigh: %d, NominalVal: %d, MeanVal: %d },\n",
				indent, "", i, rangeLow, rangeHigh, nominal, meanVal)
		} else {
			fmt.Printf("%*s/*%d*/ { RangeLow: %d, RangeHigh: %d,\n", indent, "", i, rangeLow, rangeHigh)
			fmt.Printf("%*sNominalVal: %d, MeanVal: %d },\n",
				indent*2, "", nominal, meanVal)
		}
	}
	fmt.Printf("}\n")
}

// Compute round(sqrt(2)^n) for 0 <= n < 128 and return as a uint64 accurate in
// all 64 bits.
//
func powRoot2(n uint) (pow64 uint64) {
	var (
		bigBase  big.Float
		bigPow   big.Float
		bigFudge big.Float
	)
	bigBase.SetPrec(128)
	bigBase.SetInt64(2)
	bigBase.Sqrt(&bigBase)

	bigPow.SetPrec(128)
	bigPow.SetInt64(1)
	for i := uint(1); i <= n; i++ {
		bigPow.Mul(&bigPow, &bigBase)
	}

	// bigPow.Uint64() rounds by truncating toward zero so add 0.500 to get
	// the effect of rounding to the nearest value
	bigFudge.SetFloat64(0.5)
	bigPow.Add(&bigPow, &bigFudge)
	pow64, _ = bigPow.Uint64()
	return
}

// print a list of which bucket the first 256 values go in and the average
// value represented by the bucket
//
func showDistr(bucketTable []uint8) {

	// track info for each bucket
	firstVal := make([]int, 17)
	lastVal := make([]int, 17)
	total := make([]int, 17)
	var lastIdx uint8

	for i := 0; i < 256; i += 1 {
		idx := bucketTable[i]
		if firstVal[idx] == 0 {
			firstVal[idx] = i
		}
		total[idx] += i
		lastVal[idx] = i
		lastIdx = idx
	}

	// don't print the last bucket because the range and average is wrong (capped at 255)
	for i := uint8(0); i < lastIdx-1; i += 1 {
		fmt.Printf("Bucket %2d: %3d..%3d  Average %5.1f\n",
			i, firstVal[i], lastVal[i], float64(total[i])/float64((lastVal[i]-firstVal[i]+1)))
	}
	fmt.Printf("\n")
}

/*
 * -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * Everything below this line is (manually) auto-generated by running genLog2Table()
 * genLogRoot2Table(), except for the comment, which is preserved by hand.
 *
 * If you want to change the tables, change the routines that generate them.
 * -+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 */

// Tables for the computation of log base 2 and log base sqrt(2) for 0 .. 255,
// rounded to the nearest integar, for use as indices into a statistics buckets.
//
// Note that in both tables the entry for 0 is 0 (instead of -Inf) and the entry
// for 1 is 1 (instead of 0).  This means the tables differentiate between
// adding 0 and 1 to a bucketized statistic and precisely track the number of 0
// and 1 values added (the log base sqrt(2) tables also precisely track the
// number of 2, 3, and 4 values added).
//
// One consequence is that the log base 2 statistics require 65 buckets for 64
// bit numbers instead of 64 buckets.
//
var log2RoundIdxTable = [256]uint8{
	//    0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
	// -Inf  0.0  1.0  1.6  2.0  2.3  2.6  2.8  3.0  3.2  3.3  3.5  3.6  3.7  3.8  3.9
	0, 1, 2, 3, 3, 3, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5,

	//   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
	//  4.0  4.1  4.2  4.2  4.3  4.4  4.5  4.5  4.6  4.6  4.7  4.8  4.8  4.9  4.9  5.0
	5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6,

	//   32   33   34   35   36   37   38   39   40   41   42   43   44   45   46   47
	//  5.0  5.0  5.1  5.1  5.2  5.2  5.2  5.3  5.3  5.4  5.4  5.4  5.5  5.5  5.5  5.6
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7,

	//   48   49   50   51   52   53   54   55   56   57   58   59   60   61   62   63
	//  5.6  5.6  5.6  5.7  5.7  5.7  5.8  5.8  5.8  5.8  5.9  5.9  5.9  5.9  6.0  6.0
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,

	//   64   65   66   67   68   69   70   71   72   73   74   75   76   77   78   79
	//  6.0  6.0  6.0  6.1  6.1  6.1  6.1  6.1  6.2  6.2  6.2  6.2  6.2  6.3  6.3  6.3
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,

	//   80   81   82   83   84   85   86   87   88   89   90   91   92   93   94   95
	//  6.3  6.3  6.4  6.4  6.4  6.4  6.4  6.4  6.5  6.5  6.5  6.5  6.5  6.5  6.6  6.6
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8,

	//   96   97   98   99  100  101  102  103  104  105  106  107  108  109  110  111
	//  6.6  6.6  6.6  6.6  6.6  6.7  6.7  6.7  6.7  6.7  6.7  6.7  6.8  6.8  6.8  6.8
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,

	//  112  113  114  115  116  117  118  119  120  121  122  123  124  125  126  127
	//  6.8  6.8  6.8  6.8  6.9  6.9  6.9  6.9  6.9  6.9  6.9  6.9  7.0  7.0  7.0  7.0
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,

	//  128  129  130  131  132  133  134  135  136  137  138  139  140  141  142  143
	//  7.0  7.0  7.0  7.0  7.0  7.1  7.1  7.1  7.1  7.1  7.1  7.1  7.1  7.1  7.1  7.2
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,

	//  144  145  146  147  148  149  150  151  152  153  154  155  156  157  158  159
	//  7.2  7.2  7.2  7.2  7.2  7.2  7.2  7.2  7.2  7.3  7.3  7.3  7.3  7.3  7.3  7.3
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,

	//  160  161  162  163  164  165  166  167  168  169  170  171  172  173  174  175
	//  7.3  7.3  7.3  7.3  7.4  7.4  7.4  7.4  7.4  7.4  7.4  7.4  7.4  7.4  7.4  7.5
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,

	//  176  177  178  179  180  181  182  183  184  185  186  187  188  189  190  191
	//  7.5  7.5  7.5  7.5  7.5  7.5  7.5  7.5  7.5  7.5  7.5  7.5  7.6  7.6  7.6  7.6
	8, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,

	//  192  193  194  195  196  197  198  199  200  201  202  203  204  205  206  207
	//  7.6  7.6  7.6  7.6  7.6  7.6  7.6  7.6  7.6  7.7  7.7  7.7  7.7  7.7  7.7  7.7
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,

	//  208  209  210  211  212  213  214  215  216  217  218  219  220  221  222  223
	//  7.7  7.7  7.7  7.7  7.7  7.7  7.7  7.7  7.8  7.8  7.8  7.8  7.8  7.8  7.8  7.8
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,

	//  224  225  226  227  228  229  230  231  232  233  234  235  236  237  238  239
	//  7.8  7.8  7.8  7.8  7.8  7.8  7.8  7.9  7.9  7.9  7.9  7.9  7.9  7.9  7.9  7.9
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,

	//  240  241  242  243  244  245  246  247  248  249  250  251  252  253  254  255
	//  7.9  7.9  7.9  7.9  7.9  7.9  7.9  7.9  8.0  8.0  8.0  8.0  8.0  8.0  8.0  8.0
	9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
}

var log2RoundBucketTable = [65]BucketInfo{
	/*0*/ {RangeLow: 0, RangeHigh: 0, NominalVal: 0, MeanVal: 0},
	/*1*/ {RangeLow: 1, RangeHigh: 1, NominalVal: 1, MeanVal: 1},
	/*2*/ {RangeLow: 2, RangeHigh: 2, NominalVal: 2, MeanVal: 2},
	/*3*/ {RangeLow: 3, RangeHigh: 5, NominalVal: 4, MeanVal: 4},
	/*4*/ {RangeLow: 6, RangeHigh: 11, NominalVal: 8, MeanVal: 8},
	/*5*/ {RangeLow: 12, RangeHigh: 22, NominalVal: 16, MeanVal: 17},
	/*6*/ {RangeLow: 23, RangeHigh: 45, NominalVal: 32, MeanVal: 34},
	/*7*/ {RangeLow: 46, RangeHigh: 90, NominalVal: 64, MeanVal: 68},
	/*8*/ {RangeLow: 91, RangeHigh: 181, NominalVal: 128, MeanVal: 136},
	/*9*/ {RangeLow: 182, RangeHigh: 363, NominalVal: 256, MeanVal: 272},
	/*10*/ {RangeLow: 364, RangeHigh: 727, NominalVal: 512, MeanVal: 545},
	/*11*/ {RangeLow: 728, RangeHigh: 1455, NominalVal: 1024, MeanVal: 1091},
	/*12*/ {RangeLow: 1456, RangeHigh: 2911, NominalVal: 2048, MeanVal: 2183},
	/*13*/ {RangeLow: 2912, RangeHigh: 5823, NominalVal: 4096, MeanVal: 4367},
	/*14*/ {RangeLow: 5824, RangeHigh: 11647, NominalVal: 8192, MeanVal: 8735},
	/*15*/ {RangeLow: 11648, RangeHigh: 23295, NominalVal: 16384, MeanVal: 17471},
	/*16*/ {RangeLow: 23296, RangeHigh: 46591, NominalVal: 32768, MeanVal: 34943},
	/*17*/ {RangeLow: 46592, RangeHigh: 93183, NominalVal: 65536, MeanVal: 69887},
	/*18*/ {RangeLow: 93184, RangeHigh: 186367, NominalVal: 131072, MeanVal: 139775},
	/*19*/ {RangeLow: 186368, RangeHigh: 372735, NominalVal: 262144, MeanVal: 279551},
	/*20*/ {RangeLow: 372736, RangeHigh: 745471, NominalVal: 524288, MeanVal: 559103},
	/*21*/ {RangeLow: 745472, RangeHigh: 1490943, NominalVal: 1048576, MeanVal: 1118207},
	/*22*/ {RangeLow: 1490944, RangeHigh: 2981887, NominalVal: 2097152, MeanVal: 2236415},
	/*23*/ {RangeLow: 2981888, RangeHigh: 5963775, NominalVal: 4194304, MeanVal: 4472831},
	/*24*/ {RangeLow: 5963776, RangeHigh: 11927551, NominalVal: 8388608, MeanVal: 8945663},
	/*25*/ {RangeLow: 11927552, RangeHigh: 23855103, NominalVal: 16777216, MeanVal: 17891327},
	/*26*/ {RangeLow: 23855104, RangeHigh: 47710207, NominalVal: 33554432, MeanVal: 35782655},
	/*27*/ {RangeLow: 47710208, RangeHigh: 95420415, NominalVal: 67108864, MeanVal: 71565311},
	/*28*/ {RangeLow: 95420416, RangeHigh: 190840831, NominalVal: 134217728, MeanVal: 143130623},
	/*29*/ {RangeLow: 190840832, RangeHigh: 381681663, NominalVal: 268435456, MeanVal: 286261247},
	/*30*/ {RangeLow: 381681664, RangeHigh: 763363327, NominalVal: 536870912, MeanVal: 572522495},
	/*31*/ {RangeLow: 763363328, RangeHigh: 1526726655, NominalVal: 1073741824, MeanVal: 1145044991},
	/*32*/ {RangeLow: 1526726656, RangeHigh: 3053453311, NominalVal: 2147483648, MeanVal: 2290089983},
	/*33*/ {RangeLow: 3053453312, RangeHigh: 6106906623, NominalVal: 4294967296, MeanVal: 4580179967},
	/*34*/ {RangeLow: 6106906624, RangeHigh: 12213813247, NominalVal: 8589934592, MeanVal: 9160359935},
	/*35*/ {RangeLow: 12213813248, RangeHigh: 24427626495, NominalVal: 17179869184, MeanVal: 18320719871},
	/*36*/ {RangeLow: 24427626496, RangeHigh: 48855252991, NominalVal: 34359738368, MeanVal: 36641439743},
	/*37*/ {RangeLow: 48855252992, RangeHigh: 97710505983, NominalVal: 68719476736, MeanVal: 73282879487},
	/*38*/ {RangeLow: 97710505984, RangeHigh: 195421011967, NominalVal: 137438953472, MeanVal: 146565758975},
	/*39*/ {RangeLow: 195421011968, RangeHigh: 390842023935, NominalVal: 274877906944, MeanVal: 293131517951},
	/*40*/ {RangeLow: 390842023936, RangeHigh: 781684047871, NominalVal: 549755813888, MeanVal: 586263035903},
	/*41*/ {RangeLow: 781684047872, RangeHigh: 1563368095743,
		NominalVal: 1099511627776, MeanVal: 1172526071807},
	/*42*/ {RangeLow: 1563368095744, RangeHigh: 3126736191487,
		NominalVal: 2199023255552, MeanVal: 2345052143615},
	/*43*/ {RangeLow: 3126736191488, RangeHigh: 6253472382975,
		NominalVal: 4398046511104, MeanVal: 4690104287231},
	/*44*/ {RangeLow: 6253472382976, RangeHigh: 12506944765951,
		NominalVal: 8796093022208, MeanVal: 9380208574463},
	/*45*/ {RangeLow: 12506944765952, RangeHigh: 25013889531903,
		NominalVal: 17592186044416, MeanVal: 18760417148927},
	/*46*/ {RangeLow: 25013889531904, RangeHigh: 50027779063807,
		NominalVal: 35184372088832, MeanVal: 37520834297855},
	/*47*/ {RangeLow: 50027779063808, RangeHigh: 100055558127615,
		NominalVal: 70368744177664, MeanVal: 75041668595711},
	/*48*/ {RangeLow: 100055558127616, RangeHigh: 200111116255231,
		NominalVal: 140737488355328, MeanVal: 150083337191423},
	/*49*/ {RangeLow: 200111116255232, RangeHigh: 400222232510463,
		NominalVal: 281474976710656, MeanVal: 300166674382847},
	/*50*/ {RangeLow: 400222232510464, RangeHigh: 800444465020927,
		NominalVal: 562949953421312, MeanVal: 600333348765695},
	/*51*/ {RangeLow: 800444465020928, RangeHigh: 1600888930041855,
		NominalVal: 1125899906842624, MeanVal: 1200666697531391},
	/*52*/ {RangeLow: 1600888930041856, RangeHigh: 3201777860083711,
		NominalVal: 2251799813685248, MeanVal: 2401333395062783},
	/*53*/ {RangeLow: 3201777860083712, RangeHigh: 6403555720167423,
		NominalVal: 4503599627370496, MeanVal: 4802666790125567},
	/*54*/ {RangeLow: 6403555720167424, RangeHigh: 12807111440334847,
		NominalVal: 9007199254740992, MeanVal: 9605333580251135},
	/*55*/ {RangeLow: 12807111440334848, RangeHigh: 25614222880669695,
		NominalVal: 18014398509481984, MeanVal: 19210667160502271},
	/*56*/ {RangeLow: 25614222880669696, RangeHigh: 51228445761339391,
		NominalVal: 36028797018963968, MeanVal: 38421334321004543},
	/*57*/ {RangeLow: 51228445761339392, RangeHigh: 102456891522678783,
		NominalVal: 72057594037927936, MeanVal: 76842668642009087},
	/*58*/ {RangeLow: 102456891522678784, RangeHigh: 204913783045357567,
		NominalVal: 144115188075855872, MeanVal: 153685337284018175},
	/*59*/ {RangeLow: 204913783045357568, RangeHigh: 409827566090715135,
		NominalVal: 288230376151711744, MeanVal: 307370674568036351},
	/*60*/ {RangeLow: 409827566090715136, RangeHigh: 819655132181430271,
		NominalVal: 576460752303423488, MeanVal: 614741349136072703},
	/*61*/ {RangeLow: 819655132181430272, RangeHigh: 1639310264362860543,
		NominalVal: 1152921504606846976, MeanVal: 1229482698272145407},
	/*62*/ {RangeLow: 1639310264362860544, RangeHigh: 3278620528725721087,
		NominalVal: 2305843009213693952, MeanVal: 2458965396544290815},
	/*63*/ {RangeLow: 3278620528725721088, RangeHigh: 6557241057451442175,
		NominalVal: 4611686018427387904, MeanVal: 4917930793088581631},
	/*64*/ {RangeLow: 6557241057451442176, RangeHigh: 18446744073709551615,
		NominalVal: 9223372036854775808, MeanVal: 12501992565580496895},
}

var logRoot2RoundIdxTable = [256]uint8{
	//    0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
	// -Inf  0.0  2.0  3.2  4.0  4.6  5.2  5.6  6.0  6.3  6.6  6.9  7.2  7.4  7.6  7.8
	0, 1, 2, 3, 4, 5, 5, 6, 6, 6, 7, 7, 7, 7, 8, 8,

	//   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
	//  8.0  8.2  8.3  8.5  8.6  8.8  8.9  9.0  9.2  9.3  9.4  9.5  9.6  9.7  9.8  9.9
	8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10,

	//   32   33   34   35   36   37   38   39   40   41   42   43   44   45   46   47
	// 10.0 10.1 10.2 10.3 10.3 10.4 10.5 10.6 10.6 10.7 10.8 10.9 10.9 11.0 11.0 11.1
	10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11,

	//   48   49   50   51   52   53   54   55   56   57   58   59   60   61   62   63
	// 11.2 11.2 11.3 11.3 11.4 11.5 11.5 11.6 11.6 11.7 11.7 11.8 11.8 11.9 11.9 12.0
	11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,

	//   64   65   66   67   68   69   70   71   72   73   74   75   76   77   78   79
	// 12.0 12.0 12.1 12.1 12.2 12.2 12.3 12.3 12.3 12.4 12.4 12.5 12.5 12.5 12.6 12.6
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 13, 13, 13,

	//   80   81   82   83   84   85   86   87   88   89   90   91   92   93   94   95
	// 12.6 12.7 12.7 12.8 12.8 12.8 12.9 12.9 12.9 13.0 13.0 13.0 13.0 13.1 13.1 13.1
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,

	//   96   97   98   99  100  101  102  103  104  105  106  107  108  109  110  111
	// 13.2 13.2 13.2 13.3 13.3 13.3 13.3 13.4 13.4 13.4 13.5 13.5 13.5 13.5 13.6 13.6
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 14, 14, 14, 14,

	//  112  113  114  115  116  117  118  119  120  121  122  123  124  125  126  127
	// 13.6 13.6 13.7 13.7 13.7 13.7 13.8 13.8 13.8 13.8 13.9 13.9 13.9 13.9 14.0 14.0
	14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,

	//  128  129  130  131  132  133  134  135  136  137  138  139  140  141  142  143
	// 14.0 14.0 14.0 14.1 14.1 14.1 14.1 14.2 14.2 14.2 14.2 14.2 14.3 14.3 14.3 14.3
	14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,

	//  144  145  146  147  148  149  150  151  152  153  154  155  156  157  158  159
	// 14.3 14.4 14.4 14.4 14.4 14.4 14.5 14.5 14.5 14.5 14.5 14.6 14.6 14.6 14.6 14.6
	14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15,

	//  160  161  162  163  164  165  166  167  168  169  170  171  172  173  174  175
	// 14.6 14.7 14.7 14.7 14.7 14.7 14.8 14.8 14.8 14.8 14.8 14.8 14.9 14.9 14.9 14.9
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,

	//  176  177  178  179  180  181  182  183  184  185  186  187  188  189  190  191
	// 14.9 14.9 15.0 15.0 15.0 15.0 15.0 15.0 15.0 15.1 15.1 15.1 15.1 15.1 15.1 15.2
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,

	//  192  193  194  195  196  197  198  199  200  201  202  203  204  205  206  207
	// 15.2 15.2 15.2 15.2 15.2 15.2 15.3 15.3 15.3 15.3 15.3 15.3 15.3 15.4 15.4 15.4
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,

	//  208  209  210  211  212  213  214  215  216  217  218  219  220  221  222  223
	// 15.4 15.4 15.4 15.4 15.5 15.5 15.5 15.5 15.5 15.5 15.5 15.5 15.6 15.6 15.6 15.6
	15, 15, 15, 15, 15, 15, 15, 15, 16, 16, 16, 16, 16, 16, 16, 16,

	//  224  225  226  227  228  229  230  231  232  233  234  235  236  237  238  239
	// 15.6 15.6 15.6 15.7 15.7 15.7 15.7 15.7 15.7 15.7 15.7 15.8 15.8 15.8 15.8 15.8
	16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,

	//  240  241  242  243  244  245  246  247  248  249  250  251  252  253  254  255
	// 15.8 15.8 15.8 15.8 15.9 15.9 15.9 15.9 15.9 15.9 15.9 15.9 16.0 16.0 16.0 16.0
	16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
}

var logRoot2RoundBucketTable = [128]BucketInfo{
	/*0*/ {RangeLow: 0, RangeHigh: 0, NominalVal: 0, MeanVal: 0},
	/*1*/ {RangeLow: 1, RangeHigh: 1, NominalVal: 1, MeanVal: 1},
	/*2*/ {RangeLow: 2, RangeHigh: 2, NominalVal: 2, MeanVal: 2},
	/*3*/ {RangeLow: 3, RangeHigh: 3, NominalVal: 3, MeanVal: 3},
	/*4*/ {RangeLow: 4, RangeHigh: 4, NominalVal: 4, MeanVal: 4},
	/*5*/ {RangeLow: 5, RangeHigh: 6, NominalVal: 6, MeanVal: 5},
	/*6*/ {RangeLow: 7, RangeHigh: 9, NominalVal: 8, MeanVal: 8},
	/*7*/ {RangeLow: 10, RangeHigh: 13, NominalVal: 11, MeanVal: 11},
	/*8*/ {RangeLow: 14, RangeHigh: 19, NominalVal: 16, MeanVal: 16},
	/*9*/ {RangeLow: 20, RangeHigh: 26, NominalVal: 23, MeanVal: 23},
	/*10*/ {RangeLow: 27, RangeHigh: 38, NominalVal: 32, MeanVal: 32},
	/*11*/ {RangeLow: 39, RangeHigh: 53, NominalVal: 45, MeanVal: 46},
	/*12*/ {RangeLow: 54, RangeHigh: 76, NominalVal: 64, MeanVal: 65},
	/*13*/ {RangeLow: 77, RangeHigh: 107, NominalVal: 91, MeanVal: 92},
	/*14*/ {RangeLow: 108, RangeHigh: 152, NominalVal: 128, MeanVal: 130},
	/*15*/ {RangeLow: 153, RangeHigh: 215, NominalVal: 181, MeanVal: 184},
	/*16*/ {RangeLow: 216, RangeHigh: 305, NominalVal: 256, MeanVal: 260},
	/*17*/ {RangeLow: 306, RangeHigh: 431, NominalVal: 362, MeanVal: 368},
	/*18*/ {RangeLow: 432, RangeHigh: 611, NominalVal: 512, MeanVal: 521},
	/*19*/ {RangeLow: 612, RangeHigh: 863, NominalVal: 724, MeanVal: 737},
	/*20*/ {RangeLow: 864, RangeHigh: 1223, NominalVal: 1024, MeanVal: 1043},
	/*21*/ {RangeLow: 1224, RangeHigh: 1727, NominalVal: 1448, MeanVal: 1475},
	/*22*/ {RangeLow: 1728, RangeHigh: 2447, NominalVal: 2048, MeanVal: 2087},
	/*23*/ {RangeLow: 2448, RangeHigh: 3455, NominalVal: 2896, MeanVal: 2951},
	/*24*/ {RangeLow: 3456, RangeHigh: 4895, NominalVal: 4096, MeanVal: 4175},
	/*25*/ {RangeLow: 4896, RangeHigh: 6911, NominalVal: 5793, MeanVal: 5903},
	/*26*/ {RangeLow: 6912, RangeHigh: 9791, NominalVal: 8192, MeanVal: 8351},
	/*27*/ {RangeLow: 9792, RangeHigh: 13823, NominalVal: 11585, MeanVal: 11807},
	/*28*/ {RangeLow: 13824, RangeHigh: 19583, NominalVal: 16384, MeanVal: 16703},
	/*29*/ {RangeLow: 19584, RangeHigh: 27647, NominalVal: 23170, MeanVal: 23615},
	/*30*/ {RangeLow: 27648, RangeHigh: 39167, NominalVal: 32768, MeanVal: 33407},
	/*31*/ {RangeLow: 39168, RangeHigh: 55295, NominalVal: 46341, MeanVal: 47231},
	/*32*/ {RangeLow: 55296, RangeHigh: 78335, NominalVal: 65536, MeanVal: 66815},
	/*33*/ {RangeLow: 78336, RangeHigh: 110591, NominalVal: 92682, MeanVal: 94463},
	/*34*/ {RangeLow: 110592, RangeHigh: 156671, NominalVal: 131072, MeanVal: 133631},
	/*35*/ {RangeLow: 156672, RangeHigh: 221183, NominalVal: 185364, MeanVal: 188927},
	/*36*/ {RangeLow: 221184, RangeHigh: 313343, NominalVal: 262144, MeanVal: 267263},
	/*37*/ {RangeLow: 313344, RangeHigh: 442367, NominalVal: 370728, MeanVal: 377855},
	/*38*/ {RangeLow: 442368, RangeHigh: 626687, NominalVal: 524288, MeanVal: 534527},
	/*39*/ {RangeLow: 626688, RangeHigh: 884735, NominalVal: 741455, MeanVal: 755711},
	/*40*/ {RangeLow: 884736, RangeHigh: 1253375, NominalVal: 1048576, MeanVal: 1069055},
	/*41*/ {RangeLow: 1253376, RangeHigh: 1769471, NominalVal: 1482910, MeanVal: 1511423},
	/*42*/ {RangeLow: 1769472, RangeHigh: 2506751, NominalVal: 2097152, MeanVal: 2138111},
	/*43*/ {RangeLow: 2506752, RangeHigh: 3538943, NominalVal: 2965821, MeanVal: 3022847},
	/*44*/ {RangeLow: 3538944, RangeHigh: 5013503, NominalVal: 4194304, MeanVal: 4276223},
	/*45*/ {RangeLow: 5013504, RangeHigh: 7077887, NominalVal: 5931642, MeanVal: 6045695},
	/*46*/ {RangeLow: 7077888, RangeHigh: 10027007, NominalVal: 8388608, MeanVal: 8552447},
	/*47*/ {RangeLow: 10027008, RangeHigh: 14155775, NominalVal: 11863283, MeanVal: 12091391},
	/*48*/ {RangeLow: 14155776, RangeHigh: 20054015, NominalVal: 16777216, MeanVal: 17104895},
	/*49*/ {RangeLow: 20054016, RangeHigh: 28311551, NominalVal: 23726566, MeanVal: 24182783},
	/*50*/ {RangeLow: 28311552, RangeHigh: 40108031, NominalVal: 33554432, MeanVal: 34209791},
	/*51*/ {RangeLow: 40108032, RangeHigh: 56623103, NominalVal: 47453133, MeanVal: 48365567},
	/*52*/ {RangeLow: 56623104, RangeHigh: 80216063, NominalVal: 67108864, MeanVal: 68419583},
	/*53*/ {RangeLow: 80216064, RangeHigh: 113246207, NominalVal: 94906266, MeanVal: 96731135},
	/*54*/ {RangeLow: 113246208, RangeHigh: 160432127, NominalVal: 134217728, MeanVal: 136839167},
	/*55*/ {RangeLow: 160432128, RangeHigh: 226492415, NominalVal: 189812531, MeanVal: 193462271},
	/*56*/ {RangeLow: 226492416, RangeHigh: 320864255, NominalVal: 268435456, MeanVal: 273678335},
	/*57*/ {RangeLow: 320864256, RangeHigh: 452984831, NominalVal: 379625062, MeanVal: 386924543},
	/*58*/ {RangeLow: 452984832, RangeHigh: 641728511, NominalVal: 536870912, MeanVal: 547356671},
	/*59*/ {RangeLow: 641728512, RangeHigh: 905969663, NominalVal: 759250125, MeanVal: 773849087},
	/*60*/ {RangeLow: 905969664, RangeHigh: 1283457023, NominalVal: 1073741824, MeanVal: 1094713343},
	/*61*/ {RangeLow: 1283457024, RangeHigh: 1811939327, NominalVal: 1518500250, MeanVal: 1547698175},
	/*62*/ {RangeLow: 1811939328, RangeHigh: 2566914047, NominalVal: 2147483648, MeanVal: 2189426687},
	/*63*/ {RangeLow: 2566914048, RangeHigh: 3623878655, NominalVal: 3037000500, MeanVal: 3095396351},
	/*64*/ {RangeLow: 3623878656, RangeHigh: 5133828095, NominalVal: 4294967296, MeanVal: 4378853375},
	/*65*/ {RangeLow: 5133828096, RangeHigh: 7247757311, NominalVal: 6074001000, MeanVal: 6190792703},
	/*66*/ {RangeLow: 7247757312, RangeHigh: 10267656191, NominalVal: 8589934592, MeanVal: 8757706751},
	/*67*/ {RangeLow: 10267656192, RangeHigh: 14495514623, NominalVal: 12148002000, MeanVal: 12381585407},
	/*68*/ {RangeLow: 14495514624, RangeHigh: 20535312383, NominalVal: 17179869184, MeanVal: 17515413503},
	/*69*/ {RangeLow: 20535312384, RangeHigh: 28991029247, NominalVal: 24296004000, MeanVal: 24763170815},
	/*70*/ {RangeLow: 28991029248, RangeHigh: 41070624767, NominalVal: 34359738368, MeanVal: 35030827007},
	/*71*/ {RangeLow: 41070624768, RangeHigh: 57982058495, NominalVal: 48592008000, MeanVal: 49526341631},
	/*72*/ {RangeLow: 57982058496, RangeHigh: 82141249535, NominalVal: 68719476736, MeanVal: 70061654015},
	/*73*/ {RangeLow: 82141249536, RangeHigh: 115964116991, NominalVal: 97184015999, MeanVal: 99052683263},
	/*74*/ {RangeLow: 115964116992, RangeHigh: 164282499071, NominalVal: 137438953472, MeanVal: 140123308031},
	/*75*/ {RangeLow: 164282499072, RangeHigh: 231928233983, NominalVal: 194368031998, MeanVal: 198105366527},
	/*76*/ {RangeLow: 231928233984, RangeHigh: 328564998143, NominalVal: 274877906944, MeanVal: 280246616063},
	/*77*/ {RangeLow: 328564998144, RangeHigh: 463856467967, NominalVal: 388736063997, MeanVal: 396210733055},
	/*78*/ {RangeLow: 463856467968, RangeHigh: 657129996287, NominalVal: 549755813888, MeanVal: 560493232127},
	/*79*/ {RangeLow: 657129996288, RangeHigh: 927712935935, NominalVal: 777472127994, MeanVal: 792421466111},
	/*80*/ {RangeLow: 927712935936, RangeHigh: 1314259992575,
		NominalVal: 1099511627776, MeanVal: 1120986464255},
	/*81*/ {RangeLow: 1314259992576, RangeHigh: 1855425871871,
		NominalVal: 1554944255988, MeanVal: 1584842932223},
	/*82*/ {RangeLow: 1855425871872, RangeHigh: 2628519985151,
		NominalVal: 2199023255552, MeanVal: 2241972928511},
	/*83*/ {RangeLow: 2628519985152, RangeHigh: 3710851743743,
		NominalVal: 3109888511975, MeanVal: 3169685864447},
	/*84*/ {RangeLow: 3710851743744, RangeHigh: 5257039970303,
		NominalVal: 4398046511104, MeanVal: 4483945857023},
	/*85*/ {RangeLow: 5257039970304, RangeHigh: 7421703487487,
		NominalVal: 6219777023951, MeanVal: 6339371728895},
	/*86*/ {RangeLow: 7421703487488, RangeHigh: 10514079940607,
		NominalVal: 8796093022208, MeanVal: 8967891714047},
	/*87*/ {RangeLow: 10514079940608, RangeHigh: 14843406974975,
		NominalVal: 12439554047902, MeanVal: 12678743457791},
	/*88*/ {RangeLow: 14843406974976, RangeHigh: 21028159881215,
		NominalVal: 17592186044416, MeanVal: 17935783428095},
	/*89*/ {RangeLow: 21028159881216, RangeHigh: 29686813949951,
		NominalVal: 24879108095804, MeanVal: 25357486915583},
	/*90*/ {RangeLow: 29686813949952, RangeHigh: 42056319762431,
		NominalVal: 35184372088832, MeanVal: 35871566856191},
	/*91*/ {RangeLow: 42056319762432, RangeHigh: 59373627899903,
		NominalVal: 49758216191608, MeanVal: 50714973831167},
	/*92*/ {RangeLow: 59373627899904, RangeHigh: 84112639524863,
		NominalVal: 70368744177664, MeanVal: 71743133712383},
	/*93*/ {RangeLow: 84112639524864, RangeHigh: 118747255799807,
		NominalVal: 99516432383215, MeanVal: 101429947662335},
	/*94*/ {RangeLow: 118747255799808, RangeHigh: 168225279049727,
		NominalVal: 140737488355328, MeanVal: 143486267424767},
	/*95*/ {RangeLow: 168225279049728, RangeHigh: 237494511599615,
		NominalVal: 199032864766430, MeanVal: 202859895324671},
	/*96*/ {RangeLow: 237494511599616, RangeHigh: 336450558099455,
		NominalVal: 281474976710656, MeanVal: 286972534849535},
	/*97*/ {RangeLow: 336450558099456, RangeHigh: 474989023199231,
		NominalVal: 398065729532861, MeanVal: 405719790649343},
	/*98*/ {RangeLow: 474989023199232, RangeHigh: 672901116198911,
		NominalVal: 562949953421312, MeanVal: 573945069699071},
	/*99*/ {RangeLow: 672901116198912, RangeHigh: 949978046398463,
		NominalVal: 796131459065722, MeanVal: 811439581298687},
	/*100*/ {RangeLow: 949978046398464, RangeHigh: 1345802232397823,
		NominalVal: 1125899906842624, MeanVal: 1147890139398143},
	/*101*/ {RangeLow: 1345802232397824, RangeHigh: 1899956092796927,
		NominalVal: 1592262918131443, MeanVal: 1622879162597375},
	/*102*/ {RangeLow: 1899956092796928, RangeHigh: 2691604464795647,
		NominalVal: 2251799813685248, MeanVal: 2295780278796287},
	/*103*/ {RangeLow: 2691604464795648, RangeHigh: 3799912185593855,
		NominalVal: 3184525836262886, MeanVal: 3245758325194751},
	/*104*/ {RangeLow: 3799912185593856, RangeHigh: 5383208929591295,
		NominalVal: 4503599627370496, MeanVal: 4591560557592575},
	/*105*/ {RangeLow: 5383208929591296, RangeHigh: 7599824371187711,
		NominalVal: 6369051672525773, MeanVal: 6491516650389503},
	/*106*/ {RangeLow: 7599824371187712, RangeHigh: 10766417859182591,
		NominalVal: 9007199254740992, MeanVal: 9183121115185151},
	/*107*/ {RangeLow: 10766417859182592, RangeHigh: 15199648742375423,
		NominalVal: 12738103345051545, MeanVal: 12983033300779007},
	/*108*/ {RangeLow: 15199648742375424, RangeHigh: 21532835718365183,
		NominalVal: 18014398509481984, MeanVal: 18366242230370303},
	/*109*/ {RangeLow: 21532835718365184, RangeHigh: 30399297484750847,
		NominalVal: 25476206690103090, MeanVal: 25966066601558015},
	/*110*/ {RangeLow: 30399297484750848, RangeHigh: 43065671436730367,
		NominalVal: 36028797018963968, MeanVal: 36732484460740607},
	/*111*/ {RangeLow: 43065671436730368, RangeHigh: 60798594969501695,
		NominalVal: 50952413380206181, MeanVal: 51932133203116031},
	/*112*/ {RangeLow: 60798594969501696, RangeHigh: 86131342873460735,
		NominalVal: 72057594037927936, MeanVal: 73464968921481215},
	/*113*/ {RangeLow: 86131342873460736, RangeHigh: 121597189939003391,
		NominalVal: 101904826760412361, MeanVal: 103864266406232063},
	/*114*/ {RangeLow: 121597189939003392, RangeHigh: 172262685746921471,
		NominalVal: 144115188075855872, MeanVal: 146929937842962431},
	/*115*/ {RangeLow: 172262685746921472, RangeHigh: 243194379878006783,
		NominalVal: 203809653520824722, MeanVal: 207728532812464127},
	/*116*/ {RangeLow: 243194379878006784, RangeHigh: 344525371493842943,
		NominalVal: 288230376151711744, MeanVal: 293859875685924863},
	/*117*/ {RangeLow: 344525371493842944, RangeHigh: 486388759756013567,
		NominalVal: 407619307041649444, MeanVal: 415457065624928255},
	/*118*/ {RangeLow: 486388759756013568, RangeHigh: 689050742987685887,
		NominalVal: 576460752303423488, MeanVal: 587719751371849727},
	/*119*/ {RangeLow: 689050742987685888, RangeHigh: 972777519512027135,
		NominalVal: 815238614083298888, MeanVal: 830914131249856511},
	/*120*/ {RangeLow: 972777519512027136, RangeHigh: 1378101485975371775,
		NominalVal: 1152921504606846976, MeanVal: 1175439502743699455},
	/*121*/ {RangeLow: 1378101485975371776, RangeHigh: 1945555039024054271,
		NominalVal: 1630477228166597777, MeanVal: 1661828262499713023},
	/*122*/ {RangeLow: 1945555039024054272, RangeHigh: 2756202971950743551,
		NominalVal: 2305843009213693952, MeanVal: 2350879005487398911},
	/*123*/ {RangeLow: 2756202971950743552, RangeHigh: 3891110078048108543,
		NominalVal: 3260954456333195553, MeanVal: 3323656524999426047},
	/*124*/ {RangeLow: 3891110078048108544, RangeHigh: 5512405943901487103,
		NominalVal: 4611686018427387904, MeanVal: 4701758010974797823},
	/*125*/ {RangeLow: 5512405943901487104, RangeHigh: 7782220156096217087,
		NominalVal: 6521908912666391106, MeanVal: 6647313049998852095},
	/*126*/ {RangeLow: 7782220156096217088, RangeHigh: 11024811887802974207,
		NominalVal: 9223372036854775808, MeanVal: 9403516021949595647},
	/*127*/ {RangeLow: 11024811887802974208, RangeHigh: 18446744073709551615,
		NominalVal: 13043817825332782212, MeanVal: 14735777980756262911},
}
