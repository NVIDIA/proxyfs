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

	// return index where 2^index is closest to the value (but 0 maps to
	// index 0 and 1 maps to index 1)
	logRoot2Index := func(val int) (logRoot2_x float64, idx uint) {

		logRoot2_x = math.Log(float64(val)) / math.Log(math.Sqrt(2))
		if val <= 1 {
			idx = uint(val)
		} else {
			lowIdx := math.Floor(logRoot2_x)
			highIdx := math.Ceil(logRoot2_x)

			if float64(val)-math.Pow(math.Sqrt(2), lowIdx) <
				math.Pow(math.Sqrt(2), highIdx)-float64(val) {
				idx = uint(lowIdx)
			} else {
				idx = uint(highIdx)
			}
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

	// return index where 2^(index - 1) is closest to the value (but 0 maps
	// to index 0 and other values are shifted by 1, i.e. index-1
	log2Index := func(val int) (log2_x float64, idx uint) {

		log2_x = math.Log(float64(val)) / math.Log(2)
		if val <= 1 {
			idx = uint(val)
		} else {
			lowIdx := math.Floor(log2_x)
			highIdx := math.Ceil(log2_x)

			if float64(val)-math.Pow(2, lowIdx) <
				math.Pow(2, highIdx)-float64(val) {
				idx = uint(lowIdx) + 1
			} else {
				idx = uint(highIdx) + 1
			}
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
	5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6,

	//   32   33   34   35   36   37   38   39   40   41   42   43   44   45   46   47
	//  5.0  5.0  5.1  5.1  5.2  5.2  5.2  5.3  5.3  5.4  5.4  5.4  5.5  5.5  5.5  5.6
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,

	//   48   49   50   51   52   53   54   55   56   57   58   59   60   61   62   63
	//  5.6  5.6  5.6  5.7  5.7  5.7  5.8  5.8  5.8  5.8  5.9  5.9  5.9  5.9  6.0  6.0
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,

	//   64   65   66   67   68   69   70   71   72   73   74   75   76   77   78   79
	//  6.0  6.0  6.0  6.1  6.1  6.1  6.1  6.1  6.2  6.2  6.2  6.2  6.2  6.3  6.3  6.3
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,

	//   80   81   82   83   84   85   86   87   88   89   90   91   92   93   94   95
	//  6.3  6.3  6.4  6.4  6.4  6.4  6.4  6.4  6.5  6.5  6.5  6.5  6.5  6.5  6.6  6.6
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,

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
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,

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
	/*5*/ {RangeLow: 12, RangeHigh: 23, NominalVal: 16, MeanVal: 17},
	/*6*/ {RangeLow: 24, RangeHigh: 47, NominalVal: 32, MeanVal: 35},
	/*7*/ {RangeLow: 48, RangeHigh: 95, NominalVal: 64, MeanVal: 71},
	/*8*/ {RangeLow: 96, RangeHigh: 191, NominalVal: 128, MeanVal: 143},
	/*9*/ {RangeLow: 192, RangeHigh: 383, NominalVal: 256, MeanVal: 287},
	/*10*/ {RangeLow: 384, RangeHigh: 767, NominalVal: 512, MeanVal: 575},
	/*11*/ {RangeLow: 768, RangeHigh: 1535, NominalVal: 1024, MeanVal: 1151},
	/*12*/ {RangeLow: 1536, RangeHigh: 3071, NominalVal: 2048, MeanVal: 2303},
	/*13*/ {RangeLow: 3072, RangeHigh: 6143, NominalVal: 4096, MeanVal: 4607},
	/*14*/ {RangeLow: 6144, RangeHigh: 12287, NominalVal: 8192, MeanVal: 9215},
	/*15*/ {RangeLow: 12288, RangeHigh: 24575, NominalVal: 16384, MeanVal: 18431},
	/*16*/ {RangeLow: 24576, RangeHigh: 49151, NominalVal: 32768, MeanVal: 36863},
	/*17*/ {RangeLow: 49152, RangeHigh: 98303, NominalVal: 65536, MeanVal: 73727},
	/*18*/ {RangeLow: 98304, RangeHigh: 196607, NominalVal: 131072, MeanVal: 147455},
	/*19*/ {RangeLow: 196608, RangeHigh: 393215, NominalVal: 262144, MeanVal: 294911},
	/*20*/ {RangeLow: 393216, RangeHigh: 786431, NominalVal: 524288, MeanVal: 589823},
	/*21*/ {RangeLow: 786432, RangeHigh: 1572863, NominalVal: 1048576, MeanVal: 1179647},
	/*22*/ {RangeLow: 1572864, RangeHigh: 3145727, NominalVal: 2097152, MeanVal: 2359295},
	/*23*/ {RangeLow: 3145728, RangeHigh: 6291455, NominalVal: 4194304, MeanVal: 4718591},
	/*24*/ {RangeLow: 6291456, RangeHigh: 12582911, NominalVal: 8388608, MeanVal: 9437183},
	/*25*/ {RangeLow: 12582912, RangeHigh: 25165823, NominalVal: 16777216, MeanVal: 18874367},
	/*26*/ {RangeLow: 25165824, RangeHigh: 50331647, NominalVal: 33554432, MeanVal: 37748735},
	/*27*/ {RangeLow: 50331648, RangeHigh: 100663295, NominalVal: 67108864, MeanVal: 75497471},
	/*28*/ {RangeLow: 100663296, RangeHigh: 201326591, NominalVal: 134217728, MeanVal: 150994943},
	/*29*/ {RangeLow: 201326592, RangeHigh: 402653183, NominalVal: 268435456, MeanVal: 301989887},
	/*30*/ {RangeLow: 402653184, RangeHigh: 805306367, NominalVal: 536870912, MeanVal: 603979775},
	/*31*/ {RangeLow: 805306368, RangeHigh: 1610612735, NominalVal: 1073741824, MeanVal: 1207959551},
	/*32*/ {RangeLow: 1610612736, RangeHigh: 3221225471, NominalVal: 2147483648, MeanVal: 2415919103},
	/*33*/ {RangeLow: 3221225472, RangeHigh: 6442450943, NominalVal: 4294967296, MeanVal: 4831838207},
	/*34*/ {RangeLow: 6442450944, RangeHigh: 12884901887, NominalVal: 8589934592, MeanVal: 9663676415},
	/*35*/ {RangeLow: 12884901888, RangeHigh: 25769803775, NominalVal: 17179869184, MeanVal: 19327352831},
	/*36*/ {RangeLow: 25769803776, RangeHigh: 51539607551, NominalVal: 34359738368, MeanVal: 38654705663},
	/*37*/ {RangeLow: 51539607552, RangeHigh: 103079215103, NominalVal: 68719476736, MeanVal: 77309411327},
	/*38*/ {RangeLow: 103079215104, RangeHigh: 206158430207, NominalVal: 137438953472, MeanVal: 154618822655},
	/*39*/ {RangeLow: 206158430208, RangeHigh: 412316860415, NominalVal: 274877906944, MeanVal: 309237645311},
	/*40*/ {RangeLow: 412316860416, RangeHigh: 824633720831, NominalVal: 549755813888, MeanVal: 618475290623},
	/*41*/ {RangeLow: 824633720832, RangeHigh: 1649267441663,
		NominalVal: 1099511627776, MeanVal: 1236950581247},
	/*42*/ {RangeLow: 1649267441664, RangeHigh: 3298534883327,
		NominalVal: 2199023255552, MeanVal: 2473901162495},
	/*43*/ {RangeLow: 3298534883328, RangeHigh: 6597069766655,
		NominalVal: 4398046511104, MeanVal: 4947802324991},
	/*44*/ {RangeLow: 6597069766656, RangeHigh: 13194139533311,
		NominalVal: 8796093022208, MeanVal: 9895604649983},
	/*45*/ {RangeLow: 13194139533312, RangeHigh: 26388279066623,
		NominalVal: 17592186044416, MeanVal: 19791209299967},
	/*46*/ {RangeLow: 26388279066624, RangeHigh: 52776558133247,
		NominalVal: 35184372088832, MeanVal: 39582418599935},
	/*47*/ {RangeLow: 52776558133248, RangeHigh: 105553116266495,
		NominalVal: 70368744177664, MeanVal: 79164837199871},
	/*48*/ {RangeLow: 105553116266496, RangeHigh: 211106232532991,
		NominalVal: 140737488355328, MeanVal: 158329674399743},
	/*49*/ {RangeLow: 211106232532992, RangeHigh: 422212465065983,
		NominalVal: 281474976710656, MeanVal: 316659348799487},
	/*50*/ {RangeLow: 422212465065984, RangeHigh: 844424930131967,
		NominalVal: 562949953421312, MeanVal: 633318697598975},
	/*51*/ {RangeLow: 844424930131968, RangeHigh: 1688849860263935,
		NominalVal: 1125899906842624, MeanVal: 1266637395197951},
	/*52*/ {RangeLow: 1688849860263936, RangeHigh: 3377699720527871,
		NominalVal: 2251799813685248, MeanVal: 2533274790395903},
	/*53*/ {RangeLow: 3377699720527872, RangeHigh: 6755399441055743,
		NominalVal: 4503599627370496, MeanVal: 5066549580791807},
	/*54*/ {RangeLow: 6755399441055744, RangeHigh: 13510798882111487,
		NominalVal: 9007199254740992, MeanVal: 10133099161583615},
	/*55*/ {RangeLow: 13510798882111488, RangeHigh: 27021597764222975,
		NominalVal: 18014398509481984, MeanVal: 20266198323167231},
	/*56*/ {RangeLow: 27021597764222976, RangeHigh: 54043195528445951,
		NominalVal: 36028797018963968, MeanVal: 40532396646334463},
	/*57*/ {RangeLow: 54043195528445952, RangeHigh: 108086391056891903,
		NominalVal: 72057594037927936, MeanVal: 81064793292668927},
	/*58*/ {RangeLow: 108086391056891904, RangeHigh: 216172782113783807,
		NominalVal: 144115188075855872, MeanVal: 162129586585337855},
	/*59*/ {RangeLow: 216172782113783808, RangeHigh: 432345564227567615,
		NominalVal: 288230376151711744, MeanVal: 324259173170675711},
	/*60*/ {RangeLow: 432345564227567616, RangeHigh: 864691128455135231,
		NominalVal: 576460752303423488, MeanVal: 648518346341351423},
	/*61*/ {RangeLow: 864691128455135232, RangeHigh: 1729382256910270463,
		NominalVal: 1152921504606846976, MeanVal: 1297036692682702847},
	/*62*/ {RangeLow: 1729382256910270464, RangeHigh: 3458764513820540927,
		NominalVal: 2305843009213693952, MeanVal: 2594073385365405695},
	/*63*/ {RangeLow: 3458764513820540928, RangeHigh: 6917529027641081855,
		NominalVal: 4611686018427387904, MeanVal: 5188146770730811391},
	/*64*/ {RangeLow: 6917529027641081856, RangeHigh: 18446744073709551615,
		NominalVal: 9223372036854775808, MeanVal: 12682136550675316735},
}

var logRoot2RoundIdxTable = [256]uint8{
	//    0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
	// -Inf  0.0  2.0  3.2  4.0  4.6  5.2  5.6  6.0  6.3  6.6  6.9  7.2  7.4  7.6  7.8
	0, 1, 2, 3, 4, 5, 5, 6, 6, 6, 7, 7, 7, 7, 8, 8,

	//   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
	//  8.0  8.2  8.3  8.5  8.6  8.8  8.9  9.0  9.2  9.3  9.4  9.5  9.6  9.7  9.8  9.9
	8, 8, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10,

	//   32   33   34   35   36   37   38   39   40   41   42   43   44   45   46   47
	// 10.0 10.1 10.2 10.3 10.3 10.4 10.5 10.6 10.6 10.7 10.8 10.9 10.9 11.0 11.0 11.1
	10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11,

	//   48   49   50   51   52   53   54   55   56   57   58   59   60   61   62   63
	// 11.2 11.2 11.3 11.3 11.4 11.5 11.5 11.6 11.6 11.7 11.7 11.8 11.8 11.9 11.9 12.0
	11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12,

	//   64   65   66   67   68   69   70   71   72   73   74   75   76   77   78   79
	// 12.0 12.0 12.1 12.1 12.2 12.2 12.3 12.3 12.3 12.4 12.4 12.5 12.5 12.5 12.6 12.6
	12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 13, 13,

	//   80   81   82   83   84   85   86   87   88   89   90   91   92   93   94   95
	// 12.6 12.7 12.7 12.8 12.8 12.8 12.9 12.9 12.9 13.0 13.0 13.0 13.0 13.1 13.1 13.1
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,

	//   96   97   98   99  100  101  102  103  104  105  106  107  108  109  110  111
	// 13.2 13.2 13.2 13.3 13.3 13.3 13.3 13.4 13.4 13.4 13.5 13.5 13.5 13.5 13.6 13.6
	13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 14, 14,

	//  112  113  114  115  116  117  118  119  120  121  122  123  124  125  126  127
	// 13.6 13.6 13.7 13.7 13.7 13.7 13.8 13.8 13.8 13.8 13.9 13.9 13.9 13.9 14.0 14.0
	14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,

	//  128  129  130  131  132  133  134  135  136  137  138  139  140  141  142  143
	// 14.0 14.0 14.0 14.1 14.1 14.1 14.1 14.2 14.2 14.2 14.2 14.2 14.3 14.3 14.3 14.3
	14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,

	//  144  145  146  147  148  149  150  151  152  153  154  155  156  157  158  159
	// 14.3 14.4 14.4 14.4 14.4 14.4 14.5 14.5 14.5 14.5 14.5 14.6 14.6 14.6 14.6 14.6
	14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15,

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
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 16, 16, 16, 16, 16,

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
	/*9*/ {RangeLow: 20, RangeHigh: 27, NominalVal: 23, MeanVal: 23},
	/*10*/ {RangeLow: 28, RangeHigh: 38, NominalVal: 32, MeanVal: 33},
	/*11*/ {RangeLow: 39, RangeHigh: 54, NominalVal: 45, MeanVal: 46},
	/*12*/ {RangeLow: 55, RangeHigh: 77, NominalVal: 64, MeanVal: 66},
	/*13*/ {RangeLow: 78, RangeHigh: 109, NominalVal: 91, MeanVal: 93},
	/*14*/ {RangeLow: 110, RangeHigh: 154, NominalVal: 128, MeanVal: 132},
	/*15*/ {RangeLow: 155, RangeHigh: 218, NominalVal: 181, MeanVal: 186},
	/*16*/ {RangeLow: 219, RangeHigh: 309, NominalVal: 256, MeanVal: 264},
	/*17*/ {RangeLow: 310, RangeHigh: 437, NominalVal: 362, MeanVal: 373},
	/*18*/ {RangeLow: 438, RangeHigh: 619, NominalVal: 512, MeanVal: 528},
	/*19*/ {RangeLow: 620, RangeHigh: 875, NominalVal: 724, MeanVal: 747},
	/*20*/ {RangeLow: 876, RangeHigh: 1239, NominalVal: 1024, MeanVal: 1057},
	/*21*/ {RangeLow: 1240, RangeHigh: 1751, NominalVal: 1448, MeanVal: 1495},
	/*22*/ {RangeLow: 1752, RangeHigh: 2479, NominalVal: 2048, MeanVal: 2115},
	/*23*/ {RangeLow: 2480, RangeHigh: 3503, NominalVal: 2896, MeanVal: 2991},
	/*24*/ {RangeLow: 3504, RangeHigh: 4959, NominalVal: 4096, MeanVal: 4231},
	/*25*/ {RangeLow: 4960, RangeHigh: 7007, NominalVal: 5793, MeanVal: 5983},
	/*26*/ {RangeLow: 7008, RangeHigh: 9919, NominalVal: 8192, MeanVal: 8463},
	/*27*/ {RangeLow: 9920, RangeHigh: 14015, NominalVal: 11585, MeanVal: 11967},
	/*28*/ {RangeLow: 14016, RangeHigh: 19839, NominalVal: 16384, MeanVal: 16927},
	/*29*/ {RangeLow: 19840, RangeHigh: 28031, NominalVal: 23170, MeanVal: 23935},
	/*30*/ {RangeLow: 28032, RangeHigh: 39679, NominalVal: 32768, MeanVal: 33855},
	/*31*/ {RangeLow: 39680, RangeHigh: 56063, NominalVal: 46341, MeanVal: 47871},
	/*32*/ {RangeLow: 56064, RangeHigh: 79359, NominalVal: 65536, MeanVal: 67711},
	/*33*/ {RangeLow: 79360, RangeHigh: 112127, NominalVal: 92682, MeanVal: 95743},
	/*34*/ {RangeLow: 112128, RangeHigh: 158719, NominalVal: 131072, MeanVal: 135423},
	/*35*/ {RangeLow: 158720, RangeHigh: 224255, NominalVal: 185364, MeanVal: 191487},
	/*36*/ {RangeLow: 224256, RangeHigh: 317439, NominalVal: 262144, MeanVal: 270847},
	/*37*/ {RangeLow: 317440, RangeHigh: 448511, NominalVal: 370728, MeanVal: 382975},
	/*38*/ {RangeLow: 448512, RangeHigh: 634879, NominalVal: 524288, MeanVal: 541695},
	/*39*/ {RangeLow: 634880, RangeHigh: 897023, NominalVal: 741455, MeanVal: 765951},
	/*40*/ {RangeLow: 897024, RangeHigh: 1269759, NominalVal: 1048576, MeanVal: 1083391},
	/*41*/ {RangeLow: 1269760, RangeHigh: 1794047, NominalVal: 1482910, MeanVal: 1531903},
	/*42*/ {RangeLow: 1794048, RangeHigh: 2539519, NominalVal: 2097152, MeanVal: 2166783},
	/*43*/ {RangeLow: 2539520, RangeHigh: 3588095, NominalVal: 2965821, MeanVal: 3063807},
	/*44*/ {RangeLow: 3588096, RangeHigh: 5079039, NominalVal: 4194304, MeanVal: 4333567},
	/*45*/ {RangeLow: 5079040, RangeHigh: 7176191, NominalVal: 5931642, MeanVal: 6127615},
	/*46*/ {RangeLow: 7176192, RangeHigh: 10158079, NominalVal: 8388608, MeanVal: 8667135},
	/*47*/ {RangeLow: 10158080, RangeHigh: 14352383, NominalVal: 11863283, MeanVal: 12255231},
	/*48*/ {RangeLow: 14352384, RangeHigh: 20316159, NominalVal: 16777216, MeanVal: 17334271},
	/*49*/ {RangeLow: 20316160, RangeHigh: 28704767, NominalVal: 23726566, MeanVal: 24510463},
	/*50*/ {RangeLow: 28704768, RangeHigh: 40632319, NominalVal: 33554432, MeanVal: 34668543},
	/*51*/ {RangeLow: 40632320, RangeHigh: 57409535, NominalVal: 47453133, MeanVal: 49020927},
	/*52*/ {RangeLow: 57409536, RangeHigh: 81264639, NominalVal: 67108864, MeanVal: 69337087},
	/*53*/ {RangeLow: 81264640, RangeHigh: 114819071, NominalVal: 94906266, MeanVal: 98041855},
	/*54*/ {RangeLow: 114819072, RangeHigh: 162529279, NominalVal: 134217728, MeanVal: 138674175},
	/*55*/ {RangeLow: 162529280, RangeHigh: 229638143, NominalVal: 189812531, MeanVal: 196083711},
	/*56*/ {RangeLow: 229638144, RangeHigh: 325058559, NominalVal: 268435456, MeanVal: 277348351},
	/*57*/ {RangeLow: 325058560, RangeHigh: 459276287, NominalVal: 379625062, MeanVal: 392167423},
	/*58*/ {RangeLow: 459276288, RangeHigh: 650117119, NominalVal: 536870912, MeanVal: 554696703},
	/*59*/ {RangeLow: 650117120, RangeHigh: 918552575, NominalVal: 759250125, MeanVal: 784334847},
	/*60*/ {RangeLow: 918552576, RangeHigh: 1300234239, NominalVal: 1073741824, MeanVal: 1109393407},
	/*61*/ {RangeLow: 1300234240, RangeHigh: 1837105151, NominalVal: 1518500250, MeanVal: 1568669695},
	/*62*/ {RangeLow: 1837105152, RangeHigh: 2600468479, NominalVal: 2147483648, MeanVal: 2218786815},
	/*63*/ {RangeLow: 2600468480, RangeHigh: 3674210303, NominalVal: 3037000500, MeanVal: 3137339391},
	/*64*/ {RangeLow: 3674210304, RangeHigh: 5200936959, NominalVal: 4294967296, MeanVal: 4437573631},
	/*65*/ {RangeLow: 5200936960, RangeHigh: 7348420607, NominalVal: 6074001000, MeanVal: 6274678783},
	/*66*/ {RangeLow: 7348420608, RangeHigh: 10401873919, NominalVal: 8589934592, MeanVal: 8875147263},
	/*67*/ {RangeLow: 10401873920, RangeHigh: 14696841215, NominalVal: 12148002000, MeanVal: 12549357567},
	/*68*/ {RangeLow: 14696841216, RangeHigh: 20803747839, NominalVal: 17179869184, MeanVal: 17750294527},
	/*69*/ {RangeLow: 20803747840, RangeHigh: 29393682431, NominalVal: 24296004000, MeanVal: 25098715135},
	/*70*/ {RangeLow: 29393682432, RangeHigh: 41607495679, NominalVal: 34359738368, MeanVal: 35500589055},
	/*71*/ {RangeLow: 41607495680, RangeHigh: 58787364863, NominalVal: 48592008000, MeanVal: 50197430271},
	/*72*/ {RangeLow: 58787364864, RangeHigh: 83214991359, NominalVal: 68719476736, MeanVal: 71001178111},
	/*73*/ {RangeLow: 83214991360, RangeHigh: 117574729727, NominalVal: 97184015999, MeanVal: 100394860543},
	/*74*/ {RangeLow: 117574729728, RangeHigh: 166429982719, NominalVal: 137438953472, MeanVal: 142002356223},
	/*75*/ {RangeLow: 166429982720, RangeHigh: 235149459455, NominalVal: 194368031998, MeanVal: 200789721087},
	/*76*/ {RangeLow: 235149459456, RangeHigh: 332859965439, NominalVal: 274877906944, MeanVal: 284004712447},
	/*77*/ {RangeLow: 332859965440, RangeHigh: 470298918911, NominalVal: 388736063997, MeanVal: 401579442175},
	/*78*/ {RangeLow: 470298918912, RangeHigh: 665719930879, NominalVal: 549755813888, MeanVal: 568009424895},
	/*79*/ {RangeLow: 665719930880, RangeHigh: 940597837823, NominalVal: 777472127994, MeanVal: 803158884351},
	/*80*/ {RangeLow: 940597837824, RangeHigh: 1331439861759,
		NominalVal: 1099511627776, MeanVal: 1136018849791},
	/*81*/ {RangeLow: 1331439861760, RangeHigh: 1881195675647,
		NominalVal: 1554944255988, MeanVal: 1606317768703},
	/*82*/ {RangeLow: 1881195675648, RangeHigh: 2662879723519,
		NominalVal: 2199023255552, MeanVal: 2272037699583},
	/*83*/ {RangeLow: 2662879723520, RangeHigh: 3762391351295,
		NominalVal: 3109888511975, MeanVal: 3212635537407},
	/*84*/ {RangeLow: 3762391351296, RangeHigh: 5325759447039,
		NominalVal: 4398046511104, MeanVal: 4544075399167},
	/*85*/ {RangeLow: 5325759447040, RangeHigh: 7524782702591,
		NominalVal: 6219777023951, MeanVal: 6425271074815},
	/*86*/ {RangeLow: 7524782702592, RangeHigh: 10651518894079,
		NominalVal: 8796093022208, MeanVal: 9088150798335},
	/*87*/ {RangeLow: 10651518894080, RangeHigh: 15049565405183,
		NominalVal: 12439554047902, MeanVal: 12850542149631},
	/*88*/ {RangeLow: 15049565405184, RangeHigh: 21303037788159,
		NominalVal: 17592186044416, MeanVal: 18176301596671},
	/*89*/ {RangeLow: 21303037788160, RangeHigh: 30099130810367,
		NominalVal: 24879108095804, MeanVal: 25701084299263},
	/*90*/ {RangeLow: 30099130810368, RangeHigh: 42606075576319,
		NominalVal: 35184372088832, MeanVal: 36352603193343},
	/*91*/ {RangeLow: 42606075576320, RangeHigh: 60198261620735,
		NominalVal: 49758216191608, MeanVal: 51402168598527},
	/*92*/ {RangeLow: 60198261620736, RangeHigh: 85212151152639,
		NominalVal: 70368744177664, MeanVal: 72705206386687},
	/*93*/ {RangeLow: 85212151152640, RangeHigh: 120396523241471,
		NominalVal: 99516432383215, MeanVal: 102804337197055},
	/*94*/ {RangeLow: 120396523241472, RangeHigh: 170424302305279,
		NominalVal: 140737488355328, MeanVal: 145410412773375},
	/*95*/ {RangeLow: 170424302305280, RangeHigh: 240793046482943,
		NominalVal: 199032864766430, MeanVal: 205608674394111},
	/*96*/ {RangeLow: 240793046482944, RangeHigh: 340848604610559,
		NominalVal: 281474976710656, MeanVal: 290820825546751},
	/*97*/ {RangeLow: 340848604610560, RangeHigh: 481586092965887,
		NominalVal: 398065729532861, MeanVal: 411217348788223},
	/*98*/ {RangeLow: 481586092965888, RangeHigh: 681697209221119,
		NominalVal: 562949953421312, MeanVal: 581641651093503},
	/*99*/ {RangeLow: 681697209221120, RangeHigh: 963172185931775,
		NominalVal: 796131459065722, MeanVal: 822434697576447},
	/*100*/ {RangeLow: 963172185931776, RangeHigh: 1363394418442239,
		NominalVal: 1125899906842624, MeanVal: 1163283302187007},
	/*101*/ {RangeLow: 1363394418442240, RangeHigh: 1926344371863551,
		NominalVal: 1592262918131443, MeanVal: 1644869395152895},
	/*102*/ {RangeLow: 1926344371863552, RangeHigh: 2726788836884479,
		NominalVal: 2251799813685248, MeanVal: 2326566604374015},
	/*103*/ {RangeLow: 2726788836884480, RangeHigh: 3852688743727103,
		NominalVal: 3184525836262886, MeanVal: 3289738790305791},
	/*104*/ {RangeLow: 3852688743727104, RangeHigh: 5453577673768959,
		NominalVal: 4503599627370496, MeanVal: 4653133208748031},
	/*105*/ {RangeLow: 5453577673768960, RangeHigh: 7705377487454207,
		NominalVal: 6369051672525773, MeanVal: 6579477580611583},
	/*106*/ {RangeLow: 7705377487454208, RangeHigh: 10907155347537919,
		NominalVal: 9007199254740992, MeanVal: 9306266417496063},
	/*107*/ {RangeLow: 10907155347537920, RangeHigh: 15410754974908415,
		NominalVal: 12738103345051545, MeanVal: 13158955161223167},
	/*108*/ {RangeLow: 15410754974908416, RangeHigh: 21814310695075839,
		NominalVal: 18014398509481984, MeanVal: 18612532834992127},
	/*109*/ {RangeLow: 21814310695075840, RangeHigh: 30821509949816831,
		NominalVal: 25476206690103090, MeanVal: 26317910322446335},
	/*110*/ {RangeLow: 30821509949816832, RangeHigh: 43628621390151679,
		NominalVal: 36028797018963968, MeanVal: 37225065669984255},
	/*111*/ {RangeLow: 43628621390151680, RangeHigh: 61643019899633663,
		NominalVal: 50952413380206181, MeanVal: 52635820644892671},
	/*112*/ {RangeLow: 61643019899633664, RangeHigh: 87257242780303359,
		NominalVal: 72057594037927936, MeanVal: 74450131339968511},
	/*113*/ {RangeLow: 87257242780303360, RangeHigh: 123286039799267327,
		NominalVal: 101904826760412361, MeanVal: 105271641289785343},
	/*114*/ {RangeLow: 123286039799267328, RangeHigh: 174514485560606719,
		NominalVal: 144115188075855872, MeanVal: 148900262679937023},
	/*115*/ {RangeLow: 174514485560606720, RangeHigh: 246572079598534655,
		NominalVal: 203809653520824722, MeanVal: 210543282579570687},
	/*116*/ {RangeLow: 246572079598534656, RangeHigh: 349028971121213439,
		NominalVal: 288230376151711744, MeanVal: 297800525359874047},
	/*117*/ {RangeLow: 349028971121213440, RangeHigh: 493144159197069311,
		NominalVal: 407619307041649444, MeanVal: 421086565159141375},
	/*118*/ {RangeLow: 493144159197069312, RangeHigh: 698057942242426879,
		NominalVal: 576460752303423488, MeanVal: 595601050719748095},
	/*119*/ {RangeLow: 698057942242426880, RangeHigh: 986288318394138623,
		NominalVal: 815238614083298888, MeanVal: 842173130318282751},
	/*120*/ {RangeLow: 986288318394138624, RangeHigh: 1396115884484853759,
		NominalVal: 1152921504606846976, MeanVal: 1191202101439496191},
	/*121*/ {RangeLow: 1396115884484853760, RangeHigh: 1972576636788277247,
		NominalVal: 1630477228166597777, MeanVal: 1684346260636565503},
	/*122*/ {RangeLow: 1972576636788277248, RangeHigh: 2792231768969707519,
		NominalVal: 2305843009213693952, MeanVal: 2382404202878992383},
	/*123*/ {RangeLow: 2792231768969707520, RangeHigh: 3945153273576554495,
		NominalVal: 3260954456333195553, MeanVal: 3368692521273131007},
	/*124*/ {RangeLow: 3945153273576554496, RangeHigh: 5584463537939415039,
		NominalVal: 4611686018427387904, MeanVal: 4764808405757984767},
	/*125*/ {RangeLow: 5584463537939415040, RangeHigh: 7890306547153108991,
		NominalVal: 6521908912666391106, MeanVal: 6737385042546262015},
	/*126*/ {RangeLow: 7890306547153108992, RangeHigh: 11168927075878830079,
		NominalVal: 9223372036854775808, MeanVal: 9529616811515969535},
	/*127*/ {RangeLow: 11168927075878830080, RangeHigh: 18446744073709551615,
		NominalVal: 13043817825332782212, MeanVal: 14807835574794190847},
}
