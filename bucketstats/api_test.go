package bucketstats

import (
	"fmt"
	"math/rand"
	"testing"
)

// a structure containing all of the bucketstats statistics types and other
// fields; useful for testing
type allStatTypes struct {
	MyName   string // not a statistic
	bar      int    // also not a statistic
	Total1   Total
	Average1 Average
	Bucket1  BucketLog2Round
	Bucket2  BucketLogRoot2Round
}

func TestTables(t *testing.T) {
	// showDistr(log2RoundIdxTable[:])
	// showDistr(logRoot2RoundIdxTable[:])

	// generate the tables (arrays) for tables.go.  the tables are already
	// there, but this is where they came from.  the stdout could be cut and
	// pasted at the end of tables.go.
	//
	//genLog2Table()
	//genLogRoot2Table()
}

// verify that all of the bucketstats statistics types satisfy the appropriate
// interface (this is really a compile time test; it fails if they don't)
func TestBucketStatsInterfaces(t *testing.T) {
	var (
		Total1       Total
		Average1     Average
		Bucket2      BucketLog2Round
		BucketRoot2  BucketLogRoot2Round
		TotalIface   Totaler
		AverageIface Averager
		BucketIface  Bucketer
	)

	// all the types are Totaler(s)
	TotalIface = &Total1
	TotalIface = &Average1
	TotalIface = &Bucket2
	TotalIface = &BucketRoot2

	// most of the types are also Averager(s)
	AverageIface = &Average1
	AverageIface = &Bucket2
	AverageIface = &BucketRoot2

	// and the bucket types are Bucketer(s)
	BucketIface = &Bucket2
	BucketIface = &BucketRoot2

	// keep the compiler happy by doing something with the local variables
	AverageIface = BucketIface
	TotalIface = AverageIface
	_ = TotalIface
}

func TestRegister(t *testing.T) {

	var (
		testFunc func()
		panicStr string
	)

	// registering a struct with all of the statist types should not panic
	var myStats allStatTypes = allStatTypes{
		Total1:   Total{Name: "mytotaler"},
		Average1: Average{Name: "First_Average"},
		Bucket1:  BucketLog2Round{Name: "bucket_log2"},
		Bucket2:  BucketLogRoot2Round{Name: "bucket_logroot2"},
	}
	Register("main", "myStats", &myStats)

	// unregister-ing and re-register-ing myStats is also fine
	UnRegister("main", "myStats")
	Register("main", "myStats", &myStats)

	// its also OK to unregister stats that don't exist
	UnRegister("main", "neverStats")

	// but registering it twice should panic
	testFunc = func() {
		Register("main", "myStats", &myStats)
	}
	panicStr = catchAPanic(testFunc)
	if panicStr == "" {
		t.Errorf("Register() of \"main\", \"myStats\" twice should have paniced")
	}
	UnRegister("main", "myStats")

	// a statistics group must have at least one of package and group name
	UnRegister("main", "myStats")

	Register("", "myStats", &myStats)
	UnRegister("", "myStats")

	Register("main", "", &myStats)
	UnRegister("main", "")

	testFunc = func() {
		Register("", "", &myStats)
	}
	panicStr = catchAPanic(testFunc)
	if panicStr == "" {
		t.Errorf("Register() of statistics group without a name didn't panic")
	}

	// Registering a struct without any bucketstats statistics is also OK
	emptyStats := struct {
		someInt    int
		someString string
		someFloat  float64
	}{}

	testFunc = func() {
		Register("main", "emptyStats", &emptyStats)
	}
	panicStr = catchAPanic(testFunc)
	if panicStr != "" {
		t.Errorf("Register() of struct without statistics paniced: %s", panicStr)
	}

	// Registering unnamed and uninitialized statistics should name and init
	// them, but not change the name if one is already assigned
	var myStats2 allStatTypes = allStatTypes{}
	Register("main", "myStats2", &myStats2)
	if myStats2.Total1.Name != "Total1" || myStats.Total1.Name != "mytotaler" {
		t.Errorf("After Register() a Totaler name is incorrect '%s' or '%s'",
			myStats2.Total1.Name, myStats.Total1.Name)
	}
	if myStats2.Average1.Name != "Average1" || myStats.Average1.Name != "First_Average" {
		t.Errorf("After Register() an Average name is incorrect '%s' or '%s'",
			myStats2.Average1.Name, myStats.Average1.Name)
	}
	if myStats2.Bucket1.Name != "Bucket1" || myStats.Bucket1.Name != "bucket_log2" {
		t.Errorf("After Register() an Average name is incorrect '%s' or '%s'",
			myStats2.Bucket1.Name, myStats.Bucket1.Name)
	}
	if myStats2.Bucket2.Name != "Bucket2" || myStats.Bucket2.Name != "bucket_logroot2" {
		t.Errorf("After Register() an Average name is incorrect '%s' or '%s'",
			myStats2.Bucket2.Name, myStats.Bucket2.Name)
	}
	// (values are somewhat arbitrary and can change)
	if myStats2.Bucket1.NBucket != 65 || myStats2.Bucket2.NBucket != 128 {
		t.Errorf("After Register() NBucket was not initialized got %d and %d",
			myStats2.Bucket1.NBucket, myStats2.Bucket2.NBucket)
	}
	UnRegister("main", "myStats2")

	// try with minimal number of buckets
	var myStats3 allStatTypes = allStatTypes{
		Bucket1: BucketLog2Round{NBucket: 1},
		Bucket2: BucketLogRoot2Round{NBucket: 1},
	}
	Register("main", "myStats3", &myStats3)
	// (minimum number of buckets is somewhat arbitrary and may change)
	if myStats3.Bucket1.NBucket != 10 || myStats3.Bucket2.NBucket != 17 {
		t.Errorf("After Register() NBucket was not initialized got %d and %d",
			myStats3.Bucket1.NBucket, myStats3.Bucket2.NBucket)
	}
	UnRegister("main", "myStats3")

	// two fields with the same name ("Average1") will panic
	var myStats4 allStatTypes = allStatTypes{
		Total1:   Total{Name: "mytotaler"},
		Average1: Average{},
		Bucket1:  BucketLog2Round{Name: "Average1"},
	}
	testFunc = func() {
		Register("main", "myStats4", &myStats4)
	}
	panicStr = catchAPanic(testFunc)
	if panicStr == "" {
		t.Errorf("Register() of struct with duplicate field names should panic")
	}

	// verify illegal characters in names are replaced with underscore ('_')
	var myStats5 allStatTypes = allStatTypes{
		Total1:   Total{Name: "my bogus totaler name"},
		Average1: Average{Name: "you*can't*put*splat*in*a*name"},
		Bucket1:  BucketLog2Round{Name: "and you can't use spaces either"},
		Bucket2:  BucketLogRoot2Round{Name: ":colon #sharp \nNewline \tTab \bBackspace!bad"},
	}
	Register("m*a:i#n", "m y s t a t s 5", &myStats5)

	if myStats5.Total1.Name != "my_bogus_totaler_name" {
		t.Errorf("Register() did not replace illegal characters in Total1")
	}
	if myStats5.Average1.Name != "you_can't_put_splat_in_a_name" {
		t.Errorf("Register() did not replace illegal characters in Average1")
	}
	if myStats5.Bucket1.Name != "and_you_can't_use_spaces_either" {
		t.Errorf("Register() did not replace illegal characters in Bucket1")
	}
	if myStats5.Bucket2.Name != "_colon__sharp__Newline__Tab__Backspace!bad" {
		t.Errorf("Register() did not replace illegal characters in Bucket2")
	}

	// verify it was registered with scrubbed name
	var statsString string
	statsString = SprintStats(StatsFormatHumanReadable, "m_a_i_n", "m_y_s_t_a_t_s_5")
	if statsString == "" {
		t.Errorf("SprintStats() of '%s' '%s' did not find mystats5", "m_a_i_n", "m_y_s_t_a_t_s_5")
	}

	// but it can also be printed with the bogus name
	statsString = SprintStats(StatsFormatHumanReadable, "m*a:i#n", "m y s t a t s 5")
	if statsString == "" {
		t.Errorf("SprintStats() of '%s' '%s' did not find mystats5", "m*a:i#n", "m y s t a t s 5")
	}
	UnRegister("m*a:i#n", "m y s t a t s 5")
}

// All of the bucketstats statistics are Totaler(s); test them
func TestTotaler(t *testing.T) {
	var (
		totaler         Totaler
		totalerGroup    allStatTypes = allStatTypes{}
		totalerGroupMap map[string]Totaler
		name            string
		total           uint64
	)

	totalerGroupMap = map[string]Totaler{
		"Total":          &totalerGroup.Total1,
		"Average":        &totalerGroup.Average1,
		"BucketLog2":     &totalerGroup.Bucket1,
		"BucketLogRoot2": &totalerGroup.Bucket2,
	}

	// must be registered (inited) before use
	Register("main", "TotalerStat", &totalerGroup)

	// all totalers should start out at 0
	for name, totaler = range totalerGroupMap {
		if totaler.TotalGet() != 0 {
			t.Errorf("%s started at total %d instead of 0", name, totaler.TotalGet())
		}
	}

	// after incrementing twice they should be 2
	for _, totaler = range totalerGroupMap {
		totaler.Increment()
		totaler.Increment()
	}
	for name, totaler = range totalerGroupMap {
		if totaler.TotalGet() != 2 {
			t.Errorf("%s at total %d instead of 2 after 2 increments", name, totaler.TotalGet())
		}
	}

	// after adding 0 total should still be 2
	for _, totaler = range totalerGroupMap {
		totaler.Add(0)
		totaler.Add(0)
	}
	for name, totaler = range totalerGroupMap {
		if totaler.TotalGet() != 2 {
			t.Errorf("%s got total %d instead of 2 after adding 0", name, totaler.TotalGet())
		}
	}

	// after adding 4 and 8 they must all total to 14
	//
	// (this does not work when adding values larger than 8 where the mean
	// value of buckets for bucketized statistics diverges from the nominal
	// value, i.e. adding 64 will produce totals of 70 for BucketLog2 and 67
	// for BucketLogRoot2 because the meanVal for the bucket 64 is put in
	// are 68 and 65, respectively)
	for _, totaler = range totalerGroupMap {
		totaler.Add(4)
		totaler.Add(8)
	}
	for name, totaler = range totalerGroupMap {
		if totaler.TotalGet() != 14 {
			t.Errorf("%s at total %d instead of 6 after adding 4 and 8", name, totaler.TotalGet())
		}
	}

	// Sprint for each should do something for all stats types
	// (not really making the effort to parse the string)
	for name, totaler = range totalerGroupMap {
		prettyPrint := totaler.Sprint(StatsFormatHumanReadable, "fu", "bar")
		if prettyPrint == "" {
			t.Errorf("%s returned an empty string for its Sprint() method", name)
		}
		fmt.Printf("%s: %s", name, prettyPrint)
	}

	// The Total returned for bucketized statistics will vary depending on
	// the actual numbers used can can be off by more then 33% (Log2) or
	// 17.2% (LogRoot2) in the worst case, and less in the average case.
	//
	// Empirically for 25 million runs using 1024 numbers each the error is
	// no more than 10.0% (Log2 buckets) and 5.0% (LogRoot2 buckets).
	//
	// Run the test 1000 times -- note that go produces the same sequence of
	// "random" numbers each time for the same seed, so statistical variation
	// is not going to cause random test failures.
	var (
		log2RoundErrorPctMax        float64 = 33.3333333333333
		log2RoundErrorPctLikely     float64 = 10
		logRoot2RoundErrorPctMax    float64 = 17.241379310
		logRoot2RoundErrorPctLikely float64 = 5.0
	)

	rand.Seed(2)
	for loop := 0; loop < 1000; loop++ {

		var (
			newTotalerGroup allStatTypes
			errPct          float64
		)

		totalerGroupMap = map[string]Totaler{
			"Total":          &newTotalerGroup.Total1,
			"Average":        &newTotalerGroup.Average1,
			"BucketLog2":     &newTotalerGroup.Bucket1,
			"BucketLogRoot2": &newTotalerGroup.Bucket2,
		}

		// newTotalerGroup must be registered (inited) before use

		UnRegister("main", "TotalerStat")
		Register("main", "TotalerStat", &newTotalerGroup)

		// add 1,0240 random numbers uniformly distributed [0, 6106906623)
		//
		// 6106906623 is RangeHigh for bucket 33 of BucketLog2Round and
		// 5133828095 is RangeHigh for bucket 64 of BucketLogRoot2Round;
		// using 5133828095 makes BucketLogRoot2Round look better and
		// BucketLog2Round look worse.
		total = 0
		for i := 0; i < 1024; i++ {
			randVal := uint64(rand.Int63n(6106906623))
			//randVal := uint64(rand.Int63n(5133828095))

			total += randVal
			for _, totaler = range totalerGroupMap {
				totaler.Add(randVal)
			}
		}

		// validate total for each statistic; barring a run of extremely
		// bad luck we expect the bucket stats will be less then
		// log2RoundErrorPctLikely and logRoot2RoundErrorPctLikely,
		// respectively
		if newTotalerGroup.Total1.TotalGet() != total {
			t.Errorf("Total1 total is %d instead of %d", newTotalerGroup.Total1.TotalGet(), total)
		}
		if newTotalerGroup.Average1.TotalGet() != total {
			t.Errorf("Average1 total is %d instead of %d", newTotalerGroup.Average1.TotalGet(), total)
		}

		errPct = (float64(newTotalerGroup.Bucket1.TotalGet())/float64(total) - 1) * 100
		if errPct > log2RoundErrorPctMax || errPct < -log2RoundErrorPctMax {
			t.Fatalf("BucketLog2Round total exceeds maximum possible error 33%%: "+
				"%d instead of %d  error %1.3f%%",
				newTotalerGroup.Bucket1.TotalGet(), total, errPct)

		}
		if errPct > log2RoundErrorPctLikely || errPct < -log2RoundErrorPctLikely {
			t.Errorf("BucketLog2Round total exceeds maximum likely error: %d instead of %d  error %1.3f%%",
				newTotalerGroup.Bucket1.TotalGet(), total, errPct)
		}

		errPct = (float64(newTotalerGroup.Bucket2.TotalGet())/float64(total) - 1) * 100
		if errPct > logRoot2RoundErrorPctMax || errPct < -logRoot2RoundErrorPctMax {
			t.Fatalf("BucketLogRoot2Round total exceeds maximum possible error 17.2%%: "+
				"%d instead of %d  error %1.3f%%",
				newTotalerGroup.Bucket2.TotalGet(), total, errPct)
		}
		if errPct > logRoot2RoundErrorPctLikely || errPct < -logRoot2RoundErrorPctLikely {
			t.Errorf("BucketLogRoot2Round total exceeds maximum likely error: "+
				"%d instead of %d  error %1.3f%%",
				newTotalerGroup.Bucket2.TotalGet(), total, errPct)
		}

	}

}

func TestSprintStats(t *testing.T) {

	var (
		testFunc func()
		panicStr string
	)

	// sprinting unregistered stats group should panic
	testFunc = func() {
		fmt.Print(SprintStats(StatsFormatHumanReadable, "main", "no-such-stats"))
	}
	panicStr = catchAPanic(testFunc)
	if panicStr == "" {
		t.Errorf("SprintStats() of unregistered statistic group did not panic")
	}
}

// verify that all of the bucketstats statistics types satisfy the appropriate
// interface (this is really a compile time test)
func testBucketStatsInterfaces(t *testing.T) {
	var (
		Total1       Total
		Average1     Average
		Bucket2      BucketLog2Round
		BucketRoot2  BucketLogRoot2Round
		TotalIface   Totaler
		AverageIface Averager
		BucketIface  Bucketer
	)

	// all the types are Totaler(s)
	TotalIface = &Total1
	TotalIface = &Average1
	TotalIface = &Bucket2
	TotalIface = &BucketRoot2

	// most of the types are also Averager(s)
	AverageIface = &Average1
	AverageIface = &Bucket2
	AverageIface = &BucketRoot2

	// and the bucket types are Bucketer(s)
	BucketIface = &Bucket2
	BucketIface = &BucketRoot2

	// keep the compiler happy by doing something with the local variables
	AverageIface = BucketIface
	TotalIface = AverageIface
	_ = TotalIface
}

// Invoke function aFunc, which is expected to panic.  If it does, return the
// value returned by recover() as a string, otherwise return the empty string.
//
// If panic() is called with a nil argument then this function also returns the
// empty string.
//
func catchAPanic(aFunc func()) (panicStr string) {

	defer func() {
		// if recover() returns !nil then return it as a string
		panicVal := recover()
		if panicVal != nil {
			panicStr = fmt.Sprintf("%v", panicVal)
		}
	}()

	aFunc()
	return
}
