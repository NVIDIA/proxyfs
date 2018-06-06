package bucketstats

import (
	"fmt"
	"testing"
)

// a structure containing all of the bucketstats statistics types and other
// fields; useful for testing
type allStatTypes struct {
	MyName   string // not a statistic
	counter  int    // also not a statistic
	Count1   Count
	Average1 Average
	Bucket1  BucketLog2Round
	Bucket2  BucketLogRoot2Round
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
		Count1       Count
		Average1     Average
		Bucket2      BucketLog2Round
		BucketRoot2  BucketLogRoot2Round
		CountIface   Counter
		AverageIface Averager
		BucketIface  Bucketer
	)

	// all the types are Counter(s)
	CountIface = &Count1
	CountIface = &Average1
	CountIface = &Bucket2
	CountIface = &BucketRoot2

	// most of the types are also Averager(s)
	AverageIface = &Average1
	AverageIface = &Bucket2
	AverageIface = &BucketRoot2

	// and the bucket types are Bucketer(s)
	BucketIface = &Bucket2
	BucketIface = &BucketRoot2

	// keep the compiler happy by doing something with the local variables
	AverageIface = BucketIface
	CountIface = AverageIface
	_ = CountIface
}

func TestRegister(t *testing.T) {

	var (
		testFunc func()
		panicStr string
	)

	// registering a struct with all of the statist types should not panic
	var myStats allStatTypes = allStatTypes{
		Count1:   Count{Name: "mycounter"},
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
	if myStats2.Count1.Name != "Count1" || myStats.Count1.Name != "mycounter" {
		t.Errorf("After Register() a Count name is incorrect '%s' or '%s'",
			myStats2.Count1.Name, myStats.Count1.Name)
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
		Count1:   Count{Name: "mycounter"},
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
		Count1:   Count{Name: "my bogus counter name"},
		Average1: Average{Name: "you*can't*put*splat*in*a*name"},
		Bucket1:  BucketLog2Round{Name: "and you can't use spaces either"},
		Bucket2:  BucketLogRoot2Round{Name: ":colon #sharp \nNewline \tTab \bBackspace!bad"},
	}
	Register("m*a:i#n", "m y s t a t s 5", &myStats5)

	if myStats5.Count1.Name != "my_bogus_counter_name" {
		t.Errorf("Register() did not replace illegal characters in Count1")
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
		Count1       Count
		Average1     Average
		Bucket2      BucketLog2Round
		BucketRoot2  BucketLogRoot2Round
		CountIface   Counter
		AverageIface Averager
		BucketIface  Bucketer
	)

	// all the types are Counter(s)
	CountIface = &Count1
	CountIface = &Average1
	CountIface = &Bucket2
	CountIface = &BucketRoot2

	// most of the types are also Averager(s)
	AverageIface = &Average1
	AverageIface = &Bucket2
	AverageIface = &BucketRoot2

	// and the bucket types are Bucketer(s)
	BucketIface = &Bucket2
	BucketIface = &BucketRoot2

	// keep the compiler happy by doing something with the local variables
	AverageIface = BucketIface
	CountIface = AverageIface
	_ = CountIface
}
