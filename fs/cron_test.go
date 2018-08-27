package fs

import (
	"fmt"
	"testing"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
)

func TestTimeParsing(t *testing.T) {
	tp, _ := time.Parse("09:30", "11:57")
	fmt.Println(tp)
	tn := time.Now()
	tz, off := tn.Zone()
	fmt.Println("tn  ==", tn)
	fmt.Println("tz  ==", tz)
	fmt.Println("off ==", off)
	loc := time.FixedZone("UTC-8", -8*60*60)
	td := time.Date(2009, time.November, 10, 23, 0, 0, 0, loc)
	fmt.Println("td in RFC822                format is:", td.Format(time.RFC822))
	fmt.Println("td in RFC3339               format is:", td.Format(time.RFC3339))
	fmt.Println("tn in RFC822                format is:", tn.Format(time.RFC822))
	fmt.Println("tn in RFC3339               format is:", tn.Format(time.RFC3339))
	fmt.Println("tn in \"2006-01-02T15:04:05\" format is:", tn.Format("2006-01-02T15:04:05"))
	ll, _ := time.LoadLocation("America/New_York")
	fmt.Println("time.LoadLocation(\"America/New_York\") ==", ll)
}

func TestLoadSnapShotPolicy(t *testing.T) {
	var (
		err                error
		snapShotPolicy     *snapShotPolicyStruct
		testConfMap        conf.ConfMap
		testConfMapStrings []string
	)

	// Case 0 - no SnapShotPolicy

	testConfMapStrings = []string{}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 0: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	snapShotPolicy, err = loadSnapShotPolicy(testConfMap, "TestVolume")
	if nil != err {
		t.Fatalf("Case 0: loadSnapShotPolicy(testConfMap, \"TestVolume\") failed: %v", err)
	}

	if nil != snapShotPolicy {
		t.Fatalf("Case 0: loadSnapShotPolicy(testConfMap, \"TestVolume\") returned non-nil snapShotPolicy")
	}

	// Case 1 - SnapShotPolicy with empty ScheduleList and no TimeZone

	testConfMapStrings = []string{
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 1: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	snapShotPolicy, err = loadSnapShotPolicy(testConfMap, "TestVolume")
	if nil != err {
		t.Fatalf("Case 1: loadSnapShotPolicy(testConfMap, \"TestVolume\") failed: %v", err)
	}

	// TODO

	// Case 2 - SnapShotPolicy with empty ScheduleList and empty TimeZone

	testConfMapStrings = []string{
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 2: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	snapShotPolicy, err = loadSnapShotPolicy(testConfMap, "TestVolume")
	if nil != err {
		t.Fatalf("Case 2: loadSnapShotPolicy(testConfMap, \"TestVolume\") failed: %v", err)
	}

	// TODO

	// Case 3 - SnapShotPolicy with empty ScheduleList and TimeZone of "UTC"

	testConfMapStrings = []string{
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=UTC",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 3: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	snapShotPolicy, err = loadSnapShotPolicy(testConfMap, "TestVolume")
	if nil != err {
		t.Fatalf("Case 3: loadSnapShotPolicy(testConfMap, \"TestVolume\") failed: %v", err)
	}

	// TODO

	// Case 4 - SnapShotPolicy with empty ScheduleList and TimeZone of "Local"

	testConfMapStrings = []string{
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=Local",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 4: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	snapShotPolicy, err = loadSnapShotPolicy(testConfMap, "TestVolume")
	if nil != err {
		t.Fatalf("Case 4: loadSnapShotPolicy(testConfMap, \"TestVolume\") failed: %v", err)
	}

	// TODO

	// Case 5 - SnapShotPolicy with exhaustive ScheduleList and a specific TimeZone

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *",
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotSchedule:HourlySnapShotSchedule.CronTab=0 * * * *",
		"SnapShotSchedule:HourlySnapShotSchedule.Keep=23",
		"SnapShotSchedule:DailySnapShotSchedule.CronTab=0 0 * * *",
		"SnapShotSchedule:DailySnapShotSchedule.Keep=6",
		"SnapShotSchedule:WeeklySnapShotSchedule.CronTab=0 0 * * 0",
		"SnapShotSchedule:WeeklySnapShotSchedule.Keep=8",
		"SnapShotSchedule:MonthlySnapShotSchedule.CronTab=0 0 1 * *",
		"SnapShotSchedule:MonthlySnapShotSchedule.Keep=11",
		"SnapShotSchedule:YearlySnapShotSchedule.CronTab=0 0 1 1 *",
		"SnapShotSchedule:YearlySnapShotSchedule.Keep=4",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule,HourlySnapShotSchedule,DailySnapShotSchedule,WeeklySnapShotSchedule,MonthlySnapShotSchedule,YearlySnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=America/Los_Angeles",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 5: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	snapShotPolicy, err = loadSnapShotPolicy(testConfMap, "TestVolume")
	if nil != err {
		t.Fatalf("Case 5: loadSnapShotPolicy(testConfMap, \"TestVolume\") failed: %v", err)
	}

	// TODO
}
