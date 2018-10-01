package inode

import (
	"testing"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
)

func TestLoadSnapShotPolicy(t *testing.T) {
	var (
		err                error
		testConfMap        conf.ConfMap
		testConfMapStrings []string
		volume             *volumeStruct
	)

	// Case 1 - no SnapShotPolicy

	testConfMapStrings = []string{}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 1: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("Case 1: loadSnapShotPolicy() failed: %v", err)
	}

	if nil != volume.snapShotPolicy {
		t.Fatalf("Case 1: loadSnapShotPolicy() returned non-nil snapShotPolicy")
	}

	// Case 2 - SnapShotPolicy with empty ScheduleList and no TimeZone

	testConfMapStrings = []string{
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 2: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("Case 2: loadSnapShotPolicy() failed: %v", err)
	}

	if nil != volume.snapShotPolicy {
		t.Fatalf("Case 2: loadSnapShotPolicy() returned non-nil snapShotPolicy")
	}

	// Case 3 - SnapShotPolicy with trivial ScheduleList and no TimeZone

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 3: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("Case 3: loadSnapShotPolicy() failed: %v", err)
	}

	if "CommonSnapShotPolicy" != volume.snapShotPolicy.name {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy with unexpected .name")
	}
	if 1 != len(volume.snapShotPolicy.schedule) {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy with unexpected .schedule")
	}
	if "MinutelySnapShotSchedule" != volume.snapShotPolicy.schedule[0].name {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .name")
	}
	if volume.snapShotPolicy.schedule[0].minuteSpecified {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .minuteSpecified")
	}
	if volume.snapShotPolicy.schedule[0].hourSpecified {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .hourSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfMonthSpecified {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].monthSpecified {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfWeekSpecified {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfWeekSpecified")
	}
	if 59 != volume.snapShotPolicy.schedule[0].keep {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .keep")
	}
	if "UTC" != volume.snapShotPolicy.location.String() {
		t.Fatalf("Case 3: loadSnapShotPolicy() returned snapShotPolicy with unexpected .location")
	}

	// Case 4 - SnapShotPolicy with trivial ScheduleList and empty TimeZone

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 4: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("Case 4: loadSnapShotPolicy() failed: %v", err)
	}

	if "CommonSnapShotPolicy" != volume.snapShotPolicy.name {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy with unexpected .name")
	}
	if 1 != len(volume.snapShotPolicy.schedule) {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy with unexpected .schedule")
	}
	if "MinutelySnapShotSchedule" != volume.snapShotPolicy.schedule[0].name {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .name")
	}
	if volume.snapShotPolicy.schedule[0].minuteSpecified {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .minuteSpecified")
	}
	if volume.snapShotPolicy.schedule[0].hourSpecified {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .hourSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfMonthSpecified {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].monthSpecified {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfWeekSpecified {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfWeekSpecified")
	}
	if 59 != volume.snapShotPolicy.schedule[0].keep {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .keep")
	}
	if "UTC" != volume.snapShotPolicy.location.String() {
		t.Fatalf("Case 4: loadSnapShotPolicy() returned snapShotPolicy with unexpected .location")
	}

	// Case 5 - SnapShotPolicy with trivial ScheduleList and TimeZone of "UTC"

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=UTC",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 5: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("Case 5: loadSnapShotPolicy() failed: %v", err)
	}

	if "CommonSnapShotPolicy" != volume.snapShotPolicy.name {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy with unexpected .name")
	}
	if 1 != len(volume.snapShotPolicy.schedule) {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy with unexpected .schedule")
	}
	if "MinutelySnapShotSchedule" != volume.snapShotPolicy.schedule[0].name {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .name")
	}
	if volume.snapShotPolicy.schedule[0].minuteSpecified {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .minuteSpecified")
	}
	if volume.snapShotPolicy.schedule[0].hourSpecified {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .hourSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfMonthSpecified {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].monthSpecified {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfWeekSpecified {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfWeekSpecified")
	}
	if 59 != volume.snapShotPolicy.schedule[0].keep {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .keep")
	}
	if "UTC" != volume.snapShotPolicy.location.String() {
		t.Fatalf("Case 5: loadSnapShotPolicy() returned snapShotPolicy with unexpected .location")
	}

	// Case 6 - SnapShotPolicy with trivial ScheduleList and TimeZone of "Local"

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=Local",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 6: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("Case 6: loadSnapShotPolicy() failed: %v", err)
	}

	if "CommonSnapShotPolicy" != volume.snapShotPolicy.name {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy with unexpected .name")
	}
	if 1 != len(volume.snapShotPolicy.schedule) {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy with unexpected .schedule")
	}
	if "MinutelySnapShotSchedule" != volume.snapShotPolicy.schedule[0].name {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .name")
	}
	if volume.snapShotPolicy.schedule[0].minuteSpecified {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .minuteSpecified")
	}
	if volume.snapShotPolicy.schedule[0].hourSpecified {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .hourSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfMonthSpecified {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].monthSpecified {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfWeekSpecified {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfWeekSpecified")
	}
	if 59 != volume.snapShotPolicy.schedule[0].keep {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .keep")
	}
	if "Local" != volume.snapShotPolicy.location.String() {
		t.Fatalf("Case 6: loadSnapShotPolicy() returned snapShotPolicy with unexpected .location")
	}

	// Case 7 - SnapShotPolicy with exhaustive ScheduleList and a specific TimeZone

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotSchedule:HourlySnapShotSchedule.CronTab=0 * * * *", //   ==> snapShotPolicy.schedule[1]
		"SnapShotSchedule:HourlySnapShotSchedule.Keep=23",
		"SnapShotSchedule:DailySnapShotSchedule.CronTab=0 0 * * *", //    ==> snapShotPolicy.schedule[2]
		"SnapShotSchedule:DailySnapShotSchedule.Keep=6",
		"SnapShotSchedule:WeeklySnapShotSchedule.CronTab=0 0 * * 0", //   ==> snapShotPolicy.schedule[3]
		"SnapShotSchedule:WeeklySnapShotSchedule.Keep=8",
		"SnapShotSchedule:MonthlySnapShotSchedule.CronTab=0 0 1 * *", //  ==> snapShotPolicy.schedule[4]
		"SnapShotSchedule:MonthlySnapShotSchedule.Keep=11",
		"SnapShotSchedule:YearlySnapShotSchedule.CronTab=0 0 1 1 *", //   ==> snapShotPolicy.schedule[5]
		"SnapShotSchedule:YearlySnapShotSchedule.Keep=4",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule,HourlySnapShotSchedule,DailySnapShotSchedule,WeeklySnapShotSchedule,MonthlySnapShotSchedule,YearlySnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=America/Los_Angeles",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("Case 7: conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("Case 7: loadSnapShotPolicy() failed: %v", err)
	}

	if "CommonSnapShotPolicy" != volume.snapShotPolicy.name {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy with unexpected .name")
	}

	if 6 != len(volume.snapShotPolicy.schedule) {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy with unexpected .schedule")
	}

	if "MinutelySnapShotSchedule" != volume.snapShotPolicy.schedule[0].name {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .name")
	}
	if volume.snapShotPolicy.schedule[0].minuteSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .minuteSpecified")
	}
	if volume.snapShotPolicy.schedule[0].hourSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .hourSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfMonthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].monthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[0].dayOfWeekSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .dayOfWeekSpecified")
	}
	if 59 != volume.snapShotPolicy.schedule[0].keep {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[0] with unexpected .keep")
	}

	if "HourlySnapShotSchedule" != volume.snapShotPolicy.schedule[1].name {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .name")
	}
	if !volume.snapShotPolicy.schedule[1].minuteSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .minuteSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[1].minute {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .minute")
	}
	if volume.snapShotPolicy.schedule[1].hourSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .hourSpecified")
	}
	if volume.snapShotPolicy.schedule[1].dayOfMonthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[1].monthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[1].dayOfWeekSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .dayOfWeekSpecified")
	}
	if 23 != volume.snapShotPolicy.schedule[1].keep {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[1] with unexpected .keep")
	}

	if "DailySnapShotSchedule" != volume.snapShotPolicy.schedule[2].name {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .name")
	}
	if !volume.snapShotPolicy.schedule[2].minuteSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .minuteSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[2].minute {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .minute")
	}
	if !volume.snapShotPolicy.schedule[2].hourSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .hourSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[2].hour {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .hour")
	}
	if volume.snapShotPolicy.schedule[2].dayOfMonthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[2].monthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[2].dayOfWeekSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .dayOfWeekSpecified")
	}
	if 6 != volume.snapShotPolicy.schedule[2].keep {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[2] with unexpected .keep")
	}

	if "WeeklySnapShotSchedule" != volume.snapShotPolicy.schedule[3].name {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .name")
	}
	if !volume.snapShotPolicy.schedule[3].minuteSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .minuteSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[3].minute {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .minute")
	}
	if !volume.snapShotPolicy.schedule[3].hourSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .hourSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[3].hour {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .hour")
	}
	if volume.snapShotPolicy.schedule[3].dayOfMonthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .dayOfMonthSpecified")
	}
	if volume.snapShotPolicy.schedule[3].monthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .monthSpecified")
	}
	if !volume.snapShotPolicy.schedule[3].dayOfWeekSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .dayOfWeekSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[3].dayOfWeek {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .dayOfWeek")
	}
	if 8 != volume.snapShotPolicy.schedule[3].keep {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[3] with unexpected .keep")
	}

	if "MonthlySnapShotSchedule" != volume.snapShotPolicy.schedule[4].name {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .name")
	}
	if !volume.snapShotPolicy.schedule[4].minuteSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .minuteSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[4].minute {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .minute")
	}
	if !volume.snapShotPolicy.schedule[4].hourSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .hourSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[4].hour {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .hour")
	}
	if !volume.snapShotPolicy.schedule[4].dayOfMonthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .dayOfMonthSpecified")
	}
	if 1 != volume.snapShotPolicy.schedule[4].dayOfMonth {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .dayOfMonth")
	}
	if volume.snapShotPolicy.schedule[4].monthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .monthSpecified")
	}
	if volume.snapShotPolicy.schedule[4].dayOfWeekSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .dayOfWeekSpecified")
	}
	if 11 != volume.snapShotPolicy.schedule[4].keep {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[4] with unexpected .keep")
	}

	if "YearlySnapShotSchedule" != volume.snapShotPolicy.schedule[5].name {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .name")
	}
	if !volume.snapShotPolicy.schedule[5].minuteSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .minuteSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[5].minute {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .minute")
	}
	if !volume.snapShotPolicy.schedule[5].hourSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .hourSpecified")
	}
	if 0 != volume.snapShotPolicy.schedule[5].hour {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .hour")
	}
	if !volume.snapShotPolicy.schedule[5].dayOfMonthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .dayOfMonthSpecified")
	}
	if 1 != volume.snapShotPolicy.schedule[5].dayOfMonth {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .dayOfMonth")
	}
	if !volume.snapShotPolicy.schedule[5].monthSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .monthSpecified")
	}
	if 1 != volume.snapShotPolicy.schedule[5].month {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .month")
	}
	if volume.snapShotPolicy.schedule[5].dayOfWeekSpecified {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .dayOfWeekSpecified")
	}
	if 4 != volume.snapShotPolicy.schedule[5].keep {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy.schedule[5] with unexpected .keep")
	}

	if "America/Los_Angeles" != volume.snapShotPolicy.location.String() {
		t.Fatalf("Case 7: loadSnapShotPolicy() returned snapShotPolicy with unexpected .location")
	}
}

func TestSnapShotScheduleCompare(t *testing.T) {
	var (
		err                error
		matches            bool
		matchingTime       time.Time
		mismatchingTime    time.Time
		testConfMap        conf.ConfMap
		testConfMapStrings []string
		volume             *volumeStruct
	)

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotSchedule:HourlySnapShotSchedule.CronTab=0 * * * *", //   ==> snapShotPolicy.schedule[1]
		"SnapShotSchedule:HourlySnapShotSchedule.Keep=23",
		"SnapShotSchedule:DailySnapShotSchedule.CronTab=0 0 * * *", //    ==> snapShotPolicy.schedule[2]
		"SnapShotSchedule:DailySnapShotSchedule.Keep=6",
		"SnapShotSchedule:WeeklySnapShotSchedule.CronTab=0 0 * * 0", //   ==> snapShotPolicy.schedule[3]
		"SnapShotSchedule:WeeklySnapShotSchedule.Keep=8",
		"SnapShotSchedule:MonthlySnapShotSchedule.CronTab=0 0 1 * *", //  ==> snapShotPolicy.schedule[4]
		"SnapShotSchedule:MonthlySnapShotSchedule.Keep=11",
		"SnapShotSchedule:YearlySnapShotSchedule.CronTab=0 0 1 1 *", //   ==> snapShotPolicy.schedule[5]
		"SnapShotSchedule:YearlySnapShotSchedule.Keep=4",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule,HourlySnapShotSchedule,DailySnapShotSchedule,WeeklySnapShotSchedule,MonthlySnapShotSchedule,YearlySnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=America/Los_Angeles",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("loadSnapShotPolicy() failed: %v", err)
	}

	matchingTime = time.Date(2017, time.January, 1, 0, 0, 0, 0, volume.snapShotPolicy.location) // A Sunday

	mismatchingTime = time.Date(2017, time.January, 1, 0, 0, 1, 0, volume.snapShotPolicy.location) // +1 second
	matches = volume.snapShotPolicy.schedule[0].compare(matchingTime)
	if !matches {
		t.Fatalf("snapShotPolicy.schedule[0].compare(matchingTime) should have returned true")
	}
	matches = volume.snapShotPolicy.schedule[0].compare(mismatchingTime)
	if matches {
		t.Fatalf("snapShotPolicy.schedule[0].compare(mismatchingTime) should have returned false")
	}

	mismatchingTime = time.Date(2017, time.January, 1, 0, 1, 0, 0, volume.snapShotPolicy.location) // +1 minute
	matches = volume.snapShotPolicy.schedule[1].compare(matchingTime)
	if !matches {
		t.Fatalf("snapShotPolicy.schedule[1].compare(matchingTime) should have returned true")
	}
	matches = volume.snapShotPolicy.schedule[1].compare(mismatchingTime)
	if matches {
		t.Fatalf("snapShotPolicy.schedule[1].compare(mismatchingTime) should have returned false")
	}

	mismatchingTime = time.Date(2017, time.January, 1, 1, 0, 0, 0, volume.snapShotPolicy.location) // +1 hour
	matches = volume.snapShotPolicy.schedule[2].compare(matchingTime)
	if !matches {
		t.Fatalf("snapShotPolicy.schedule[2].compare(matchingTime) should have returned true")
	}
	matches = volume.snapShotPolicy.schedule[2].compare(mismatchingTime)
	if matches {
		t.Fatalf("snapShotPolicy.schedule[2].compare(mismatchingTime) should have returned false")
	}

	mismatchingTime = time.Date(2017, time.January, 2, 0, 0, 0, 0, volume.snapShotPolicy.location) // +1 day
	matches = volume.snapShotPolicy.schedule[3].compare(matchingTime)
	if !matches {
		t.Fatalf("snapShotPolicy.schedule[3].compare(matchingTime) should have returned true")
	}
	matches = volume.snapShotPolicy.schedule[3].compare(mismatchingTime)
	if matches {
		t.Fatalf("snapShotPolicy.schedule[3].compare(mismatchingTime) should have returned false")
	}

	mismatchingTime = time.Date(2017, time.January, 2, 0, 0, 0, 0, volume.snapShotPolicy.location) // A Monday
	matches = volume.snapShotPolicy.schedule[4].compare(matchingTime)
	if !matches {
		t.Fatalf("snapShotPolicy.schedule[4].compare(matchingTime) should have returned true")
	}
	matches = volume.snapShotPolicy.schedule[4].compare(mismatchingTime)
	if matches {
		t.Fatalf("snapShotPolicy.schedule[4].compare(mismatchingTime) should have returned false")
	}

	mismatchingTime = time.Date(2017, time.February, 1, 0, 0, 0, 0, volume.snapShotPolicy.location) // +1 month
	matches = volume.snapShotPolicy.schedule[5].compare(matchingTime)
	if !matches {
		t.Fatalf("snapShotPolicy.schedule[5].compare(matchingTime) should have returned true")
	}
	matches = volume.snapShotPolicy.schedule[5].compare(mismatchingTime)
	if matches {
		t.Fatalf("snapShotPolicy.schedule[5].compare(mismatchingTime) should have returned false")
	}
}

func TestSnapShotScheduleNext(t *testing.T) {
	var (
		err                error
		nextTime           time.Time
		testConfMap        conf.ConfMap
		testConfMapStrings []string
		timeNow            time.Time
		volume             *volumeStruct
	)

	testConfMapStrings = []string{
		"SnapShotSchedule:MinutelySnapShotSchedule.CronTab=* * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:MinutelySnapShotSchedule.Keep=59",
		"SnapShotSchedule:HourlySnapShotSchedule.CronTab=0 * * * *", //   ==> snapShotPolicy.schedule[1]
		"SnapShotSchedule:HourlySnapShotSchedule.Keep=23",
		"SnapShotSchedule:DailySnapShotSchedule.CronTab=0 0 * * *", //    ==> snapShotPolicy.schedule[2]
		"SnapShotSchedule:DailySnapShotSchedule.Keep=6",
		"SnapShotSchedule:WeeklySnapShotSchedule.CronTab=0 0 * * 0", //   ==> snapShotPolicy.schedule[3]
		"SnapShotSchedule:WeeklySnapShotSchedule.Keep=8",
		"SnapShotSchedule:MonthlySnapShotSchedule.CronTab=0 0 1 * *", //  ==> snapShotPolicy.schedule[4]
		"SnapShotSchedule:MonthlySnapShotSchedule.Keep=11",
		"SnapShotSchedule:YearlySnapShotSchedule.CronTab=0 0 1 1 *", //   ==> snapShotPolicy.schedule[5]
		"SnapShotSchedule:YearlySnapShotSchedule.Keep=4",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=MinutelySnapShotSchedule,HourlySnapShotSchedule,DailySnapShotSchedule,WeeklySnapShotSchedule,MonthlySnapShotSchedule,YearlySnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=America/Los_Angeles",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("loadSnapShotPolicy() failed: %v", err)
	}

	timeNow = time.Date(2017, time.January, 1, 0, 0, 0, 0, volume.snapShotPolicy.location) // A Sunday

	nextTime = volume.snapShotPolicy.schedule[0].next(timeNow)
	if !nextTime.Equal(time.Date(2017, time.January, 1, 0, 1, 0, 0, volume.snapShotPolicy.location)) {
		t.Fatalf("snapShotPolicy.schedule[0].next(timeNow) returned unexpected time: %v", nextTime)
	}

	nextTime = volume.snapShotPolicy.schedule[1].next(timeNow)
	if !nextTime.Equal(time.Date(2017, time.January, 1, 1, 0, 0, 0, volume.snapShotPolicy.location)) {
		t.Fatalf("snapShotPolicy.schedule[1].next(timeNow) returned unexpected time: %v", nextTime)
	}

	nextTime = volume.snapShotPolicy.schedule[2].next(timeNow)
	if !nextTime.Equal(time.Date(2017, time.January, 2, 0, 0, 0, 0, volume.snapShotPolicy.location)) {
		t.Fatalf("snapShotPolicy.schedule[2].next(timeNow) returned unexpected time: %v", nextTime)
	}

	nextTime = volume.snapShotPolicy.schedule[3].next(timeNow)
	if !nextTime.Equal(time.Date(2017, time.January, 8, 0, 0, 0, 0, volume.snapShotPolicy.location)) {
		t.Fatalf("snapShotPolicy.schedule[3].next(timeNow) returned unexpected time: %v", nextTime)
	}

	nextTime = volume.snapShotPolicy.schedule[4].next(timeNow)
	if !nextTime.Equal(time.Date(2017, time.February, 1, 0, 0, 0, 0, volume.snapShotPolicy.location)) {
		t.Fatalf("snapShotPolicy.schedule[4].next(timeNow) returned unexpected time: %v", nextTime)
	}

	nextTime = volume.snapShotPolicy.schedule[5].next(timeNow)
	if !nextTime.Equal(time.Date(2018, time.January, 1, 0, 0, 0, 0, volume.snapShotPolicy.location)) {
		t.Fatalf("snapShotPolicy.schedule[5].next(timeNow) returned unexpected time: %v", nextTime)
	}
}

func TestSnapShotPolicyNext(t *testing.T) {
	var (
		err                    error
		testConfMap            conf.ConfMap
		testConfMapStrings     []string
		timeMidnight           time.Time
		timeMidnightNext       time.Time
		timeOhThirtyAM         time.Time
		timeOneFortyFiveAM     time.Time
		timeOneFortyFiveAMNext time.Time
		timeTwoAM              time.Time
		volume                 *volumeStruct
	)

	testConfMapStrings = []string{
		"SnapShotSchedule:HalfPastTheHourSnapShotSchedule.CronTab=30 * * * *", // ==> snapShotPolicy.schedule[0]
		"SnapShotSchedule:HalfPastTheHourSnapShotSchedule.Keep=99",
		"SnapShotSchedule:TwoAMSnapShotSchedule.CronTab=0 2 * * *", //            ==> snapShotPolicy.schedule[1]
		"SnapShotSchedule:TwoAMSnapShotSchedule.Keep=99",
		"SnapShotPolicy:CommonSnapShotPolicy.ScheduleList=HalfPastTheHourSnapShotSchedule,TwoAMSnapShotSchedule",
		"SnapShotPolicy:CommonSnapShotPolicy.TimeZone=America/Los_Angeles",
		"Volume:TestVolume.SnapShotPolicy=CommonSnapShotPolicy",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	volume = &volumeStruct{volumeName: "TestVolume", snapShotPolicy: nil}

	err = volume.loadSnapShotPolicy(testConfMap)
	if nil != err {
		t.Fatalf("loadSnapShotPolicy() failed: %v", err)
	}

	timeMidnight = time.Date(2017, time.January, 1, 0, 0, 0, 0, volume.snapShotPolicy.location)
	timeOhThirtyAM = time.Date(2017, time.January, 1, 0, 30, 0, 0, volume.snapShotPolicy.location)
	timeOneFortyFiveAM = time.Date(2017, time.January, 1, 1, 45, 0, 0, volume.snapShotPolicy.location)
	timeTwoAM = time.Date(2017, time.January, 1, 2, 0, 0, 0, volume.snapShotPolicy.location)

	timeMidnightNext = volume.snapShotPolicy.schedule[0].next(timeMidnight)
	timeOneFortyFiveAMNext = volume.snapShotPolicy.schedule[1].next(timeOneFortyFiveAM)

	if timeMidnightNext != timeOhThirtyAM {
		t.Fatalf("snapShotPolicy.schedule[0].next(timeMidnight) returned unexpected time: %v", timeMidnightNext)
	}
	if timeOneFortyFiveAMNext != timeTwoAM {
		t.Fatalf("snapShotPolicy.schedule[1].next(timeOneFortyFiveAM) returned unexpected time: %v", timeOneFortyFiveAMNext)
	}

	timeMidnightNext = volume.snapShotPolicy.next(timeMidnight)
	timeOneFortyFiveAMNext = volume.snapShotPolicy.next(timeOneFortyFiveAM)

	if timeMidnightNext != timeOhThirtyAM {
		t.Fatalf("snapShotPolicy.next(timeMidnight) returned unexpected time: %v", timeMidnightNext)
	}
	if timeOneFortyFiveAMNext != timeTwoAM {
		t.Fatalf("snapShotPolicy.next(timeOneFortyFiveAM) returned unexpected time: %v", timeOneFortyFiveAMNext)
	}
}
