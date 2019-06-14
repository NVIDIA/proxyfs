package inode

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
)

type snapShotScheduleStruct struct {
	name                string
	policy              *snapShotPolicyStruct
	minuteSpecified     bool
	minute              int // 0-59
	hourSpecified       bool
	hour                int // 0-23
	dayOfMonthSpecified bool
	dayOfMonth          int // 1-31
	monthSpecified      bool
	month               time.Month // 1-12
	dayOfWeekSpecified  bool
	dayOfWeek           time.Weekday // 0-6 (0 == Sunday)
	keep                uint64
	count               uint64 // computed by scanning each time daemon() awakes
}

type snapShotPolicyStruct struct {
	name          string
	volume        *volumeStruct
	schedule      []*snapShotScheduleStruct
	location      *time.Location
	stopChan      chan struct{}
	doneWaitGroup sync.WaitGroup
}

func (vS *volumeStruct) loadSnapShotPolicy(confMap conf.ConfMap) (err error) {
	var (
		cronTabStringSlice          []string
		dayOfMonthAsU64             uint64
		dayOfWeekAsU64              uint64
		hourAsU64                   uint64
		minuteAsU64                 uint64
		monthAsU64                  uint64
		snapShotPolicy              *snapShotPolicyStruct
		snapShotPolicyName          string
		snapShotPolicySectionName   string
		snapShotSchedule            *snapShotScheduleStruct
		snapShotScheduleList        []string
		snapShotScheduleName        string
		snapShotScheduleSectionName string
		timeZone                    string
		volumeSectionName           string
	)

	// Default to no snapShotPolicy found
	vS.snapShotPolicy = nil

	volumeSectionName = "Volume:" + vS.volumeName

	snapShotPolicyName, err = confMap.FetchOptionValueString(volumeSectionName, "SnapShotPolicy")
	if nil != err {
		// Default to setting snapShotPolicy to nil and returning success
		err = nil
		return
	}

	snapShotPolicy = &snapShotPolicyStruct{name: snapShotPolicyName, volume: vS}

	snapShotPolicySectionName = "SnapShotPolicy:" + snapShotPolicyName

	snapShotScheduleList, err = confMap.FetchOptionValueStringSlice(snapShotPolicySectionName, "ScheduleList")
	if nil != err {
		return
	}
	if 0 == len(snapShotScheduleList) {
		// If ScheduleList is empty, set snapShotPolicy to nil and return success
		err = nil
		return
	}
	snapShotPolicy.schedule = make([]*snapShotScheduleStruct, 0, len(snapShotScheduleList))
	for _, snapShotScheduleName = range snapShotScheduleList {
		snapShotScheduleSectionName = "SnapShotSchedule:" + snapShotScheduleName

		snapShotSchedule = &snapShotScheduleStruct{name: snapShotScheduleName, policy: snapShotPolicy}

		cronTabStringSlice, err = confMap.FetchOptionValueStringSlice(snapShotScheduleSectionName, "CronTab")
		if nil != err {
			return
		}
		if 5 != len(cronTabStringSlice) {
			err = fmt.Errorf("%v.CronTab must be a 5 element crontab time specification", snapShotScheduleSectionName)
			return
		}

		if "*" == cronTabStringSlice[0] {
			snapShotSchedule.minuteSpecified = false
		} else {
			snapShotSchedule.minuteSpecified = true

			minuteAsU64, err = strconv.ParseUint(cronTabStringSlice[0], 10, 8)
			if nil != err {
				return
			}
			if 59 < minuteAsU64 {
				err = fmt.Errorf("%v.CronTab[0] must be valid minute (0-59)", snapShotScheduleSectionName)
				return
			}

			snapShotSchedule.minute = int(minuteAsU64)
		}

		if "*" == cronTabStringSlice[1] {
			snapShotSchedule.hourSpecified = false
		} else {
			snapShotSchedule.hourSpecified = true

			hourAsU64, err = strconv.ParseUint(cronTabStringSlice[1], 10, 8)
			if nil != err {
				return
			}
			if 23 < hourAsU64 {
				err = fmt.Errorf("%v.CronTab[1] must be valid hour (0-23)", snapShotScheduleSectionName)
				return
			}

			snapShotSchedule.hour = int(hourAsU64)
		}

		if "*" == cronTabStringSlice[2] {
			snapShotSchedule.dayOfMonthSpecified = false
		} else {
			snapShotSchedule.dayOfMonthSpecified = true

			dayOfMonthAsU64, err = strconv.ParseUint(cronTabStringSlice[2], 10, 8)
			if nil != err {
				return
			}
			if (0 == dayOfMonthAsU64) || (31 < dayOfMonthAsU64) {
				err = fmt.Errorf("%v.CronTab[2] must be valid dayOfMonth (1-31)", snapShotScheduleSectionName)
				return
			}

			snapShotSchedule.dayOfMonth = int(dayOfMonthAsU64)
		}

		if "*" == cronTabStringSlice[3] {
			snapShotSchedule.monthSpecified = false
		} else {
			snapShotSchedule.monthSpecified = true

			monthAsU64, err = strconv.ParseUint(cronTabStringSlice[3], 10, 8)
			if nil != err {
				return
			}
			if (0 == monthAsU64) || (12 < monthAsU64) {
				err = fmt.Errorf("%v.CronTab[3] must be valid month (1-12)", snapShotScheduleSectionName)
				return
			}

			snapShotSchedule.month = time.Month(monthAsU64)
		}

		if "*" == cronTabStringSlice[4] {
			snapShotSchedule.dayOfWeekSpecified = false
		} else {
			snapShotSchedule.dayOfWeekSpecified = true

			dayOfWeekAsU64, err = strconv.ParseUint(cronTabStringSlice[4], 10, 8)
			if nil != err {
				return
			}
			if 6 < dayOfWeekAsU64 {
				err = fmt.Errorf("%v.CronTab[4] must be valid dayOfWeek (0-6)", snapShotScheduleSectionName)
				return
			}

			snapShotSchedule.dayOfWeek = time.Weekday(dayOfWeekAsU64)
		}

		snapShotSchedule.keep, err = confMap.FetchOptionValueUint64(snapShotScheduleSectionName, "Keep")
		if nil != err {
			return
		}

		if snapShotSchedule.dayOfWeekSpecified && (snapShotSchedule.dayOfMonthSpecified || snapShotSchedule.monthSpecified) {
			err = fmt.Errorf("%v.CronTab must not specify DayOfWeek if DayOfMonth and/or Month are specified", snapShotScheduleSectionName)
			return
		}

		snapShotPolicy.schedule = append(snapShotPolicy.schedule, snapShotSchedule)
	}

	timeZone, err = confMap.FetchOptionValueString(snapShotPolicySectionName, "TimeZone")

	if nil == err {
		snapShotPolicy.location, err = time.LoadLocation(timeZone)
		if nil != err {
			return
		}
	} else { // nil != err
		// If not present, default to UTC
		snapShotPolicy.location = time.UTC
	}

	// If we reach here, we've successfully loaded the snapShotPolicy

	vS.snapShotPolicy = snapShotPolicy
	err = nil
	return
}

func (snapShotPolicy *snapShotPolicyStruct) up() {
	snapShotPolicy.stopChan = make(chan struct{}, 1)
	snapShotPolicy.doneWaitGroup.Add(1)
	go snapShotPolicy.daemon()
}

func (snapShotPolicy *snapShotPolicyStruct) down() {
	snapShotPolicy.stopChan <- struct{}{}
	snapShotPolicy.doneWaitGroup.Wait()
}

func (snapShotPolicy *snapShotPolicyStruct) daemon() {
	var (
		err                error
		nextDuration       time.Duration
		nextTime           time.Time
		nextTimePreviously time.Time
		snapShotName       string
		timeNow            time.Time
	)

	nextTimePreviously = time.Date(2000, time.January, 1, 0, 0, 0, 0, snapShotPolicy.location)

	for {
		timeNow = time.Now().In(snapShotPolicy.location)
		nextTime = snapShotPolicy.next(timeNow)
		for {
			if !nextTime.Equal(nextTimePreviously) {
				break
			}
			// We took the last snapshot so quickly, next() returned the same nextTime
			time.Sleep(time.Second)
			timeNow = time.Now().In(snapShotPolicy.location)
			nextTime = snapShotPolicy.next(timeNow)
		}
		nextDuration = nextTime.Sub(timeNow)
		select {
		case _ = <-snapShotPolicy.stopChan:
			snapShotPolicy.doneWaitGroup.Done()
			return
		case <-time.After(nextDuration):
			for time.Now().In(snapShotPolicy.location).Before(nextTime) {
				// If time.After() returned a bit too soon, loop until it is our time
				time.Sleep(100 * time.Millisecond)
			}
			snapShotName = strings.Replace(nextTime.Format(time.RFC3339), ":", ".", -1)
			_, err = snapShotPolicy.volume.SnapShotCreate(snapShotName)
			if nil != err {
				logger.WarnWithError(err)
			}
			snapShotPolicy.prune()
		}
		nextTimePreviously = nextTime
	}
}

func (snapShotPolicy *snapShotPolicyStruct) prune() {
	var (
		err               error
		keep              bool
		matches           bool
		matchesAtLeastOne bool
		snapShot          headhunter.SnapShotStruct
		snapShotList      []headhunter.SnapShotStruct
		snapShotSchedule  *snapShotScheduleStruct
		snapShotTime      time.Time
	)

	// First, zero each snapShotSchedule.count

	for _, snapShotSchedule = range snapShotPolicy.schedule {
		snapShotSchedule.count = 0
	}

	// Now fetch the reverse time-ordered list of snapshots

	snapShotList = snapShotPolicy.volume.headhunterVolumeHandle.SnapShotListByTime(true)

	// Now walk snapShotList looking for snapshots to prune

	for _, snapShot = range snapShotList {
		snapShotTime, err = time.Parse(time.RFC3339, strings.Replace(snapShot.Name, ".", ":", -1))
		if nil != err {
			// SnapShot was not formatted to match a potential SnapShotPolicy/Schedule...skip it
			continue
		}

		// Compare against each snapShotSchedule

		keep = false
		matchesAtLeastOne = false

		for _, snapShotSchedule = range snapShotPolicy.schedule {
			matches = snapShotSchedule.compare(snapShotTime)
			if matches {
				matchesAtLeastOne = true
				snapShotSchedule.count++
				if snapShotSchedule.count <= snapShotSchedule.keep {
					keep = true
				}
			}
		}

		if matchesAtLeastOne && !keep {
			// Although this snapshot "matchesAtLeastOne",
			//   no snapShotSchedule said "keep" it

			err = snapShotPolicy.volume.SnapShotDelete(snapShot.ID)
			if nil != err {
				logger.WarnWithError(err)
			}
		}
	}
}

// thisTime is presumably the snapShotSchedule.policy.location-local parsed snapShotStruct.name
func (snapShotSchedule *snapShotScheduleStruct) compare(thisTime time.Time) (matches bool) {
	var (
		dayOfMonth    int
		dayOfWeek     time.Weekday
		hour          int
		minute        int
		month         time.Month
		truncatedTime time.Time
		year          int
	)

	hour, minute, _ = thisTime.Clock()
	year, month, dayOfMonth = thisTime.Date()
	dayOfWeek = thisTime.Weekday()

	truncatedTime = time.Date(year, month, dayOfMonth, hour, minute, 0, 0, snapShotSchedule.policy.location)
	if !truncatedTime.Equal(thisTime) {
		matches = false
		return
	}

	if snapShotSchedule.minuteSpecified {
		if snapShotSchedule.minute != minute {
			matches = false
			return
		}
	}

	if snapShotSchedule.hourSpecified {
		if snapShotSchedule.hour != hour {
			matches = false
			return
		}
	}

	if snapShotSchedule.dayOfMonthSpecified {
		if snapShotSchedule.dayOfMonth != dayOfMonth {
			matches = false
			return
		}
	}

	if snapShotSchedule.monthSpecified {
		if snapShotSchedule.month != month {
			matches = false
			return
		}
	}

	if snapShotSchedule.dayOfWeekSpecified {
		if snapShotSchedule.dayOfWeek != dayOfWeek {
			matches = false
			return
		}
	}

	// If we make it this far, thisTime matches snapShotSchedule

	matches = true
	return
}

// Since time.Truncate() only truncates with respect to UTC, it is unsafe

func truncateToStartOfMinute(untruncatedTime time.Time, loc *time.Location) (truncatedTime time.Time) {
	var (
		day   int
		hour  int
		min   int
		month time.Month
		year  int
	)

	hour, min, _ = untruncatedTime.Clock()
	year, month, day = untruncatedTime.Date()

	truncatedTime = time.Date(year, month, day, hour, min, 0, 0, loc)

	return
}

func truncateToStartOfHour(untruncatedTime time.Time, loc *time.Location) (truncatedTime time.Time) {
	var (
		day   int
		hour  int
		month time.Month
		year  int
	)

	hour, _, _ = untruncatedTime.Clock()
	year, month, day = untruncatedTime.Date()

	truncatedTime = time.Date(year, month, day, hour, 0, 0, 0, loc)

	return
}

func truncateToStartOfDay(untruncatedTime time.Time, loc *time.Location) (truncatedTime time.Time) {
	var (
		day   int
		month time.Month
		year  int
	)

	year, month, day = untruncatedTime.Date()

	truncatedTime = time.Date(year, month, day, 0, 0, 0, 0, loc)

	return
}

func truncateToStartOfMonth(untruncatedTime time.Time, loc *time.Location) (truncatedTime time.Time) {
	var (
		month time.Month
		year  int
	)

	year, month, _ = untruncatedTime.Date()

	truncatedTime = time.Date(year, month, 1, 0, 0, 0, 0, loc)

	return
}

// timeNow is presumably time.Now() localized to snapShotSchedule.policy.location...
//   ...but provided here so that each invocation of the per snapShotSchedule
//      within a snapShotPolicy can use the same value
func (snapShotSchedule *snapShotScheduleStruct) next(timeNow time.Time) (nextTime time.Time) {
	var (
		dayOfMonth      int
		dayOfWeek       time.Weekday
		hour            int
		minute          int
		month           time.Month
		numDaysToAdd    int
		numHoursToAdd   int
		numMinutesToAdd int
		year            int
	)

	// Ensure nextTime is at least at the start of the next minute
	nextTime = truncateToStartOfMinute(timeNow, snapShotSchedule.policy.location).Add(time.Minute)

	if snapShotSchedule.minuteSpecified {
		minute = nextTime.Minute()
		if snapShotSchedule.minute == minute {
			// We don't need to advance nextTime
		} else {
			// No need to (again) truncate nextTime back to the start of the minute
			// Now advance nextTime to align with minute
			if snapShotSchedule.minute > minute {
				numMinutesToAdd = snapShotSchedule.minute - minute
			} else { // snapShotSchedule.minute < minute
				numMinutesToAdd = snapShotSchedule.minute + 60 - minute
			}
			nextTime = nextTime.Add(time.Duration(numMinutesToAdd) * time.Minute)
		}
	}

	if snapShotSchedule.hourSpecified {
		hour = nextTime.Hour()
		if snapShotSchedule.hour == hour {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the hour
			nextTime = truncateToStartOfHour(nextTime, snapShotSchedule.policy.location)
			// Restore minuteSpecified if necessary
			if snapShotSchedule.minuteSpecified {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.minute) * time.Minute)
			}
			// Now advance nextTime to align with hour
			if snapShotSchedule.hour > hour {
				numHoursToAdd = snapShotSchedule.hour - hour
			} else { // snapShotSchedule.hour < hour
				numHoursToAdd = snapShotSchedule.hour + 24 - hour
			}
			nextTime = nextTime.Add(time.Duration(numHoursToAdd) * time.Hour)
		}
	}

	if snapShotSchedule.dayOfMonthSpecified {
		dayOfMonth = nextTime.Day()
		if snapShotSchedule.dayOfMonth == dayOfMonth {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the day
			nextTime = truncateToStartOfDay(nextTime, snapShotSchedule.policy.location)
			// Restore minuteSpecified and/or hourSpecified if necessary
			if snapShotSchedule.minuteSpecified {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.minute) * time.Minute)
			}
			if snapShotSchedule.hourSpecified {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.hour) * time.Hour)
			}
			// Now advance nextTime to align with dayOfMonth
			// Note: This unfortunately iterative approach avoids complicated
			//       adjustments for the non-fixed number of days in a month
			for {
				nextTime = nextTime.Add(24 * time.Hour)
				dayOfMonth = nextTime.Day()
				if snapShotSchedule.dayOfMonth == dayOfMonth {
					break
				}
			}
		}
	}

	if snapShotSchedule.monthSpecified {
		month = nextTime.Month()
		if snapShotSchedule.month == month {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the month
			nextTime = truncateToStartOfMonth(nextTime, snapShotSchedule.policy.location)
			// Restore minuteSpecified, hourSpecified, and/or dayOfMonthSpecified if necessary
			if snapShotSchedule.minuteSpecified {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.minute) * time.Minute)
			}
			if snapShotSchedule.hourSpecified {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.hour) * time.Hour)
			}
			if snapShotSchedule.dayOfMonthSpecified {
				nextTime = nextTime.Add(time.Duration((snapShotSchedule.dayOfMonth-1)*24) * time.Hour)
			}
			// Now advance nextTime to align with month
			// Note: This unfortunately iterative approach avoids complicated
			//       adjustments for the non-fixed number of days in a month
			hour, minute, _ = nextTime.Clock()
			year, month, dayOfMonth = nextTime.Date()
			if !snapShotSchedule.dayOfMonthSpecified {
				dayOfMonth = 1
			}
			for {
				if time.December == month {
					month = time.January
					year++
				} else {
					month++
				}
				nextTime = time.Date(year, month, dayOfMonth, hour, minute, 0, 0, snapShotSchedule.policy.location)
				year, month, dayOfMonth = nextTime.Date()
				if snapShotSchedule.dayOfMonthSpecified {
					if (snapShotSchedule.month == month) && (snapShotSchedule.dayOfMonth == dayOfMonth) {
						break
					} else {
						dayOfMonth = snapShotSchedule.dayOfMonth
					}
				} else {
					if (snapShotSchedule.month == month) && (1 == dayOfMonth) {
						break
					} else {
						dayOfMonth = 1
					}
				}
			}
		}
	}

	if snapShotSchedule.dayOfWeekSpecified {
		dayOfWeek = nextTime.Weekday()
		if time.Weekday(snapShotSchedule.dayOfWeek) == dayOfWeek {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the day
			nextTime = truncateToStartOfDay(nextTime, snapShotSchedule.policy.location)
			// Restore minuteSpecified and/or hourSpecified if necessary
			if snapShotSchedule.minuteSpecified {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.minute) * time.Minute)
			}
			if snapShotSchedule.hourSpecified {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.hour) * time.Hour)
			}
			// Now advance nextTime to align with dayOfWeek
			if time.Weekday(snapShotSchedule.dayOfWeek) > dayOfWeek {
				numDaysToAdd = int(snapShotSchedule.dayOfWeek) - int(dayOfWeek)
			} else { // time.Weekday(snapShotSchedule.dayOfWeek) < dayOfWeek
				numDaysToAdd = int(snapShotSchedule.dayOfWeek) + 7 - int(dayOfWeek)
			}
			nextTime = nextTime.Add(time.Duration(24*numDaysToAdd) * time.Hour)
		}
	}

	return
}

// timeNow is presumably time.Now() localized to snapShotPolicy.location...
//   ...but provided here primarily to enable easy testing
func (snapShotPolicy *snapShotPolicyStruct) next(timeNow time.Time) (nextTime time.Time) {
	var (
		nextTimeForSnapShotSchedule time.Time
		nextTimeHasBeenSet          bool
		snapShotSchedule            *snapShotScheduleStruct
	)

	nextTimeHasBeenSet = false

	for _, snapShotSchedule = range snapShotPolicy.schedule {
		nextTimeForSnapShotSchedule = snapShotSchedule.next(timeNow)
		if nextTimeHasBeenSet {
			if nextTimeForSnapShotSchedule.Before(nextTime) {
				nextTime = nextTimeForSnapShotSchedule
			}
		} else {
			nextTime = nextTimeForSnapShotSchedule
			nextTimeHasBeenSet = true
		}
	}

	return
}
