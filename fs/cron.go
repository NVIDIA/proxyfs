package fs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/utils"
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
}

type snapShotPolicyStruct struct {
	name     string
	schedule []*snapShotScheduleStruct
	location *time.Location
}

func loadSnapShotPolicy(confMap conf.ConfMap, volumeName string) (snapShotPolicy *snapShotPolicyStruct, err error) {
	var (
		cronTabStringSlice          []string
		dayOfMonthAsU64             uint64
		dayOfWeekAsU64              uint64
		hourAsU64                   uint64
		minuteAsU64                 uint64
		monthAsU64                  uint64
		snapShotPolicyName          string
		snapShotPolicySectionName   string
		snapShotSchedule            *snapShotScheduleStruct
		snapShotScheduleList        []string
		snapShotScheduleName        string
		snapShotScheduleSectionName string
		timeZone                    string
		volumeSectionName           string
	)

	volumeSectionName = utils.VolumeNameConfSection(volumeName)

	snapShotPolicyName, err = confMap.FetchOptionValueString(volumeSectionName, "SnapShotPolicy")
	if nil != err {
		// For now, we will default to returning nil (and success)
		snapShotPolicy = nil
		err = nil
		return
	}

	snapShotPolicy = &snapShotPolicyStruct{name: snapShotPolicyName}

	snapShotPolicySectionName = "SnapShotPolicy:" + snapShotPolicyName

	snapShotScheduleList, err = confMap.FetchOptionValueStringSlice(snapShotPolicySectionName, "ScheduleList")
	if nil != err {
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
			}

			snapShotSchedule.dayOfWeek = time.Weekday(dayOfWeekAsU64)
		}

		snapShotSchedule.keep, err = confMap.FetchOptionValueUint64(snapShotScheduleSectionName, "Keep")
		if nil != err {
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

	err = nil
	return
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

	// If we make it this far, t matches snapShotSchedule

	matches = true
	return
}

// timeNow is presumably time.Now()...but provided here so that each invocation of the
// per snapShotSchedule (within a snapShotPolicy) can use the same time.Now()
func (snapShotSchedule *snapShotScheduleStruct) next(timeNow time.Time) (nextTime time.Time) {
	var (
		dayOfMonth int
		dayOfWeek  time.Weekday
		hour       int
		minute     int
		month      time.Month
		year       int
	)

	// Ensure nextTime is at least at the start of the next minute
	nextTime = timeNow.Truncate(time.Minute).Add(time.Minute)

	if snapShotSchedule.dayOfWeekSpecified {
		dayOfWeek = nextTime.Weekday()
		if time.Weekday(snapShotSchedule.dayOfWeek) == dayOfWeek {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the day
			nextTime = nextTime.Truncate(24 * time.Hour)
			// Now advance nextTime to align with dayOfWeek
			if time.Weekday(snapShotSchedule.dayOfWeek) > dayOfWeek {
				nextTime = nextTime.Add((time.Duration((int(snapShotSchedule.dayOfWeek)-int(dayOfWeek))*24) * time.Hour))
			} else { // time.Weekday(snapShotSchedule.dayOfWeek) < dayOfWeek
				nextTime = nextTime.Add((time.Duration((int(snapShotSchedule.dayOfWeek)+int(7)-int(dayOfWeek))*24) * time.Hour))
			}
		}
	}

	if snapShotSchedule.monthSpecified {
		year, month, _ = nextTime.Date()
		if time.Month(snapShotSchedule.month) == month {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the month
			nextTime = time.Date(year, month, 1, 0, 0, 0, 0, snapShotSchedule.policy.location)
			// Now advance nextTime to align with month
			if time.Month(snapShotSchedule.month) > month {
				nextTime = nextTime.Add((time.Duration((int(snapShotSchedule.month)-int(month))*24) * time.Hour))
			} else {
				nextTime = nextTime.Add((time.Duration((int(snapShotSchedule.month)+int(12)-int(month))*24) * time.Hour))
			}
		}
	}

	if snapShotSchedule.dayOfMonthSpecified {
		dayOfMonth = nextTime.Day()
		if snapShotSchedule.dayOfMonth == dayOfMonth {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the day
			nextTime = nextTime.Truncate(24 * time.Hour)
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

	if snapShotSchedule.hourSpecified {
		hour = nextTime.Hour()
		if snapShotSchedule.hour == hour {
			// We don't need to advance nextTime
		} else {
			// First truncate nextTime back to the start of the hour
			nextTime = nextTime.Truncate(time.Hour)
			// Now advance nextTime to align with hour
			if snapShotSchedule.hour > hour {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.hour-hour) * time.Hour)
			} else { // snapShotSchedule.hour < hour
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.hour+int(24)-hour) * time.Hour)
			}
		}
	}

	if snapShotSchedule.minuteSpecified {
		minute = nextTime.Minute()
		if snapShotSchedule.minute == minute {
			// We don't need to advance nextTime
		} else {
			// No need to (again) truncate nextTime back to the start of the minute
			// Now advance nextTime to align with minute
			if snapShotSchedule.minute > minute {
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.minute-minute) * time.Minute)
			} else { // snapShotSchedule.minute < minute
				nextTime = nextTime.Add(time.Duration(snapShotSchedule.minute+int(60)-minute) * time.Minute)
			}
		}
	}

	return
}

// timeNow is presumably time.Now()...but provided here primarily to enable easy testing
func (snapShotPolicy *snapShotPolicyStruct) next(timeNow time.Time) (nextTime time.Time) {
	var (
		nextTimeForSnapShotSchedule time.Time
		snapShotSchedule            *snapShotScheduleStruct
	)

	nextTime = timeNow

	for _, snapShotSchedule = range snapShotPolicy.schedule {
		nextTimeForSnapShotSchedule = snapShotSchedule.next(timeNow)
		if nextTimeForSnapShotSchedule.Before(nextTime) {
			nextTime = nextTimeForSnapShotSchedule
		}
	}

	return
}
