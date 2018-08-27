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
	minuteSpecified     bool
	minute              uint8 // 0-59
	hourSpecified       bool
	hour                uint8 // 0-23
	dayOfMonthSpecified bool
	dayOfMonth          uint8 // 1-31
	monthSpecified      bool
	month               uint8 // 1-12
	dayOfWeekSpecified  bool
	dayOfWeek           uint8 // 0-6 (0 == Sunday)
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

		snapShotSchedule = &snapShotScheduleStruct{name: snapShotScheduleName}

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

			snapShotSchedule.minute = uint8(minuteAsU64)
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

			snapShotSchedule.hour = uint8(hourAsU64)
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

			snapShotSchedule.dayOfMonth = uint8(dayOfMonthAsU64)
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

			snapShotSchedule.month = uint8(monthAsU64)
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

			snapShotSchedule.dayOfWeek = uint8(dayOfWeekAsU64)
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
