package conf

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ConfMap is accessed via compMap[section_name][option_name][option_value_index] or via the methods below

type ConfMapOption []string
type ConfMapSection map[string]ConfMapOption
type ConfMap map[string]ConfMapSection

// MakeConfMap returns an newly created empty ConfMap
func MakeConfMap() (confMap ConfMap) {
	confMap = make(ConfMap)
	return
}

// MakeConfMapFromFile returns a newly created ConfMap loaded with the contents of the confFilePath-specified file
func MakeConfMapFromFile(confFilePath string) (confMap ConfMap, err error) {
	confMap = MakeConfMap()
	err = confMap.UpdateFromFile(confFilePath)
	return
}

// MakeConfMapFromStrings returns a newly created ConfMap loaded with the contents specified in confStrings
func MakeConfMapFromStrings(confStrings []string) (confMap ConfMap, err error) {
	confMap = MakeConfMap()
	for _, confString := range confStrings {
		err = confMap.UpdateFromString(confString)
		if nil != err {
			err = fmt.Errorf("Error building confMap from conf strings: %v", err)
			return
		}
	}

	err = nil
	return
}

// RegEx components used below:

const assignment = "([ \t]*[=:][ \t]*)"
const dot = "(\\.)"
const leftBracket = "(\\[)"
const rightBracket = "(\\])"
const sectionName = "([0-9A-Za-z_\\-/:\\.]+)"
const separator = "([ \t]+|([ \t]*,[ \t]*))"

const token = "(([0-9A-Za-z_\\*\\-/:\\.\\[\\]]+)\\$?)"
const whiteSpace = "([ \t]+)"

// A string to load looks like:

//   <section_name_0>.<option_name_0> =
//     or
//   <section_name_1>.<option_name_1> : <value_1>
//     or
//   <section_name_2>.<option_name_2> = <value_2>, <value_3>
//     or
//   <section_name_3>.<option_name_3> : <value_4> <value_5>,<value_6>

var stringRE = regexp.MustCompile("\\A" + token + dot + token + assignment + "(" + token + "(" + separator + token + ")*)?\\z")
var sectionNameOptionNameSeparatorRE = regexp.MustCompile(dot)

// A .INI/.conf file to load typically looks like:
//
//   [<section_name_1>]
//   <option_name_0> :
//   <option_name_1> = <value_1>
//   <option_name_2> : <value_2> <value_3>
//   <option_name_3> = <value_4> <value_5>,<value_6>
//
//   # A comment on it's own line starting with '#'
//   ; A comment on it's own line starting with ';'
//
//   [<section_name_2>]          ; A comment at the end of a line starting with ';'
//   <option_name_4> : <value_7> # A comment at the end of a line starting with '#'
//
// One .INI/.conf file may include another before/between/after its own sections like:
//
//   [<section_name_3>]
//   <option_name_5> = <value_8>
//
//   .include <included .INI/.conf path>
//
//   [<section_name_4>]
//   <option_name_6> : <value_9>

// Section Name lines are of the form:

var sectionHeaderLineRE = regexp.MustCompile("\\A" + leftBracket + token + rightBracket + "\\z")
var sectionNameRE = regexp.MustCompile(sectionName)

// Option Name:Value lines are of the form:

var optionLineRE = regexp.MustCompile("\\A" + token + assignment + "(" + token + "(" + separator + token + ")*)?\\z")

var optionNameOptionValuesSeparatorRE = regexp.MustCompile(assignment)
var optionValueSeparatorRE = regexp.MustCompile(separator)

// Include lines are of the form:

var includeLineRE = regexp.MustCompile("\\A\\.include" + whiteSpace + token + "\\z")
var includeFilePathSeparatorRE = regexp.MustCompile(whiteSpace)

// UpdateFromString modifies a pre-existing ConfMap based on an update
// specified in confString (e.g., from an extra command-line argument)
func (confMap ConfMap) UpdateFromString(confString string) (err error) {
	confStringTrimmed := strings.Trim(confString, " \t") // Trim leading & trailing spaces & tabs

	if 0 == len(confStringTrimmed) {
		err = fmt.Errorf("trimmed confString: \"%v\" was found to be empty", confString)
		return
	}

	if !stringRE.MatchString(confStringTrimmed) {
		err = fmt.Errorf("malformed confString: \"%v\"", confString)
		return
	}

	// confStringTrimmed well formed, so extract Section Name, Option Name, and Values

	confStringSectionNameOptionPayloadStrings := sectionNameOptionNameSeparatorRE.Split(confStringTrimmed, 2)

	sectionName := confStringSectionNameOptionPayloadStrings[0]
	optionPayload := confStringSectionNameOptionPayloadStrings[1]

	confStringOptionNameOptionValuesStrings := optionNameOptionValuesSeparatorRE.Split(optionPayload, 2)

	optionName := confStringOptionNameOptionValuesStrings[0]
	optionValues := confStringOptionNameOptionValuesStrings[1]

	optionValuesSplit := optionValueSeparatorRE.Split(optionValues, -1)

	if (1 == len(optionValuesSplit)) && ("" == optionValuesSplit[0]) {
		// Handle special case where optionValuesSplit == []string{""}... changing it to []string{}

		optionValuesSplit = []string{}
	}

	section, found := confMap[sectionName]

	if !found {
		// Need to create new Section

		section = make(ConfMapSection)
		confMap[sectionName] = section
	}

	section[optionName] = optionValuesSplit

	// If we reach here, confString successfully processed

	err = nil
	return
}

// UpdateFromStrings modifies a pre-existing ConfMap based on an update
// specified in confStrings (e.g., from an extra command-line argument)
func (confMap ConfMap) UpdateFromStrings(confStrings []string) (err error) {
	for _, confString := range confStrings {
		err = confMap.UpdateFromString(confString)
		if nil != err {
			return
		}
	}
	err = nil
	return
}

// UpdateFromFile modifies a pre-existing ConfMap based on updates specified in confFilePath
func (confMap ConfMap) UpdateFromFile(confFilePath string) (err error) {
	var (
		absConfFilePath                          string
		confFileBytes                            []byte
		confFileBytesLineOffsetStart             int
		confFileBytesOffset                      int
		currentLine                              string
		currentLineDotIncludeIncludePathStrings  []string
		currentLineNumber                        int
		currentLineOptionNameOptionValuesStrings []string
		currentSection                           ConfMapSection
		currentSectionName                       string
		dirAbsConfFilePath                       string
		found                                    bool
		lastRune                                 rune
		nestedConfFilePath                       string
		optionName                               string
		optionValues                             string
		optionValuesSplit                        []string
		runeSize                                 int
	)

	if "-" == confFilePath {
		confFileBytes, err = ioutil.ReadAll(os.Stdin)
		if nil != err {
			return
		}
	} else {
		confFileBytes, err = ioutil.ReadFile(confFilePath)
		if nil != err {
			return
		}
	}

	lastRune = '\n'

	for len(confFileBytes) > confFileBytesOffset {
		// Consume next rune

		lastRune, runeSize = utf8.DecodeRune(confFileBytes[confFileBytesOffset:])
		if utf8.RuneError == lastRune {
			err = fmt.Errorf("file %v contained invalid UTF-8 at byte %v", confFilePath, confFileBytesOffset)
			return
		}

		if '\n' == lastRune {
			// Terminate currentLine adding (non-empty) trimmed version to confFileLines

			currentLineNumber += 1

			if confFileBytesLineOffsetStart < confFileBytesOffset {
				currentLine = string(confFileBytes[confFileBytesLineOffsetStart:confFileBytesOffset])

				currentLine = strings.SplitN(currentLine, ";", 2)[0] // Trim comment after ';'
				currentLine = strings.SplitN(currentLine, "#", 2)[0] // Trim comment after '#'
				currentLine = strings.Trim(currentLine, " \t")       // Trim leading & trailing spaces & tabs

				if 0 < len(currentLine) {
					// Process non-empty, non-comment portion of currentLine

					if includeLineRE.MatchString(currentLine) {
						// Include found

						currentLineDotIncludeIncludePathStrings = includeFilePathSeparatorRE.Split(currentLine, 2)

						nestedConfFilePath = currentLineDotIncludeIncludePathStrings[1]

						if '/' != nestedConfFilePath[0] {
							// Need to adjust for relative path

							absConfFilePath, err = filepath.Abs(confFilePath)
							if nil != err {
								return
							}

							dirAbsConfFilePath = filepath.Dir(absConfFilePath)

							nestedConfFilePath = dirAbsConfFilePath + "/" + nestedConfFilePath
						}

						err = confMap.UpdateFromFile(nestedConfFilePath)
						if nil != err {
							return
						}

						currentSectionName = ""
					} else if sectionHeaderLineRE.MatchString(currentLine) {
						// Section Header found

						currentSectionName = sectionNameRE.FindString(currentLine)
					} else {
						if "" == currentSectionName {
							// Options only allowed within a Section

							err = fmt.Errorf("file %v did not start with a Section Name", confFilePath)
							return
						}

						// Option within currentSectionName possibly found

						if !optionLineRE.MatchString(currentLine) {
							// Expected valid Option Line

							err = fmt.Errorf("file %v malformed line '%v'", confFilePath, currentLine)
							return
						}

						// Option Line found, so extract Option Name and Option Values

						currentLineOptionNameOptionValuesStrings = optionNameOptionValuesSeparatorRE.Split(currentLine, 2)

						optionName = currentLineOptionNameOptionValuesStrings[0]
						optionValues = currentLineOptionNameOptionValuesStrings[1]

						optionValuesSplit = optionValueSeparatorRE.Split(optionValues, -1)

						if (1 == len(optionValuesSplit)) && ("" == optionValuesSplit[0]) {
							// Handle special case where optionValuesSplit == []string{""}... changing it to []string{}

							optionValuesSplit = []string{}
						}

						// Insert or Update confMap creating a new Section if necessary

						currentSection, found = confMap[currentSectionName]

						if !found {
							// Need to create the new Section

							currentSection = make(ConfMapSection)
							confMap[currentSectionName] = currentSection
						}

						currentSection[optionName] = optionValuesSplit
					}
				}
			}

			// Record where next line would start

			confFileBytesLineOffsetStart = confFileBytesOffset + runeSize
		}

		// Loop back for next rune

		confFileBytesOffset += runeSize
	}

	if '\n' != lastRune {
		err = fmt.Errorf("file %v did not end in a '\n' character", confFilePath)
		return
	}

	// If we reach here, confFilePath successfully processed

	err = nil
	return
}

// VerifyOptionValueIsEmpty returns an error if [sectionName]valueName's string value is not empty
func (confMap ConfMap) VerifyOptionValueIsEmpty(sectionName string, optionName string) (err error) {
	section, ok := confMap[sectionName]
	if !ok {
		err = fmt.Errorf("[%v] missing", sectionName)
		return
	}

	option, ok := section[optionName]
	if !ok {
		err = fmt.Errorf("[%v]%v missing", sectionName, optionName)
		return
	}

	if 0 == len(option) {
		err = nil
	} else {
		err = fmt.Errorf("[%v]%v must have no value", sectionName, optionName)
	}

	return
}

// FetchOptionValueStringSlice returns [sectionName]valueName's string values as a (non-emptry) []string
func (confMap ConfMap) FetchOptionValueStringSlice(sectionName string, optionName string) (optionValue []string, err error) {
	optionValue = []string{}

	section, ok := confMap[sectionName]
	if !ok {
		err = fmt.Errorf("[%v] missing", sectionName)
		return
	}

	option, ok := section[optionName]
	if !ok {
		err = fmt.Errorf("[%v]%v missing", sectionName, optionName)
		return
	}

	optionValue = option

	return
}

// FetchOptionValueString returns [sectionName]valueName's single string value
func (confMap ConfMap) FetchOptionValueString(sectionName string, optionName string) (optionValue string, err error) {
	optionValue = ""

	optionValueSlice, err := confMap.FetchOptionValueStringSlice(sectionName, optionName)
	if nil != err {
		return
	}

	if 1 != len(optionValueSlice) {
		err = fmt.Errorf("[%v]%v must be single-valued", sectionName, optionName)
		return
	}

	optionValue = optionValueSlice[0]

	err = nil
	return
}

// FetchOptionValueBool returns [sectionName]valueName's single string value converted to a bool
func (confMap ConfMap) FetchOptionValueBool(sectionName string, optionName string) (optionValue bool, err error) {
	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueStringDownshifted := strings.ToLower(optionValueString)

	switch optionValueStringDownshifted {
	case "yes":
		fallthrough
	case "on":
		fallthrough
	case "true":
		optionValue = true
	case "no":
		fallthrough
	case "off":
		fallthrough
	case "false":
		optionValue = false
	default:
		err = fmt.Errorf("Couldn't interpret %q as boolean (expected one of 'true'/'false'/'yes'/'no'/'on'/'off')", optionValueString)
		return
	}

	err = nil
	return
}

// FetchOptionValueUint8 returns [sectionName]valueName's single string value converted to a uint8
func (confMap ConfMap) FetchOptionValueUint8(sectionName string, optionName string) (optionValue uint8, err error) {
	optionValue = 0

	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueUint64, strconvErr := strconv.ParseUint(optionValueString, 10, 8)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseUint() error: %v", sectionName, optionName, strconvErr)
		return
	}

	optionValue = uint8(optionValueUint64)

	err = nil
	return
}

// FetchOptionValueUint16 returns [sectionName]valueName's single string value converted to a uint16
func (confMap ConfMap) FetchOptionValueUint16(sectionName string, optionName string) (optionValue uint16, err error) {
	optionValue = 0

	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueUint64, strconvErr := strconv.ParseUint(optionValueString, 10, 16)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseUint() error: %v", sectionName, optionName, strconvErr)
		return
	}

	optionValue = uint16(optionValueUint64)

	err = nil
	return
}

// FetchOptionValueUint32 returns [sectionName]valueName's single string value converted to a uint32
func (confMap ConfMap) FetchOptionValueUint32(sectionName string, optionName string) (optionValue uint32, err error) {
	optionValue = 0

	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueUint64, strconvErr := strconv.ParseUint(optionValueString, 10, 32)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseUint() error: %v", sectionName, optionName, strconvErr)
		return
	}

	optionValue = uint32(optionValueUint64)

	err = nil
	return
}

// FetchOptionValueUint64 returns [sectionName]valueName's single string value converted to a uint64
func (confMap ConfMap) FetchOptionValueUint64(sectionName string, optionName string) (optionValue uint64, err error) {
	optionValue = 0

	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueUint64, strconvErr := strconv.ParseUint(optionValueString, 10, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseUint() error: %v", sectionName, optionName, strconvErr)
		return
	}

	optionValue = uint64(optionValueUint64)

	err = nil
	return
}

// FetchOptionValueFloat32 returns [sectionName]valueName's single string value converted to a float32
func (confMap ConfMap) FetchOptionValueFloat32(sectionName string, optionName string) (optionValue float32, err error) {
	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueAsFloat64, strconvErr := strconv.ParseFloat(optionValueString, 32)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseFloat() error: %v", sectionName, optionName, strconvErr)
		return
	}

	optionValue = float32(optionValueAsFloat64) // strconv.ParseFloat(,32) guarantees this will work
	err = nil
	return
}

// FetchOptionValueFloat64 returns [sectionName]valueName's single string value converted to a float32
func (confMap ConfMap) FetchOptionValueFloat64(sectionName string, optionName string) (optionValue float64, err error) {
	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValue, strconvErr := strconv.ParseFloat(optionValueString, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseFloat() error: %v", sectionName, optionName, strconvErr)
		return
	}

	err = nil
	return
}

// FetchOptionValueFloatScaledToUint32 returns [sectionName]valueName's single string value converted to a float64, multiplied by the uint32 multiplier, as a uint32
func (confMap ConfMap) FetchOptionValueFloatScaledToUint32(sectionName string, optionName string, multiplier uint32) (optionValue uint32, err error) {
	optionValue = 0

	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueFloat64, strconvErr := strconv.ParseFloat(optionValueString, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseFloat() error: %v", sectionName, optionName, strconvErr)
		return
	}

	if optionValueFloat64 < float64(0.0) {
		err = fmt.Errorf("[%v]%v is negative", sectionName, optionName)
		return
	}

	optionValueFloat64Scaled := optionValueFloat64*float64(multiplier) + float64(0.5)

	if optionValueFloat64Scaled >= float64(uint32(0xFFFFFFFF)) {
		err = fmt.Errorf("[%v]%v after scaling won't fit in uint32", sectionName, optionName)
		return
	}

	optionValue = uint32(optionValueFloat64Scaled)

	err = nil
	return
}

// FetchOptionValueFloatScaledToUint64 returns [sectionName]valueName's single string value converted to a float64, multiplied by the uint64 multiplier, as a uint64
func (confMap ConfMap) FetchOptionValueFloatScaledToUint64(sectionName string, optionName string, multiplier uint64) (optionValue uint64, err error) {
	optionValue = 0

	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		return
	}

	optionValueFloat64, strconvErr := strconv.ParseFloat(optionValueString, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v strconv.ParseFloat() error: %v", sectionName, optionName, strconvErr)
		return
	}

	if optionValueFloat64 < float64(0.0) {
		err = fmt.Errorf("[%v]%v is negative", sectionName, optionName)
		return
	}

	optionValueFloat64Scaled := optionValueFloat64*float64(multiplier) + float64(0.5)

	if optionValueFloat64Scaled >= float64(uint64(0xFFFFFFFFFFFFFFFF)) {
		err = fmt.Errorf("[%v]%v after scaling won't fit in uint64", sectionName, optionName)
		return
	}

	optionValue = uint64(optionValueFloat64Scaled)

	err = nil
	return
}

// FetchOptionValueDuration returns [sectionName]valueName's single string value converted to a time.Duration
func (confMap ConfMap) FetchOptionValueDuration(sectionName string, optionName string) (optionValue time.Duration, err error) {
	optionValueString, err := confMap.FetchOptionValueString(sectionName, optionName)
	if nil != err {
		optionValue = time.Since(time.Now()) // Roughly zero
		return
	}

	optionValue, err = time.ParseDuration(optionValueString)
	if nil != err {
		return
	}

	if 0.0 > optionValue.Seconds() {
		err = fmt.Errorf("[%v]%v is negative", sectionName, optionName)
		return
	}

	err = nil
	return
}

// FetchOptionValueUUID returns [sectionName]valueName's single string value converted to a UUID ([16]byte)
//
// From RFC 4122, a UUID string is defined as follows:
//
//   UUID                   = time-low "-" time-mid "-" time-high-and-version "-" clock-seq-and-reserved clock-seq-low "-" node
//   time-low               = 4hexOctet
//   time-mid               = 2hexOctet
//   time-high-and-version  = 2hexOctet
//   clock-seq-and-reserved = hexOctet
//   clock-seq-low          = hexOctet
//   node                   = 6hexOctet
//   hexOctet               = hexDigit hexDigit
//   hexDigit               = "0" / "1" / "2" / "3" / "4" / "5" / "6" / "7" / "8" / "9" / "a" / "b" / "c" / "d" / "e" / "f" / "A" / "B" / "C" / "D" / "E" / "F"
//
// From RFC 4122, a UUID (i.e. "in memory") is defined as follows (BigEndian/NetworkByteOrder):
//
//      0                   1                   2                   3
//       0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//      |                          time_low                             |
//      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//      |       time_mid                |         time_hi_and_version   |
//      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//      |clk_seq_hi_res |  clk_seq_low  |         node (0-1)            |
//      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//      |                         node (2-5)                            |
//      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
func (confMap ConfMap) FetchOptionValueUUID(sectionName string, optionName string) (optionValue []byte, err error) {
	optionValue = make([]byte, 16)

	optionValueSlice, err := confMap.FetchOptionValueStringSlice(sectionName, optionName)
	if nil != err {
		return
	}

	if 1 != len(optionValueSlice) {
		err = fmt.Errorf("[%v]%v must be single-valued", sectionName, optionName)
		return
	}

	uuidString := optionValueSlice[0]

	if (8 + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 12) != len(uuidString) {
		err = fmt.Errorf("[%v]%v UUID string (\"%v\") has invalid length (%v)", sectionName, optionName, uuidString, len(uuidString))
		return
	}

	if ('-' != uuidString[8]) || ('-' != uuidString[13]) || ('-' != uuidString[18]) || ('-' != uuidString[23]) {
		err = fmt.Errorf("[%v]%v UUID string (\"%v\") has missing '-' separators", sectionName, optionName, uuidString)
		return
	}

	timeLowUint64, strconvErr := strconv.ParseUint(uuidString[:8], 16, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v time_low (\"%v\") invalid", sectionName, optionName, uuidString[:8])
		return
	}

	timeMidUint64, strconvErr := strconv.ParseUint(uuidString[9:13], 16, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v time_mid (\"%v\") invalid", sectionName, optionName, uuidString[9:13])
		return
	}

	timeHiAndVersionUint64, strconvErr := strconv.ParseUint(uuidString[14:18], 16, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v time_hi_and_version (\"%v\") invalid", sectionName, optionName, uuidString[14:18])
		return
	}

	clkSeqHiResUint64, strconvErr := strconv.ParseUint(uuidString[19:21], 16, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v clk_seq_hi_res (\"%v\") invalid", sectionName, optionName, uuidString[19:21])
		return
	}

	clkClkSeqLowUint64, strconvErr := strconv.ParseUint(uuidString[21:23], 16, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v clk_seq_low (\"%v\") invalid", sectionName, optionName, uuidString[21:23])
		return
	}

	nodeUint64, strconvErr := strconv.ParseUint(uuidString[24:], 16, 64)
	if nil != strconvErr {
		err = fmt.Errorf("[%v]%v node (\"%v\") invalid", sectionName, optionName, uuidString[24:])
		return
	}

	optionValue[0x0] = byte((timeLowUint64 >> 0x18) & 0xFF)
	optionValue[0x1] = byte((timeLowUint64 >> 0x10) & 0xFF)
	optionValue[0x2] = byte((timeLowUint64 >> 0x08) & 0xFF)
	optionValue[0x3] = byte((timeLowUint64 >> 0x00) & 0xFF)

	optionValue[0x4] = byte((timeMidUint64 >> 0x08) & 0xFF)
	optionValue[0x5] = byte((timeMidUint64 >> 0x00) & 0xFF)

	optionValue[0x6] = byte((timeHiAndVersionUint64 >> 0x08) & 0xFF)
	optionValue[0x7] = byte((timeHiAndVersionUint64 >> 0x00) & 0xFF)

	optionValue[0x8] = byte((clkSeqHiResUint64 >> 0x00) & 0xFF)

	optionValue[0x9] = byte((clkClkSeqLowUint64 >> 0x00) & 0xFF)

	optionValue[0xA] = byte((nodeUint64 >> 0x28) & 0xFF)
	optionValue[0xB] = byte((nodeUint64 >> 0x20) & 0xFF)
	optionValue[0xC] = byte((nodeUint64 >> 0x18) & 0xFF)
	optionValue[0xD] = byte((nodeUint64 >> 0x10) & 0xFF)
	optionValue[0xE] = byte((nodeUint64 >> 0x08) & 0xFF)
	optionValue[0xF] = byte((nodeUint64 >> 0x00) & 0xFF)

	err = nil
	return
}

// DumpConfMapToFile outputs the ConfMap to a confFilePath-specified file with the perm-specified os.FileMode
func (confMap ConfMap) DumpConfMapToFile(confFilePath string, perm os.FileMode) (err error) {
	var (
		bufToOutput      []byte
		firstOption      bool
		firstSection     bool
		optionName       string
		optionNameLen    int
		optionNameMaxLen int
		option           string
		options          ConfMapOption
		section          ConfMapSection
		sectionName      string
	)

	firstSection = true
	for sectionName, section = range confMap {
		if firstSection {
			firstSection = false
		} else {
			bufToOutput = append(bufToOutput, '\n')
		}
		bufToOutput = append(bufToOutput, '[')
		bufToOutput = append(bufToOutput, []byte(sectionName)...)
		bufToOutput = append(bufToOutput, ']')
		bufToOutput = append(bufToOutput, '\n')
		optionNameMaxLen = 0
		for optionName = range section {
			optionNameLen = len(optionName)
			if optionNameLen > optionNameMaxLen {
				optionNameMaxLen = optionNameLen
			}
		}
		for optionName, options = range section {
			bufToOutput = append(bufToOutput, []byte(optionName)...)
			optionNameLen = len(optionName)
			bufToOutput = append(bufToOutput, bytes.Repeat([]byte(" "), optionNameMaxLen-optionNameLen+1)...)
			bufToOutput = append(bufToOutput, ':')
			firstOption = true
			for _, option = range options {
				if firstOption {
					firstOption = false
				} else {
					bufToOutput = append(bufToOutput, ',')
				}
				bufToOutput = append(bufToOutput, ' ')
				bufToOutput = append(bufToOutput, []byte(option)...)
			}
			bufToOutput = append(bufToOutput, '\n')
		}
	}
	if !firstSection {
		bufToOutput = append(bufToOutput, '\n')
	}

	err = ioutil.WriteFile(confFilePath, bufToOutput, perm)

	return // err as returned from ioutil.WriteFile() suffices here
}
