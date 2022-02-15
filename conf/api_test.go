// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package conf

import (
	"bytes"
	"encoding/base64"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const errnoEACCES = int(13)

var tempFile1Name string
var tempFile2Name string
var tempFile3Name string
var tempFile4Name string
var tempFile5Name string
var tempFile6Name string
var tempFile7Name string
var confStringToTest1 string
var confStringToTest2 string
var confStringToTest3 string

func TestMain(m *testing.M) {
	tempFile1, errorTempFile1 := ioutil.TempFile(os.TempDir(), "TestConfFile1_")
	if nil != errorTempFile1 {
		os.Exit(errnoEACCES)
	}

	tempFile1Name = tempFile1.Name()

	io.WriteString(tempFile1, "# A comment on it's own line\n")
	io.WriteString(tempFile1, "[TestNamespace:Test_-_Section]\n")
	io.WriteString(tempFile1, "Test_-_Option : TestValue1,TestValue2 # A comment at the end of a line\n")

	tempFile1.Close()

	tempFile2, errorTempFile2 := ioutil.TempFile(os.TempDir(), "TestConfFile2_")
	if nil != errorTempFile2 {
		os.Remove(tempFile1Name)
		os.Exit(errnoEACCES)
	}

	tempFile2Name = tempFile2.Name()

	io.WriteString(tempFile2, "; A comment on it's own line\r\n")
	io.WriteString(tempFile2, "[TestNamespace:Test_-_Section] ; A comment at the end of a line\r\n")
	io.WriteString(tempFile2, "Test_-_Option =\r\n")

	tempFile2.Close()

	tempFile3, errorTempFile3 := ioutil.TempFile(os.TempDir(), "TestConfFile3_")
	if nil != errorTempFile3 {
		os.Remove(tempFile1Name)
		os.Remove(tempFile2Name)
		os.Exit(errnoEACCES)
	}

	tempFile3Name = tempFile3.Name()

	io.WriteString(tempFile3, "[TestNamespace:Test_-_Section]\n")
	io.WriteString(tempFile3, "Test_-_Option = http://Test.Value.3/ TestValue4$\tTestValue5$\n")

	tempFile3.Close()

	tempFile4, errorTempFile4 := ioutil.TempFile(os.TempDir(), "TestConfFile4_")
	if nil != errorTempFile4 {
		os.Remove(tempFile1Name)
		os.Remove(tempFile2Name)
		os.Remove(tempFile3Name)
		os.Exit(errnoEACCES)
	}

	tempFile4Name = tempFile4.Name()

	tempFile4.Close()

	tempFile5, errorTempFile5 := ioutil.TempFile(os.TempDir(), "TestConfFile5_")
	if nil != errorTempFile5 {
		os.Remove(tempFile1Name)
		os.Remove(tempFile2Name)
		os.Remove(tempFile3Name)
		os.Remove(tempFile4Name)
		os.Exit(errnoEACCES)
	}

	tempFile5Name = tempFile5.Name()

	io.WriteString(tempFile5, ".include ./"+filepath.Base(tempFile4Name)+"\n")

	tempFile5.Close()

	tempFile6, errorTempFile6 := ioutil.TempFile(os.TempDir(), "TestConfFile6_")
	if nil != errorTempFile6 {
		os.Remove(tempFile1Name)
		os.Remove(tempFile2Name)
		os.Remove(tempFile3Name)
		os.Remove(tempFile4Name)
		os.Remove(tempFile5Name)
		os.Exit(errnoEACCES)
	}

	tempFile6Name = tempFile6.Name()

	io.WriteString(tempFile6, "[TestNamespace:Test_-_Section_-_1]\n")
	io.WriteString(tempFile6, "Option_-_1_-_No_-_Values  :\n")
	io.WriteString(tempFile6, "Option_-_2_-_One_-_Value  : Value_-_1\n")
	io.WriteString(tempFile6, "Option_-_3_-_Two_-_Values : Value_-_1, Value_-_2\n")
	io.WriteString(tempFile6, "\n")
	io.WriteString(tempFile6, "[TestNamespace:Test_-_Section_-_2]\n")
	io.WriteString(tempFile6, "Option : Value\n")

	tempFile6.Close()

	tempFile7, errorTempFile7 := ioutil.TempFile(os.TempDir(), "TestConfFile7_")
	if nil != errorTempFile7 {
		os.Remove(tempFile1Name)
		os.Remove(tempFile2Name)
		os.Remove(tempFile3Name)
		os.Remove(tempFile4Name)
		os.Remove(tempFile5Name)
		os.Remove(tempFile6Name)
		os.Exit(errnoEACCES)
	}

	tempFile7Name = tempFile7.Name()

	// Leave it empty... will be used as output file

	tempFile7.Close()

	confStringToTest1 = "TestNamespace:Test_-_Section.Test_-_Option = TestValue6,http://Test.Value_-_7/"
	confStringToTest2 = "TestNamespace:Test_-_Section.Test_-_Option = TestValue8$,http://Test.Value_-_9/$"
	confStringToTest3 = "TestNamespace:Test_-_Section.Test_-_Option ="

	mRunReturn := m.Run()

	os.Remove(tempFile1Name)
	os.Remove(tempFile2Name)
	os.Remove(tempFile3Name)
	os.Remove(tempFile4Name)
	os.Remove(tempFile5Name)
	os.Remove(tempFile6Name)
	os.Remove(tempFile7Name)

	os.Exit(mRunReturn)
}

func TestUpdate(t *testing.T) {
	var confMap = MakeConfMap()

	err := confMap.UpdateFromFile(tempFile1Name)
	if nil != err {
		t.Fatalf("UpdateConfMapFromFile(\"%v\") returned: \"%v\"", tempFile1Name, err)
	}

	confMapSection, ok := confMap["TestNamespace:Test_-_Section"]
	if !ok {
		t.Fatalf("confMap[\"%v\"] missing", "TestNamespace:Test_-_Section")
	}

	confMapOption, ok := confMapSection["Test_-_Option"]
	if !ok {
		t.Fatalf("confMapSection[\"%v\"] missing", "Test_-_Option")
	}

	if 2 != len(confMapOption) {
		t.Fatalf("confMapSection[\"%v\"] constains unexpected number of values (%v)", "Test_-_Option", len(confMapOption))
	}

	if "TestValue1" != string(confMapOption[0]) {
		t.Fatalf("confMapOption != \"%v\"", "TestValue1")
	}

	if "TestValue2" != string(confMapOption[1]) {
		t.Fatalf("confMapOption != \"%v\"", "TestValue2")
	}

	err = confMap.UpdateFromFile(tempFile2Name)
	if nil != err {
		t.Fatalf("UpdateConfMapFromFile(\"%v\") returned: \"%v\"", tempFile2Name, err)
	}

	confMapSection, ok = confMap["TestNamespace:Test_-_Section"]
	if !ok {
		t.Fatalf("confMap[\"%v\"] missing", "Test_-_Section")
	}

	confMapOption, ok = confMapSection["Test_-_Option"]
	if !ok {
		t.Fatalf("confMapSection[\"%v\"] missing", "Test_-_Option")
	}

	if 0 != len(confMapOption) {
		t.Fatalf("confMapSection[\"%v\"] constains unexpected number of values (%v)", "Test_-_Option", len(confMapOption))
	}

	err = confMap.UpdateFromFile(tempFile3Name)
	if nil != err {
		t.Fatalf("UpdateConfMapFromFile(\"%v\") returned: \"%v\"", tempFile3Name, err)
	}

	confMapSection, ok = confMap["TestNamespace:Test_-_Section"]
	if !ok {
		t.Fatalf("confMap[\"%v\"] missing", "Test_-_Section")
	}

	confMapOption, ok = confMapSection["Test_-_Option"]
	if !ok {
		t.Fatalf("confMapSection[\"%v\"] missing", "Test_-_Option")
	}

	if 3 != len(confMapOption) {
		t.Fatalf("confMapSection[\"%v\"] constains unexpected number of values (%v)", "Test_-_Option", len(confMapOption))
	}

	if "http://Test.Value.3/" != string(confMapOption[0]) {
		t.Fatalf("confMapOption != \"%v\"", "http://Test.Value.3/")
	}

	if "TestValue4$" != string(confMapOption[1]) {
		t.Fatalf("confMapOption != \"%v\"", "TestValue4$")
	}

	if "TestValue5$" != string(confMapOption[2]) {
		t.Fatalf("confMapOption != \"%v\"", "TestValue5$")
	}

	err = confMap.UpdateFromString(confStringToTest1)

	if nil != err {
		t.Fatalf("UpdateConfMapFromString(\"%v\") returned: \"%v\"", confStringToTest1, err)
	}

	confMapSection, ok = confMap["TestNamespace:Test_-_Section"]
	if !ok {
		t.Fatalf("confMap[\"%v\"] missing", "Test_-_Section")
	}

	confMapOption, ok = confMapSection["Test_-_Option"]
	if !ok {
		t.Fatalf("confMapSection[\"%v\"] missing", "Test_-_Option")
	}

	if 2 != len(confMapOption) {
		t.Fatalf("confMapSection[\"%v\"] constains unexpected number of values (%v)", "Test_-_Option", len(confMapOption))
	}

	if "TestValue6" != string(confMapOption[0]) {
		t.Fatalf("confMapOption != \"%v\"", "TestValue6")
	}

	if "http://Test.Value_-_7/" != string(confMapOption[1]) {
		t.Fatalf("confMapOption != \"%v\"", "http://Test.Value_-_7/")
	}

	err = confMap.UpdateFromString(confStringToTest2)

	if nil != err {
		t.Fatalf("UpdateConfMapFromString(\"%v\") returned: \"%v\"", confStringToTest2, err)
	}

	confMapSection, ok = confMap["TestNamespace:Test_-_Section"]
	if !ok {
		t.Fatalf("confMap[\"%v\"] missing", "Test_-_Section")
	}

	confMapOption, ok = confMapSection["Test_-_Option"]
	if !ok {
		t.Fatalf("confMapSection[\"%v\"] missing", "Test_-_Option")
	}

	if 2 != len(confMapOption) {
		t.Fatalf("confMapSection[\"%v\"] constains unexpected number of values (%v)", "Test_-_Option", len(confMapOption))
	}

	if "TestValue8$" != string(confMapOption[0]) {
		t.Fatalf("confMapOption != \"%v\"", "TestValue8$")
	}

	if "http://Test.Value_-_9/$" != string(confMapOption[1]) {
		t.Fatalf("confMapOption != \"%v\"", "http://Test.Value_-_9/$")
	}

	err = confMap.UpdateFromString(confStringToTest3)

	if nil != err {
		t.Fatalf("UpdateConfMapFromString(\"%v\") returned: \"%v\"", confStringToTest3, err)
	}

	confMapOption, ok = confMapSection["Test_-_Option"]
	if !ok {
		t.Fatalf("confMapSection[\"%v\"] missing", "Test_-_Option")
	}

	if 0 != len(confMapOption) {
		t.Fatalf("confMapSection[\"%v\"] constains unexpected number of values (%v)", "Test_-_Option", len(confMapOption))
	}
}

func TestFromFileConstructor(t *testing.T) {
	confMap, err := MakeConfMapFromFile(tempFile3Name)
	if nil != err {
		t.Fatalf("MakeConfMapFromFile(): expected err to be nil, got %#v", err)
	}

	values, err := confMap.FetchOptionValueStringSlice("TestNamespace:Test_-_Section", "Test_-_Option")
	if err != nil {
		t.Fatalf("FetchOptionValueStringSlice(): expected err to be nil, got %#v", err)
	}
	expected := "http://Test.Value.3/"
	if values[0] != expected {
		t.Fatalf("FetchOptionValueStringSlice(): expected %#v, got %#v", expected, values[0])
	}
}

func TestFromFileConstructorNonexistentFile(t *testing.T) {
	_, err := MakeConfMapFromFile("/does/not/exist")
	expectedErr := "open /does/not/exist: no such file or directory"
	if err.Error() != expectedErr {
		t.Fatalf("expected err to be %#v, got %#v", expectedErr, err.Error())
	}
}

func TestFetch(t *testing.T) {
	const (
		base64DecodedString              = "Now is the time"
		base64DecodedStringSliceElement0 = "for all good men"
		base64DecodedStringSliceElement1 = "to come to the aid of their country."
	)
	var (
		base64EncodedString              = base64.StdEncoding.EncodeToString([]byte(base64DecodedString))
		base64EncodedStringSliceElement0 = base64.StdEncoding.EncodeToString([]byte(base64DecodedStringSliceElement0))
		base64EncodedStringSliceElement1 = base64.StdEncoding.EncodeToString([]byte(base64DecodedStringSliceElement1))
		confMap                          = MakeConfMap()
		err                              error
	)

	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionStringSlice1=")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionStringSlice1=: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionStringSlice2=TestString1,TestString2")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionStringSlice2=TestString1,TestString2: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionString=TestString3")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionString=TestString3: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionBase64String=" + base64EncodedString)
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionBase64String=" + base64EncodedString)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionBase64StringSlice=" + base64EncodedStringSliceElement0 + "," + base64EncodedStringSliceElement1)
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionBase64StringSlice=" + base64EncodedStringSliceElement0 + "," + base64EncodedStringSliceElement1)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionBool=true")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionBool=true: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionUint8=91")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionUint8=91: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionUint16=12")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionUint16=12: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionUint32=345")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionUint32=345: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionUint64=6789")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionUint64=6789: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionMilliseconds32=0.123")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionMilliseconds32=0.123: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionMilliseconds64=0.456")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionMilliseconds64=0.456: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionDuration=1.2s")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionDuration=1.2s: %v", err)
	}
	err = confMap.UpdateFromString("TestNamespace:Test_-_Section.Test_-_OptionGUIDString=12345678-1234-1234-1234-123456789ABC")
	if nil != err {
		t.Fatalf("Couldn't add TestNamespace:Test_-_Section.Test_-_OptionGUIDString=12345678-1234-1234-1234-123456789ABC: %v", err)
	}

	err = confMap.VerifyOptionIsMissing("TestNamespace:Test_-_Section", "Test_-_OptionStringSlice0")
	if nil != err {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionStringSlice0 should have verified as missing")
	}
	err = confMap.VerifyOptionIsMissing("TestNamespace:Test_-_Section", "Test_-_OptionStringSlice1")
	if nil == err {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionStringSlice1 should have verified as not missing")
	}

	err = confMap.VerifyOptionValueIsEmpty("TestNamespace:Test_-_Section", "Test_-_OptionStringSlice1")
	if nil != err {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionStringSlice1 should have verified as empty")
	}
	err = confMap.VerifyOptionValueIsEmpty("TestNamespace:Test_-_Section", "Test_-_OptionStringSlice2")
	if nil == err {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionStringSlice2 should have verified as not empty")
	}

	testStringSlice1, err := confMap.FetchOptionValueStringSlice("TestNamespace:Test_-_Section", "Test_-_OptionStringSlice1")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionStringSlice1: %v", err)
	}
	testStringSlice2, err := confMap.FetchOptionValueStringSlice("TestNamespace:Test_-_Section", "Test_-_OptionStringSlice2")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionStringSlice2: %v", err)
	}
	testString, err := confMap.FetchOptionValueString("TestNamespace:Test_-_Section", "Test_-_OptionString")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionString: %v", err)
	}
	testBase64String, err := confMap.FetchOptionValueBase64String("TestNamespace:Test_-_Section", "Test_-_OptionBase64String")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionBase64String: %v", err)
	}
	testBase64StringSlice, err := confMap.FetchOptionValueBase64StringSlice("TestNamespace:Test_-_Section", "Test_-_OptionBase64StringSlice")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionBase64StringSlice: %v", err)
	}
	testBool, err := confMap.FetchOptionValueBool("TestNamespace:Test_-_Section", "Test_-_OptionBool")
	if err != nil {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionBool: %v", err)
	}
	testUint8, err := confMap.FetchOptionValueUint8("TestNamespace:Test_-_Section", "Test_-_OptionUint8")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionUint8: %v", err)
	}
	testUint16, err := confMap.FetchOptionValueUint16("TestNamespace:Test_-_Section", "Test_-_OptionUint16")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionUint16: %v", err)
	}
	testUint32, err := confMap.FetchOptionValueUint32("TestNamespace:Test_-_Section", "Test_-_OptionUint32")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionUint32: %v", err)
	}
	testUint64, err := confMap.FetchOptionValueUint64("TestNamespace:Test_-_Section", "Test_-_OptionUint64")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionUint64: %v", err)
	}
	testFloat32, err := confMap.FetchOptionValueFloat32("TestNamespace:Test_-_Section", "Test_-_OptionMilliseconds32")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionMilliseconds32 as float32: %v", err)
	}
	testFloat64, err := confMap.FetchOptionValueFloat64("TestNamespace:Test_-_Section", "Test_-_OptionMilliseconds64")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionMilliseconds64 as float64: %v", err)
	}
	testScaledUint32, err := confMap.FetchOptionValueFloatScaledToUint32("TestNamespace:Test_-_Section", "Test_-_OptionMilliseconds32", 1000)
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionMilliseconds32 as uint32: %v", err)
	}
	testScaledUint64, err := confMap.FetchOptionValueFloatScaledToUint64("TestNamespace:Test_-_Section", "Test_-_OptionMilliseconds64", 1000)
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionMilliseconds64 as uint64: %v", err)
	}
	testDuration, err := confMap.FetchOptionValueDuration("TestNamespace:Test_-_Section", "Test_-_OptionDuration")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.TestDuration: %v", err)
	}
	testGUID, err := confMap.FetchOptionValueUUID("TestNamespace:Test_-_Section", "Test_-_OptionGUIDString")
	if nil != err {
		t.Fatalf("Couldn't fetch TestNamespace:Test_-_Section.Test_-_OptionGUIDString: %v", err)
	}

	if 0 != len(testStringSlice1) {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionStringSlice1 contained unexpected value(s)")
	}
	if (2 != len(testStringSlice2)) || ("TestString1" != testStringSlice2[0]) || ("TestString2" != testStringSlice2[1]) {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionStringSlice2 contained unexpected value(s)")
	}
	if "TestString3" != testString {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionString contained unexpected value")
	}
	if base64DecodedString != testBase64String {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionBase64String contained unexpected value")
	}
	if (2 != len(testBase64StringSlice)) || (base64DecodedStringSliceElement0 != base64DecodedStringSliceElement0) || (base64DecodedStringSliceElement1 != base64DecodedStringSliceElement1) {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionBase64StringSlice contained unexpected value(s)")
	}
	if testBool != true {
		t.Fatalf("TestNamespace:Test_-_Section.TestBool contained unexpected value")
	}
	if uint8(91) != testUint8 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionUint8 contained unexpected value")
	}
	if uint16(12) != testUint16 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionUint16 contained unexpected value")
	}
	if uint32(345) != testUint32 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionUint32 contained unexpected value")
	}
	if uint64(6789) != testUint64 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionUint64 contained unexpected value")
	}
	if float32(0.123) != testFloat32 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionMilliseconds32 contained unexpected float32 value")
	}
	if float64(0.456) != testFloat64 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionMilliseconds64 contained unexpected float64 value")
	}
	if uint32(123) != testScaledUint32 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionMilliseconds32 contained unexpected uint32 value")
	}
	if uint64(456) != testScaledUint64 {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionMilliseconds64 contained unexpected uint64 value")
	}
	timeBase := time.Time{}
	timeBasePlusTestDuration := timeBase.Add(testDuration)
	if (1 != timeBasePlusTestDuration.Second()) || (200000000 != timeBasePlusTestDuration.Nanosecond()) {
		t.Fatalf("TestNamespace:Test_-_Section.TestDuration contained unexpected value")
	}
	if 0 != bytes.Compare([]byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC}, testGUID) {
		t.Fatalf("TestNamespace:Test_-_Section.Test_-_OptionGUIDString contained unexpected value")
	}
}

func TestInclude(t *testing.T) {
	_, err := MakeConfMapFromFile(tempFile5Name)
	if nil != err {
		t.Fatalf("MakeConfMapFromFile() of <tempdir>/%v that includes <tempdir>/%v got %#v", filepath.Base(tempFile5Name), filepath.Base(tempFile4Name), err)
	}
}

func TestDump(t *testing.T) {
	var (
		confMap6 = MakeConfMap()
		confMap7 = MakeConfMap()
	)

	err := confMap6.UpdateFromFile(tempFile6Name)
	if nil != err {
		t.Fatalf("UpdateConfMapFromFile(\"%v\") returned: \"%v\"", tempFile6Name, err)
	}

	err = confMap6.DumpConfMapToFile(tempFile7Name, os.FileMode(0600))
	if nil != err {
		t.Fatalf("DumpConfMapToFile() returned: \"%v\"", err)
	}

	err = confMap7.UpdateFromFile(tempFile7Name)
	if nil != err {
		t.Fatalf("UpdateConfMapFromFile(\"%v\") returned: \"%v\"", tempFile7Name, err)
	}

	if !reflect.DeepEqual(confMap6, confMap7) {
		t.Fatalf("DumpConfMapToFile() failed to reproduce \"%v\" into \"%v\"", tempFile6Name, tempFile7Name)
	}
}

// test SetOptionIfMissing and SetSectionIfMissing
func TestIfMissing(t *testing.T) {

	assert := assert.New(t)

	// create an empty confMap and populate it with one option (in one section)
	confMap := MakeConfMap()

	testSectionName0 := "TestSection0"
	testSectionName1 := "TestSection1e"
	testOptionName0 := "Test_-_Option0"
	testOptionName1 := "Test_-_Option1"
	testOptionString0 := "OptionValue0"
	missingOptionString0 := "DefaultString0"
	missingOptionString1 := "DefaultString1"

	confStringToSection0Option0 := testSectionName0 + "." + testOptionName0 + " = " + testOptionString0

	// populate testSectionName0.testOptionName0
	err := confMap.UpdateFromString(confStringToSection0Option0)
	assert.NoError(err, "UpdateFromString() should not fail with a valid option string")

	// verify option was set
	optionString, err := confMap.FetchOptionValueString(testSectionName0, testOptionName0)
	assert.NoError(err, "FetchOptionValueString() cannot fail a valid request")
	assert.Equal(testOptionString0, optionString, "FetchOptionValueString should return the option that was set")

	missingOptionValue0 := ConfMapOption{missingOptionString0}
	missingOptionValue1 := ConfMapOption{missingOptionString1}

	// a new ConfMapOption should not overwrite the existing one
	confMap.SetOptionIfMissing(testSectionName0, testOptionName0, missingOptionValue0)

	optionString, err = confMap.FetchOptionValueString(testSectionName0, testOptionName0)
	assert.NoError(err, "FetchOptionalValueString should not fail since the option exists")
	assert.Equal(testOptionString0, optionString, "SetOptionIfMissing should not change existing optionValue")

	// but a new ConfMapOption should replace a missing one
	optionString, err = confMap.FetchOptionValueString(testSectionName0, testOptionName1)
	assert.Error(err, "FetchOptionalValueString should fail since '%s' has not been set", testOptionName1)

	confMap.SetOptionIfMissing(testSectionName0, testOptionName1, missingOptionValue1)

	optionString, err = confMap.FetchOptionValueString(testSectionName0, testOptionName1)
	assert.NoError(err, "FetchOptionalValueString should not fail since missing value was set")
	assert.Equal(missingOptionString1, optionString, "SetOptionIfMissing should have set missing value")

	// repeat a similar set of tests for sections
	//
	// verify option still exists
	optionString, err = confMap.FetchOptionValueString(testSectionName0, testOptionName0)
	assert.NoError(err, "FetchOptionValueString() cannot fail a valid request")

	// a new ConfMapSection should not overwrite an existing one
	missingSection0 := ConfMapSection{
		testOptionName0: ConfMapOption{missingOptionString0},
		testOptionName1: ConfMapOption{missingOptionString1},
	}
	confMap.SetSectionIfMissing(testSectionName0, missingSection0)

	optionString, err = confMap.FetchOptionValueString(testSectionName0, testOptionName0)
	assert.NoError(err, "FetchOptionalValueString should not fail since the option exists")
	assert.Equal(testOptionString0, optionString, "SetOptionIfMissing should not change existing optionValue")

	optionString, err = confMap.FetchOptionValueString(testSectionName0, testOptionName1)
	assert.NoError(err, "second option should have been added")
	assert.Equal(missingOptionString1, optionString, "SetOptionIfMissing should not change existing optionValue")

	// but a new ConfMapSection should replace a missing one
	confMap.SetSectionIfMissing(testSectionName1, missingSection0)

	optionString, err = confMap.FetchOptionValueString(testSectionName1, testOptionName0)
	assert.NoError(err, "FetchOptionalValueString should not fail since section was added")
	assert.Equal(missingOptionString0, optionString, "missing Option should have been set")

	optionString, err = confMap.FetchOptionValueString(testSectionName1, testOptionName1)
	assert.NoError(err, "FetchOptionalValueString should not fail since section was added")
	assert.Equal(missingOptionString1, optionString, "missing Option should have been set")
}
