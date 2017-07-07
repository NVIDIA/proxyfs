package utf

import "bytes"
import "strings"
import "testing"

func TestUTF16ByteSliceToString(t *testing.T) {
	var goodUTF16ByteSliceLittleEndian = []byte{0x55, 0x00, 0x0054, 0x00, 0x46, 0x00, 0x2D, 0x00, 0x31, 0x00, 0x36, 0x00}
	var goodUTF16ByteSliceBigEndian = []byte{0x00, 0x55, 0x00, 0x0054, 0x00, 0x46, 0x00, 0x2D, 0x00, 0x31, 0x00, 0x36}
	var goodUTF16ByteSliceToStringExpected = "UTF-16"
	var badUTF16ByteSlice = []byte{0x01, 0x02, 0x03}

	goodUTF16ByteSliceLittleEndianToStringReturned, err := UTF16ByteSliceToString(goodUTF16ByteSliceLittleEndian, LittleEndian)

	if nil != err {
		t.Fatalf("UTF16ByteSliceToString(goodUTF16ByteSliceLittleEndian, LittleEndian) failed unexpectedly: %v", err)
	}
	if 0 != strings.Compare(goodUTF16ByteSliceLittleEndianToStringReturned, goodUTF16ByteSliceToStringExpected) {
		t.Fatalf("UTF16ByteSliceToString(goodUTF16ByteSliceLittleEndian, LittleEndian) returned \"%v\" instead of the expected \"%v\"", goodUTF16ByteSliceLittleEndianToStringReturned, goodUTF16ByteSliceToStringExpected)
	}

	goodUTF16ByteSliceBigEndianToStringReturned, err := UTF16ByteSliceToString(goodUTF16ByteSliceBigEndian, BigEndian)

	if nil != err {
		t.Fatalf("UTF16ByteSliceToString(goodUTF16ByteSliceBigEndian, BigEndian) failed unexpectedly: %v", err)
	}
	if 0 != strings.Compare(goodUTF16ByteSliceBigEndianToStringReturned, goodUTF16ByteSliceToStringExpected) {
		t.Fatalf("UTF16ByteSliceToString(goodUTF16ByteSliceBigEndian, BigEndian) returned \"%v\" instead of the expected \"%v\"", goodUTF16ByteSliceBigEndianToStringReturned, goodUTF16ByteSliceToStringExpected)
	}

	_, err = UTF16ByteSliceToString(badUTF16ByteSlice, LittleEndian)

	if nil == err {
		t.Fatalf("UTF16ByteSliceToString(badUTF16ByteSlice, LittleEndian) succeeded unexpectedly")
	}

	_, err = UTF16ByteSliceToString(badUTF16ByteSlice, BigEndian)

	if nil == err {
		t.Fatalf("UTF16ByteSliceToString(badUTF16ByteSlice, BigEndian) succeeded unexpectedly")
	}
}

func TestStringToUTF16ByteSlice(t *testing.T) {
	var utf8String = "UTF-16"
	var u8BufUTF16LittleEndianExpected = []byte{0x55, 0x00, 0x0054, 0x00, 0x46, 0x00, 0x2D, 0x00, 0x31, 0x00, 0x36, 0x00}
	var u8BufUTF16BigEndianExpected = []byte{0x00, 0x55, 0x00, 0x0054, 0x00, 0x46, 0x00, 0x2D, 0x00, 0x31, 0x00, 0x36}

	u8BufUTF16LittleEndianReturned := StringToUTF16ByteSlice(utf8String, LittleEndian)

	if 0 != bytes.Compare(u8BufUTF16LittleEndianReturned, u8BufUTF16LittleEndianExpected) {
		t.Fatalf("StringToUTF16ByteSlice(utf8String, LittleEndian) returned unexpected u8Buf")
	}

	u8BufUTF16BigEndianReturned := StringToUTF16ByteSlice(utf8String, BigEndian)

	if 0 != bytes.Compare(u8BufUTF16BigEndianReturned, u8BufUTF16BigEndianExpected) {
		t.Fatalf("StringToUTF16ByteSlice(utf8String, BigEndian) returned unexpected u8Buf")
	}
}

func TestUTF8ByteSliceToString(t *testing.T) {
	var goodUTF8ByteSlice = []byte{0x55, 0x54, 0x46, 0x2D, 0x38}
	var goodUTF8ByteSliceToStringExpected = "UTF-8"
	var badUTF8ByteSlice = []byte{0xFF}

	goodUTF8ByteSliceToStringReturned, err := UTF8ByteSliceToString(goodUTF8ByteSlice)

	if nil != err {
		t.Fatalf("UTF8ByteSliceToString(goodUTF8ByteSlice) failed unexpectedly: %v", err)
	}
	if 0 != strings.Compare(goodUTF8ByteSliceToStringExpected, goodUTF8ByteSliceToStringReturned) {
		t.Fatalf("UTF8ByteSliceToString(goodUTF16leByteSlice) returned \"%v\" instead of the expected \"%v\"", goodUTF8ByteSliceToStringReturned, goodUTF8ByteSliceToStringExpected)
	}

	_, err = UTF8ByteSliceToString(badUTF8ByteSlice)

	if nil == err {
		t.Fatalf("UTF8ByteSliceToString(badUTF8ByteSlice) succeeded unexpectedly")
	}
}

func TestStringToUTF8ByteSlice(t *testing.T) {
	var utf8String = "UTF-8"
	var utf8StringToUTF8ByteSliceExpected = []byte{0x55, 0x54, 0x46, 0x2D, 0x38}

	utf8StringToUTF8ByteSliceReturned := StringToUTF8ByteSlice(utf8String)

	if 0 != bytes.Compare(utf8StringToUTF8ByteSliceReturned, utf8StringToUTF8ByteSliceExpected) {
		t.Fatalf("StringToUTF8ByteSlice(utf8String) returned unexpected u8Buf")
	}
}
