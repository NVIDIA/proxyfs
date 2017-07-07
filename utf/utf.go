// Package utf provides utilities for working with UTF strings, notably including UTF16.
package utf

import "bytes"
import "encoding/binary"
import "fmt"
import "unicode/utf16"
import "unicode/utf8"

var LittleEndian binary.ByteOrder = binary.LittleEndian
var BigEndian binary.ByteOrder = binary.BigEndian

func UTF16ByteSliceToString(u8Buf []byte, byteOrder binary.ByteOrder) (utf8String string, err error) {
	// Set default return values

	utf8String = ""
	err = nil

	// Ensure []byte can be interpretted as []uint16

	if 0 != (len(u8Buf) % 2) {
		err = fmt.Errorf("UTF-16-LE requires []byte with even number of bytes")
		return
	}

	// Convert u8Buf ([]byte) to u16Buf ([]uint16)

	numUint16s := len(u8Buf) / 2
	u16Reader := bytes.NewReader(u8Buf)
	u16Buf := make([]uint16, numUint16s)

	err = binary.Read(u16Reader, byteOrder, &u16Buf)
	if nil != err {
		return
	}

	// Convert u16Buf ([]uint16) to runeForm ([]rune)

	runeFormSlice := utf16.Decode(u16Buf)

	// Encode runeFormSlice elements into bytes.Buffer

	var runeByteBuffer bytes.Buffer

	for _, runeFormElement := range runeFormSlice {
		_, _ = runeByteBuffer.WriteRune(runeFormElement)
	}

	// Return resultant string from runeByteBuffer

	utf8String = runeByteBuffer.String()

	return
}

func StringToUTF16ByteSlice(utf8String string, byteOrder binary.ByteOrder) (u8Buf []byte) {
	runeArray := []rune(utf8String)
	u16Slice := utf16.Encode(runeArray)

	u8Buf = make([]byte, (2 * len(u16Slice)))

	for i := 0; i < len(u16Slice); i++ {
		if binary.LittleEndian == byteOrder {
			u8Buf[(2*i)+0] = byte((u16Slice[i] >> 0) & 0xFF)
			u8Buf[(2*i)+1] = byte((u16Slice[i] >> 8) & 0xFF)
		} else { // binary.BigEndian == byteOrder
			u8Buf[(2*i)+1] = byte((u16Slice[i] >> 0) & 0xFF)
			u8Buf[(2*i)+0] = byte((u16Slice[i] >> 8) & 0xFF)
		}
	}

	return
}

func UTF8ByteSliceToString(u8Buf []byte) (utf8String string, err error) {
	if !utf8.Valid(u8Buf) {
		utf8String = ""
		err = fmt.Errorf("Not valid UTF-8")
		return
	}

	utf8String = string(u8Buf)
	err = nil

	return
}

func StringToUTF8ByteSlice(utf8String string) (u8Buf []byte) {
	u8Buf = []byte(utf8String)

	return
}
