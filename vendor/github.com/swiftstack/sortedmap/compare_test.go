package sortedmap

import "testing"

func TestCompareFunctions(t *testing.T) {
	var (
		result int
		err    error
	)

	result, err = CompareInt(int(1), int(2))
	if nil != err {
		t.Fatalf("CompareInt(int(1),int(2)) should not have failed")
	}
	if result >= 0 {
		t.Fatalf("CompareInt(int(1),int(2)) should have been < 0")
	}
	result, err = CompareInt(int(2), int(2))
	if nil != err {
		t.Fatalf("CompareInt(int(2),int(2)) should not have failed")
	}
	if result != 0 {
		t.Fatalf("CompareInt(int(2),int(2)) should have been == 0")
	}
	result, err = CompareInt(int(3), int(2))
	if nil != err {
		t.Fatalf("CompareInt(int(3),int(2)) should not have failed")
	}
	if result <= 0 {
		t.Fatalf("CompareInt(int(3),int(2)) should have been > 0")
	}
	_, err = CompareInt(int(2), uint32(2))
	if nil == err {
		t.Fatalf("CompareInt(int(2),uint32(2)) should have failed")
	}
	_, err = CompareInt(uint32(2), int(2))
	if nil == err {
		t.Fatalf("CompareInt(uint32(2),int(2)) should have failed")
	}

	result, err = CompareUint16(uint16(1), uint16(2))
	if nil != err {
		t.Fatalf("CompareUint16(uint16(1),uint16(2)) should not have failed")
	}
	if result >= 0 {
		t.Fatalf("CompareUint16(uint32(1),uint32(2)) should have been < 0")
	}
	result, err = CompareUint16(uint16(2), uint16(2))
	if nil != err {
		t.Fatalf("CompareUint16(uint16(2),uint16(2)) should not have failed")
	}
	if result != 0 {
		t.Fatalf("CompareUint16(uint16(2),uint16(2)) should have been == 0")
	}
	result, err = CompareUint16(uint16(3), uint16(2))
	if nil != err {
		t.Fatalf("CompareUint16(uint16(3),uint16(2)) should not have failed")
	}
	if result <= 0 {
		t.Fatalf("CompareUint16(uint16(3),uint16(2)) should have been > 0")
	}
	_, err = CompareUint16(uint32(2), uint64(2))
	if nil == err {
		t.Fatalf("CompareUint16(uint16(2), uint32(2)) should have failed")
	}
	_, err = CompareUint16(uint64(2), uint32(2))
	if nil == err {
		t.Fatalf("CompareUint16(uint32(2), uint16(2)) should have failed")
	}

	result, err = CompareUint32(uint32(1), uint32(2))
	if nil != err {
		t.Fatalf("CompareUint32(uint32(1),uint32(2)) should not have failed")
	}
	if result >= 0 {
		t.Fatalf("CompareUint32(uint32(1),uint32(2)) should have been < 0")
	}
	result, err = CompareUint32(uint32(2), uint32(2))
	if nil != err {
		t.Fatalf("CompareUint32(uint32(2),uint32(2)) should not have failed")
	}
	if result != 0 {
		t.Fatalf("CompareUint32(uint32(2),uint32(2)) should have been == 0")
	}
	result, err = CompareUint32(uint32(3), uint32(2))
	if nil != err {
		t.Fatalf("CompareUint32(uint32(3),uint32(2)) should not have failed")
	}
	if result <= 0 {
		t.Fatalf("CompareUint32(uint32(3),uint32(2)) should have been > 0")
	}
	_, err = CompareUint32(uint32(2), uint64(2))
	if nil == err {
		t.Fatalf("CompareUint32(uint32(2), uint64(2)) should have failed")
	}
	_, err = CompareUint32(uint64(2), uint32(2))
	if nil == err {
		t.Fatalf("CompareUint32(uint64(2), uint32(2)) should have failed")
	}

	result, err = CompareUint64(uint64(1), uint64(2))
	if nil != err {
		t.Fatalf("CompareUint64(uint32(1),uint32(2)) should not have failed")
	}
	if result >= 0 {
		t.Fatalf("CompareUint64(uint64(1),uint64(2)) should have been < 0")
	}
	result, err = CompareUint64(uint64(2), uint64(2))
	if nil != err {
		t.Fatalf("CompareUint64(uint32(2),uint32(2)) should not have failed")
	}
	if result != 0 {
		t.Fatalf("CompareUint64(uint64(2),uint64(2)) should have been == 0")
	}
	result, err = CompareUint64(uint64(3), uint64(2))
	if nil != err {
		t.Fatalf("CompareUint64(uint32(3),uint32(2)) should not have failed")
	}
	if result <= 0 {
		t.Fatalf("CompareUint64(uint64(3),uint64(2)) should have been > 0")
	}
	_, err = CompareUint64(uint64(2), string("2"))
	if nil == err {
		t.Fatalf("CompareUint64(uint64(2), string(\"2\")) should have failed")
	}
	_, err = CompareUint64(string("2"), uint64(2))
	if nil == err {
		t.Fatalf("CompareUint64(string(\"2\"), uint64(2)) should have failed")
	}

	result, err = CompareString(string("1"), string("2"))
	if nil != err {
		t.Fatalf("CompareString(string(\"1\"), string(\"2\")) should not have failed")
	}
	if result >= 0 {
		t.Fatalf("CompareString(string(\"1\"), string(\"2\")) should have been < 0")
	}
	result, err = CompareString(string("2"), string("2"))
	if nil != err {
		t.Fatalf("CompareString(string(\"2\"), string(\"2\")) should not have failed")
	}
	if result != 0 {
		t.Fatalf("CompareString(string(\"2\"), string(\"2\")) should have been == 0")
	}
	result, err = CompareString(string("3"), string("2"))
	if nil != err {
		t.Fatalf("CompareString(string(\"3\"), string(\"2\")) should not have failed")
	}
	if result <= 0 {
		t.Fatalf("CompareString(string(\"3\"), string(\"2\")) should have been > 0")
	}
	_, err = CompareString(string("2"), []byte{2})
	if nil == err {
		t.Fatalf("CompareString(string(\"2\"), []byte{2}) should have failed")
	}
	_, err = CompareString([]byte{2}, string("2"))
	if nil == err {
		t.Fatalf("CompareString([]byte{2}, string(\"2\")) should have failed")
	}

	result, err = CompareByteSlice([]byte{1}, []byte{2})
	if nil != err {
		t.Fatalf("CompareByteSlice([]byte{1}, []byte{2}) should not have failed")
	}
	if result >= 0 {
		t.Fatalf("CompareByteSlice([]byte{1}, []byte{2}) should have been < 0")
	}
	result, err = CompareByteSlice([]byte{2}, []byte{2})
	if nil != err {
		t.Fatalf("CompareByteSlice([]byte{2}, []byte{2}) should not have failed")
	}
	if result != 0 {
		t.Fatalf("CompareByteSlice([]byte{2}, []byte{2}) should have been == 0")
	}
	result, err = CompareByteSlice([]byte{3}, []byte{2})
	if nil != err {
		t.Fatalf("CompareByteSlice([]byte{3}, []byte{2}) should not have failed")
	}
	if result <= 0 {
		t.Fatalf("CompareByteSlice([]byte{3}, []byte{2}) should have been > 0")
	}
	_, err = CompareByteSlice([]byte{2}, int(2))
	if nil == err {
		t.Fatalf("CompareByteSlice([]byte{2}, int(2)) should have failed")
	}

	_, err = CompareByteSlice(int(2), []byte{2})
	if nil == err {
		t.Fatalf("CompareByteSlice(int(2), []byte{2}) should have failed")
	}
}
