package swiftclient

import (
	"testing"
)

func TestUtils(t *testing.T) {
	masterHeaders := make(map[string][]string)
	masterList := make([]string, 0)

	toAddHeaders1 := make(map[string][]string)
	toAddList1 := make([]string, 0)

	toAddHeaders2 := make(map[string][]string)
	toAddList2 := make([]string, 0)

	toAddHeaders3 := make(map[string][]string)
	toAddList3 := make([]string, 0)

	toAddHeaders1["Content-Length"] = []string{"4"}
	toAddHeaders1["Dummy-Header-W"] = []string{"W"}
	toAddHeaders1["Dummy-Header-X"] = []string{"XA"}
	toAddHeaders1["Dummy-Header-Z"] = []string{}
	toAddList1 = []string{"A", "B"}

	toAddHeaders2["Content-Length"] = []string{"6"}
	toAddHeaders2["Dummy-Header-W"] = []string{"W"}
	toAddHeaders2["Dummy-Header-X"] = []string{"XB", "XC"}
	toAddHeaders2["Dummy-Header-Y"] = []string{"Y"}
	toAddList2 = []string{"C", "D", "E"}

	toAddHeaders3["Content-Length"] = []string{"8"}
	toAddHeaders3["Dummy-Header-W"] = []string{"W"}
	toAddHeaders3["Dummy-Header-Z"] = []string{"ZA", "ZB"}
	toAddList3 = []string{}

	mergeHeadersAndList(masterHeaders, &masterList, toAddHeaders1, &toAddList1)
	mergeHeadersAndList(masterHeaders, &masterList, toAddHeaders2, &toAddList2)
	mergeHeadersAndList(masterHeaders, &masterList, toAddHeaders3, &toAddList3)

	if 5 != len(masterHeaders) {
		t.Fatalf("masterHeaders had unexpected len (%v)", len(masterHeaders))
	}

	masterContentLength, ok := masterHeaders["Content-Length"]
	if !ok {
		t.Fatalf("masterHeaders[\"Content-Length\"] returned !ok")
	}
	if 1 != len(masterContentLength) {
		t.Fatalf("masterHeaders[\"Content-Length\"] had unexpected len (%v)", len(masterContentLength))
	}
	if "18" != masterContentLength[0] {
		t.Fatalf("masterHeaders[\"Content-Length\"] returned unexpected value ([\"%v\"])", masterContentLength[0])
	}

	masterDummyHeaderW, ok := masterHeaders["Dummy-Header-W"]
	if !ok {
		t.Fatalf("masterHeaders[\"Dummy-Header-W\"] returned !ok")
	}
	if 1 != len(masterDummyHeaderW) {
		t.Fatalf("masterHeaders[\"Dummy-Header-W\"] had unexpected len (%v)", len(masterDummyHeaderW))
	}
	if "W" != masterDummyHeaderW[0] {
		t.Fatalf("masterHeaders[\"Dummy-Header-W\"] had unexpected value ([\"%v\"])", masterDummyHeaderW[0])
	}

	masterDummyHeaderX, ok := masterHeaders["Dummy-Header-X"]
	if !ok {
		t.Fatalf("masterHeaders[\"Dummy-Header-X\"] returned !ok")
	}
	if 3 != len(masterDummyHeaderX) {
		t.Fatalf("masterHeaders[\"Dummy-Header-X\"] had unexpected len (%v)", len(masterDummyHeaderX))
	}
	if "XA" != masterDummyHeaderX[0] {
		t.Fatalf("masterHeaders[\"Dummy-Header-X\"][0] had unexpected value (\"%v\")", masterDummyHeaderX[0])
	}
	if "XB" != masterDummyHeaderX[1] {
		t.Fatalf("masterHeaders[\"Dummy-Header-X\"][1] had unexpected value (\"%v\")", masterDummyHeaderX[1])
	}
	if "XC" != masterDummyHeaderX[2] {
		t.Fatalf("masterHeaders[\"Dummy-Header-X\"][2] had unexpected value (\"%v\")", masterDummyHeaderX[2])
	}

	masterDummyHeaderY, ok := masterHeaders["Dummy-Header-Y"]
	if !ok {
		t.Fatalf("masterHeaders[\"Dummy-Header-Y\"] returned !ok")
	}
	if 1 != len(masterDummyHeaderY) {
		t.Fatalf("masterHeaders[\"Dummy-Header-Y\"] had unexpected len (%v)", len(masterDummyHeaderY))
	}
	if "Y" != masterDummyHeaderY[0] {
		t.Fatalf("masterHeaders[\"Dummy-Header-Y\"] had unexpected value ([\"%v\"])", masterDummyHeaderY[0])
	}

	masterDummyHeaderZ, ok := masterHeaders["Dummy-Header-Z"]
	if !ok {
		t.Fatalf("masterHeaders[\"Dummy-Header-Z\"] returned !ok")
	}
	if 2 != len(masterDummyHeaderZ) {
		t.Fatalf("masterHeaders[\"Dummy-Header-Z\"] had unexpected len (%v)", len(masterDummyHeaderZ))
	}
	if "ZA" != masterDummyHeaderZ[0] {
		t.Fatalf("masterHeaders[\"Dummy-Header-Z\"][0] had unexpected value (\"%v\")", masterDummyHeaderZ[0])
	}
	if "ZB" != masterDummyHeaderZ[1] {
		t.Fatalf("masterHeaders[\"Dummy-Header-Z\"][1] had unexpected value (\"%v\")", masterDummyHeaderZ[1])
	}

	if 5 != len(masterList) {
		t.Fatalf("masterList had unexpected len (%v)", len(masterContentLength))
	}
	if "A" != masterList[0] {
		t.Fatalf("masterList[0] had unexpected value (\"%v\")", masterList[0])
	}
	if "B" != masterList[1] {
		t.Fatalf("masterList[1] had unexpected value (\"%v\")", masterList[1])
	}
	if "C" != masterList[2] {
		t.Fatalf("masterList[2] had unexpected value (\"%v\")", masterList[2])
	}
	if "D" != masterList[3] {
		t.Fatalf("masterList[3] had unexpected value (\"%v\")", masterList[3])
	}
	if "E" != masterList[4] {
		t.Fatalf("masterList[4] had unexpected value (\"%v\")", masterList[4])
	}
}
