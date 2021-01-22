// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"encoding/json"
	"fmt"
	"testing"
)

// TestComputeLivenessCheckAssignments tests the named func against the following config:
//
//   PeerA - hosting VolumeGroupAA & VolumeGroupAB
//   PeerB - hosting VolumeGroupBA, VolumeGroupBB, & VolumeGroupBC
//   PeerC - hosting VolumeGroupCA, VolumeGroupCB, & VolumeGroupCC
//   PeerD - not hosting any VolumeGroups
//
//   VolumeGroupAA - hosting VolumeAAA & VolumeAAB
//   VolumeGroupAB - hosting VolumeABA, VolumeABB, and VolumeABC
//
//   VolumeGroupBA - hosting VolumeBAA
//   VolumeGroupBB - hosting VolumeBBA & VolumeBBB
//   VolumeGroupBC - hosting VolumeBCA
//
//   VolumeGroupCA - hosting VolumeCAA, VolumeCAB, & VolumeCAC
//   VolumeGroupCB - hosting VolumeCBA & VolumeCBB
//   VolumeGroupCC - hosting no volumes
//
//   We will always use livenessCheckRedundancy == 3
//   We will always use whoAmI == "PeerA"
//   We will vary observingPeerNameList == []{"PeerA"} and []{"PeerA", "PeerB", "PeerC", "PeerD"}
//
func TestComputeLivenessCheckAssignments(t *testing.T) {
	var (
		// Peers (other than whoAmI)
		peerB *peerStruct
		peerC *peerStruct
		peerD *peerStruct
		// VolumeGroups
		volumeGroupAA *volumeGroupStruct
		volumeGroupAB *volumeGroupStruct
		volumeGroupBA *volumeGroupStruct
		volumeGroupBB *volumeGroupStruct
		volumeGroupBC *volumeGroupStruct
		volumeGroupCA *volumeGroupStruct
		volumeGroupCB *volumeGroupStruct
		volumeGroupCC *volumeGroupStruct
		// Volumes
		volumeAAA *volumeStruct
		volumeAAB *volumeStruct
		volumeABA *volumeStruct
		volumeABB *volumeStruct
		volumeABC *volumeStruct
		volumeBAA *volumeStruct
		volumeBBA *volumeStruct
		volumeBBB *volumeStruct
		volumeBCA *volumeStruct
		volumeCAA *volumeStruct
		volumeCAB *volumeStruct
		volumeCAC *volumeStruct
		volumeCBA *volumeStruct
		volumeCBB *volumeStruct
		// Other locals
		err                                      error
		externalLivenessReportForAllPeers        *LivenessReportStruct
		externalLivenessReportForAllPeersAsJSON  []byte
		externalLivenessReportForJustPeerA       *LivenessReportStruct
		externalLivenessReportForJustPeerAAsJSON []byte
		internalLivenessReportForAllPeers        *internalLivenessReportStruct
		internalLivenessReportForJustPeerA       *internalLivenessReportStruct
		observingPeernameListWithAllPeers        []string
		observingPeernameListWithJustPeerA       []string
	)

	// Setup baseline globals

	globals.whoAmI = "PeerA"
	globals.myVolumeGroupMap = make(map[string]*volumeGroupStruct)

	volumeGroupAA = &volumeGroupStruct{
		peer:      nil,
		name:      "VolumeGroupAA",
		volumeMap: make(map[string]*volumeStruct),
	}
	globals.myVolumeGroupMap[volumeGroupAA.name] = volumeGroupAA

	volumeAAA = &volumeStruct{
		volumeGroup: volumeGroupAA,
		name:        "VolumeAAA",
	}
	volumeGroupAA.volumeMap[volumeAAA.name] = volumeAAA

	volumeAAB = &volumeStruct{
		volumeGroup: volumeGroupAA,
		name:        "VolumeAAB",
	}
	volumeGroupAA.volumeMap[volumeAAB.name] = volumeAAB

	volumeGroupAB = &volumeGroupStruct{
		peer:      nil,
		name:      "VolumeGroupAB",
		volumeMap: make(map[string]*volumeStruct),
	}
	globals.myVolumeGroupMap[volumeGroupAB.name] = volumeGroupAB

	volumeABA = &volumeStruct{
		volumeGroup: volumeGroupAB,
		name:        "VolumeABA",
	}
	volumeGroupAB.volumeMap[volumeAAA.name] = volumeABA

	volumeABB = &volumeStruct{
		volumeGroup: volumeGroupAB,
		name:        "VolumeABB",
	}
	volumeGroupAB.volumeMap[volumeAAB.name] = volumeABB

	volumeABC = &volumeStruct{
		volumeGroup: volumeGroupAB,
		name:        "VolumeABC",
	}
	volumeGroupAB.volumeMap[volumeAAB.name] = volumeABC

	peerB = &peerStruct{
		name:           "PeerB",
		volumeGroupMap: make(map[string]*volumeGroupStruct),
	}

	volumeGroupBA = &volumeGroupStruct{
		peer:      peerB,
		name:      "VolumeGroupBA",
		volumeMap: make(map[string]*volumeStruct),
	}
	peerB.volumeGroupMap[volumeGroupBA.name] = volumeGroupBA

	volumeBAA = &volumeStruct{
		volumeGroup: volumeGroupBA,
		name:        "VolumeBAA",
	}
	volumeGroupBA.volumeMap[volumeBAA.name] = volumeBAA

	volumeGroupBB = &volumeGroupStruct{
		peer:      peerB,
		name:      "VolumeGroupBB",
		volumeMap: make(map[string]*volumeStruct),
	}
	peerB.volumeGroupMap[volumeGroupBB.name] = volumeGroupBB

	volumeBBA = &volumeStruct{
		volumeGroup: volumeGroupBA,
		name:        "VolumeBBA",
	}
	volumeGroupBB.volumeMap[volumeBBA.name] = volumeBBA

	volumeBBB = &volumeStruct{
		volumeGroup: volumeGroupBA,
		name:        "VolumeBBB",
	}
	volumeGroupBB.volumeMap[volumeBBB.name] = volumeBBB

	volumeGroupBC = &volumeGroupStruct{
		peer:      peerB,
		name:      "VolumeGroupBC",
		volumeMap: make(map[string]*volumeStruct),
	}
	peerB.volumeGroupMap[volumeGroupBC.name] = volumeGroupBC

	volumeBCA = &volumeStruct{
		volumeGroup: volumeGroupBC,
		name:        "VolumeBCA",
	}
	volumeGroupBC.volumeMap[volumeBCA.name] = volumeBCA

	peerC = &peerStruct{
		name:           "PeerC",
		volumeGroupMap: make(map[string]*volumeGroupStruct),
	}

	volumeGroupCA = &volumeGroupStruct{
		peer:      peerC,
		name:      "VolumeGroupCA",
		volumeMap: make(map[string]*volumeStruct),
	}
	peerC.volumeGroupMap[volumeGroupCA.name] = volumeGroupCA

	volumeCAA = &volumeStruct{
		volumeGroup: volumeGroupCA,
		name:        "VolumeCAA",
	}
	volumeGroupCA.volumeMap[volumeCAA.name] = volumeCAA

	volumeCAB = &volumeStruct{
		volumeGroup: volumeGroupCA,
		name:        "VolumeCAB",
	}
	volumeGroupCA.volumeMap[volumeCAB.name] = volumeCAB

	volumeCAC = &volumeStruct{
		volumeGroup: volumeGroupCA,
		name:        "VolumeCAC",
	}
	volumeGroupCA.volumeMap[volumeCAC.name] = volumeCAC

	volumeGroupCB = &volumeGroupStruct{
		peer:      peerC,
		name:      "VolumeGroupCB",
		volumeMap: make(map[string]*volumeStruct),
	}
	peerC.volumeGroupMap[volumeGroupCB.name] = volumeGroupCB

	volumeCBA = &volumeStruct{
		volumeGroup: volumeGroupCB,
		name:        "VolumeCBA",
	}
	volumeGroupCB.volumeMap[volumeCBA.name] = volumeCBA

	volumeCBB = &volumeStruct{
		volumeGroup: volumeGroupCB,
		name:        "VolumeCBB",
	}
	volumeGroupCB.volumeMap[volumeCBB.name] = volumeCBB

	volumeGroupCC = &volumeGroupStruct{
		peer:      peerC,
		name:      "VolumeGroupCC",
		volumeMap: make(map[string]*volumeStruct),
	}
	peerC.volumeGroupMap[volumeGroupCC.name] = volumeGroupCC

	peerD = &peerStruct{
		name:           "PeerB",
		volumeGroupMap: make(map[string]*volumeGroupStruct),
	}

	globals.peersByName = make(map[string]*peerStruct)

	globals.peersByName["PeerB"] = peerB
	globals.peersByName["PeerC"] = peerC
	globals.peersByName["PeerD"] = peerD

	globals.volumeToCheckList = make([]*volumeStruct, 0)

	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeAAA)
	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeAAB)

	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeABA)
	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeABB)
	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeABC)

	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeBAA)

	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeBBA)
	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeBBB)

	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeBCA)

	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeCAA)
	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeCAB)
	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeCAC)

	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeCBA)
	globals.volumeToCheckList = append(globals.volumeToCheckList, volumeCBB)

	globals.emptyServingPeerToCheckSet = make(map[string]struct{})

	globals.emptyServingPeerToCheckSet["PeerD"] = struct{}{}

	globals.livenessCheckRedundancy = 3

	// Validate for observingPeerNameList == []{"PeerA"}

	observingPeernameListWithJustPeerA = []string{"PeerA"}

	internalLivenessReportForJustPeerA = computeLivenessCheckAssignments(observingPeernameListWithJustPeerA)

	externalLivenessReportForJustPeerA = convertInternalToExternalLivenessReport(internalLivenessReportForJustPeerA)

	fmt.Println("externalLivenessReportForJustPeerA:")
	externalLivenessReportForJustPeerAAsJSON, err = json.MarshalIndent(externalLivenessReportForJustPeerA, "", "  ")
	if nil != err {
		t.Fatal(err)
	}
	fmt.Println(string(externalLivenessReportForJustPeerAAsJSON))

	// TODO: Actually validate it programmatically

	// Validate for observingPeerNameList == []{"PeerA", "PeerB", "PeerC", "PeerD"}

	observingPeernameListWithAllPeers = []string{"PeerA", "PeerB", "PeerC", "PeerD"}

	internalLivenessReportForAllPeers = computeLivenessCheckAssignments(observingPeernameListWithAllPeers)

	externalLivenessReportForAllPeers = convertInternalToExternalLivenessReport(internalLivenessReportForAllPeers)

	fmt.Println("externalLivenessReportForAllPeers:")
	externalLivenessReportForAllPeersAsJSON, err = json.MarshalIndent(externalLivenessReportForAllPeers, "", "  ")
	if nil != err {
		t.Fatal(err)
	}
	fmt.Println(string(externalLivenessReportForAllPeersAsJSON))

	// TODO: Actually validate it programmatically

	// All done
}
