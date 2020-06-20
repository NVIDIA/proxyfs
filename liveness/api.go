package liveness

import (
	"time"
)

const (
	StateAlive   = "alive"
	StateDead    = "dead"
	StateUnknown = "unknown"
)

type VolumeStruct struct {
	Name          string
	State         string // One of const State{Alive|Dead|Unknown}
	LastCheckTime time.Time
}

type VolumeGroupStruct struct {
	Name          string
	State         string // One of const State{Alive|Dead|Unknown}
	LastCheckTime time.Time
	Volume        []*VolumeStruct
}

type ServingPeerStruct struct {
	Name          string
	State         string // One of const State{Alive|Dead|Unknown}
	LastCheckTime time.Time
	VolumeGroup   []*VolumeGroupStruct
}

type ReconEndpointStruct struct {
	IPAddrPort             string
	MaxDiskUsagePercentage uint8
}

type ObservingPeerStruct struct {
	Name          string
	ServingPeer   []*ServingPeerStruct
	ReconEndpoint []*ReconEndpointStruct
}

type LivenessReportStruct struct {
	ObservingPeer []*ObservingPeerStruct
}

func FetchLivenessReport() (livenessReport *LivenessReportStruct) {
	livenessReport = fetchLivenessReport()
	return
}
