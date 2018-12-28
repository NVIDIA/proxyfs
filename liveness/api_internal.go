package liveness

import (
	"reflect"
	"sync"
)

/*
type FetchLivenessReportRequestStruct struct {
	MsgType MsgType // == MsgTypeFetchLivenessReportRequest
	//                 Used to request Liveness Report from who we think is the Leader
	MsgTag      uint64 // Used for matching this FetchLivenessReportRequestStruct to a subsequent FetchLivenessReportResponseStruct
	CurrentTerm uint64
}
type FetchLivenessReportResponseStruct struct {
	MsgType        MsgType               // == MsgTypeFetchLivenessReportResponse
	MsgTag         uint64                // Used for matching this FetchLivenessReportResponseStruct to a previous FetchLivenessReportRequestStruct
	CurrentTerm    uint64                // == LeaderTerm if Success === true (by definition)
	CurrentLeader  string                // If Success == false, this is who should actually be contacted for this (if known)
	Success        bool                  // == true if Leader is responding; == false if we are not the Leader
	LivenessReport *LivenessReportStruct // Liveness Report as collected by Leader
}
func sendRequest(peer *peerStruct, msgTag uint64, requestContext interface{}, requestMsg interface{}, callback func(request *requestStruct)) (err error) {
*/
func fetchLivenessReport() (livenessReport *LivenessReportStruct) {
	var (
		err                         error
		fetchLivenessReportRequest  *FetchLivenessReportRequestStruct
		fetchLivenessReportResponse *FetchLivenessReportResponseStruct
		leaderPeer                  *peerStruct
		leaderResponseDone          sync.WaitGroup
	)

	globals.Lock()
	if globals.active {
		switch reflect.ValueOf(globals.nextState) {
		case reflect.ValueOf(doCandidate):
			// Return a local-only Liveness Report
			livenessReport = &LivenessReportStruct{
				ObservingPeer: make([]*ObservingPeerStruct, 1),
			}
			livenessReport.ObservingPeer[0] = convertInternalToExternalObservingPeerReport(globals.myObservingPeerReport)
		case reflect.ValueOf(doFollower):
			leaderPeer = globals.currentLeader
			if nil == leaderPeer {
				// Special case where we just started in doFollower state but haven't yet received a HeartBeat
				livenessReport = nil
			} else {
				// Need to go ask leaderPeer for Liveness Report
				fetchLivenessReportRequest = &FetchLivenessReportRequestStruct{
					MsgType:     MsgTypeFetchLivenessReportRequest,
					MsgTag:      fetchNonceWhileLocked(),
					CurrentTerm: globals.currentTerm,
				}
				globals.Unlock()
				leaderResponseDone.Add(1)
				err = sendRequest(
					leaderPeer,
					fetchLivenessReportRequest.MsgTag,
					nil, // Only one can be outstanding... so no need for non-nil requestContext
					fetchLivenessReportRequest,
					func(request *requestStruct) {
						if request.expired {
							fetchLivenessReportResponse = nil
						} else {
							fetchLivenessReportResponse = request.responseMsg.(*FetchLivenessReportResponseStruct)
						}
						leaderResponseDone.Done()
					},
				)
				if nil != err {
					panic(err)
				}
				leaderResponseDone.Wait()
				livenessReport = fetchLivenessReportResponse.LivenessReport
				globals.Lock()
			}
		case reflect.ValueOf(doLeader):
			livenessReport = convertInternalToExternalLivenessReport(globals.livenessReport)
		}
	} else {
		livenessReport = nil
	}
	globals.Unlock()
	return
}

func convertInternalToExternalLivenessReport(internalLivenessReport *internalLivenessReportStruct) (externalLivenessReport *LivenessReportStruct) {
	var (
		internalObservingPeerReport *internalObservingPeerReportStruct
		observingPeer               *ObservingPeerStruct
	)

	if nil == internalLivenessReport {
		externalLivenessReport = nil
		return
	}

	externalLivenessReport = &LivenessReportStruct{
		ObservingPeer: make([]*ObservingPeerStruct, 0, len(internalLivenessReport.observingPeer)),
	}

	for _, internalObservingPeerReport = range internalLivenessReport.observingPeer {
		observingPeer = convertInternalToExternalObservingPeerReport(internalObservingPeerReport)
		externalLivenessReport.ObservingPeer = append(externalLivenessReport.ObservingPeer, observingPeer)
	}

	return
}

func convertExternalToInternalLivenessReport(externalLivenessReport *LivenessReportStruct) (internalLivenessReport *internalLivenessReportStruct) {
	var (
		internalObservingPeerReport *internalObservingPeerReportStruct
		observingPeer               *ObservingPeerStruct
	)

	if nil == externalLivenessReport {
		internalLivenessReport = nil
		return
	}

	internalLivenessReport = &internalLivenessReportStruct{
		observingPeer: make(map[string]*internalObservingPeerReportStruct),
	}

	for _, observingPeer = range externalLivenessReport.ObservingPeer {
		internalObservingPeerReport = convertExternalToInternalObservingPeerReport(observingPeer)
		internalLivenessReport.observingPeer[internalObservingPeerReport.name] = internalObservingPeerReport
	}

	return
}

func convertInternalToExternalObservingPeerReport(internalObservingPeerReport *internalObservingPeerReportStruct) (externalObservingPeer *ObservingPeerStruct) {
	var (
		internalServingPeerReport *internalServingPeerReportStruct
		internalVolumeGroupReport *internalVolumeGroupReportStruct
		internalVolumeReport      *internalVolumeReportStruct
		servingPeer               *ServingPeerStruct
		volume                    *VolumeStruct
		volumeGroup               *VolumeGroupStruct
	)

	if nil == internalObservingPeerReport {
		externalObservingPeer = nil
		return
	}

	externalObservingPeer = &ObservingPeerStruct{
		Name:        internalObservingPeerReport.name,
		ServingPeer: make([]*ServingPeerStruct, 0, len(internalObservingPeerReport.servingPeer)),
	}

	for _, internalServingPeerReport = range internalObservingPeerReport.servingPeer {
		servingPeer = &ServingPeerStruct{
			Name:          internalServingPeerReport.name,
			State:         internalServingPeerReport.state,
			LastCheckTime: internalServingPeerReport.lastCheckTime,
			VolumeGroup:   make([]*VolumeGroupStruct, 0, len(internalServingPeerReport.volumeGroup)),
		}

		for _, internalVolumeGroupReport = range internalServingPeerReport.volumeGroup {
			volumeGroup = &VolumeGroupStruct{
				Name:          internalVolumeGroupReport.name,
				State:         internalVolumeGroupReport.state,
				LastCheckTime: internalVolumeGroupReport.lastCheckTime,
				Volume:        make([]*VolumeStruct, 0, len(internalVolumeGroupReport.volume)),
			}

			for _, internalVolumeReport = range internalVolumeGroupReport.volume {
				volume = &VolumeStruct{
					Name:          internalVolumeReport.name,
					State:         internalVolumeReport.state,
					LastCheckTime: internalVolumeReport.lastCheckTime,
				}

				volumeGroup.Volume = append(volumeGroup.Volume, volume)
			}

			servingPeer.VolumeGroup = append(servingPeer.VolumeGroup, volumeGroup)
		}

		externalObservingPeer.ServingPeer = append(externalObservingPeer.ServingPeer, servingPeer)
	}

	return
}

func convertExternalToInternalObservingPeerReport(externalObservingPeer *ObservingPeerStruct) (internalObservingPeerReport *internalObservingPeerReportStruct) {
	var (
		internalServingPeerReport *internalServingPeerReportStruct
		internalVolumeGroupReport *internalVolumeGroupReportStruct
		internalVolumeReport      *internalVolumeReportStruct
		servingPeer               *ServingPeerStruct
		volume                    *VolumeStruct
		volumeGroup               *VolumeGroupStruct
	)

	if nil == externalObservingPeer {
		internalObservingPeerReport = nil
		return
	}

	internalObservingPeerReport = &internalObservingPeerReportStruct{
		name:        externalObservingPeer.Name,
		servingPeer: make(map[string]*internalServingPeerReportStruct),
	}

	for _, servingPeer = range externalObservingPeer.ServingPeer {
		internalServingPeerReport = &internalServingPeerReportStruct{
			observingPeer: internalObservingPeerReport,
			name:          servingPeer.Name,
			state:         servingPeer.State,
			lastCheckTime: servingPeer.LastCheckTime,
			volumeGroup:   make(map[string]*internalVolumeGroupReportStruct),
		}

		for _, volumeGroup = range servingPeer.VolumeGroup {
			internalVolumeGroupReport = &internalVolumeGroupReportStruct{
				servingPeer:   internalServingPeerReport,
				name:          volumeGroup.Name,
				state:         volumeGroup.State,
				lastCheckTime: volumeGroup.LastCheckTime,
				volume:        make(map[string]*internalVolumeReportStruct),
			}

			for _, volume = range volumeGroup.Volume {
				internalVolumeReport = &internalVolumeReportStruct{
					volumeGroup:   internalVolumeGroupReport,
					name:          volume.Name,
					state:         volume.State,
					lastCheckTime: volume.LastCheckTime,
				}

				internalVolumeGroupReport.volume[internalVolumeReport.name] = internalVolumeReport
			}

			internalServingPeerReport.volumeGroup[internalVolumeGroupReport.name] = internalVolumeGroupReport
		}

		internalObservingPeerReport.servingPeer[internalServingPeerReport.name] = internalServingPeerReport
	}

	return
}
