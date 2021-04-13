// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

import (
	"container/list"
	"strconv"

	"github.com/NVIDIA/proxyfs/bucketstats"
)

// This file contains functions in the server which
// track clients and initialize clientInfo.

func (ci *clientInfo) isEmpty() bool {
	return len(ci.completedRequest) == 0
}

func (ci *clientInfo) completedCnt() int {
	return len(ci.completedRequest)
}

func methodAndName(name string, method string) string {
	return name + "-" + method
}

func initClientInfo(cCtx *connCtx, newUniqueID uint64, server *Server) (ci *clientInfo) {
	ci = &clientInfo{cCtx: cCtx, myUniqueID: newUniqueID}
	ci.completedRequest = make(map[requestID]*completedEntry)
	ci.completedRequestLRU = list.New()
	ci.stats.PerMethodStats = make(map[string]*methodStats)

	// Register per method stats
	for m, _ := range server.svrMap {
		ms := &methodStats{Method: m}
		ci.stats.PerMethodStats[m] = ms
		bucketstats.Register("proxyfs.retryrpc", methodAndName(strconv.FormatInt(int64(ci.myUniqueID), 10), m), ms)
	}
	return
}

// Bump bucketstats for this method
func (ci *clientInfo) setMethodStats(method string, deltaTime uint64) {
	ms := ci.stats.PerMethodStats[method]
	ms.Count.Add(1)
	ms.TimeOfRPCCall.Add(deltaTime)
}

// Unregister per method bucketstats for this client
func (ci *clientInfo) unregsiterMethodStats(server *Server) {
	for m, _ := range server.svrMap {
		bucketstats.UnRegister("proxyfs.retryrpc", methodAndName(strconv.FormatInt(int64(ci.myUniqueID), 10), m))
	}
}
