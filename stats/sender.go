// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package stats

import (
	"fmt"
	"net"
	"strconv"
	// XXX TODO: Can't call logger from here since logger calls stats.
	//"github.com/NVIDIA/proxyfs/logger"
)

func sender() {
	var (
		err             error
		newStatNameLink *statNameLinkStruct
		ok              bool
		oldIncrement    uint64
		stat            *statStruct
		statBuffer      []byte = make([]byte, 0, 128)
		statIncrement   uint64
		statName        string
		tcpConn         *net.TCPConn
		udpConn         *net.UDPConn
	)

selectLoop:
	for {
		select {
		case stat = <-globals.statChan:
			statNameStr := *stat.name
			oldIncrement, ok = globals.statDeltaMap[statNameStr]
			if ok {
				globals.statDeltaMap[statNameStr] = oldIncrement + stat.increment
			} else {
				globals.statDeltaMap[statNameStr] = stat.increment

				newStatNameLink = &statNameLinkStruct{name: statNameStr, next: nil}
				if nil == globals.tailStatNameLink {
					globals.headStatNameLink = newStatNameLink
				} else {
					globals.tailStatNameLink.next = newStatNameLink
				}
				globals.tailStatNameLink = newStatNameLink
			}
			globals.Lock()
			oldIncrement, ok = globals.statFullMap[statNameStr]
			if ok {
				globals.statFullMap[statNameStr] = oldIncrement + stat.increment
			} else {
				globals.statFullMap[statNameStr] = stat.increment
			}
			globals.Unlock()
			statStructPool.Put(stat)
		case _ = <-globals.stopChan:
			globals.doneChan <- true
			return
		case <-globals.tickChan:
			if nil == globals.headStatNameLink {
				// Nothing to read
				//fmt.Printf("stats sender: got tick but nothing to read\n")
				continue selectLoop
			}

			// Handle up to maxStatsPerTimeout stats that we have right now
			// Need to balance sending over UDP with servicing our stats channel
			// since if the stats channel gets full the callers will block.
			// XXX TODO: This should probably be configurable. And do we want to keep our own stats
			//           on how many stats we handle per timeout? And how full the stats channel gets?
			maxStatsPerTimeout := 20
			statsHandled := 0
			for (globals.headStatNameLink != nil) && (statsHandled < maxStatsPerTimeout) {

				statName = globals.headStatNameLink.name
				globals.headStatNameLink = globals.headStatNameLink.next
				if nil == globals.headStatNameLink {
					globals.tailStatNameLink = nil
				}
				statIncrement, ok = globals.statDeltaMap[statName]
				if !ok {
					err = fmt.Errorf("stats.sender() should be able to find globals.statDeltaMap[\"%v\"]", statName)
					panic(err)
				}
				delete(globals.statDeltaMap, statName)
				statsHandled++

				// Write stat into our buffer
				statBuffer = []byte(statName + ":" + strconv.FormatUint(statIncrement, 10) + "|c")

				// Send stat
				// XXX TODO: should we keep the conn around and reuse it inside this for loop?
				if globals.useUDP {
					udpConn, err = net.DialUDP("udp", globals.udpLAddr, globals.udpRAddr)
					if nil != err {
						continue selectLoop
					}
					_, err = udpConn.Write(statBuffer)
					if nil != err {
						continue selectLoop
					}
					err = udpConn.Close()
					if nil != err {
						continue selectLoop
					}
				} else { // globals.useTCP
					tcpConn, err = net.DialTCP("tcp", globals.tcpLAddr, globals.tcpRAddr)
					if nil != err {
						continue selectLoop
					}
					_, err = tcpConn.Write(statBuffer)
					if nil != err {
						continue selectLoop
					}
					err = tcpConn.Close()
					if nil != err {
						continue selectLoop
					}
				}
				// Clear buffer for next time
				statBuffer = statBuffer[:0]
			}
			//fmt.Printf("handled %v stats on tick. Channel contains %v stats.\n", statsHandled, len(globals.statChan))
		}
	}
}
