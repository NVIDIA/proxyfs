package retryrpc

// This file contains functions in the server which
// keep track of clients.

func (ci *clientInfo) buildAndQPendingCtx(rID requestID, buf []byte, cCtx *connCtx, lockHeld bool) {
	pc := &pendingCtx{buf: buf, cCtx: cCtx}

	if lockHeld == false {
		ci.Lock()
	}

	ci.pendingRequest[rID] = pc

	if lockHeld == false {
		ci.Unlock()
	}
}

// NOTE: This function assumes ci Lock is held
func (ci *clientInfo) isEmpty() bool {
	if len(ci.completedRequest) == 0 &&
		len(ci.pendingRequest) == 0 {
		return true
	}
	return false
}

func (ci *clientInfo) completedCnt() int {
	return len(ci.completedRequest)
}

func (ci *clientInfo) pendingCnt() int {
	return len(ci.pendingRequest)
}
