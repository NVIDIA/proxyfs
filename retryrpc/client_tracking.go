package retryrpc

// This file contains functions in the server which
// keep track of clients.

func (mui *myUniqueInfo) buildAndQPendingCtx(rID requestID, buf []byte, cCtx *connCtx, lockHeld bool) {
	pc := &pendingCtx{buf: buf, cCtx: cCtx}

	if lockHeld == false {
		mui.Lock()
	}

	mui.pendingRequest[rID] = pc

	if lockHeld == false {
		mui.Unlock()
	}
}

// NOTE: This function assumes mui Lock is held
func (mui *myUniqueInfo) isEmpty() bool {
	if len(mui.completedRequest) == 0 &&
		len(mui.pendingRequest) == 0 {
		return true
	}
	return false
}
