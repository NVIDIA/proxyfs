// Swift Object-specific API access implementation

package swiftclient

import (
	// "fmt"
	// "net"
	// "strconv"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	// "github.com/swiftstack/ProxyFS/logger"
	// "github.com/swiftstack/ProxyFS/stats"
)

// Swift requests should be retried if they fail with a retriable error.
// RetryCtrl is used to control the retry process, including exponential backoff
// on subsequent replay attempts.  lastReq tracks the time that the last request
// was made so that time consumed by the request can be subtracted from the
// backoff amount (if a request takes 30 sec to timeout and the initial backoff
// is 10 sec, we don't want 40 sec between requests).
//
type RetryCtrl struct {
	backoff    time.Duration // backoff amount (grows each attempt)
	attemptMax int           // maximum attempts
	attemptCnt int           // number of attempts
	firstReq   time.Time     // first request start time
	lastReq    time.Time     // most recent request start time
}

func New(maxAttempt int, backoff time.Duration) RetryCtrl {
	var ctrl = RetryCtrl{attemptCnt: 0, attemptMax: maxAttempt, backoff: backoff}
	ctrl.firstReq = time.Now()
	ctrl.lastReq = ctrl.firstReq

	return ctrl
}

func (this *RetryCtrl) RetryWait() (err error) {
	this.attemptCnt++
	if this.attemptCnt >= this.attemptMax {
		return blunder.NewError(blunder.TimedOut, "Request failed after %d attempts", this.attemptCnt)
	}

	var backoff time.Duration
	backoff = time.Now().Sub(this.lastReq)
	if this.backoff > backoff {
		time.Sleep(this.backoff - backoff)
	}
	this.backoff = time.Duration(float64(this.backoff) * 1.2)

	this.lastReq = time.Now()
	return nil
}

// Perform a request until it suceeds, it fails with an unretriable error, or we
// hit the maximum retries.  doRequest() should issue the request and return
// both an error indication and a boolean indicating whether the error is
// retriable or not (if there is an error).
//
func (this *RetryCtrl) RequestWithRetry(doRequest func() (interface{}, bool, error)) (rval interface{}, err error) {
	var lastErr error
	var retriable bool

	for {
		rval, retriable, lastErr = doRequest()
		if lastErr == nil {
			return rval, nil
		}
		if !retriable {
			return rval, lastErr
		}

		// wait to retry or hit timeout
		err = this.RetryWait()
		if err != nil {
			return rval, err
		}
	}
}
