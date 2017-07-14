// Swift Object-specific API access implementation

package swiftclient

import (
	// "fmt"
	// "net"
	// "strconv"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	// "github.com/swiftstack/ProxyFS/stats"
)

// Swift requests should be retried if they fail with a retriable error.
// RetryCtrl is used to control the retry process, including exponential backoff
// on subsequent replay attempts.  lastReq tracks the time that the last request
// was made so that time consumed by the request can be subtracted from the
// backoff amount (if a request takes 30 sec to timeout and the initial delay
// is 10 sec, we don't want 40 sec between requests).
//
type RetryCtrl struct {
	attemptMax uint          // maximum attempts
	attemptCnt uint          // number of attempts
	delay      time.Duration // backoff amount (grows each attempt)
	expBackoff float64       // factor to increase delay by
	firstReq   time.Time     // first request start time
	lastReq    time.Time     // most recent request start time
}

func New(maxAttempt uint16, delay time.Duration, expBackoff float64) RetryCtrl {
	var ctrl = RetryCtrl{attemptCnt: 0, attemptMax: uint(maxAttempt), delay: delay, expBackoff: expBackoff}
	ctrl.firstReq = time.Now()
	ctrl.lastReq = ctrl.firstReq

	return ctrl
}

func (this *RetryCtrl) RetryWait() (err error) {
	this.attemptCnt++
	if this.attemptCnt >= this.attemptMax {
		return blunder.NewError(blunder.TimedOut, "Request failed after %d attempts", this.attemptCnt)
	}

	var delay time.Duration
	delay = time.Now().Sub(this.lastReq)
	if this.delay > delay {
		time.Sleep(this.delay - delay)
	}
	this.delay = time.Duration(float64(this.delay) * 2)

	this.lastReq = time.Now()
	return nil
}

// Perform a request until it suceeds, it fails with an unretriable error, or we
// hit the maximum retries.  doRequest() should issue the request and return
// both an error indication and a boolean indicating whether the error is
// retriable or not (if there is an error).
//
func (this *RetryCtrl) RequestWithRetry(doRequest func() (bool, error), opid *string) (err error) {
	var (
		lastErr   error
		retriable bool
	)
	for {
		retriable, lastErr = doRequest()
		if lastErr == nil {
			if this.attemptCnt != 0 {
				logger.Infof("retry.RequestWithRetry(): %s succeeded after %d attempts",
					opid, this.attemptCnt)
			}
			return nil
		}
		if !retriable {
			logger.ErrorWithError(lastErr, "retry.RequestWithRetry(): %s unretriable after %d attempts",
				opid, this.attemptCnt)
			return lastErr
		}

		// wait to retry or hit timeout
		err = this.RetryWait()
		if err != nil {
			logger.ErrorWithError(lastErr,
				"retry.RequestWithRetry(): %s failed after %d attempts", opid, this.attemptCnt)
			return lastErr
		}
	}
}
