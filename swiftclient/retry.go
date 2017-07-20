// Swift Object-specific API access implementation

package swiftclient

import (
	"time"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
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

type RetryStat struct {
	retryCnt     *string // increment stat for each operation that is retried (not each retry)
	retrySuccess *string // increment this for each operation where retry fixed the problem
}

func New(maxAttempt uint16, delay time.Duration, expBackoff float64) RetryCtrl {
	var ctrl = RetryCtrl{attemptCnt: 0, attemptMax: uint(maxAttempt), delay: delay, expBackoff: expBackoff}
	ctrl.firstReq = time.Now()
	ctrl.lastReq = ctrl.firstReq

	return ctrl
}

// Wait until this.delay has elapsed since the last request started and then
// update the delay with the exponential backoff and record when the next
// request was started
//
func (this *RetryCtrl) RetryWait() {
	var delay time.Duration = time.Now().Sub(this.lastReq)

	if this.delay > delay {
		time.Sleep(this.delay - delay)
	}
	this.delay = time.Duration(float64(this.delay) * this.expBackoff)

	this.lastReq = time.Now()
	return
}

// Perform a request until it suceeds, it fails with an unretriable error, or we
// hit the maximum retries.  doRequest() will issue the request and return both
// an error indication and a boolean indicating whether the error is retriable
// or not (if there is an error).
//
// if a request fails, even if this.attemptMax == 0 (retry disabled) this will
// still log an Error message indicating RequestWithRetry() failed along with
// the operation identifier (name and paramaters)
//
func (this *RetryCtrl) RequestWithRetry(doRequest func() (bool, error), opid *string, statnm *RetryStat) (err error) {
	var (
		lastErr   error
		retriable bool
	)

	retriable, lastErr = doRequest()
	if lastErr == nil {
		return nil
	}

	// doRequest(), above, counts as the first attempt though its not a
	// retry, which is why this loop goes upto this.attemptMax (consider
	// this.attemptMax == 0 and this.attemptMax == 1 cases)
	for this.attemptCnt = 1; retriable && this.attemptCnt <= this.attemptMax; this.attemptCnt++ {
		if this.attemptCnt == 1 {
			stats.IncrementOperations(statnm.retryCnt)
		}
		this.RetryWait()

		retriable, lastErr = doRequest()
		if lastErr == nil {
			stats.IncrementOperations(statnm.retrySuccess)
			logger.Infof("retry.RequestWithRetry(): %s succeeded after %d attempts",
				opid, this.attemptCnt)
			return nil
		}
	}
	// lasterr != nil

	if !retriable {
		logger.ErrorWithError(lastErr, "retry.RequestWithRetry(): %s unretriable after %d attempts",
			opid, this.attemptCnt)
		return lastErr
	}
	logger.ErrorWithError(lastErr, "retry.RequestWithRetry(): %s failed after %d attempts", opid, this.attemptCnt)
	return lastErr
}
