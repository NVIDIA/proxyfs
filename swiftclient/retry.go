// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Swift Object-specific API access implementation

package swiftclient

import (
	"fmt"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/proxyfs/stats"
)

// Swift requests should be retried if they fail with a retriable error.
// RetryCtrl is used to control the retry process, including exponential backoff
// on subsequent replay attempts.  lastReq tracks the time that the last request
// was made so that time consumed by the request can be subtracted from the
// backoff amount (if a request takes 30 sec to timeout and the initial delay
// is 10 sec, we don't want 40 sec between requests).
//
type RetryCtrl struct {
	attemptMax    uint          // maximum attempts
	attemptCnt    uint          // number of attempts
	delay         time.Duration // backoff amount (grows each attempt)
	expBackoff    float64       // factor to increase delay by
	clientReqTime time.Time     // client (of swiftclient) request start time
	swiftReqTime  time.Time     // most recent Swift request start time
}

type requestStatistics struct {
	retryCnt          *string // increment stat for each operation that is retried (not each retry)
	retrySuccessCnt   *string // increment this for each operation where retry fixed the problem
	clientRequestTime *bucketstats.BucketLog2Round
	clientFailureCnt  *bucketstats.Total
	swiftRequestTime  *bucketstats.BucketLog2Round
	swiftRetryOps     *bucketstats.Average
}

func NewRetryCtrl(maxAttempt uint16, delay time.Duration, expBackoff float64) *RetryCtrl {
	var ctrl = RetryCtrl{attemptCnt: 0, attemptMax: uint(maxAttempt), delay: delay, expBackoff: expBackoff}
	ctrl.clientReqTime = time.Now()
	ctrl.swiftReqTime = ctrl.clientReqTime

	return &ctrl
}

// Wait until this.delay has elapsed since the last request started and then
// update the delay with the exponential backoff and record when the next
// request was started
//
func (this *RetryCtrl) RetryWait() {
	var delay time.Duration = time.Now().Sub(this.swiftReqTime)

	if this.delay > delay {
		time.Sleep(this.delay - delay)
	}
	this.delay = time.Duration(float64(this.delay) * this.expBackoff)
	this.swiftReqTime = time.Now()
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
func (this *RetryCtrl) RequestWithRetry(doRequest func() (bool, error), opid *string, reqstat *requestStatistics) (err error) {
	var (
		lastErr   error
		retriable bool
		elapsed   time.Duration
	)

	// start performing the request now
	this.attemptCnt = 1
	retriable, lastErr = doRequest()

	elapsed = time.Since(this.clientReqTime)
	reqstat.swiftRequestTime.Add(uint64(elapsed.Nanoseconds() / time.Microsecond.Nanoseconds()))

	if lastErr == nil {
		reqstat.clientRequestTime.Add(uint64(elapsed.Nanoseconds() / time.Microsecond.Nanoseconds()))

		return nil
	}

	// doRequest(), above, counts as the first attempt though its not a
	// retry, which is why this loop goes to <= this.attemptMax (consider
	// this.attemptMax == 0 (retries disabled) and this.attemptMax == 1
	// cases).
	//
	// if retries are enabled but the first failure is not retriable then
	// increment reqstat.retryCnt even though no retry is performed because
	// users are likely to assume that:
	//     reqstat.retryCnt - reqstat.retrySuccess == count_of_failures
	if this.attemptMax != 0 {
		stats.IncrementOperations(reqstat.retryCnt)
	}
	for retriable && this.attemptCnt <= this.attemptMax {
		this.RetryWait()
		this.attemptCnt++
		retriable, lastErr = doRequest()

		// RetryWait() set this.swiftReqTime to Now()
		elapsed = time.Since(this.swiftReqTime)
		reqstat.swiftRequestTime.Add(uint64(elapsed.Nanoseconds() / time.Microsecond.Nanoseconds()))

		if lastErr == nil {
			elapsed = time.Since(this.clientReqTime)
			reqstat.clientRequestTime.Add(uint64(elapsed.Nanoseconds() / time.Microsecond.Nanoseconds()))
			reqstat.swiftRetryOps.Add(1)
			stats.IncrementOperations(reqstat.retrySuccessCnt)

			logger.Infof("retry.RequestWithRetry(): %s succeeded after %d attempts in %4.3f sec",
				*opid, this.attemptCnt, elapsed.Seconds())
			return nil
		}

		// bump retry count (as failure)
		reqstat.swiftRetryOps.Add(0)
	}
	// lasterr != nil

	// the client request failed (and is finished)
	elapsed = time.Since(this.clientReqTime)
	reqstat.clientRequestTime.Add(uint64(elapsed.Nanoseconds() / time.Microsecond.Nanoseconds()))
	reqstat.clientFailureCnt.Add(1)

	var errstring string
	if !retriable {
		errstring = fmt.Sprintf(
			"retry.RequestWithRetry(): %s failed after %d attempts in %4.3f sec with unretriable error",
			*opid, this.attemptCnt, elapsed.Seconds())
	} else {
		errstring = fmt.Sprintf(
			"retry.RequestWithRetry(): %s failed after %d attempts in %4.3f sec with retriable error",
			*opid, this.attemptCnt, elapsed.Seconds())
	}
	logger.ErrorWithError(lastErr, errstring)
	return lastErr
}
