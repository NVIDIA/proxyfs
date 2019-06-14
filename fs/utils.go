package fs

import (
	"container/list"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/swiftstack/ProxyFS/stats"
)

func (tryLockBackoffContext *tryLockBackoffContextStruct) backoff() {
	var (
		serializedBackoffListElement *list.Element
	)

	stats.IncrementOperations(&stats.InodeTryLockBackoffOps)

	if 0 < tryLockBackoffContext.backoffsCompleted {
		if tryLockBackoffContext.backoffsCompleted < globals.tryLockSerializationThreshhold {
			// Just do normal delay backoff

			stats.IncrementOperations(&stats.InodeTryLockDelayedBackoffOps)

			backoffSleep()
		} else {
			stats.IncrementOperations(&stats.InodeTryLockSerializedBackoffOps)

			// Push us onto the serializedBackoffList

			globals.Lock()

			serializedBackoffListElement = globals.serializedBackoffList.PushBack(tryLockBackoffContext)

			if 1 == globals.serializedBackoffList.Len() {
				// Nobody else is currently serialized... so just delay

				globals.Unlock()

				backoffSleep()
			} else {
				// We are not the only one on serializedBackoffList... so we must wait

				tryLockBackoffContext.Add(1)

				globals.Unlock()

				tryLockBackoffContext.Wait()

				// Now delay to get prior tryLockBackoffContext a chance to complete

				backoffSleep()
			}

			// Now remove us from the front of serializedBackoffList

			globals.Lock()

			_ = globals.serializedBackoffList.Remove(serializedBackoffListElement)

			// Now check if there is a tryLockBackoffContext "behind" us

			serializedBackoffListElement = globals.serializedBackoffList.Front()

			globals.Unlock()

			if nil != serializedBackoffListElement {
				// Awake the tryLockBackoffContext "behind" us

				serializedBackoffListElement.Value.(*tryLockBackoffContextStruct).Done()
			}
		}
	}

	tryLockBackoffContext.backoffsCompleted++
}

func backoffSleep() {
	var (
		err          error
		randomBigInt *big.Int
		thisDelay    time.Duration
	)

	randomBigInt, err = rand.Int(rand.Reader, big.NewInt(int64(globals.tryLockBackoffMax)-int64(globals.tryLockBackoffMin)))
	if nil != err {
		err = fmt.Errorf("fs.backoffSleep() failed in call to rand.Int(): %v", err)
		panic(err)
	}

	thisDelay = time.Duration(int64(globals.tryLockBackoffMin) + randomBigInt.Int64())

	time.Sleep(thisDelay)
}
