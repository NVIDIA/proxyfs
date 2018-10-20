package consensus

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"os"
	"time"
)

// oneKeyTxn is a helper function which creates a context and modifies the value of one key.
func (cs *EtcdConn) oneKeyTxn(key string, ifValue string, thenValue string, elseValue string, timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	_, err = cs.kvc.Txn(ctx).

		// TODO - should only allow transaction if local node state is
		// !DEAD.  What about other node states like STARTED?
		// If local is DEAD won't we die?  How guarantee that?

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(key), "=", ifValue),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(key, thenValue),

	// the "Else" if "!="
	).Else(
		clientv3.OpPut(key, elseValue),
	).Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// SetNodeStateIfSame will only change the state of the node if the
// node state and heartbeat time have not changed.
// TODO - consolidate with existing functions if possible
func (cs *EtcdConn) setNodeStateIfSame(nodeName string, newState NodeState, existingState NodeState,
	hb time.Time) (err error) {

	if (newState <= INITIALNS) || (newState >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeStateKey := makeNodeStateKey(nodeName)
	nodeHbKey := makeNodeHbKey(nodeName)
	existingHbTime, err := hb.MarshalText()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// TODO - should only allow transaction if local node state is
		// !DEAD.  What about other node states like STARTED?
		// If local is DEAD won't we die?  How guarantee that?

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(nodeStateKey), "=", existingState.String()),
			clientv3.Compare(clientv3.Value(nodeHbKey), "=", string(existingHbTime)),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(nodeStateKey, newState.String()),

	// the "Else" if "!="
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// SetNodeState updates the state of the node in etcd using a transaction.
// TODO - this correct?  Probably should verify that we are doing a valid
// state transition and panic if not.
// TODO - review for cleanup
// TODO - only update HB time if node is local node!!!!
func (cs *EtcdConn) setNodeState(nodeName string, newState NodeState) (err error) {

	if (newState <= INITIALNS) || (newState >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeStateKey := makeNodeStateKey(nodeName)
	nodeHbKey := makeNodeHbKey(nodeName)
	currentTime, err := time.Now().UTC().MarshalText()
	if err != nil {
		fmt.Printf("time.Now() returned err: %v\n", err)
		os.Exit(-1)
	}

	// Update heartbeat time to close hole where see ONLINE state before
	// get a new heartbeat value.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(nodeStateKey), "=", INITIALNS.String()),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(nodeStateKey, newState.String()),
		clientv3.OpPut(nodeHbKey, string(currentTime)),

	// Regardless of current state - set new time.
	// TODO - should only update HB time if local node!
	).Else(
		clientv3.OpPut(nodeStateKey, newState.String()),
		clientv3.OpPut(nodeHbKey, string(currentTime)),
	).Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// SetNodeStateForced ignores the existing node state value and
// sets it to the new state.   Generally, this is only needed for
// the STARTING state.
func (cs *EtcdConn) setNodeStateForced(nodeName string, state NodeState) (err error) {

	// TODO - probaly should verify that node state transitions
	// are correct.
	if (state <= INITIALNS) || (state >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeKey := makeNodeStateKey(nodeName)

	if state == STARTINGNS {
		// We will not get a watch event if the current state stored
		// in etcd is already STARTING.  (This could happen if lose power
		// to whole cluster while in STARTING state).
		//
		// Therefore, set state to INITIAL and then to STARTING to
		// guarantee we get a watch event.
		//
		// Regardless of existing state, reset it to INITIAL
		_ = cs.oneKeyTxn(nodeKey, INITIALNS.String(), INITIALNS.String(), INITIALNS.String(), 5*time.Second)
	}

	// Regardless of existing state - set to new state
	_ = cs.oneKeyTxn(nodeKey, INITIALNS.String(), state.String(), state.String(), 5*time.Second)

	return
}
