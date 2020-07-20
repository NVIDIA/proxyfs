package jrpcfs

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func gor(chS chan string, chC chan struct{}) {
	var (
		chNil    chan time.Time
		timerNil *time.Timer
	)

	chNil = nil
	timerNil = &time.Timer{}
	timerNil = time.NewTimer(time.Second)
	fmt.Printf("timerNil: %#v\n", timerNil)

	for {
		select {
		case s := <-chS:
			fmt.Printf("gor() read string \"%s\" while reading chS\n", s)
		case _, _ = <-chC:
			fmt.Printf("gor() apparently got a close on chC - exiting\n")
			runtime.Goexit()
		case <-chNil:
			fmt.Printf("gor() surprisingly read something from chNil\n")
		case <-timerNil.C:
			fmt.Printf("gor() surprisingly read something from timerNil.C\n")
			fmt.Printf("timerNil: %#v\n", timerNil)
		}
	}
}

func TestRpcLease(t *testing.T) {
	chS := make(chan string)
	chC := make(chan struct{})
	go gor(chS, chC)
	chS <- "String 1"
	time.Sleep(2 * time.Second)
	chS <- "String 2"
	time.Sleep(2 * time.Second)
	close(chC)
	time.Sleep(2 * time.Second)
}
