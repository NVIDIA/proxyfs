package main

import (
	"testing"
	"time"
)

func TestFunctional(t *testing.T) {
	testSetup(t)
	time.Sleep(2 * time.Second) // TODO
	testTeardown(t)
}
