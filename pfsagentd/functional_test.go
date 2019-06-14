package main

import (
	"testing"
	"time"
)

func TestFunctional(t *testing.T) {
	testSetup(t)
	defer testTeardown(t)

	time.Sleep(2 * time.Second) // TODO
}
