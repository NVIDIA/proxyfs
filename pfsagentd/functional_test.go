package main

import (
	"testing"
)

func TestFunctional(t *testing.T) {
	testSetup(t)
	// time.Sleep(10 * time.Second) // UNDO/TODO
	testTeardown(t)
}
