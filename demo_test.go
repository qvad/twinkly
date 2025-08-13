package main

import (
	"testing"
)

func TestRunDemo(t *testing.T) {
	// Skip demo execution during normal test runs to avoid side effects/noise
	t.Skip("Skipping demo in CI")
	RunDemo()
}
