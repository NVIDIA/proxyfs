package platform

const (
	// GoHeapAllocationMultiplier defines the float64 overhead of memory allocations
	// in the Golang runtime. This multiplier is > 1 primarily due to the desire to
	// avoid overly fragmenting RAM unless/until the Golang Garbage Collector
	// implements any form of compaction.
	GoHeapAllocationMultiplier = float64(2.0)
)
