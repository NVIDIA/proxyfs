package platform

const (
	// GoHeapAllocationMultiplier defines the float64 overhead of memory allocations
	// in the Golang runtime. This multiplier is > 1 primarily due to the desire to
	// avoid overly fragmenting RAM unless/until the Golang Garbage Collector
	// implements any form of compaction.
	//
	// With experimentation, a GoHeapAllocationMultiplier of 2.0 appears to be just
	// about right for nearly all access paths/patterns. This appears true for all
	// Linux local access (FUSE, SMB loopback, and NFS loopback) as well as SMB access
	// from from a Windows client and NFS access from a Mac. The one outlier is a
	// Mac doing SMB access. In this case, a GoHeapAllocationMultiplier of 4.0 appears
	// to be appropriate.
	//
	// Still, with multiple SMB read streams (Windows 7 & Windows 10) doing RoboCopy's,
	// one test exceeded even that 4.0 value. Hence, the current setting will be very
	// conservative to hopefully avoid all such memory fragmentation concerns at
	// steady state until such time that a more definitive solution is available.
	GoHeapAllocationMultiplier = float64(10.0)
)
