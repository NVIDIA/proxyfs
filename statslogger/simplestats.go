package statslogger

// Track statistics for the min, max, and mean number of free connections
// over a time interval (yes, this could be generalized)
//
// This is very simple, but still seems worth breaking out into a separate file.
//
type SimpleStats struct {
	min     int64
	max     int64
	total   int64
	samples int64
}

func (sp *SimpleStats) Clear() {
	sp.min = 0
	sp.max = 0
	sp.total = 0
	sp.samples = 0
}

func (sp *SimpleStats) Sample(cnt int64) {
	if sp.samples == 0 {
		sp.min = cnt
	}
	if sp.min > cnt {
		sp.min = cnt
	}
	if sp.max < cnt {
		sp.max = cnt
	}
	sp.total += cnt
	sp.samples++
}

func (sp *SimpleStats) Mean() int64 {
	if sp.samples == 0 {
		return 0
	}
	return sp.total / sp.samples
}

func (sp *SimpleStats) Min() int64 {
	return sp.min
}

func (sp *SimpleStats) Max() int64 {
	return sp.max
}

func (sp *SimpleStats) Samples() int64 {
	return sp.samples
}

func (sp *SimpleStats) Total() int64 {
	return sp.total
}
