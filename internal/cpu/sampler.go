package cpu

// Sampler remembers the previous reading so callers only have to hand it each new one.
type Sampler struct {
	prev Times
	have bool
}

// Next compares cur with the previous reading and returns the busy percentage. ok is false for the
// first reading and after a counter reset (see Usage). Either way cur becomes the new baseline.
func (s *Sampler) Next(cur Times) (pct float64, ok bool) {
	if s.have {
		pct, ok = Usage(s.prev, cur)
	}
	s.prev, s.have = cur, true
	return pct, ok
}
