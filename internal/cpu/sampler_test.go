package cpu

import (
	"math"
	"testing"
)

func TestSampler(t *testing.T) {
	r0 := Times{4705, 150, 1120, 16250, 520, 0, 25, 0}
	r1 := Times{4755, 150, 1140, 16500, 530, 0, 30, 0}
	low := Times{100, 0, 50, 800, 0, 0, 0, 0}
	afterLow := Times{150, 0, 70, 900, 0, 0, 0, 0}

	tests := []struct {
		name    string
		reads   []Times
		wantOK  []bool
		wantPct []float64 // checked only where wantOK is true
	}{
		{
			name:   "first reading has nothing to compare with",
			reads:  []Times{r0},
			wantOK: []bool{false},
		},
		{
			name:    "second reading gives a percentage",
			reads:   []Times{r0, r1},
			wantOK:  []bool{false, true},
			wantPct: []float64{0, 22.39},
		},
		{
			name:    "reset reports nothing, then the next call uses the reset reading as baseline",
			reads:   []Times{r0, low, afterLow},
			wantOK:  []bool{false, false, true},
			wantPct: []float64{0, 0, 41.18},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s Sampler
			for i, cur := range tt.reads {
				pct, ok := s.Next(cur)
				if ok != tt.wantOK[i] {
					t.Fatalf("call %d: ok = %v, want %v", i, ok, tt.wantOK[i])
				}
				if ok && math.Abs(pct-tt.wantPct[i]) > 0.01 {
					t.Errorf("call %d: pct = %.4f, want %.2f", i, pct, tt.wantPct[i])
				}
			}
		})
	}
}
