package cpu

import (
	"math"
	"testing"
)

func TestUsage(t *testing.T) {
	tests := []struct {
		name    string
		prev    Times
		cur     Times
		wantOK  bool
		wantPct float64
	}{
		{
			name:    "normal",
			prev:    Times{4705, 150, 1120, 16250, 520, 0, 25, 0},
			cur:     Times{4755, 150, 1140, 16500, 530, 0, 30, 0},
			wantOK:  true,
			wantPct: 22.39,
		},
		{
			name:    "fully idle",
			prev:    Times{100, 0, 50, 1000, 0, 0, 0, 0},
			cur:     Times{100, 0, 50, 1100, 0, 0, 0, 0},
			wantOK:  true,
			wantPct: 0,
		},
		{
			name:    "fully busy",
			prev:    Times{100, 0, 50, 1000, 0, 0, 0, 0},
			cur:     Times{160, 0, 90, 1000, 0, 0, 0, 0},
			wantOK:  true,
			wantPct: 100,
		},
		{
			name:   "counter went down",
			prev:   Times{500, 0, 200, 3000, 0, 0, 0, 0},
			cur:    Times{100, 0, 50, 800, 0, 0, 0, 0},
			wantOK: false,
		},
		{
			name:    "new baseline after reset",
			prev:    Times{100, 0, 50, 800, 0, 0, 0, 0},
			cur:     Times{150, 0, 70, 900, 0, 0, 0, 0},
			wantOK:  true,
			wantPct: 41.18,
		},
		{
			name:   "no time passed",
			prev:   Times{100, 0, 50, 800, 0, 0, 0, 0},
			cur:    Times{100, 0, 50, 800, 0, 0, 0, 0},
			wantOK: false,
		},
		{
			name:    "iowait is idle",
			prev:    Times{100, 0, 100, 800, 100, 0, 0, 0},
			cur:     Times{100, 0, 100, 800, 150, 0, 0, 0},
			wantOK:  true,
			wantPct: 0,
		},
		{
			name:    "steal is busy",
			prev:    Times{100, 0, 100, 800, 0, 0, 0, 0},
			cur:     Times{100, 0, 100, 850, 0, 0, 0, 50},
			wantOK:  true,
			wantPct: 50,
		},
		{
			name:   "one field down, others up",
			prev:   Times{100, 0, 100, 800, 0, 0, 0, 0},
			cur:    Times{200, 0, 100, 700, 0, 0, 0, 0},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pct, ok := Usage(tt.prev, tt.cur)
			if ok != tt.wantOK {
				t.Fatalf("Usage ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && math.Abs(pct-tt.wantPct) > 0.01 {
				t.Errorf("Usage pct = %.4f, want %.2f", pct, tt.wantPct)
			}
		})
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Times
		wantErr bool
	}{
		{
			name:  "full line, per-core lines ignored",
			input: "cpu  4705 150 1120 16250 520 0 25 0 0 0\ncpu0 1 2 3 4 5 6 7 8 9 10\nintr 99\n",
			want:  Times{4705, 150, 1120, 16250, 520, 0, 25, 0},
		},
		{
			name:  "guest columns ignored",
			input: "cpu 100 0 50 1000 0 0 0 0 40 0",
			want:  Times{100, 0, 50, 1000, 0, 0, 0, 0},
		},
		{
			name:  "four fields, rest default to zero",
			input: "cpu 1 2 3 4",
			want:  Times{User: 1, Nice: 2, System: 3, Idle: 4},
		},
		{name: "fewer than four fields", input: "cpu 1 2 3", wantErr: true},
		{name: "non-numeric field", input: "cpu 12 abc 3 4", wantErr: true},
		{name: "negative first field", input: "cpu -5 150 1120 16250", wantErr: true},
		{name: "negative later field", input: "cpu 5 150 1120 16250 -1", wantErr: true},
		{name: "empty input", input: "", wantErr: true},
		{name: "no aggregate cpu line", input: "intr 5 6\n", wantErr: true},
		{name: "only a per-core line", input: "cpu0 1 2 3 4\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Parse = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// Guest time is already counted inside user/nice, so adding it to the total would double-count.
func TestUsageIgnoresGuestTime(t *testing.T) {
	prev, err := Parse("cpu 100 0 50 1000 0 0 0 0 0 0")
	if err != nil {
		t.Fatal(err)
	}
	cur, err := Parse("cpu 160 0 50 1040 0 0 0 0 30 0")
	if err != nil {
		t.Fatal(err)
	}
	pct, ok := Usage(prev, cur)
	if !ok || math.Abs(pct-60) > 0.01 {
		t.Errorf("Usage = %.2f, %v; want 60, true", pct, ok)
	}
}
