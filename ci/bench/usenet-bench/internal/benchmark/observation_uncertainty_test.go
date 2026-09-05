package benchmark

import "testing"

func TestObservationUncertaintyAcceptable(t *testing.T) {
	second := int64(1_000_000_000)
	cases := []struct {
		name        string
		uncertainty int64
		duration    int64
		want        bool
	}{
		{"short run inside the absolute floor", 47_000_000, 3 * second, true},
		{"short run at the floor", 100_000_000, 3 * second, true},
		{"short run above the floor", 100_000_001, 3 * second, false},
		{"long run inside one percent", 900_000_000, 100 * second, true},
		{"long run above one percent", 1_000_000_001, 100 * second, false},
		{"zero-width window", 0, second, true},
	}
	for _, c := range cases {
		if got := ObservationUncertaintyAcceptable(c.uncertainty, c.duration); got != c.want {
			t.Errorf("%s: ObservationUncertaintyAcceptable(%d, %d) = %v, want %v", c.name, c.uncertainty, c.duration, got, c.want)
		}
	}
}
