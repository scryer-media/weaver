package clientadapter

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Every timestamp this package hands an adapter is written to JSON and then
// re-checked against durations computed from it. A time.Time still carrying a
// monotonic reading subtracts differently before and after that round trip:
// darwin's wall clock is microsecond-granular while its monotonic clock is
// nanosecond-granular, so the two answers differ by microseconds and the
// adapter's own timing self-check rejects a run that in fact went fine.
func TestRoundedCapturesAgreeAcrossTheJSONRoundTrip(t *testing.T) {
	start := time.Now().Round(0)
	time.Sleep(time.Millisecond)
	end := time.Now().Round(0)

	if difference := end.Sub(start).Nanoseconds(); difference != roundTrip(t, end).Sub(roundTrip(t, start)).Nanoseconds() {
		t.Fatalf("a rounded capture drifted across JSON: %d ns in process", difference)
	}
}

// Without the rounding the timestamps still serialize, so nothing looks wrong
// until a duration is recomputed. Stating that here keeps the test above from
// passing for the wrong reason.
func TestAnUnroundedCaptureCarriesAMonotonicReading(t *testing.T) {
	raw := time.Now()
	if !strings.Contains(raw.String(), " m=") {
		t.Skip("this platform's time.Now carries no monotonic reading")
	}
	if strings.Contains(raw.Round(0).String(), " m=") {
		t.Fatal("Round(0) left the monotonic reading in place")
	}
	if !raw.Round(0).Equal(raw) {
		t.Fatal("Round(0) moved the wall-clock instant")
	}
}

func roundTrip(t *testing.T, at time.Time) time.Time {
	t.Helper()
	encoded, err := json.Marshal(at)
	if err != nil {
		t.Fatal(err)
	}
	var decoded time.Time
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
