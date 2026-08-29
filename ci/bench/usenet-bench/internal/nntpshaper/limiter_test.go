package nntpshaper

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestUnlimitedLimiterDoesNotRequireBurst(t *testing.T) {
	limiter, err := NewAggregateLimiter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if limiter.Enabled() {
		t.Fatal("zero rate must mean unlimited")
	}
	if err := limiter.WaitN(context.Background(), 1<<20); err != nil {
		t.Fatal(err)
	}
}

func TestShapedLimiterRequiresBurst(t *testing.T) {
	if _, err := NewAggregateLimiter(1_000_000_000, 0); err == nil {
		t.Fatal("shaped limiter without a burst should fail")
	}
}

func TestVirtualReservationsAtGigabitRates(t *testing.T) {
	tests := []struct {
		name       string
		bitsPerSec uint64
		burstBytes uint64
		wantStep   time.Duration
	}{
		{name: "1G", bitsPerSec: 1_000_000_000, burstBytes: 125_000_000, wantStep: time.Second},
		{name: "10G", bitsPerSec: 10_000_000_000, burstBytes: 1_250_000_000, wantStep: time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter, err := NewAggregateLimiter(test.bitsPerSec, test.burstBytes)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(1_700_000_000, 0)
			if start := limiter.reserveChunkAt(now, int(test.burstBytes)); !start.Equal(now.Add(-test.wantStep)) {
				t.Fatalf("initial burst starts at %s, want %s", start, now.Add(-test.wantStep))
			}
			if start := limiter.reserveChunkAt(now, int(test.burstBytes)); !start.Equal(now) {
				t.Fatalf("first paced reservation starts at %s, want %s", start, now)
			}
			if start := limiter.reserveChunkAt(now, int(test.burstBytes)); !start.Equal(now.Add(test.wantStep)) {
				t.Fatalf("second paced reservation starts at %s, want %s", start, now.Add(test.wantStep))
			}
		})
	}
}

func TestVirtualReservationsAreAggregateUnderConcurrency(t *testing.T) {
	limiter, err := NewAggregateLimiter(8_000_000, 1_000) // 1 MB/s, 1 ms chunks.
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	now := time.Unix(1_700_000_000, 0)
	starts := make(chan time.Time, workers)
	ready := make(chan struct{})
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-ready
			starts <- limiter.reserveChunkAt(now, 1_000)
		}()
	}
	close(ready)
	group.Wait()
	close(starts)

	reservations := make([]time.Time, 0, workers)
	for start := range starts {
		reservations = append(reservations, start)
	}
	sort.Slice(reservations, func(left, right int) bool { return reservations[left].Before(reservations[right]) })
	for index, start := range reservations {
		want := now.Add(time.Duration(index-1) * time.Millisecond)
		if !start.Equal(want) {
			t.Fatalf("reservation %d starts at %s, want %s", index, start, want)
		}
	}
}

func TestWaitNDoesNotSendBeyondBurstWithoutPacing(t *testing.T) {
	limiter, err := NewAggregateLimiter(8_000_000, maxPacedChunk)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	if err := limiter.WaitN(ctx, 2*maxPacedChunk); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 25*time.Millisecond {
		t.Fatalf("burst plus second chunk completed without pacing: %s", elapsed)
	}
}
