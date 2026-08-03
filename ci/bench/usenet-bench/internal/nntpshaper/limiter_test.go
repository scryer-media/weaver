package nntpshaper

import (
	"context"
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

func TestShapedLimiterPacesBytesBeyondInitialBurst(t *testing.T) {
	limiter, err := NewAggregateLimiter(8_000_000, 1024) // 1 MB/s after a 1 KiB burst.
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := limiter.WaitN(context.Background(), 32<<10); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("32 KiB at 1 MB/s after a 1 KiB burst completed too quickly: %s", elapsed)
	}
}
