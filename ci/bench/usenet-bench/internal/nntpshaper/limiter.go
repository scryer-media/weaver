// Package nntpshaper supplies a shared server-egress limiter for a transparent
// NNTP TCP proxy. It deliberately has one bucket for every downstream client
// connection, so it models a server link rather than client-specific limits.
package nntpshaper

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const maxPacedChunk = 32 << 10

// AggregateLimiter is a concurrency-safe leaky bucket. It reserves send time
// under one lock before sleeping, which prevents concurrent connections from
// independently spending more than the configured aggregate link rate.
type AggregateLimiter struct {
	rateBytesPerSecond float64
	burstBytes         int

	mu   sync.Mutex
	next time.Time
}

func NewAggregateLimiter(egressBitsPerSecond uint64, burstBytes uint64) (*AggregateLimiter, error) {
	if egressBitsPerSecond == 0 {
		return &AggregateLimiter{}, nil
	}
	if burstBytes == 0 {
		return nil, fmt.Errorf("positive burst bytes are required when egress shaping is enabled")
	}
	return &AggregateLimiter{
		rateBytesPerSecond: float64(egressBitsPerSecond) / 8,
		burstBytes:         int(burstBytes),
	}, nil
}

func (l *AggregateLimiter) Enabled() bool {
	return l != nil && l.rateBytesPerSecond > 0
}

// WaitN reserves global egress time for n bytes. A bounded initial burst is
// allowed after an idle period; all subsequent bytes are paced at the stated
// aggregate bit rate.
func (l *AggregateLimiter) WaitN(ctx context.Context, n int) error {
	if !l.Enabled() || n <= 0 {
		return nil
	}
	for n > 0 {
		chunk := n
		if chunk > maxPacedChunk {
			chunk = maxPacedChunk
		}
		if chunk > l.burstBytes {
			chunk = l.burstBytes
		}
		if chunk <= 0 {
			return fmt.Errorf("invalid egress shaper burst size %d", l.burstBytes)
		}
		if err := l.waitChunk(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

func (l *AggregateLimiter) waitChunk(ctx context.Context, n int) error {
	duration := time.Duration(float64(n) / l.rateBytesPerSecond * float64(time.Second))
	now := time.Now()
	l.mu.Lock()
	// An idle link earns at most burstBytes of credit. Represent that credit by
	// placing the next reservation up to one burst duration in the past.
	creditStart := now.Add(-time.Duration(float64(l.burstBytes) / l.rateBytesPerSecond * float64(time.Second)))
	if l.next.Before(creditStart) {
		l.next = creditStart
	}
	start := l.next
	l.next = l.next.Add(duration)
	l.mu.Unlock()

	if wait := time.Until(start); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}
