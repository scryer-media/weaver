package nntpshaper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// deliverAll runs a line's consumer and records what it delivered and when.
func deliverAll(t *testing.T, line *DelayLine) (*[]time.Duration, *bytes.Buffer, func()) {
	t.Helper()
	start := time.Now()
	var mu sync.Mutex
	times := make([]time.Duration, 0, 8)
	payload := &bytes.Buffer{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = line.Deliver(context.Background(), func(chunk []byte) error {
			mu.Lock()
			times = append(times, time.Since(start))
			payload.Write(chunk)
			mu.Unlock()
			return nil
		})
	}()
	return &times, payload, func() { <-done }
}

func TestDelayLineHoldsEachChunkForTheDelay(t *testing.T) {
	const delay = 25 * time.Millisecond
	line, err := NewDelayLine(delay, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	times, payload, wait := deliverAll(t, line)
	for index := 0; index < 20; index++ {
		if err := line.Write([]byte{byte(index)}); err != nil {
			t.Fatal(err)
		}
	}
	line.CloseWrite()
	wait()

	if payload.Len() != 20 {
		t.Fatalf("delivered %d bytes, want 20", payload.Len())
	}
	for index, at := range *times {
		if at < delay {
			t.Fatalf("chunk %d delivered after %s, before the %s delay", index, at, delay)
		}
	}
	// The whole point of a delay line rather than a sleep in the copy loop:
	// twenty chunks cost one delay, not twenty.
	if last := (*times)[len(*times)-1]; last > 4*delay {
		t.Fatalf("twenty chunks took %s; a delay line must pipeline, not serialize", last)
	}
}

func TestDelayLinePreservesOrderAndBytes(t *testing.T) {
	line, err := NewDelayLine(time.Millisecond, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, payload, wait := deliverAll(t, line)
	want := &bytes.Buffer{}
	for index := 0; index < 64; index++ {
		chunk := []byte(fmt.Sprintf("chunk-%02d;", index))
		want.Write(chunk)
		if err := line.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	line.CloseWrite()
	wait()
	if payload.String() != want.String() {
		t.Fatalf("delivered %q, want %q", payload.String(), want.String())
	}
}

func TestDelayLineCopiesTheCallersBuffer(t *testing.T) {
	line, err := NewDelayLine(5*time.Millisecond, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, payload, wait := deliverAll(t, line)
	buffer := []byte("first")
	if err := line.Write(buffer); err != nil {
		t.Fatal(err)
	}
	// The proxy reuses one read buffer while these bytes are still in flight.
	copy(buffer, "SECON")
	line.CloseWrite()
	wait()
	if payload.String() != "first" {
		t.Fatalf("delivered %q; the line must copy the caller's buffer", payload.String())
	}
}

func TestDelayLineBackpressuresAFullQueue(t *testing.T) {
	const delay = 30 * time.Millisecond
	// One chunk of capacity, so every write after the first waits for a delivery.
	line, err := NewDelayLine(delay, maxPacedChunk, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, wait := deliverAll(t, line)
	chunk := make([]byte, maxPacedChunk)
	start := time.Now()
	for index := 0; index < 3; index++ {
		if err := line.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	line.CloseWrite()
	wait()
	if elapsed < 2*delay-5*time.Millisecond {
		t.Fatalf("three writes into a one-chunk queue returned in %s; a full queue must block the producer", elapsed)
	}
}

func TestDelayLineAbortUnblocksTheProducer(t *testing.T) {
	line, err := NewDelayLine(time.Hour, maxPacedChunk, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := line.Write(make([]byte, maxPacedChunk)); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("downstream gone")
	blocked := make(chan error, 1)
	go func() { blocked <- line.Write(make([]byte, 1)) }()
	time.Sleep(10 * time.Millisecond)
	line.Abort(failure)
	select {
	case err := <-blocked:
		if !errors.Is(err, failure) {
			t.Fatalf("blocked write returned %v, want %v", err, failure)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Abort did not release a producer parked on a full queue")
	}
}

func TestDelayLineDeliverReturnsTheWriteError(t *testing.T) {
	line, err := NewDelayLine(time.Millisecond, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("client hung up")
	if err := line.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := line.Deliver(context.Background(), func([]byte) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("Deliver returned %v, want %v", err, failure)
	}
	if err := line.Write([]byte("y")); !errors.Is(err, failure) {
		t.Fatalf("write after a failed delivery returned %v, want %v", err, failure)
	}
}

func TestDelayLineDeliverStopsWithTheContext(t *testing.T) {
	line, err := NewDelayLine(time.Hour, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := line.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- line.Deliver(ctx, func([]byte) error { return nil }) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Deliver returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver ignored a cancelled context while waiting to release a chunk")
	}
}

func TestNewDelayLineRefusesUnusableShapes(t *testing.T) {
	if _, err := NewDelayLine(0, 1<<20, nil); err == nil {
		t.Fatal("accepted a zero delay")
	}
	if _, err := NewDelayLine(time.Millisecond, maxPacedChunk-1, nil); err == nil {
		t.Fatal("accepted a queue smaller than one relay chunk")
	}
}

func TestDelayLineCapacityBytesCoversTheBandwidthDelayProduct(t *testing.T) {
	// 1 Gbit/s across 50ms is a 6.25 MB product; the line carries two of them.
	capacity, err := DelayLineCapacityBytes(1_000_000_000, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if want := 12_500_000; capacity != want {
		t.Fatalf("capacity %d bytes, want %d", capacity, want)
	}
	if ceiling := DelayLineCeilingBitsPerSecond(capacity, 50*time.Millisecond); ceiling < 1_000_000_000 {
		t.Fatalf("a queue sized for the link sustains %d bit/s, below the link itself", ceiling)
	}
	slow, err := DelayLineCapacityBytes(1_000_000, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if slow != minDelayLineCapacityBytes {
		t.Fatalf("a tiny product gave %d bytes, want the %d byte floor", slow, minDelayLineCapacityBytes)
	}
	if _, err := DelayLineCapacityBytes(0, 50*time.Millisecond); err == nil {
		t.Fatal("sized a queue for an unlimited link, which has no bandwidth-delay product")
	}
	if _, err := DelayLineCapacityBytes(1_000_000_000, 0); err == nil {
		t.Fatal("sized a queue for an undelayed link")
	}
}

func TestNewUserspaceLinkSplitsTheRoundTrip(t *testing.T) {
	link, err := NewUserspaceLink(UserspaceLinkConfig{RTTMicros: 100_000, EgressBitsPerSecond: 1_000_000_000})
	if err != nil {
		t.Fatal(err)
	}
	report := link.Report()
	if report.EgressDelayMicros != 50_000 || report.IngressDelayMicros != 50_000 {
		t.Fatalf("split %dus/%dus, want half each way", report.EgressDelayMicros, report.IngressDelayMicros)
	}
	// The handshake charge is the whole round trip: netem delays the SYN
	// exchange, a proxy's local listener does not.
	if report.HandshakeDelayMicros != 100_000 || link.HandshakeDelay() != 100*time.Millisecond {
		t.Fatalf("handshake charge %dus, want the whole 100000us round trip", report.HandshakeDelayMicros)
	}
	if report.EgressMechanism != LinkDelayUserspace || report.IngressMechanism != LinkDelayUserspace {
		t.Fatalf("mechanisms %q/%q, want %q", report.EgressMechanism, report.IngressMechanism, LinkDelayUserspace)
	}
	if report.Interface != "" || report.NetemLimitPackets != 0 || report.KernelRelease != "" {
		t.Fatalf("userspace report carries netem evidence: %+v", report)
	}
	if report.Platform == "" || report.EgressQueueBytes == 0 || report.IngressQueueBytes == 0 {
		t.Fatalf("report is missing userspace fields: %+v", report)
	}
	if err := report.validateDeclared(100_000); err != nil {
		t.Fatalf("its own report does not validate: %v", err)
	}
}

func TestNewUserspaceLinkRefusesAQueueBelowTheDeclaredRate(t *testing.T) {
	// 10 Gbit/s across 25ms each way needs ~31MB in flight; 1MB caps the link
	// at a fraction of what the plan declares, so the process must not start.
	_, err := NewUserspaceLink(UserspaceLinkConfig{
		RTTMicros:           50_000,
		EgressBitsPerSecond: 10_000_000_000,
		EgressQueueBytes:    1 << 20,
		IngressQueueBytes:   1 << 20,
	})
	if err == nil {
		t.Fatal("accepted a queue that silently caps the link below its declared rate")
	}
	if _, err := NewUserspaceLink(UserspaceLinkConfig{RTTMicros: 0, EgressBitsPerSecond: 1_000_000_000}); err == nil {
		t.Fatal("accepted a link with no round trip")
	}
	if _, err := NewUserspaceLink(UserspaceLinkConfig{RTTMicros: 50_000}); err == nil {
		t.Fatal("accepted an unlimited link with no explicit queue size")
	}
}

func TestUserspaceLiveDelaysReportObservedResidency(t *testing.T) {
	link, err := NewUserspaceLink(UserspaceLinkConfig{RTTMicros: 20_000, EgressBitsPerSecond: 1_000_000_000})
	if err != nil {
		t.Fatal(err)
	}
	egress, ingress, err := link.LiveDelays(link.Report())
	if err != nil {
		t.Fatal(err)
	}
	if egress != 10_000 || ingress != 10_000 {
		t.Fatalf("before any traffic the probe reported %dus/%dus, want the configured 10000us", egress, ingress)
	}

	line, err := link.NewEgressLine()
	if err != nil {
		t.Fatal(err)
	}
	_, _, wait := deliverAll(t, line)
	if err := line.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	line.CloseWrite()
	wait()

	egress, ingress, err = link.LiveDelays(link.Report())
	if err != nil {
		t.Fatal(err)
	}
	if egress < 10_000 {
		t.Fatalf("observed residency %dus is below the %dus the line applies", egress, 10_000)
	}
	if egress > 10_000+5_000 {
		t.Fatalf("observed residency %dus is far above the 10000us delay", egress)
	}
	if ingress != 10_000 {
		t.Fatalf("the untouched direction reported %dus, want its configured 10000us", ingress)
	}
	if _, _, err := link.LiveDelays(LinkShapingReport{EgressMechanism: LinkEgressNetem}); err == nil {
		t.Fatal("the userspace probe answered for a netem report")
	}
}

func TestValidateDeclaredKeepsTheMechanismsApart(t *testing.T) {
	netem := LinkShapingReport{
		SchemaVersion: linkShapingReportSchemaVersion, Interface: "eth1", IngressDevice: "ifb-nntp",
		EgressMechanism: LinkEgressNetem, IngressMechanism: LinkIngressIFBNetem,
		RTTMicros: 100_000, EgressDelayMicros: 50_000, IngressDelayMicros: 50_000,
		NetemLimitPackets: 125_000, KernelRelease: "6.8.0",
	}
	if err := netem.validateDeclared(100_000); err != nil {
		t.Fatalf("a plain netem report no longer validates: %v", err)
	}
	withUserspaceFields := netem
	withUserspaceFields.HandshakeDelayMicros = 100_000
	if err := withUserspaceFields.validateDeclared(100_000); err == nil {
		t.Fatal("a netem report carrying userspace fields was accepted")
	}

	userspace := LinkShapingReport{
		SchemaVersion:   linkShapingReportSchemaVersion,
		EgressMechanism: LinkDelayUserspace, IngressMechanism: LinkDelayUserspace,
		RTTMicros: 100_000, EgressDelayMicros: 50_000, IngressDelayMicros: 50_000,
		HandshakeDelayMicros: 100_000, EgressQueueBytes: 1 << 20, IngressQueueBytes: 1 << 20,
		Platform: "windows/amd64",
	}
	if err := userspace.validateDeclared(100_000); err != nil {
		t.Fatalf("a userspace report does not validate: %v", err)
	}
	for name, mutate := range map[string]func(*LinkShapingReport){
		"names a device":        func(r *LinkShapingReport) { r.Interface = "eth1" },
		"carries a qdisc limit": func(r *LinkShapingReport) { r.NetemLimitPackets = 1000 },
		"carries kernel state":  func(r *LinkShapingReport) { r.KernelRelease = "6.8.0" },
		"mixed mechanisms":      func(r *LinkShapingReport) { r.IngressMechanism = LinkIngressNone },
		"no handshake charge":   func(r *LinkShapingReport) { r.HandshakeDelayMicros = 0 },
		"half a handshake":      func(r *LinkShapingReport) { r.HandshakeDelayMicros = 50_000 },
		"unset queue":           func(r *LinkShapingReport) { r.EgressQueueBytes = 0 },
		"no platform":           func(r *LinkShapingReport) { r.Platform = "" },
		"unknown mechanism":     func(r *LinkShapingReport) { r.EgressMechanism = "dummynet" },
	} {
		t.Run(name, func(t *testing.T) {
			broken := userspace
			mutate(&broken)
			if err := broken.validateDeclared(100_000); err == nil {
				t.Fatalf("accepted a userspace report that %s", name)
			}
		})
	}
}
