package nntpshaper

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Delay lines model a link's propagation delay inside this process, for hosts
// that have no tc netem: Windows and macOS. They are not a general substitute
// for netem. netem delays packets, so a client's own TCP handshake costs a
// round trip; a proxy can only delay the byte stream it relays, and the
// client's connect() to a local listener still returns immediately. The
// handshake delay below pays that round trip back where the client can observe
// it -- in the time from connect to the server greeting -- but a client that
// times TCP establishment directly still sees a local connect. That is the one
// place the userspace mechanism is an approximation, which is why it is
// reported as its own mechanism and never merged with a netem result.

// DelayObserver records the smallest residency any chunk has actually spent in
// one direction's delay lines. The minimum is the statistic worth keeping:
// queueing and scheduler jitter can only push a chunk out later than the
// configured delay, so the floor over many chunks is the closest observable
// value to the delay this process is really applying, and unlike a mean it
// does not drift when the host is busy.
type DelayObserver struct {
	minResidencyNanos atomic.Int64
}

func (o *DelayObserver) observe(residency time.Duration) {
	if o == nil || residency <= 0 {
		return
	}
	for {
		current := o.minResidencyNanos.Load()
		if current != 0 && current <= int64(residency) {
			return
		}
		if o.minResidencyNanos.CompareAndSwap(current, int64(residency)) {
			return
		}
	}
}

// MinResidencyMicros reports the observed floor and whether any chunk has been
// delivered at all. Before the first chunk there is nothing to observe and the
// caller falls back to the configured delay.
func (o *DelayObserver) MinResidencyMicros() (uint64, bool) {
	if o == nil {
		return 0, false
	}
	nanos := o.minResidencyNanos.Load()
	if nanos <= 0 {
		return 0, false
	}
	return uint64((nanos + 500) / 1_000), true
}

type delayedChunk struct {
	payload []byte
	arrival time.Time
	release time.Time
}

// DelayLine is one direction of one connection's propagation delay: a chunk
// written at t is delivered at t+delay while the producer keeps reading. It is
// a pipe, not a sleep before each write. Sleeping in the copy loop would
// serialize the stream -- every chunk would cost a full delay and a 50ms link
// would collapse to one 32KiB chunk per 50ms -- so the producer and the
// consumer run concurrently and only the delivery instant moves.
type DelayLine struct {
	delay    time.Duration
	capacity int
	observer *DelayObserver

	mu     sync.Mutex
	ready  *sync.Cond
	space  *sync.Cond
	queue  []delayedChunk
	queued int
	closed bool
	failed error
}

// NewDelayLine builds one direction's delay line. capacityBytes bounds the
// bytes in flight: when it fills, Write blocks, which is the backpressure a
// real bottleneck applies to the sender rather than an unbounded buffer.
func NewDelayLine(delay time.Duration, capacityBytes int, observer *DelayObserver) (*DelayLine, error) {
	if delay <= 0 {
		return nil, fmt.Errorf("a delay line needs a positive delay, got %s", delay)
	}
	if capacityBytes < maxPacedChunk {
		return nil, fmt.Errorf("delay line capacity %d is below one relay chunk (%d bytes)", capacityBytes, maxPacedChunk)
	}
	line := &DelayLine{delay: delay, capacity: capacityBytes, observer: observer}
	line.ready = sync.NewCond(&line.mu)
	line.space = sync.NewCond(&line.mu)
	return line, nil
}

func (l *DelayLine) Delay() time.Duration { return l.delay }

// Write hands one relayed chunk to the line. The payload is copied because the
// caller reuses its read buffer while these bytes are still in flight.
func (l *DelayLine) Write(payload []byte) error {
	for len(payload) > 0 {
		piece := payload
		if len(piece) > l.capacity {
			piece = piece[:l.capacity]
		}
		if err := l.writeChunk(piece); err != nil {
			return err
		}
		payload = payload[len(piece):]
	}
	return nil
}

func (l *DelayLine) writeChunk(payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.failed == nil && !l.closed && l.queued+len(payload) > l.capacity {
		l.space.Wait()
	}
	if l.failed != nil {
		return l.failed
	}
	if l.closed {
		return fmt.Errorf("delay line is closed")
	}
	now := time.Now()
	buffered := make([]byte, len(payload))
	copy(buffered, payload)
	l.queue = append(l.queue, delayedChunk{payload: buffered, arrival: now, release: now.Add(l.delay)})
	l.queued += len(buffered)
	l.ready.Signal()
	return nil
}

// CloseWrite declares the last chunk written. Deliver returns once the queue
// has drained, so the bytes already on the wire are not discarded when the
// source reaches EOF.
func (l *DelayLine) CloseWrite() {
	l.mu.Lock()
	l.closed = true
	l.ready.Broadcast()
	l.space.Broadcast()
	l.mu.Unlock()
}

// Abort tears the line down, failing both the producer and the consumer. It is
// how a dead connection unblocks a producer parked on a full queue.
func (l *DelayLine) Abort(err error) {
	if err == nil {
		err = fmt.Errorf("delay line aborted")
	}
	l.mu.Lock()
	if l.failed == nil {
		l.failed = err
	}
	l.closed = true
	l.ready.Broadcast()
	l.space.Broadcast()
	l.mu.Unlock()
}

// Deliver runs the consumer: it releases each chunk at its due instant and
// hands it to write. It returns nil once the producer closed the line and the
// queue is empty, and the first write error otherwise.
func (l *DelayLine) Deliver(ctx context.Context, write func([]byte) error) error {
	for {
		chunk, ok, err := l.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if wait := time.Until(chunk.release); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				l.Abort(ctx.Err())
				return ctx.Err()
			case <-timer.C:
			}
		}
		writeErr := write(chunk.payload)
		if writeErr != nil {
			l.Abort(writeErr)
			return writeErr
		}
		l.observer.observe(time.Since(chunk.arrival))
		l.mu.Lock()
		l.queued -= len(chunk.payload)
		l.space.Broadcast()
		l.mu.Unlock()
	}
}

func (l *DelayLine) next() (delayedChunk, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.queue) == 0 && !l.closed && l.failed == nil {
		l.ready.Wait()
	}
	if l.failed != nil {
		return delayedChunk{}, false, l.failed
	}
	if len(l.queue) == 0 {
		return delayedChunk{}, false, nil
	}
	chunk := l.queue[0]
	l.queue = l.queue[1:]
	return chunk, true, nil
}

// DelayLineCapacityBytes sizes one direction from the link it models. A delay
// line must hold a whole bandwidth-delay product or the queue, not the declared
// rate, becomes the bottleneck: bytes enter at the source rate and leave one
// delay later, so rate x delay bytes are always in flight. The doubling is
// headroom for burstiness, and the floor keeps a slow link's queue above one
// relay chunk.
func DelayLineCapacityBytes(egressBitsPerSecond uint64, delay time.Duration) (int, error) {
	if delay <= 0 {
		return 0, fmt.Errorf("a delay line needs a positive delay, got %s", delay)
	}
	if egressBitsPerSecond == 0 {
		return 0, fmt.Errorf("an unlimited link has no bandwidth-delay product to size a delay line from; declare an egress rate or set the queue size explicitly")
	}
	product := float64(egressBitsPerSecond) / 8 * delay.Seconds() * 2
	if product > float64(maxDelayLineCapacityBytes) {
		return 0, fmt.Errorf("a %d bit/s link delayed by %s needs a %.0f byte delay line, above the %d byte ceiling", egressBitsPerSecond, delay, product, maxDelayLineCapacityBytes)
	}
	capacity := int(product)
	if capacity < minDelayLineCapacityBytes {
		capacity = minDelayLineCapacityBytes
	}
	return capacity, nil
}

// DelayLineCeilingBitsPerSecond is the highest rate a queue of this size can
// sustain across this delay. It is what the shaper compares against the
// declared link rate, so an undersized queue is refused at startup instead of
// quietly capping every run below the rate the plan promises.
func DelayLineCeilingBitsPerSecond(capacityBytes int, delay time.Duration) uint64 {
	if capacityBytes <= 0 || delay <= 0 {
		return 0
	}
	return uint64(float64(capacityBytes) * 8 / delay.Seconds())
}

const (
	minDelayLineCapacityBytes = 1 << 20
	maxDelayLineCapacityBytes = 1 << 31
)

// UserspaceLinkConfig describes the round trip this process will carry itself.
// Queue sizes are optional: a declared egress rate sizes them from the
// bandwidth-delay product, and only an unlimited link has to state them.
type UserspaceLinkConfig struct {
	RTTMicros           uint64
	EgressBitsPerSecond uint64
	EgressQueueBytes    int
	IngressQueueBytes   int
	Platform            string
}

// UserspaceLink is the whole userspace mechanism for one shaper process: the
// split of the round trip, the delay lines every connection borrows, and the
// report and live probe the control plane attests with.
type UserspaceLink struct {
	report          LinkShapingReport
	egressDelay     time.Duration
	ingressDelay    time.Duration
	handshakeDelay  time.Duration
	egressQueue     int
	ingressQueue    int
	egressObserver  DelayObserver
	ingressObserver DelayObserver
}

// NewUserspaceLink splits the round trip half per direction and charges the
// whole of it once more when a connection opens. That third delay is not
// double counting: netem delays the SYN exchange too, so a real client waits
// one round trip for connect and another half for the greeting. A proxy's
// listener answers the SYN locally, and without the handshake charge every
// connection would come up a full round trip early.
func NewUserspaceLink(config UserspaceLinkConfig) (*UserspaceLink, error) {
	if config.RTTMicros == 0 {
		return nil, fmt.Errorf("userspace link needs a positive round trip")
	}
	ingressMicros := config.RTTMicros / 2
	egressMicros := config.RTTMicros - ingressMicros
	link := &UserspaceLink{
		egressDelay:    time.Duration(egressMicros) * time.Microsecond,
		ingressDelay:   time.Duration(ingressMicros) * time.Microsecond,
		handshakeDelay: time.Duration(config.RTTMicros) * time.Microsecond,
	}
	var err error
	if link.egressQueue, err = resolveQueueBytes("server-to-client", config.EgressQueueBytes, config.EgressBitsPerSecond, link.egressDelay); err != nil {
		return nil, err
	}
	if link.ingressQueue, err = resolveQueueBytes("client-to-server", config.IngressQueueBytes, config.EgressBitsPerSecond, link.ingressDelay); err != nil {
		return nil, err
	}
	if ceiling := DelayLineCeilingBitsPerSecond(link.egressQueue, link.egressDelay); config.EgressBitsPerSecond > 0 && ceiling < config.EgressBitsPerSecond {
		return nil, fmt.Errorf("a %d byte server-to-client queue across %s sustains at most %d bit/s, below the declared %d bit/s link", link.egressQueue, link.egressDelay, ceiling, config.EgressBitsPerSecond)
	}
	platform := config.Platform
	if platform == "" {
		platform = runtime.GOOS + "/" + runtime.GOARCH
	}
	link.report = LinkShapingReport{
		SchemaVersion:        linkShapingReportSchemaVersion,
		EgressMechanism:      LinkDelayUserspace,
		IngressMechanism:     LinkDelayUserspace,
		RTTMicros:            config.RTTMicros,
		EgressDelayMicros:    egressMicros,
		IngressDelayMicros:   ingressMicros,
		HandshakeDelayMicros: config.RTTMicros,
		EgressQueueBytes:     uint64(link.egressQueue),
		IngressQueueBytes:    uint64(link.ingressQueue),
		Platform:             platform,
	}
	if err := link.report.validateDeclared(config.RTTMicros); err != nil {
		return nil, fmt.Errorf("userspace link report: %w", err)
	}
	return link, nil
}

func resolveQueueBytes(direction string, explicit int, egressBitsPerSecond uint64, delay time.Duration) (int, error) {
	if explicit > 0 {
		if explicit < maxPacedChunk {
			return 0, fmt.Errorf("%s queue of %d bytes is below one relay chunk (%d bytes)", direction, explicit, maxPacedChunk)
		}
		return explicit, nil
	}
	capacity, err := DelayLineCapacityBytes(egressBitsPerSecond, delay)
	if err != nil {
		return 0, fmt.Errorf("size the %s queue: %w", direction, err)
	}
	return capacity, nil
}

// Report is the immutable contract this process serves.
func (l *UserspaceLink) Report() LinkShapingReport { return l.report }

// HandshakeDelay is charged once per connection, before the proxy dials
// upstream, so the client waits for a round trip it can observe.
func (l *UserspaceLink) HandshakeDelay() time.Duration { return l.handshakeDelay }

// NewEgressLine and NewIngressLine build one connection's delay lines. Every
// line in a direction shares that direction's observer, so the live probe
// reads evidence from all traffic rather than from one connection.
func (l *UserspaceLink) NewEgressLine() (*DelayLine, error) {
	return NewDelayLine(l.egressDelay, l.egressQueue, &l.egressObserver)
}

func (l *UserspaceLink) NewIngressLine() (*DelayLine, error) {
	return NewDelayLine(l.ingressDelay, l.ingressQueue, &l.ingressObserver)
}

// LiveDelays is the userspace answer to reading a qdisc back. Once bytes have
// flowed it reports the observed residency floor, which is measured delivery
// and not a restatement of configuration. Before the first byte there is
// nothing measured to report, so it falls back to the delay the running lines
// are built with -- honest here in a way it would not be for netem, because
// the mechanism is this process's own immutable state rather than a kernel
// object an operator could have removed underneath it.
func (l *UserspaceLink) LiveDelays(report LinkShapingReport) (uint64, uint64, error) {
	if report.EgressMechanism != LinkDelayUserspace {
		return 0, 0, fmt.Errorf("live probe is for %q, report declares %q", LinkDelayUserspace, report.EgressMechanism)
	}
	egress, observed := l.egressObserver.MinResidencyMicros()
	if !observed {
		egress = report.EgressDelayMicros
	}
	ingress, observed := l.ingressObserver.MinResidencyMicros()
	if !observed {
		ingress = report.IngressDelayMicros
	}
	return egress, ingress, nil
}
