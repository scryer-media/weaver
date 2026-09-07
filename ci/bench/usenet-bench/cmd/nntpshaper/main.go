// nntpshaper is a transparent, server-side NNTP egress shaper. It does not
// terminate TLS: implicit TLS bytes pass through to the public NNTP server so
// verified client TLS still validates the upstream server certificate.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntpshaper"
)

type listenerConfig struct {
	listenAddress string
	upstream      string
	label         string
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "health" {
		if err := healthCheck(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		return
	}
	if len(os.Args) > 1 {
		// The proxy is configured entirely through its environment; refusing
		// stray arguments keeps a typo from silently starting an unshaped server.
		log.Fatalf("nntpshaper takes no arguments (got %q); use `nntpshaper health --addr host:port` for the probe and NNTP_EGRESS_* / *_ADDR environment variables for the proxy", os.Args[1:])
	}
	bitsPerSecond, err := uintEnv("NNTP_EGRESS_BITS_PER_SECOND", 0)
	if err != nil {
		log.Fatal(err)
	}
	burstBytes, err := uintEnv("NNTP_EGRESS_BURST_BYTES", 0)
	if err != nil {
		log.Fatal(err)
	}
	limiter, err := nntpshaper.NewAggregateLimiter(bitsPerSecond, burstBytes)
	if err != nil {
		log.Fatal(err)
	}
	rttMicros, err := uintEnv("NNTP_RTT_MICROS", 0)
	if err != nil {
		log.Fatal(err)
	}
	queueBytes, err := uintEnv("NNTP_DELAY_QUEUE_BYTES", 0)
	if err != nil {
		log.Fatal(err)
	}
	// A configured round trip is rendered before this process serves anything,
	// and its report is the contract the control plane attests. Without one the
	// process refuses to serve rather than present an unshaped path as a
	// delayed one. On Linux the container entrypoint renders it with tc and
	// writes the report; on a host with no tc the proxy carries the delay
	// itself and builds the report from its own configuration.
	mechanism := stringEnv("NNTP_RTT_MECHANISM", nntpshaper.LinkEgressNetem)
	var linkShaping *nntpshaper.LinkShapingReport
	var liveDelays nntpshaper.LiveDelayProbe
	var userspaceLink *nntpshaper.UserspaceLink
	switch mechanism {
	case nntpshaper.LinkEgressNetem:
		if rttMicros > 0 {
			linkShaping, err = nntpshaper.LoadLinkShapingReport(stringEnv("NNTP_LINK_REPORT_PATH", "/run/nntpshaper-link.json"), rttMicros)
			if err != nil {
				log.Fatal(err)
			}
			if _, _, err := nntpshaper.TCLiveDelays(*linkShaping); err != nil {
				log.Fatalf("verify configured round trip: %v", err)
			}
			liveDelays = nntpshaper.TCLiveDelays
		}
	case nntpshaper.LinkDelayUserspace:
		if rttMicros > 0 {
			userspaceLink, err = nntpshaper.NewUserspaceLink(nntpshaper.UserspaceLinkConfig{
				RTTMicros:           rttMicros,
				EgressBitsPerSecond: bitsPerSecond,
				EgressQueueBytes:    int(queueBytes),
				IngressQueueBytes:   int(queueBytes),
			})
			if err != nil {
				log.Fatal(err)
			}
			report := userspaceLink.Report()
			linkShaping = &report
			liveDelays = userspaceLink.LiveDelays
		}
	default:
		log.Fatalf("unknown round-trip mechanism %q; want %q or %q", mechanism, nntpshaper.LinkEgressNetem, nntpshaper.LinkDelayUserspace)
	}
	executableSHA256, err := nntpshaper.CurrentExecutableSHA256()
	if err != nil {
		log.Fatal(err)
	}
	attestation := nntpshaper.NewAttestation(nntpshaper.AttestationConfig{
		EgressBitsPerSecond: bitsPerSecond,
		BurstBytes:          burstBytes,
		RTTMicros:           rttMicros,
		LinkShaping:         linkShaping,
		LiveDelays:          liveDelays,
		Build: nntpshaper.BuildIdentity{
			ExecutableSHA256: executableSHA256,
			ImageIdentity:    stringEnv("NNTP_SHAPER_IMAGE_IDENTITY", ""),
			Version:          stringEnv("NNTP_SHAPER_BUILD_VERSION", "dev"),
			Commit:           stringEnv("NNTP_SHAPER_BUILD_COMMIT", "unknown"),
			BuildTime:        stringEnv("NNTP_SHAPER_BUILD_TIME", "unknown"),
		},
	})
	configs := []listenerConfig{
		{listenAddress: stringEnv("LISTEN_ADDR", ":119"), upstream: stringEnv("UPSTREAM_ADDR", "nntp-upstream:119"), label: "plaintext"},
		{listenAddress: stringEnv("TLS_LISTEN_ADDR", ":563"), upstream: stringEnv("TLS_UPSTREAM_ADDR", "nntp-upstream:563"), label: "tls"},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	listeners := make([]net.Listener, 0, len(configs))
	for _, config := range configs {
		listener, err := net.Listen("tcp", config.listenAddress)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			log.Fatalf("listen %s (%s): %v", config.label, config.listenAddress, err)
		}
		listeners = append(listeners, listener)
		log.Printf("%s listener %s -> %s; aggregate egress=%d bits/s burst=%d bytes rtt=%dus via %s", config.label, listener.Addr(), config.upstream, bitsPerSecond, burstBytes, rttMicros, mechanism)
	}
	controlListener, err := net.Listen("tcp", stringEnv("CONTROL_LISTEN_ADDR", ":8080"))
	if err != nil {
		for _, opened := range listeners {
			_ = opened.Close()
		}
		log.Fatalf("listen shaper control plane: %v", err)
	}
	controlServer := &http.Server{Handler: attestation.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := controlServer.Serve(controlListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("serve shaper control plane: %v", err)
		}
	}()
	log.Printf("shaper control plane %s", controlListener.Addr())

	var workers sync.WaitGroup
	for index, listener := range listeners {
		workers.Add(1)
		go func(listener net.Listener, config listenerConfig) {
			defer workers.Done()
			serve(ctx, listener, config, limiter, attestation, userspaceLink)
		}(listener, configs[index])
	}
	<-ctx.Done()
	for _, listener := range listeners {
		_ = listener.Close()
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := controlServer.Shutdown(shutdownContext); err != nil {
		log.Printf("shutdown shaper control plane: %v", err)
	}
	workers.Wait()
}

func healthCheck(args []string) error {
	return healthCheckWithClient(args, &http.Client{Timeout: 2 * time.Second})
}

func healthCheckWithClient(args []string, client *http.Client) error {
	flags := flag.NewFlagSet("health", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("addr", "127.0.0.1:8080", "shaper control-plane address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	response, err := client.Get("http://" + *address + "/v1/health")
	if err != nil {
		return fmt.Errorf("request shaper health: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("shaper health returned %s", response.Status)
	}
	return nil
}

func serve(ctx context.Context, listener net.Listener, config listenerConfig, limiter *nntpshaper.AggregateLimiter, attestation *nntpshaper.Attestation, link *nntpshaper.UserspaceLink) {
	for {
		client, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("accept %s: %v", config.label, err)
			continue
		}
		go proxy(ctx, client, config, limiter, attestation, link)
	}
}

func proxy(ctx context.Context, client net.Conn, config listenerConfig, limiter *nntpshaper.AggregateLimiter, attestation *nntpshaper.Attestation, link *nntpshaper.UserspaceLink) {
	defer client.Close()
	sourceIdentity := downstreamSource(client.RemoteAddr())
	release, err := attestation.OpenDownstream(sourceIdentity)
	if err != nil {
		log.Printf("reject %s downstream %s: %v", config.label, client.RemoteAddr(), err)
		return
	}
	defer release()
	// Charge the connection's round trip before the upstream is dialled. netem
	// delays the SYN exchange, so a real client waits a round trip for connect
	// and another half for the greeting; this proxy's listener answers the SYN
	// locally, and without the charge every connection would come up a full
	// round trip early.
	if link != nil {
		if err := sleepContext(ctx, link.HandshakeDelay()); err != nil {
			return
		}
	}
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", config.upstream)
	if err != nil {
		log.Printf("dial %s upstream %s for %s: %v", config.label, config.upstream, client.RemoteAddr(), err)
		return
	}
	defer upstream.Close()

	upstreamDone := make(chan struct{})
	go func() {
		defer close(upstreamDone)
		// The client's command stream is relayed byte for byte; the census
		// only reads a copy of what was forwarded. The TLS listener relays
		// ciphertext the shaper cannot read, so it carries no census: parsing
		// it yielded random pseudo-commands, never an article count.
		var census *nntpshaper.CommandCensus
		if config.label != "tls" {
			census = nntpshaper.NewCommandCensus(attestation)
		}
		writer := &censusWriter{upstream: upstream, census: census}
		if err := copyUpstream(ctx, writer, client, link); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
			log.Printf("proxy %s upstream %s: %v", config.label, client.RemoteAddr(), err)
		}
		closeWrite(upstream)
	}()
	if err := copyDownstream(ctx, client, upstream, limiter, attestation, sourceIdentity, link); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
		log.Printf("proxy %s downstream %s: %v", config.label, client.RemoteAddr(), err)
	}
	_ = client.Close()
	_ = upstream.Close()
	<-upstreamDone
}

// copyDownstream relays server bytes to the client. The link's own order is
// preserved: the limiter paces the bytes onto the wire first, and the delay
// line then carries them for the propagation delay, exactly as serialization
// precedes propagation on a real link.
func copyDownstream(ctx context.Context, destination net.Conn, source net.Conn, limiter *nntpshaper.AggregateLimiter, attestation *nntpshaper.Attestation, sourceIdentity string, link *nntpshaper.UserspaceLink) error {
	deliver := func(payload []byte) error {
		return writeDownstream(destination, payload, attestation, sourceIdentity)
	}
	if link == nil {
		return relay(ctx, source, limiter, deliver)
	}
	line, err := link.NewEgressLine()
	if err != nil {
		return err
	}
	return throughDelayLine(ctx, line, deliver, func() error { return relay(ctx, source, limiter, line.Write) })
}

// copyUpstream relays the client's command stream. It carries the other half
// of the round trip and is never rate limited: the shaper models a server's
// egress link, not the client's uplink.
func copyUpstream(ctx context.Context, destination io.Writer, source net.Conn, link *nntpshaper.UserspaceLink) error {
	deliver := func(payload []byte) error {
		_, err := destination.Write(payload)
		return err
	}
	if link == nil {
		return relay(ctx, source, nil, deliver)
	}
	line, err := link.NewIngressLine()
	if err != nil {
		return err
	}
	return throughDelayLine(ctx, line, deliver, func() error { return relay(ctx, source, nil, line.Write) })
}

// throughDelayLine runs one direction's producer against its delay line and
// drains the line before returning, so bytes already in flight when the source
// reaches EOF are delivered rather than dropped.
func throughDelayLine(ctx context.Context, line *nntpshaper.DelayLine, deliver func([]byte) error, produce func() error) error {
	delivered := make(chan error, 1)
	go func() { delivered <- line.Deliver(ctx, deliver) }()
	produceErr := produce()
	if produceErr != nil && !errors.Is(produceErr, io.EOF) {
		line.Abort(produceErr)
	} else {
		line.CloseWrite()
	}
	deliverErr := <-delivered
	if produceErr != nil && !errors.Is(produceErr, io.EOF) {
		return produceErr
	}
	if deliverErr != nil {
		return deliverErr
	}
	return produceErr
}

// relay reads one direction and hands each chunk to write, pacing first when a
// limiter is supplied.
func relay(ctx context.Context, source net.Conn, limiter *nntpshaper.AggregateLimiter, write func([]byte) error) error {
	buffer := make([]byte, 32<<10)
	for {
		count, readErr := source.Read(buffer)
		if count > 0 {
			if err := limiter.WaitN(ctx, count); err != nil {
				return err
			}
			if err := write(buffer[:count]); err != nil {
				return err
			}
		}
		if readErr != nil {
			return readErr
		}
	}
}

// sleepContext waits out a delay unless the process is shutting down.
func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// censusWriter forwards client bytes upstream and feeds the forwarded prefix
// to the command census. A short or failed upstream write is reported as-is;
// only the bytes that actually went upstream are counted as sent commands.
type censusWriter struct {
	upstream io.Writer
	census   *nntpshaper.CommandCensus
}

func (writer *censusWriter) Write(payload []byte) (int, error) {
	written, err := writer.upstream.Write(payload)
	if written > 0 && writer.census != nil {
		writer.census.Observe(payload[:written])
	}
	return written, err
}

func writeDownstream(destination net.Conn, payload []byte, attestation *nntpshaper.Attestation, sourceIdentity string) error {
	for len(payload) > 0 {
		written, err := destination.Write(payload)
		if written < 0 || written > len(payload) {
			return fmt.Errorf("downstream write returned invalid byte count %d", written)
		}
		if written > 0 {
			attestation.AddDownstreamBytes(sourceIdentity, written)
			payload = payload[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func downstreamSource(address net.Addr) string {
	if address == nil {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(address.String())
	if err == nil && host != "" {
		if parsed, parseErr := netip.ParseAddr(host); parseErr == nil {
			return parsed.Unmap().String()
		}
		return host
	}
	return address.String()
}

func closeWrite(connection net.Conn) {
	if tcp, ok := connection.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
}

func stringEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func uintEnv(name string, fallback uint64) (uint64, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}
