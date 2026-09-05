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
	executableSHA256, err := nntpshaper.CurrentExecutableSHA256()
	if err != nil {
		log.Fatal(err)
	}
	attestation := nntpshaper.NewAttestation(nntpshaper.AttestationConfig{
		EgressBitsPerSecond: bitsPerSecond,
		BurstBytes:          burstBytes,
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
		log.Printf("%s listener %s -> %s; aggregate egress=%d bits/s burst=%d bytes", config.label, listener.Addr(), config.upstream, bitsPerSecond, burstBytes)
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
			serve(ctx, listener, config, limiter, attestation)
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

func serve(ctx context.Context, listener net.Listener, config listenerConfig, limiter *nntpshaper.AggregateLimiter, attestation *nntpshaper.Attestation) {
	for {
		client, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("accept %s: %v", config.label, err)
			continue
		}
		go proxy(ctx, client, config, limiter, attestation)
	}
}

func proxy(ctx context.Context, client net.Conn, config listenerConfig, limiter *nntpshaper.AggregateLimiter, attestation *nntpshaper.Attestation) {
	defer client.Close()
	sourceIdentity := downstreamSource(client.RemoteAddr())
	release, err := attestation.OpenDownstream(sourceIdentity)
	if err != nil {
		log.Printf("reject %s downstream %s: %v", config.label, client.RemoteAddr(), err)
		return
	}
	defer release()
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", config.upstream)
	if err != nil {
		log.Printf("dial %s upstream %s for %s: %v", config.label, config.upstream, client.RemoteAddr(), err)
		return
	}
	defer upstream.Close()

	upstreamDone := make(chan struct{})
	go func() {
		// The client's command stream is relayed byte for byte; the census
		// only reads a copy of what was forwarded.
		_, _ = io.Copy(&censusWriter{upstream: upstream, census: nntpshaper.NewCommandCensus(attestation)}, client)
		closeWrite(upstream)
		close(upstreamDone)
	}()
	if err := copyDownstream(ctx, client, upstream, limiter, attestation, sourceIdentity); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
		log.Printf("proxy %s downstream %s: %v", config.label, client.RemoteAddr(), err)
	}
	_ = client.Close()
	_ = upstream.Close()
	<-upstreamDone
}

func copyDownstream(ctx context.Context, destination net.Conn, source net.Conn, limiter *nntpshaper.AggregateLimiter, attestation *nntpshaper.Attestation, sourceIdentity string) error {
	buffer := make([]byte, 32<<10)
	for {
		count, readErr := source.Read(buffer)
		if count > 0 {
			if err := limiter.WaitN(ctx, count); err != nil {
				return err
			}
			if err := writeDownstream(destination, buffer[:count], attestation, sourceIdentity); err != nil {
				return err
			}
		}
		if readErr != nil {
			return readErr
		}
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
	if written > 0 {
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
