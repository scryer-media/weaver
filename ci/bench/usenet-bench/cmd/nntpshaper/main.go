// nntpshaper is a transparent, server-side NNTP egress shaper. It does not
// terminate TLS: implicit TLS bytes pass through to the public NNTP server so
// verified client TLS still validates the upstream server certificate.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntpshaper"
)

type listenerConfig struct {
	listenAddress string
	upstream      string
	label         string
}

func main() {
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

	var workers sync.WaitGroup
	for index, listener := range listeners {
		workers.Add(1)
		go func(listener net.Listener, config listenerConfig) {
			defer workers.Done()
			serve(ctx, listener, config, limiter)
		}(listener, configs[index])
	}
	<-ctx.Done()
	for _, listener := range listeners {
		_ = listener.Close()
	}
	workers.Wait()
}

func serve(ctx context.Context, listener net.Listener, config listenerConfig, limiter *nntpshaper.AggregateLimiter) {
	for {
		client, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("accept %s: %v", config.label, err)
			continue
		}
		go proxy(ctx, client, config, limiter)
	}
}

func proxy(ctx context.Context, client net.Conn, config listenerConfig, limiter *nntpshaper.AggregateLimiter) {
	defer client.Close()
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", config.upstream)
	if err != nil {
		log.Printf("dial %s upstream %s for %s: %v", config.label, config.upstream, client.RemoteAddr(), err)
		return
	}
	defer upstream.Close()

	upstreamDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(upstream, client)
		closeWrite(upstream)
		close(upstreamDone)
	}()
	if err := copyDownstream(ctx, client, upstream, limiter); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
		log.Printf("proxy %s downstream %s: %v", config.label, client.RemoteAddr(), err)
	}
	_ = client.Close()
	_ = upstream.Close()
	<-upstreamDone
}

func copyDownstream(ctx context.Context, destination net.Conn, source net.Conn, limiter *nntpshaper.AggregateLimiter) error {
	buffer := make([]byte, 32<<10)
	for {
		count, readErr := source.Read(buffer)
		if count > 0 {
			if err := limiter.WaitN(ctx, count); err != nil {
				return err
			}
			if _, err := destination.Write(buffer[:count]); err != nil {
				return err
			}
		}
		if readErr != nil {
			return readErr
		}
	}
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
