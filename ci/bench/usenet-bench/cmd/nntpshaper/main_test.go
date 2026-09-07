package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntpshaper"
)

func TestHealthCheck(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v1/health" {
			t.Fatalf("path=%q, want /v1/health", request.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(""))}, nil
	})}

	if err := healthCheckWithClient([]string{"--addr", "shaper:8080"}, client); err != nil {
		t.Fatal(err)
	}
}

func TestHealthCheckRejectsUnhealthyResponse(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	err := healthCheckWithClient([]string{"--addr", "shaper:8080"}, client)
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("error=%v, want 503 response", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestWriteDownstreamHandlesShortWrites(t *testing.T) {
	attestation := nntpshaper.NewAttestation(nntpshaper.AttestationConfig{})
	destination := &shortWriteConn{limit: 2}
	if err := writeDownstream(destination, []byte("hello"), attestation, "test-source"); err != nil {
		t.Fatal(err)
	}
	if got := destination.String(); got != "hello" {
		t.Fatalf("written=%q, want hello", got)
	}
	if got := attestation.Snapshot().DownstreamBytes; got != 5 {
		t.Fatalf("downstream bytes=%d, want 5", got)
	}
}

type shortWriteConn struct {
	bytes.Buffer
	limit int
}

func (connection *shortWriteConn) Read([]byte) (int, error) { return 0, io.EOF }
func (connection *shortWriteConn) Write(payload []byte) (int, error) {
	if len(payload) > connection.limit {
		payload = payload[:connection.limit]
	}
	return connection.Buffer.Write(payload)
}
func (connection *shortWriteConn) Close() error                     { return nil }
func (connection *shortWriteConn) LocalAddr() net.Addr              { return shaperAddr("local") }
func (connection *shortWriteConn) RemoteAddr() net.Addr             { return shaperAddr("remote") }
func (connection *shortWriteConn) SetDeadline(time.Time) error      { return nil }
func (connection *shortWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (connection *shortWriteConn) SetWriteDeadline(time.Time) error { return nil }

type shaperAddr string

func (address shaperAddr) Network() string { return "test" }
func (address shaperAddr) String() string  { return string(address) }

func TestCensusWriterCountsOnlyForwardedBytes(t *testing.T) {
	attestation := nntpshaper.NewAttestation(nntpshaper.AttestationConfig{})
	if err := attestation.AcquireExecutionLease(strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	upstream := &bytes.Buffer{}
	writer := &censusWriter{upstream: upstream, census: nntpshaper.NewCommandCensus(attestation)}
	if _, err := io.Copy(writer, strings.NewReader("BODY <a@example>\r\nBODY <a@example>\r\n")); err != nil {
		t.Fatal(err)
	}
	if upstream.String() != "BODY <a@example>\r\nBODY <a@example>\r\n" {
		t.Fatalf("upstream received %q", upstream.String())
	}
	snapshot := attestation.Snapshot()
	if snapshot.ArticleRequests != 2 || snapshot.DistinctArticleRequests != 1 || snapshot.RepeatedArticleRequests != 1 {
		t.Fatalf("unexpected census: %+v", snapshot)
	}
	// A failed upstream write forwards nothing and counts nothing.
	failing := &censusWriter{upstream: failingWriter{}, census: nntpshaper.NewCommandCensus(attestation)}
	if _, err := failing.Write([]byte("BODY <b@example>\r\n")); err == nil {
		t.Fatal("expected the upstream failure to surface")
	}
	if got := attestation.Snapshot().ArticleRequests; got != 2 {
		t.Fatalf("a command that never reached upstream was counted: %d", got)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// fakeNNTP is the smallest upstream that behaves like a news server for
// timing purposes: it greets on connect and answers each command line.
func fakeNNTP(t *testing.T, bodyBytes int) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer connection.Close()
				if _, err := connection.Write([]byte("200 ready\r\n")); err != nil {
					return
				}
				reader := bufio.NewReader(connection)
				for {
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					body := append([]byte("222 body\r\n"), bytes.Repeat([]byte("a"), bodyBytes)...)
					if _, err := connection.Write(body); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func shapedTestProxy(t *testing.T, rttMicros uint64, bodyBytes int) net.Addr {
	t.Helper()
	upstream := fakeNNTP(t, bodyBytes)
	link, err := nntpshaper.NewUserspaceLink(nntpshaper.UserspaceLinkConfig{
		RTTMicros:           rttMicros,
		EgressBitsPerSecond: 1_000_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := nntpshaper.NewAggregateLimiter(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	attestation := nntpshaper.NewAttestation(nntpshaper.AttestationConfig{RTTMicros: rttMicros, StartedAt: time.Now()})
	if err := attestation.AcquireExecutionLease(strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = front.Close() })
	go serve(ctx, front, listenerConfig{upstream: upstream.Addr().String(), label: "plaintext"}, limiter, attestation, link)
	return front.Addr()
}

func TestProxyChargesTheUserspaceRoundTrip(t *testing.T) {
	const rtt = 60 * time.Millisecond
	address := shapedTestProxy(t, uint64(rtt/time.Microsecond), 0)

	start := time.Now()
	client, err := net.Dial("tcp", address.String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reader := bufio.NewReader(client)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	greeting := time.Since(start)
	// A real client waits one round trip for the TCP handshake and another
	// half for the greeting to propagate back.
	if greeting < rtt*3/2-5*time.Millisecond {
		t.Fatalf("greeting arrived after %s, want at least one and a half %s round trips", greeting, rtt)
	}
	if greeting > rtt*3/2+250*time.Millisecond {
		t.Fatalf("greeting arrived after %s, far beyond one and a half %s round trips", greeting, rtt)
	}

	start = time.Now()
	if _, err := client.Write([]byte("BODY <article@bench>\r\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	exchange := time.Since(start)
	if exchange < rtt-5*time.Millisecond {
		t.Fatalf("a command and its response took %s, want a whole %s round trip", exchange, rtt)
	}
	if exchange > rtt+250*time.Millisecond {
		t.Fatalf("a command and its response took %s, far beyond one %s round trip", exchange, rtt)
	}
}

func TestProxyDelaysWithoutSerializingThroughput(t *testing.T) {
	const rtt = 60 * time.Millisecond
	const body = 1 << 20
	address := shapedTestProxy(t, uint64(rtt/time.Microsecond), body)

	client, err := net.Dial("tcp", address.String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reader := bufio.NewReader(client)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := client.Write([]byte("BODY <article@bench>\r\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(reader, make([]byte, body)); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	// A megabyte crosses the delay line in thirty-odd chunks. They ride the
	// link together, so the body costs one round trip -- not one per chunk.
	if elapsed > 3*rtt {
		t.Fatalf("a %d byte body took %s across a %s link; the delay line is serializing the stream", body, elapsed, rtt)
	}
}
