package main

import (
	"bytes"
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
