package weaver

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
)

// scriptedCappedProvider answers the first `refusals` connections the way the
// NNTP provider answers one over its chaos cap, and serves connection metrics
// to every connection after that. It stands in for the window after a capped
// round in which a stopped Weaver's sockets are still on the provider's books.
func scriptedCappedProvider(t *testing.T, refusals int32) (host, port string, served *atomic.Int32) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	var seen atomic.Int32
	served = &atomic.Int32{}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if seen.Add(1) <= refusals {
					_, _ = conn.Write([]byte("502 Too many connections\r\n"))
					return
				}
				reader := bufio.NewReader(conn)
				_, _ = conn.Write([]byte("200 scripted ready\r\n"))
				for _, reply := range []string{"381 password\r\n", "281 authenticated\r\n"} {
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
					_, _ = conn.Write([]byte(reply))
				}
				command, err := reader.ReadString('\n')
				if err != nil || !strings.HasPrefix(command, "METRICS CONNECTIONS") {
					return
				}
				served.Add(1)
				_, _ = conn.Write([]byte(`290 {"attempted":9,"accepted":4,"rejected":5,"active":0,"peak_active":4,"configured_limit":4}` + "\r\n"))
			}()
		}
	}()

	host, port, err = net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	return host, port, served
}

// The refusal a full cap answers with has to be recognisable, or the caller
// that can wait for a slot cannot tell it from a provider that is broken.
func TestConnectionCapRefusalIsItsOwnError(t *testing.T) {
	host, port, _ := scriptedCappedProvider(t, 1)

	_, err := openNntpCommandSession(host, port, true)
	if err == nil {
		t.Fatal("a session over the cap must not open")
	}
	if !errors.Is(err, errNntpConnectionsSaturated) {
		t.Fatalf("err = %v, want the saturated-cap sentinel", err)
	}
}

// Any other bad greeting is the provider being wrong, and must not be mistaken
// for a slot that will free itself.
func TestOtherBadGreetingsAreNotCapSaturation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("400 service temporarily unavailable\r\n"))
	}()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}

	_, err = openNntpCommandSession(host, port, true)
	if err == nil {
		t.Fatal("a 400 greeting must not open a session")
	}
	if errors.Is(err, errNntpConnectionsSaturated) {
		t.Fatalf("a 400 greeting was read as a saturated cap: %v", err)
	}
}

// The counters are read once the stopped Weaver's sockets have drained, not
// on a fixed budget that expires while they are still on the books.
func TestConnectionMetricsWaitOutASaturatedCap(t *testing.T) {
	// More refusals than the plain fetch's own attempts, so the wait is the
	// only thing that can get through.
	host, port, served := scriptedCappedProvider(t, 8)

	metrics, err := waitForFreeNntpConnectionMetrics(host, port)
	if err != nil {
		t.Fatalf("waiting for a free slot: %v", err)
	}
	if served.Load() != 1 {
		t.Fatalf("metrics served %d times, want exactly one answered read", served.Load())
	}
	if metrics.ConfiguredLimit != 4 || metrics.PeakActive != 4 || metrics.Rejected != 5 {
		t.Fatalf("metrics = %+v, want the round's counters intact", metrics)
	}
}
