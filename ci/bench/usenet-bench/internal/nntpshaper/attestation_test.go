package nntpshaper

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFileSHA256(t *testing.T) {
	path := t.TempDir() + "/shaper"
	contents := []byte("immutable benchmark binary")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	digest, err := FileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", sha256.Sum256(contents))
	if digest != want {
		t.Fatalf("digest=%s, want %s", digest, want)
	}
}

func TestCurrentExecutableSHA256(t *testing.T) {
	digest, err := CurrentExecutableSHA256()
	if err != nil {
		t.Fatal(err)
	}
	if len(digest) != sha256.Size*2 {
		t.Fatalf("digest length=%d, want %d", len(digest), sha256.Size*2)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		t.Fatalf("digest is not hexadecimal: %v", err)
	}
}

func TestAttestationHandlerExposesVersionedSnapshot(t *testing.T) {
	started := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	attestation := NewAttestation(AttestationConfig{
		EgressBitsPerSecond: 10_000_000_000,
		BurstBytes:          1_250_000_000,
		Build:               BuildIdentity{ExecutableSHA256: "f00d", ImageIdentity: "example@sha256:beef", Version: "v1.2.3", Commit: "abc123", BuildTime: "2026-08-10T00:00:00Z"},
		StartedAt:           started,
	})
	if err := attestation.AcquireExecutionLease(strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	release, err := attestation.OpenDownstream("172.18.0.2")
	if err != nil {
		t.Fatal(err)
	}
	attestation.AddDownstreamBytes("172.18.0.2", 42)

	for _, path := range []string{"/v1/health", "/v1/stats"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		attestation.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d, want 200", path, response.Code)
		}
		var snapshot Snapshot
		if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.SchemaVersion != attestationSchemaVersion || snapshot.Status != "ok" || !snapshot.StartedAt.Equal(started) {
			t.Fatalf("unexpected control snapshot: %+v", snapshot)
		}
		if snapshot.ConfiguredEgressBitsPerSecond != 10_000_000_000 || snapshot.ConfiguredBurstBytes != 1_250_000_000 {
			t.Fatalf("missing configured link: %+v", snapshot)
		}
		if snapshot.DownstreamConnections != 1 || snapshot.ActiveDownstreamConnections != 1 || snapshot.DownstreamBytes != 42 {
			t.Fatalf("missing live counters: %+v", snapshot)
		}
		if snapshot.DownstreamSourceConnections["172.18.0.2"] != 1 || snapshot.DownstreamSourceBytes["172.18.0.2"] != 42 {
			t.Fatalf("missing source-attributed counters: %+v", snapshot)
		}
		if snapshot.Build.ExecutableSHA256 != "f00d" || snapshot.Build.ImageIdentity != "example@sha256:beef" || snapshot.Build.Commit != "abc123" {
			t.Fatalf("missing build identity: %+v", snapshot.Build)
		}
	}
	release()
}

func TestAttestationCountsConcurrentDownstreamDelivery(t *testing.T) {
	attestation := NewAttestation(AttestationConfig{})
	if err := attestation.AcquireExecutionLease(strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	const workers = 64
	const bytesPerWorker = 4096
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			source := "172.18.0.2"
			release, err := attestation.OpenDownstream(source)
			if err != nil {
				t.Error(err)
				return
			}
			defer release()
			attestation.AddDownstreamBytes(source, bytesPerWorker)
		}()
	}
	group.Wait()

	snapshot := attestation.Snapshot()
	if snapshot.DownstreamConnections != workers || snapshot.ActiveDownstreamConnections != 0 {
		t.Fatalf("unexpected connection counters: %+v", snapshot)
	}
	if snapshot.DownstreamBytes != workers*bytesPerWorker {
		t.Fatalf("bytes=%d, want %d", snapshot.DownstreamBytes, workers*bytesPerWorker)
	}
}

func TestExecutionLeaseIsExclusiveAndSourceBound(t *testing.T) {
	attestation := NewAttestation(AttestationConfig{})
	leaseID := strings.Repeat("c", 64)
	if _, err := attestation.OpenDownstream("172.18.0.2"); err == nil {
		t.Fatal("downstream connection was accepted without an execution lease")
	}
	if err := attestation.AcquireExecutionLease(leaseID); err != nil {
		t.Fatal(err)
	}
	if err := attestation.AcquireExecutionLease(strings.Repeat("d", 64)); err == nil {
		t.Fatal("second execution lease was accepted")
	}
	release, err := attestation.OpenDownstream("172.18.0.2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := attestation.OpenDownstream("172.18.0.3"); err == nil {
		t.Fatal("execution lease accepted a second downstream source")
	}
	if err := attestation.ReleaseExecutionLease(leaseID); err == nil {
		t.Fatal("active execution lease was released with an open connection")
	}
	release()
	if err := attestation.ReleaseExecutionLease(leaseID); err != nil {
		t.Fatal(err)
	}
}
