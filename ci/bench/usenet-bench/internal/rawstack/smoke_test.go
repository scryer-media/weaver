package rawstack

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// TestRawStackSmoke starts a real server and shaper. It is skipped unless the
// operator points it at a directory holding both binaries.
func TestRawStackSmoke(t *testing.T) {
	root := os.Getenv("NNTPBENCH_RAWSTACK_ROOT")
	if root == "" {
		t.Skip("set NNTPBENCH_RAWSTACK_ROOT to a directory with bin/, articles/ and password")
	}
	stack, err := New(Config{
		BinDir: root + "/bin", DataDir: root + "/articles", CertDir: root + "/certs",
		LogDir: root + "/logs", Username: "fixture-user", PasswordFile: root + "/password",
		Pipelining: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := benchmark.ResolveServerLinkProfile("1gbit", 0, 0, 100_000)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := stack.Start(context.Background(), profile); err != nil {
		t.Fatal(err)
	}
	t.Logf("stack healthy in %s", time.Since(start))
	defer stack.Stop()

	// The shaper serves nothing without an execution lease, exactly as it
	// refuses a run that did not claim the link.
	lease := strings.Repeat("a", 64)
	if _, err := benchmark.AcquireShaperExecutionLease(context.Background(), nil, stack.ControlURL(), lease); err != nil {
		t.Fatal(err)
	}

	dialStart := time.Now()
	connection, err := net.Dial("tcp", net.JoinHostPort(stack.Host(), stack.PlaintextPort()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	greeting, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(dialStart)
	t.Logf("greeting %q after %s", strings.TrimSpace(greeting), elapsed)
	if elapsed < 140*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Fatalf("greeting after %s; a 100ms round trip owes one and a half of them", elapsed)
	}

	// The snapshot is taken between runs, with no client attached, exactly as
	// the controller takes it.
	_ = connection.Close()
	time.Sleep(200 * time.Millisecond)
	snapshot, err := benchmark.FetchShaperSnapshot(context.Background(), nil, stack.ControlURL())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LinkShaping == nil {
		t.Fatal("the shaper attests no link shaping")
	}
	if err := snapshot.ValidateFor(profile); err != nil {
		t.Fatalf("the live attestation does not satisfy the plan: %v", err)
	}
	report, _ := json.Marshal(snapshot.LinkShaping)
	t.Logf("attested link: %s", report)
	if _, err := os.Stat(stack.CAFile()); err != nil {
		t.Fatalf("the server generated no CA: %v", err)
	}
}
