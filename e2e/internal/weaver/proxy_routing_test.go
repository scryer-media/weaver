package weaver

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/composeutil"
)

func TestProxyRoutingFlowRunsInBothDatastores(t *testing.T) {
	spec, ok := weaverReleaseFlowSpecFor("proxy-routing")
	if !ok || spec.Kind != weaverReleaseFlowBehavior {
		t.Fatal("proxy-routing must be a default runtime behavior flow")
	}
	if !slices.Equal(spec.Datastores, releaseDatastoreMatrix()) || !slices.Contains(spec.Services, "proxy-fixture") {
		t.Fatalf("proxy routing missing datastore or fixture coverage: %#v", spec)
	}
}

func TestProxyRoutingNetworkCapturesHostDNS(t *testing.T) {
	root := filepath.Join(t.TempDir(), "path with spaces")
	phase := &weaverReleasePhase{RootDir: root, NetworkSubnet: "10.249.93.0/24", ComposeOverride: filepath.Join(root, "network.yml")}
	if err := composeutil.WriteNetworkOverride(phase.ComposeOverride, phase.NetworkSubnet); err != nil {
		t.Fatal(err)
	}
	if err := writeProxyRoutingNetwork(phase); err != nil {
		t.Fatal(err)
	}
	resolver, err := os.ReadFile(filepath.Join(root, "proxy-resolv.conf"))
	if err != nil || string(resolver) != "nameserver 10.249.93.250\noptions timeout:1 attempts:1\n" {
		t.Fatalf("unexpected resolver: %q, %v", resolver, err)
	}
	override, err := os.ReadFile(phase.ComposeOverride)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"subnet: \"10.249.93.0/24\"", "ipv4_address: 10.249.93.250", "PROXY_FIXTURE_IP: 10.249.93.250", "proxy-resolv.conf:/etc/resolv.conf:ro", "condition: service_healthy"} {
		if !strings.Contains(string(override), required) {
			t.Errorf("missing %q in proxy network override", required)
		}
	}
	phase.NetworkSubnet = "10.249.93.0/30"
	if err := writeProxyRoutingNetwork(phase); err == nil {
		t.Fatal("out-of-subnet fixture IP accepted")
	}
}
