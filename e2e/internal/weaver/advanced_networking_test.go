package weaver

import (
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func advancedTestPhase(t *testing.T, spec weaverReleaseFlowSpec, egress ...string) *weaverReleasePhase {
	t.Helper()
	root := t.TempDir()
	return &weaverReleasePhase{
		Flow:            spec.Name,
		RootDir:         root,
		NetworkSubnet:   "10.250.7.0/24",
		EgressSubnets:   egress,
		ComposeOverride: filepath.Join(root, "network.compose.override.yml"),
		Spec:            spec,
	}
}

type composeOverride struct {
	Networks map[string]struct {
		IPAM struct {
			Config []map[string]string `json:"config"`
		} `json:"ipam"`
	} `json:"networks"`
	Services map[string]struct {
		Networks    map[string]map[string]string `json:"networks"`
		Environment map[string]string            `json:"environment"`
		Volumes     []string                     `json:"volumes"`
		CapAdd      []string                     `json:"cap_add"`
		CapDrop     []string                     `json:"cap_drop"`
	} `json:"services"`
}

func readOverride(t *testing.T, phase *weaverReleasePhase) composeOverride {
	t.Helper()
	if err := writeAdvancedNetworkOverride(phase); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(phase.ComposeOverride)
	if err != nil {
		t.Fatal(err)
	}
	var override composeOverride
	if err := json.Unmarshal(body, &override); err != nil {
		t.Fatalf("override is not JSON (and so not YAML): %v", err)
	}
	return override
}

func TestEgressOverrideAttachesEveryDestinationToBothNetworks(t *testing.T) {
	phase := advancedTestPhase(t, advancedNetworkingReleaseFlow(), "10.249.1.0/24", "10.249.2.0/24")
	override := readOverride(t, phase)

	for name, subnet := range map[string]string{"default": "10.250.7.0/24", "egress-a": "10.249.1.0/24", "egress-b": "10.249.2.0/24"} {
		config := override.Networks[name].IPAM.Config
		if len(config) != 1 || config[0]["subnet"] != subnet || !strings.HasSuffix(config[0]["ip_range"], ".128/26") {
			t.Fatalf("network %s ipam = %v, want subnet %s with a dynamic range above the fixed hosts", name, config, subnet)
		}
	}
	for _, host := range advancedNetworkHosts {
		attachments := override.Services[host.Service].Networks
		for _, name := range []string{"egress-a", "egress-b"} {
			if attachments[name]["ipv4_address"] == "" {
				t.Fatalf("%s has no fixed address on %s: %v", host.Service, name, attachments)
			}
		}
		if _, ok := attachments["default"]; !ok {
			t.Fatalf("%s leaves the default network: %v", host.Service, attachments)
		}
	}
	weaver := override.Services["weaver"]
	if weaver.Networks["egress-a"]["ipv4_address"] != "10.249.1.10" || weaver.Networks["egress-b"]["ipv4_address"] != "10.249.2.10" {
		t.Fatalf("weaver egress addresses = %v", weaver.Networks)
	}
	if !slices.Equal(weaver.CapAdd, []string{"NET_RAW"}) || weaver.Environment["WEAVER_RETAIN_NET_RAW"] != "true" {
		t.Fatalf("weaver must keep NET_RAW: cap_add=%v env=%v", weaver.CapAdd, weaver.Environment)
	}
	if len(weaver.Volumes) != 1 || !strings.HasSuffix(weaver.Volumes[0], ":/etc/resolv.conf:ro") {
		t.Fatalf("weaver resolver mount = %v", weaver.Volumes)
	}
	resolver, err := os.ReadFile(filepath.Join(phase.RootDir, "proxy-resolv.conf"))
	if err != nil || !strings.Contains(string(resolver), "nameserver 10.250.7.250") {
		t.Fatalf("resolver = %q, %v", resolver, err)
	}

	fixture := override.Services["proxy-fixture"]
	if fixture.Networks["default"]["ipv4_address"] != "10.250.7.250" {
		t.Fatalf("proxy fixture resolver address = %v", fixture.Networks["default"])
	}
	if got := fixture.Environment["PROXY_FIXTURE_ADDRESSES"]; got != "10.250.7.250,10.249.1.23,10.249.2.23" {
		t.Fatalf("PROXY_FIXTURE_ADDRESSES = %q", got)
	}
	var hosts map[string][]string
	if err := json.Unmarshal([]byte(fixture.Environment["PROXY_FIXTURE_HOSTS"]), &hosts); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(hosts["nntp"], []string{"10.249.1.20", "10.249.2.20"}) {
		t.Fatalf("nntp host entry = %v", hosts["nntp"])
	}
	if _, ok := hosts["weaver"]; ok {
		t.Fatal("the fixture must not answer for Weaver itself")
	}
	if _, ok := hosts["weaver-postgres"]; ok {
		t.Fatal("weaver-postgres stays on Docker's resolver")
	}

	playwright := override.Services["weaver-playwright"].Environment
	if playwright["E2E_WEAVER_EGRESS_A_IP"] != "10.249.1.10" || playwright["E2E_NET_B_SUBNET"] != "10.249.2.0/24" || playwright["E2E_WEAVER_NET_RAW"] != "retained" {
		t.Fatalf("playwright environment = %v", playwright)
	}
}

func TestNoNetRawOverrideDropsTheCapabilityOnOneNetwork(t *testing.T) {
	override := readOverride(t, advancedTestPhase(t, advancedNetworkingNoNetRawReleaseFlow()))
	if len(override.Networks) != 1 {
		t.Fatalf("networks = %v, want only default", override.Networks)
	}
	weaver := override.Services["weaver"]
	if !slices.Equal(weaver.CapDrop, []string{"NET_RAW"}) || len(weaver.CapAdd) != 0 || weaver.Environment["WEAVER_RETAIN_NET_RAW"] != "false" {
		t.Fatalf("weaver capabilities = add %v drop %v env %v", weaver.CapAdd, weaver.CapDrop, weaver.Environment)
	}
	if weaver.Networks["default"]["ipv4_address"] != "10.250.7.10" {
		t.Fatalf("weaver address = %v", weaver.Networks)
	}
	if got := override.Services["weaver-playwright"].Environment["E2E_WEAVER_NET_RAW"]; got != "dropped" {
		t.Fatalf("E2E_WEAVER_NET_RAW = %q", got)
	}
}

func TestFixtureAddressOverridePinsOnlyTheFixture(t *testing.T) {
	override := readOverride(t, advancedTestPhase(t, eventScriptsReleaseFlow()))
	if len(override.Services) != 1 {
		t.Fatalf("services = %v, want only proxy-fixture", override.Services)
	}
	fixture := override.Services["proxy-fixture"]
	if fixture.Networks["default"]["ipv4_address"] != "10.250.7.250" || fixture.Environment["PROXY_FIXTURE_IP"] != "10.250.7.250" {
		t.Fatalf("proxy fixture = %+v", fixture)
	}
}

func TestEgressOverrideRefusesMissingEgressSubnets(t *testing.T) {
	phase := advancedTestPhase(t, advancedNetworkingReleaseFlow(), "10.249.1.0/24")
	if err := writeAdvancedNetworkOverride(phase); err == nil {
		t.Fatal("an egress layout with one egress subnet unexpectedly produced an override")
	}
}

func TestNetworkingFlowsLayerTheNetworkingComposeFile(t *testing.T) {
	phase := advancedTestPhase(t, advancedNetworkingReleaseFlow(), "10.249.1.0/24", "10.249.2.0/24")
	files := strings.Split(phase.env()["COMPOSE_FILE"], string(os.PathListSeparator))
	want := []string{
		filepath.Join(e2eDir(), "docker-compose.yml"),
		filepath.Join(e2eDir(), advancedNetworkingComposeFile),
		phase.ComposeOverride,
	}
	if !slices.Equal(files, want) {
		t.Fatalf("COMPOSE_FILE = %v, want %v", files, want)
	}
	if phase.env()["E2E_TOXIPROXY_CONFIG"] != "./services/toxiproxy/networking.json" {
		t.Fatal("advanced networking must use the per-member Toxiproxy config")
	}
	if phase.env()["E2E_RUST_TOOLCHAIN"] == "" {
		t.Fatal("the tunnel fixture build needs the pinned toolchain")
	}
}

func TestExtendedNetworkingRunsOnlyByName(t *testing.T) {
	all, err := resolveWeaverReleaseGateSpecs("all")
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range all {
		if spec.Name == "advanced-networking-extended" {
			t.Fatal("the extended lane resolved into all")
		}
	}
	named, err := resolveWeaverReleaseGateSpecs("advanced-networking-extended")
	if err != nil || len(named) != 1 || named[0].Env["E2E_WEAVER_EXTENDED"] != "1" {
		t.Fatalf("named extended lane = %#v, %v", named, err)
	}
}

func TestReleaseFlowPlaywrightScriptsExist(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(weaverE2ETestRoot(t), "playwright-weaver", "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Scripts map[string]string `json:"scripts"`
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, spec := range weaverReleaseFlowSpecs {
		if spec.Kind == weaverReleaseFlowCommand {
			continue
		}
		script, ok := manifest.Scripts["test:"+spec.PlaywrightScript]
		if !ok {
			t.Fatalf("%s has no npm script test:%s", spec.Name, spec.PlaywrightScript)
		}
		for stage, stageScript := range spec.StageScripts {
			if !slices.Contains(spec.Stages, stage) {
				t.Fatalf("%s maps a script to stage %s, which it never runs", spec.Name, stage)
			}
			if _, ok := manifest.Scripts["test:"+stageScript]; !ok {
				t.Fatalf("%s stage %s has no npm script test:%s", spec.Name, stage, stageScript)
			}
		}
		if len(spec.SpecFiles) > 0 {
			for _, file := range spec.SpecFiles {
				if !strings.Contains(script, "tests/"+file) && !strings.Contains(script, "tests/network-") {
					t.Fatalf("test:%s does not run %s: %q", spec.PlaywrightScript, file, script)
				}
			}
		}
	}
	if _, ok := manifest.Scripts["test:two-nic"]; !ok {
		t.Fatal("the two-nic runner needs test:two-nic")
	}
	for name, script := range manifest.Scripts {
		if strings.Contains(script, "tests/network-") && name != "test:advanced-networking-extended" && !strings.Contains(script, "@extended") {
			t.Fatalf("%s may run @extended scenarios: %q", name, script)
		}
	}
}

func TestTwoNICConfigRequiresConfirmationAndEnvironment(t *testing.T) {
	values := map[string]string{
		"E2E_TWO_NIC_REMOTE":         "user@192.0.2.10",
		"E2E_TWO_NIC_REMOTE_E2E_DIR": "/home/user/weaver/e2e",
		"E2E_TWO_NIC_STACK_HOST":     "192.0.2.10",
		"E2E_TWO_NIC_EGRESSES":       "wifi=interface:en0,lan=interface:en7,src=source:192.0.2.20",
		"E2E_TWO_NIC_CAPTURE_IFACES": "en0,en7",
		"E2E_TWO_NIC_EXPECT":         `{"wifi":"192.0.2.20","lan":"192.0.2.21"}`,
		"WEAVER_BIN":                 "/usr/local/bin/weaver",
	}
	getenv := func(key string) string { return values[key] }
	if _, err := loadTwoNICConfig(twoNICLaneM, getenv); err == nil || !strings.Contains(err.Error(), "E2E_TWO_NIC_CONFIRM") {
		t.Fatalf("unconfirmed run = %v", err)
	}
	values["E2E_TWO_NIC_CONFIRM"] = "1"
	cfg, err := loadTwoNICConfig(twoNICLaneM, getenv)
	if err != nil {
		t.Fatal(err)
	}
	want := []twoNICEgress{
		{Name: "wifi", Kind: "INTERFACE", Value: "en0"},
		{Name: "lan", Kind: "INTERFACE", Value: "en7"},
		{Name: "src", Kind: "SOURCE_ADDRESS", Value: "192.0.2.20"},
	}
	if !slices.Equal(cfg.Egresses, want) || cfg.WeaverHost != "127.0.0.1" || cfg.WifiDevice != "en0" {
		t.Fatalf("lane M config = %+v", cfg)
	}
	if _, err := loadTwoNICConfig(twoNICLaneL, getenv); err == nil || !strings.Contains(err.Error(), "E2E_TWO_NIC_WEAVER_IMAGE") {
		t.Fatalf("lane L without its image = %v", err)
	}
	if _, err := parseTwoNICEgresses("wifi=en0"); err == nil {
		t.Fatal("an egress without a binding kind parsed")
	}
}

func TestTwoNICCommandsStayInsideTheirProject(t *testing.T) {
	cfg := twoNICConfig{Lane: twoNICLaneM, Remote: "user@192.0.2.10", RemoteE2EDir: "/srv/e2e dir", StackHost: "192.0.2.10"}
	stack := twoNICStackCommands(cfg)
	if stack.up[0] != "ssh" || !strings.Contains(stack.up[2], "'-p' '"+twoNICProject+"'") || !strings.Contains(stack.up[2], "cd '/srv/e2e dir'") {
		t.Fatalf("lane M stack up = %q", stack.up)
	}
	if !strings.Contains(stack.down[2], "'down' '-v'") {
		t.Fatalf("lane M stack down = %q", stack.down)
	}
	script := twoNICLinkCycleScript("enp100s0", 19190)
	down := strings.Index(script, "ip link set dev enp100s0 down")
	up := strings.Index(script, "ip link set dev enp100s0 up")
	if down < 0 || up < down || !strings.Contains(script, `"health":"DOWN"`) {
		t.Fatalf("link cycle script = %q", script)
	}
	run := twoNICRemoteWeaverRun(twoNICConfig{StackHost: "192.0.2.20", WeaverPort: 19190, WeaverImage: "weaver:test", TrustedCIDR: "192.0.2.20/32", WeaverHost: "192.0.2.10"}, "false")
	for _, want := range []string{"'--network' 'host'", "'WEAVER_RETAIN_NET_RAW=false'", "'weaver:test'"} {
		if !strings.Contains(run, want) {
			t.Fatalf("remote Weaver run %q lacks %s", run, want)
		}
	}
}

func TestFetchableNetworkCandidatesAreOutsideTheBlockedRanges(t *testing.T) {
	benchmarking := netip.MustParsePrefix("198.18.0.0/15")
	candidates := weaverReleaseFetchableNetworkCandidates([]*weaverReleasePhase{{Project: "weaver-release-event-scripts"}})
	if len(candidates) != 512 {
		t.Fatalf("got %d candidates, want 512", len(candidates))
	}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		prefix := netip.MustParsePrefix(candidate)
		if !benchmarking.Contains(prefix.Addr()) || prefix.Bits() != 24 {
			t.Fatalf("candidate %s is not a /24 in %s", candidate, benchmarking)
		}
		if prefix.Addr().IsPrivate() || prefix.Addr().IsLoopback() || prefix.Addr().IsLinkLocalUnicast() {
			t.Fatalf("candidate %s is in a range Weaver refuses to fetch from", candidate)
		}
		if seen[candidate] {
			t.Fatalf("candidate %s repeats", candidate)
		}
		seen[candidate] = true
	}
	if !weaverNetworkLayoutFixtureAddress.servesNzbUrls() || weaverNetworkLayoutEgress.servesNzbUrls() {
		t.Fatal("only the fixture-address layout serves NZB URLs")
	}
}

func TestAdvancedNetworkingTimeScaleReachesOnlyItsFlows(t *testing.T) {
	scaled := map[string]bool{
		"advanced-networking":            true,
		"advanced-networking-no-net-raw": true,
	}
	t.Setenv("E2E_WEAVER_NETWORK_TIME_SCALE", "99")
	for _, spec := range weaverReleaseFlowSpecs {
		phase := &weaverReleasePhase{Flow: spec.Name, Spec: spec}
		got := phase.env()["E2E_WEAVER_NETWORK_TIME_SCALE"]
		want := "1"
		if scaled[spec.Name] {
			want = advancedNetworkingTimeScale
		}
		if got != want {
			t.Errorf("%s: E2E_WEAVER_NETWORK_TIME_SCALE = %q, want %q", spec.Name, got, want)
		}
	}
}
