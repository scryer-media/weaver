package weaver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

// weaverReleaseNetworkLayout selects the Compose network override a phase
// gets on top of its isolated default network.
type weaverReleaseNetworkLayout string

const (
	// weaverNetworkLayoutDefault is the plain isolated default network.
	weaverNetworkLayoutDefault weaverReleaseNetworkLayout = ""
	// weaverNetworkLayoutEgress adds egress-a and egress-b. Weaver and every
	// destination attach to both at fixed addresses, so Weaver sees two real
	// interfaces and each destination has one address per interface.
	weaverNetworkLayoutEgress weaverReleaseNetworkLayout = "egress"
	// weaverNetworkLayoutSingle gives the same services fixed addresses on the
	// default network only, so Weaver has exactly one interface.
	weaverNetworkLayoutSingle weaverReleaseNetworkLayout = "single"
	// weaverNetworkLayoutFixtureAddress pins only the proxy fixture, which
	// serves NZB URLs to the script flows; Weaver keeps Docker's resolver.
	weaverNetworkLayoutFixtureAddress weaverReleaseNetworkLayout = "fixture-address"
)

// servesNzbUrls reports whether the layout's proxy fixture serves NZB URLs
// that Weaver must be allowed to fetch.
func (layout weaverReleaseNetworkLayout) servesNzbUrls() bool {
	return layout == weaverNetworkLayoutFixtureAddress
}

func (layout weaverReleaseNetworkLayout) egressNetworks() int {
	if layout == weaverNetworkLayoutEgress {
		return 2
	}
	return 0
}

const advancedNetworkingComposeFile = "docker-compose.networking.yml"

// How many times faster than production Weaver runs its connection-plan
// timers (delivery verdicts, challenger dials, pin age, route cooldowns) in
// the advanced-networking flows, so a stage does not wait them out in real
// time. Takes effect only because the gate also runs Weaver in e2e mode.
const advancedNetworkingTimeScale = "10"

// advancedNetworkHosts are the fixed host octets on every network a layout
// attaches. The proxy fixture also owns .250 on the default network, where
// Weaver's resolver points.
var advancedNetworkHosts = []struct {
	Service string
	Octet   int
}{
	{"weaver", 10},
	{"nntp", 20},
	{"nntp2", 21},
	{"toxiproxy", 22},
	{"proxy-fixture", 23},
	{"tunnel-fixture", 24},
	{"rss-fixture", 25},
}

const proxyFixtureResolverOctet = 250

func advancedNetworkingServices() []string {
	return []string{"nntp", "nntp2", "weaver", "toxiproxy", "proxy-fixture", "tunnel-fixture", "rss-fixture", "capture"}
}

var advancedNetworkingSpecFiles = []string{
	"network-egress.spec.ts",
	"network-legs.spec.ts",
	"network-ladders.spec.ts",
	"network-pools.spec.ts",
	"network-sessions.spec.ts",
	"network-reload.spec.ts",
	"network-rss.spec.ts",
}

func advancedNetworkingArtifacts() []string {
	return append(defaultWeaverReleaseArtifacts(), "captures", "network-evidence.json")
}

func advancedNetworkingReleaseFlow() weaverReleaseFlowSpec {
	return weaverReleaseFlowSpec{
		Name:             "advanced-networking",
		Kind:             weaverReleaseFlowBehavior,
		PlaywrightScript: "advanced-networking",
		SpecFiles:        append([]string(nil), advancedNetworkingSpecFiles...),
		Services:         advancedNetworkingServices(),
		Datastores:       releaseDatastoreMatrix(),
		Artifacts:        advancedNetworkingArtifacts(),
		Timeout:          90 * time.Minute, // both stages; the initial stage alone takes ~30 min
		ComposeFiles:     []string{advancedNetworkingComposeFile},
		NetworkLayout:    weaverNetworkLayoutEgress,
		Stages:           []string{"initial", "restarted"},
		// After the restart only the @restart specs run: the rest build and
		// tear down their own state, so a second pass proves nothing new.
		StageScripts: map[string]string{"restarted": "advanced-networking-restarted"},
		Env: map[string]string{
			"E2E_TOXIPROXY_CONFIG":          "./services/toxiproxy/networking.json",
			"E2E_WEAVER_NETWORK_TIME_SCALE": advancedNetworkingTimeScale,
		},
	}
}

// The Bind refusal needs a Weaver without CAP_NET_RAW, which is a container
// property, so it runs as its own single-network phase.
func advancedNetworkingNoNetRawReleaseFlow() weaverReleaseFlowSpec {
	spec := advancedNetworkingReleaseFlow()
	spec.Name = "advanced-networking-no-net-raw"
	spec.PlaywrightScript = "advanced-networking-no-net-raw"
	spec.SpecFiles = []string{"network-egress.spec.ts"}
	spec.Datastores = []weaverDatastore{weaverDatastoreSQLite}
	spec.Timeout = 10 * time.Minute
	spec.NetworkLayout = weaverNetworkLayoutSingle
	spec.DropNetRaw = true
	spec.Stages = []string{"initial"}
	spec.StageScripts = nil
	return spec
}

// Scenarios that wait out ten-minute product timers (P04, P07, the F16 long
// form). Named runs only, like the archive matrix.
func advancedNetworkingExtendedReleaseFlow() weaverReleaseFlowSpec {
	spec := advancedNetworkingReleaseFlow()
	spec.Name = "advanced-networking-extended"
	spec.PlaywrightScript = "advanced-networking-extended"
	spec.Datastores = []weaverDatastore{weaverDatastoreSQLite}
	spec.Timeout = 90 * time.Minute
	spec.Stages = []string{"initial"}
	spec.StageScripts = nil
	spec.ExtendedOnly = true
	spec.Env["E2E_WEAVER_EXTENDED"] = "1"
	// These scenarios exist to wait out the real product timers.
	spec.Env["E2E_WEAVER_NETWORK_TIME_SCALE"] = "1"
	return spec
}

func eventScriptsReleaseFlow() weaverReleaseFlowSpec {
	return weaverReleaseFlowSpec{
		Name:             "event-scripts",
		Kind:             weaverReleaseFlowBehavior,
		PlaywrightScript: "event-scripts",
		SpecFiles:        []string{"event-scripts.spec.ts", "scan-scripts.spec.ts", "script-restart.spec.ts"},
		Services:         []string{"nntp", "nntp2", "weaver", "proxy-fixture"},
		Datastores:       releaseDatastoreMatrix(),
		Artifacts:        append(defaultWeaverReleaseArtifacts(), "script-records"),
		Timeout:          30 * time.Minute,
		NetworkLayout:    weaverNetworkLayoutFixtureAddress,
		Stages:           []string{"initial", "restarted"},
	}
}

// schedulingReleaseFlow runs the schedule and automatic-backup specs. The
// RSS fixture's counted feeds show how often a scheduled fetch ran.
func schedulingReleaseFlow() weaverReleaseFlowSpec {
	return weaverReleaseFlowSpec{
		Name:             "scheduling",
		Kind:             weaverReleaseFlowBehavior,
		PlaywrightScript: "scheduling",
		SpecFiles:        []string{"scheduling.spec.ts", "auto-backup.spec.ts"},
		Services:         []string{"nntp", "nntp2", "weaver", "rss-fixture"},
		Datastores:       releaseDatastoreMatrix(),
		Artifacts:        append(defaultWeaverReleaseArtifacts(), "script-records"),
		Timeout:          25 * time.Minute,
		Stages:           []string{"initial", "restarted", "restarted-again"},
	}
}

// schedulingTracksReleaseFlow drives every schedule action through the hold
// matrix, the track overlaps, operator overrides and one-shot occurrences.
// Between its stages the harness moves the clock while Weaver is stopped, so
// a rule's occurrence falls while Weaver is down.
func schedulingTracksReleaseFlow() weaverReleaseFlowSpec {
	return weaverReleaseFlowSpec{
		Name:              "scheduling-tracks",
		Kind:              weaverReleaseFlowBehavior,
		PlaywrightScript:  "scheduling-tracks",
		SpecFiles:         []string{"schedule-tracks.spec.ts"},
		Services:          []string{"nntp", "nntp2", "weaver", "rss-fixture"},
		Datastores:        releaseDatastoreMatrix(),
		Artifacts:         defaultWeaverReleaseArtifacts(),
		Timeout:           40 * time.Minute,
		Stages:            []string{"initial", "restarted"},
		ClockWhileStopped: true,
	}
}

// The DST case needs a zone with a transition, which is the container's TZ.
func schedulingDSTReleaseFlow() weaverReleaseFlowSpec {
	spec := schedulingReleaseFlow()
	spec.Name = "scheduling-dst"
	spec.PlaywrightScript = "scheduling-dst"
	spec.SpecFiles = []string{"auto-backup.spec.ts"}
	spec.Services = []string{"nntp", "nntp2", "weaver"}
	spec.Datastores = []weaverDatastore{weaverDatastoreSQLite}
	spec.Timeout = 10 * time.Minute
	spec.Stages = []string{"initial"}
	spec.Env = map[string]string{"E2E_WEAVER_TIMEZONE": "America/Denver"}
	return spec
}

// weaverReleaseFlowSpecFiles lists the Playwright spec files a flow runs.
func weaverReleaseFlowSpecFiles(spec weaverReleaseFlowSpec) []string {
	if len(spec.SpecFiles) > 0 {
		return append([]string(nil), spec.SpecFiles...)
	}
	return []string{spec.PlaywrightScript + ".spec.ts"}
}

func hostAddress(subnet string, octet int) (string, error) {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(subnet))
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 24 {
		return "", fmt.Errorf("network subnet %q is not an IPv4 /24 or wider", subnet)
	}
	bytes := prefix.Masked().Addr().As4()
	bytes[3] = byte(octet)
	address := netip.AddrFrom4(bytes)
	if !prefix.Contains(address) {
		return "", fmt.Errorf("host .%d is outside %s", octet, subnet)
	}
	return address.String(), nil
}

// dynamicRange keeps Docker's own allocations (Playwright, Postgres, the
// capture sidecar's peers) clear of the fixed addresses below .128.
func dynamicRange(subnet string) (string, error) {
	base, err := hostAddress(subnet, 128)
	if err != nil {
		return "", err
	}
	return base + "/26", nil
}

func networkDefinition(subnet string) (map[string]any, error) {
	ipRange, err := dynamicRange(subnet)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"ipam": map[string]any{
			"config": []any{map[string]any{"subnet": subnet, "ip_range": ipRange}},
		},
	}, nil
}

// advancedNetworkOverride builds the phase's whole generated Compose override.
// JSON is valid YAML, and marshalling keeps paths and the host table quoted.
func advancedNetworkOverride(phase *weaverReleasePhase) (map[string]any, error) {
	layout := phase.Spec.NetworkLayout
	if len(phase.EgressSubnets) != layout.egressNetworks() {
		return nil, fmt.Errorf("%s needs %d egress subnets, has %d", phase.Flow, layout.egressNetworks(), len(phase.EgressSubnets))
	}
	defaultNetwork, err := networkDefinition(phase.NetworkSubnet)
	if err != nil {
		return nil, err
	}
	networks := map[string]any{"default": defaultNetwork}
	egressNames := []string{"egress-a", "egress-b"}[:len(phase.EgressSubnets)]
	for index, name := range egressNames {
		definition, err := networkDefinition(phase.EgressSubnets[index])
		if err != nil {
			return nil, err
		}
		networks[name] = definition
	}
	resolverIP, err := hostAddress(phase.NetworkSubnet, proxyFixtureResolverOctet)
	if err != nil {
		return nil, err
	}
	services := map[string]map[string]any{}
	service := func(name string) map[string]any {
		if services[name] == nil {
			services[name] = map[string]any{}
		}
		return services[name]
	}

	if layout == weaverNetworkLayoutFixtureAddress {
		service("proxy-fixture")["networks"] = map[string]any{"default": map[string]any{"ipv4_address": resolverIP}}
		service("proxy-fixture")["environment"] = map[string]any{"PROXY_FIXTURE_IP": resolverIP}
		return map[string]any{"networks": networks, "services": services}, nil
	}

	// Every service's address on each network it joins, in network order:
	// egress-a then egress-b, or the default network for the single layout.
	hostAddresses := map[string][]string{}
	for _, host := range advancedNetworkHosts {
		attachments := map[string]any{}
		if layout == weaverNetworkLayoutSingle {
			octet := host.Octet
			if host.Service == "proxy-fixture" {
				octet = proxyFixtureResolverOctet
			}
			address, err := hostAddress(phase.NetworkSubnet, octet)
			if err != nil {
				return nil, err
			}
			attachments["default"] = map[string]any{"ipv4_address": address}
			hostAddresses[host.Service] = []string{address}
		} else {
			if host.Service == "proxy-fixture" {
				attachments["default"] = map[string]any{"ipv4_address": resolverIP}
			} else {
				attachments["default"] = map[string]any{}
			}
			for index, name := range egressNames {
				address, err := hostAddress(phase.EgressSubnets[index], host.Octet)
				if err != nil {
					return nil, err
				}
				attachments[name] = map[string]any{"ipv4_address": address}
				hostAddresses[host.Service] = append(hostAddresses[host.Service], address)
			}
		}
		service(host.Service)["networks"] = attachments
	}

	fixtureHosts := map[string][]string{}
	for name, addresses := range hostAddresses {
		if name != "weaver" {
			fixtureHosts[name] = addresses
		}
	}
	fixtureHostsJSON, _ := json.Marshal(fixtureHosts)
	networkHostsJSON, _ := json.Marshal(hostAddresses)
	fixtureAddresses := []string{resolverIP}
	if layout == weaverNetworkLayoutEgress {
		fixtureAddresses = append(fixtureAddresses, hostAddresses["proxy-fixture"]...)
	}
	service("proxy-fixture")["environment"] = map[string]any{
		"PROXY_FIXTURE_IP":        resolverIP,
		"PROXY_FIXTURE_ADDRESSES": strings.Join(fixtureAddresses, ","),
		"PROXY_FIXTURE_HOSTS":     string(fixtureHostsJSON),
	}

	resolverPath := filepath.Join(phase.RootDir, "proxy-resolv.conf")
	if err := os.WriteFile(resolverPath, []byte("nameserver "+resolverIP+"\noptions timeout:1 attempts:1\n"), 0o644); err != nil {
		return nil, err
	}
	weaver := service("weaver")
	weaver["volumes"] = []any{resolverPath + ":/etc/resolv.conf:ro"}
	weaver["depends_on"] = map[string]any{"proxy-fixture": map[string]any{"condition": "service_healthy"}}
	netRaw := "retained"
	if phase.Spec.DropNetRaw {
		netRaw = "dropped"
		weaver["cap_drop"] = []any{"NET_RAW"}
		weaver["environment"] = map[string]any{"WEAVER_RETAIN_NET_RAW": "false"}
	} else {
		// Podman's default capability set has no NET_RAW; Docker's has it.
		weaver["cap_add"] = []any{"NET_RAW"}
		weaver["environment"] = map[string]any{"WEAVER_RETAIN_NET_RAW": "true"}
	}

	playwrightEnv := map[string]any{
		"E2E_NETWORK_HOSTS":  string(networkHostsJSON),
		"E2E_WEAVER_NET_RAW": netRaw,
	}
	if layout == weaverNetworkLayoutEgress {
		playwrightEnv["E2E_NET_A_SUBNET"] = phase.EgressSubnets[0]
		playwrightEnv["E2E_NET_B_SUBNET"] = phase.EgressSubnets[1]
		playwrightEnv["E2E_WEAVER_EGRESS_A_IP"] = hostAddresses["weaver"][0]
		playwrightEnv["E2E_WEAVER_EGRESS_B_IP"] = hostAddresses["weaver"][1]
	}
	service("weaver-playwright")["environment"] = playwrightEnv
	return map[string]any{"networks": networks, "services": services}, nil
}

func writeAdvancedNetworkOverride(phase *weaverReleasePhase) error {
	override, err := advancedNetworkOverride(phase)
	if err != nil {
		return err
	}
	body, err := json.MarshalIndent(override, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(phase.ComposeOverride, append(body, '\n'), 0o644)
}

var advancedNetworkingImagesOnce struct {
	sync.Mutex
	done bool
}

// ensureAdvancedNetworkingImages builds the tunnel fixture and capture
// sidecar once before fanout, so parallel phases never race to build the
// same tag.
func ensureAdvancedNetworkingImages(specs []weaverReleaseFlowSpec) error {
	if !slices.ContainsFunc(specs, func(spec weaverReleaseFlowSpec) bool {
		return slices.Contains(spec.ComposeFiles, advancedNetworkingComposeFile)
	}) {
		return nil
	}
	advancedNetworkingImagesOnce.Lock()
	defer advancedNetworkingImagesOnce.Unlock()
	if advancedNetworkingImagesOnce.done {
		return nil
	}
	args := []string{
		"compose", "-p", composeProject(),
		"-f", filepath.Join(e2eDir(), "docker-compose.yml"),
		"-f", filepath.Join(e2eDir(), advancedNetworkingComposeFile),
		"build", "tunnel-fixture", "capture",
	}
	cmd := containerengine.Command(args...)
	cmd.Dir = e2eDir()
	cmd.Env = append(os.Environ(), "E2E_RUST_TOOLCHAIN="+weaverPinnedRustToolchain(weaverRepoPath()))
	if err := runExternalCommand(cmd, "docker compose build tunnel-fixture capture"); err != nil {
		return err
	}
	advancedNetworkingImagesOnce.done = true
	return nil
}

// The fixtures a staged flow waits on before its first Playwright run, in
// start order. Weaver itself is waited on by startWeaverReleaseStack.
var stagedFlowFixtures = []string{"proxy-fixture", "toxiproxy", "tunnel-fixture", "capture"}

// runWeaverStagedReleaseFlow runs the flow's Playwright script once per
// stage. Between stages it restarts Weaver, then the capture sidecar, which
// shares Weaver's network namespace and loses it with the restart.
func runWeaverStagedReleaseFlow(
	ctx context.Context,
	spec weaverReleaseFlowSpec,
	datastore weaverDatastore,
) error {
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return fmt.Errorf("start %s stack: %w", spec.Name, err)
	}
	for _, fixture := range stagedFlowFixtures {
		if !slices.Contains(spec.Services, fixture) {
			continue
		}
		if err := waitForDockerServiceReady(fixture, 90*time.Second); err != nil {
			return err
		}
	}
	for index, stage := range spec.Stages {
		if index > 0 {
			if err := restartWeaverForStage(spec); err != nil {
				return fmt.Errorf("%s before stage %s: %w", spec.Name, stage, err)
			}
		}
		setEnv("E2E_WEAVER_STAGE", stage)
		setEnv("E2E_WEAVER_ARTIFACT_STAGE", stage)
		script := spec.PlaywrightScript
		if stageScript, ok := spec.StageScripts[stage]; ok {
			script = stageScript
		}
		if err := runWeaverReleasePlaywright(ctx, script); err != nil {
			return fmt.Errorf("%s stage %s: %w", spec.Name, stage, err)
		}
		if err := captureWeaverReleaseStageDiagnostics(stage); err != nil {
			return fmt.Errorf("capture %s stage %s evidence: %w", spec.Name, stage, err)
		}
	}
	return nil
}

func restartWeaverForStage(spec weaverReleaseFlowSpec) error {
	if spec.ClockWhileStopped {
		if err := restartWeaverWithClockWhileStopped(); err != nil {
			return err
		}
	} else if err := dockerComposeRestart("weaver"); err != nil {
		return fmt.Errorf("restart Weaver: %w", err)
	}
	if err := waitForDockerServiceReady("weaver", 90*time.Second); err != nil {
		return err
	}
	waitForHTTP(defaultWeaverURL(), 90*time.Second)
	if slices.Contains(spec.Services, "capture") {
		if err := dockerComposeRestart("capture"); err != nil {
			return fmt.Errorf("restart capture sidecar: %w", err)
		}
		if err := waitForDockerServiceReady("capture", 90*time.Second); err != nil {
			return err
		}
	}
	log.Printf("%s: Weaver restarted between stages", spec.Name)
	return nil
}

// weaverClockWhileStoppedScript puts the instant a spec left beside the e2e
// clock file on the clock, and removes it, so a stage can make time pass
// while Weaver is down. Nothing left means the clock stays where it is.
const weaverClockWhileStoppedScript = `set -eu
pending=/e2e-clock/now.while-stopped
if [ -f "$pending" ]; then
  mv -f "$pending" /e2e-clock/now
  chown "${PUID:-1000}:${PGID:-1000}" /e2e-clock/now
  echo "e2e clock set while Weaver was stopped: $(cat /e2e-clock/now)"
fi`

// restartWeaverWithClockWhileStopped stops Weaver, moves the e2e clock to the
// instant the previous stage asked for, and starts Weaver again.
func restartWeaverWithClockWhileStopped() error {
	if err := stopWeaverReleaseService(context.Background()); err != nil {
		return fmt.Errorf("stop Weaver: %w", err)
	}
	move := containerengine.Command(dockerComposeArgs(
		"run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "weaver", "-c", weaverClockWhileStoppedScript,
	)...)
	move.Dir = e2eDir()
	move.Env = os.Environ()
	if err := runExternalCommand(move, "move the Weaver e2e clock while Weaver is stopped"); err != nil {
		return err
	}
	start := containerengine.Command(dockerComposeArgs("start", "weaver")...)
	start.Dir = e2eDir()
	if err := runExternalCommand(start, "start Weaver"); err != nil {
		return fmt.Errorf("start Weaver: %w", err)
	}
	return nil
}
