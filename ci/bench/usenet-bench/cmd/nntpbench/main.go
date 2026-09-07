// nntpbench owns product-neutral corpus seeding, schedule creation, and output
// verification. Client-specific adapters will consume its files and return
// results to the same run artifact directory.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nativeadapter"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntp"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/rawstack"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "seed":
		err = seed(os.Args[2:])
	case "seed-image":
		err = seedImage(os.Args[2:])
	case "plan":
		err = plan(os.Args[2:])
	case "server-env":
		err = serverEnv(os.Args[2:])
	case "storage-env":
		err = storageEnv(os.Args[2:])
	case "image":
		if len(os.Args) < 3 || os.Args[2] != "build" {
			err = fmt.Errorf("usage: nntpbench image build [options]")
			break
		}
		err = imageBuild(os.Args[3:])
	case "run":
		err = run(os.Args[2:])
	case "queue":
		err = queue(os.Args[2:])
	case "sequential":
		err = sequential(os.Args[2:])
	case "queue-transition":
		err = queueTransition(os.Args[2:])
	case "pin":
		err = pin(os.Args[2:])
	case "chain":
		err = chain(os.Args[2:])
	case "summarize":
		err = summarize(os.Args[2:])
	case "preflight":
		err = preflight(os.Args[2:])
	case "verify-output":
		err = verifyOutput(os.Args[2:])
	case "delete-output":
		err = deleteOutput(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "nntpbench:", err)
		var didNotFinish *benchmark.ClientDidNotFinishError
		if errors.As(err, &didNotFinish) {
			os.Exit(benchmark.ExitStatusClientDidNotFinish)
		}
		os.Exit(1)
	}
}

func seed(args []string) error {
	flags := flag.NewFlagSet("seed", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var config nntp.NyuuSeedConfig
	var nyuuDockerfile, passwordFile string
	var buildNyuuImage bool
	flags.StringVar(&config.FixtureDir, "fixture-dir", "", "generated fixture directory")
	flags.StringVar(&config.RunID, "run-id", "", "unique seed run identifier")
	flags.StringVar(&config.NZBPath, "nzb", "", "NZB output path (defaults inside the fixture directory)")
	flags.StringVar(&config.DockerBinary, "docker", "docker", "Docker executable")
	flags.StringVar(&config.Image, "nyuu-image", "weaver-nntp-bench-nyuu:0.4.2", "Nyuu Docker image")
	flags.StringVar(&config.Platform, "nyuu-platform", "linux/amd64", "Nyuu Docker platform")
	flags.StringVar(&nyuuDockerfile, "nyuu-dockerfile", "docker/nyuu/Dockerfile", "pinned Nyuu image Dockerfile")
	flags.BoolVar(&buildNyuuImage, "build-nyuu-image", true, "build the pinned Nyuu image before posting")
	flags.StringVar(&config.Network, "network", "", "Docker network containing the public NNTP server")
	flags.StringVar(&config.NNTPHost, "nntp-host", "nntp", "NNTP hostname on the Docker network")
	flags.StringVar(&config.NNTPPort, "nntp-port", "119", "plaintext NNTP port used for corpus posting")
	flags.StringVar(&config.Username, "username", "", "NNTP username")
	flags.StringVar(&config.Password, "password", "", "NNTP password")
	flags.StringVar(&passwordFile, "password-file", "", "file containing the NNTP password")
	flags.StringVar(&config.Group, "group", "alt.binaries.test", "newsgroup")
	flags.IntVar(&config.SegmentBytes, "segment-bytes", 750<<10, "raw bytes per yEnc article")
	if err := flags.Parse(args); err != nil {
		return err
	}
	password, err := resolvePassword(config.Password, passwordFile)
	if err != nil {
		return err
	}
	config.Password = password
	if buildNyuuImage {
		if err := nntp.BuildNyuuImage(context.Background(), nntp.NyuuImageConfig{
			DockerBinary: config.DockerBinary,
			Dockerfile:   nyuuDockerfile,
			Image:        config.Image,
			Platform:     config.Platform,
		}); err != nil {
			return err
		}
	}
	result, err := nntp.SeedWithNyuu(context.Background(), config)
	if err != nil {
		return err
	}
	return printJSON(result)
}

// excludeFixtures drops named fixtures from a plan's corpus. An id that is not
// present is refused: a misspelled exclusion would silently keep the fixture
// the operator meant to remove, and the run would fail on it hours later.
func excludeFixtures(fixtureIDs, excluded []string) ([]string, error) {
	if len(excluded) == 0 {
		return fixtureIDs, nil
	}
	drop := make(map[string]bool, len(excluded))
	for _, id := range excluded {
		drop[id] = true
	}
	kept := make([]string, 0, len(fixtureIDs))
	for _, id := range fixtureIDs {
		if drop[id] {
			delete(drop, id)
			continue
		}
		kept = append(kept, id)
	}
	if len(drop) > 0 {
		missing := make([]string, 0, len(drop))
		for id := range drop {
			missing = append(missing, id)
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("--exclude-fixtures names %d fixture(s) the corpus does not contain: %s",
			len(missing), strings.Join(missing, ", "))
	}
	if len(kept) == 0 {
		return nil, fmt.Errorf("--exclude-fixtures removed every fixture from the plan")
	}
	return kept, nil
}

func plan(args []string) error {
	flags := flag.NewFlagSet("plan", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var fixturesCSV, excludeFixturesCSV, corpusPath, clientsCSV, archiveToolchainsCSV, transportsCSV, targetsCSV, output, profile, serverLink string
	var storageProfileID, nfsLink string
	var repetitions int
	var seed int64
	var serverEgressBPS, serverBurstBytes uint64
	var serverRTT time.Duration
	var exclusions clientExclusionFlags
	flags.StringVar(&fixturesCSV, "fixtures", "", "comma-separated generated fixture ids")
	flags.StringVar(&excludeFixturesCSV, "exclude-fixtures", "", "comma-separated fixture ids to drop from the corpus or from --fixtures; every id must be present, so a typo is refused rather than silently keeping the fixture")
	flags.Var(&exclusions, "exclude-client", "repeatable; client:fixture-id:reason — do not run this client on this fixture; the summary records every excluded block as that client not finishing, with the reason")
	flags.StringVar(&corpusPath, "corpus", "fixtures/corpus.json", "declared corpus JSON used when --fixtures is omitted")
	flags.StringVar(&clientsCSV, "clients", "weaver,sabnzbd,nzbget", "comma-separated clients")
	flags.StringVar(&archiveToolchainsCSV, "archive-toolchains", "vanilla", "comma-separated archive toolchains; rarpar remains available only by explicit opt-in")
	flags.StringVar(&transportsCSV, "transports", "plaintext,tls", "comma-separated transports")
	flags.StringVar(&targetsCSV, "targets", "docker-linux,macos-native,windows-native", "comma-separated execution targets: docker-linux, macos-native, windows-native")
	flags.StringVar(&profile, "profile", "", "required client profile: stock or equivalent-throughput; create a separate plan for each")
	flags.StringVar(&serverLink, "server-link", benchmark.LinkUnlimited, "NNTP server aggregate egress profile: unlimited, 1gbit, 10gbit, or custom")
	flags.Uint64Var(&serverEgressBPS, "server-egress-bps", 0, "required custom server-link egress rate in bits per second")
	flags.Uint64Var(&serverBurstBytes, "server-burst-bytes", 0, "required custom server-link aggregate burst in bytes")
	flags.DurationVar(&serverRTT, "server-rtt", 0, "fixed round trip the shaper adds between client and server (whole milliseconds, e.g. 250ms or 500ms); 0 adds none")
	flags.StringVar(&storageProfileID, "storage-profile", benchmark.StorageProfileLocal, "client storage profile: local, nfs-all, or nfs-complete")
	flags.StringVar(&nfsLink, "nfs-link", "", "required NFS link profile for an nfs storage profile: nas-100mbit, nas-1gbit, or nas-2.5gbit")
	flags.IntVar(&repetitions, "repetitions", 20, "measured randomized blocks per fixture/client/transport")
	flags.Int64Var(&seed, "seed", 20260802, "deterministic scheduling seed")
	flags.StringVar(&output, "output", "", "plan JSON output path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if output == "" {
		return fmt.Errorf("--output is required")
	}
	if profile == "" {
		return fmt.Errorf("--profile is required; create separate stock and equivalent-throughput plans")
	}
	fixtureIDs := splitCSV(fixturesCSV)
	if len(fixtureIDs) == 0 {
		corpus, err := fixture.LoadCorpus(corpusPath)
		if err != nil {
			return err
		}
		fixtureIDs = corpus.FixtureIDs
	}
	fixtureIDs, err := excludeFixtures(fixtureIDs, splitCSV(excludeFixturesCSV))
	if err != nil {
		return err
	}
	clients, err := parseClients(clientsCSV)
	if err != nil {
		return err
	}
	archiveToolchains, err := parseArchiveToolchains(archiveToolchainsCSV)
	if err != nil {
		return err
	}
	transports, err := parseTransports(transportsCSV)
	if err != nil {
		return err
	}
	targets, err := parseExecutionTargets(targetsCSV)
	if err != nil {
		return err
	}
	link, err := benchmark.ResolveServerLinkProfile(serverLink, serverEgressBPS, serverBurstBytes, serverRTTMicros(serverRTT))
	if err != nil {
		return err
	}
	storage, err := resolveStoragePlanProfile(storageProfileID, nfsLink)
	if err != nil {
		return err
	}
	benchmarkPlan, err := benchmark.BuildPlan(benchmark.PlanOptions{
		FixtureIDs:        fixtureIDs,
		Clients:           clients,
		ArchiveToolchains: archiveToolchains,
		Transports:        transports,
		Targets:           targets,
		Profile:           profile,
		ServerLink:        link,
		StorageProfile:    storage,
		Repetitions:       repetitions,
		Seed:              seed,
		ClientExclusions:  exclusions,
	})
	if err != nil {
		return err
	}
	if err := benchmark.WritePlan(output, benchmarkPlan); err != nil {
		return err
	}
	return printJSON(benchmarkPlan)
}

func serverEnv(args []string) error {
	flags := flag.NewFlagSet("server-env", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var profile, output string
	var egressBPS, burstBytes uint64
	var rtt time.Duration
	flags.StringVar(&profile, "server-link", benchmark.LinkUnlimited, "server aggregate egress profile: unlimited, 1gbit, 10gbit, or custom")
	flags.Uint64Var(&egressBPS, "server-egress-bps", 0, "required custom egress rate in bits per second")
	flags.Uint64Var(&burstBytes, "server-burst-bytes", 0, "required custom aggregate burst in bytes")
	flags.DurationVar(&rtt, "server-rtt", 0, "fixed round trip the shaper adds between client and server (whole milliseconds, e.g. 250ms or 500ms); 0 adds none")
	flags.StringVar(&output, "output", "", "new Compose-compatible environment file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if output == "" {
		return fmt.Errorf("--output is required")
	}
	link, err := benchmark.ResolveServerLinkProfile(profile, egressBPS, burstBytes, serverRTTMicros(rtt))
	if err != nil {
		return err
	}
	return benchmark.WriteServerLinkEnvironment(output, link)
}

// serverRTTMicros converts the --server-rtt flag for the link resolver, which
// rejects anything that is not zero or a whole millisecond in range. A
// negative duration is folded to an out-of-range value so it is refused too.
func serverRTTMicros(rtt time.Duration) uint64 {
	if rtt < 0 {
		return uint64(benchmark.MaxServerRTT/time.Microsecond) + 1
	}
	return uint64(rtt / time.Microsecond)
}

type preflightBinary struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type preflightResult struct {
	Target      benchmark.TargetDescriptor `json:"target"`
	HostOS      string                     `json:"host_os"`
	HostMatches bool                       `json:"host_matches_target"`
	Binaries    []preflightBinary          `json:"binaries"`
	// RawStack is present only when the host also serves the benchmark, which
	// is what the native lanes do instead of running the Compose topology.
	RawStack []rawstack.Check `json:"raw_stack,omitempty"`
	// Clients holds what a product needs from the host beyond its own
	// executable. A bare install has none of it.
	Clients []preflightClientCheck `json:"clients,omitempty"`
	Ready   bool                   `json:"ready"`
}

// preflightClientCheck is a condition a never-configured product needs before
// it can run a benchmark: a tool it shells out to, or a setting the catalog has
// to carry because the harness will not render it.
type preflightClientCheck struct {
	Client string `json:"client"`
	Name   string `json:"name"`
	Detail string `json:"detail,omitempty"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

func preflight(args []string) error {
	flags := flag.NewFlagSet("preflight", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var targetText, adapterPath, weaverPath, sabPath, nzbgetPath, dockerPath string
	var chainPath, rawBinDir, rawDataDir, rawPasswordFile, rawHost string
	flags.StringVar(&targetText, "target", "", "execution target: docker-linux, macos-native, or windows-native")
	flags.StringVar(&adapterPath, "adapter", "", "path to clientadapter or nativeadapter executable")
	flags.StringVar(&weaverPath, "weaver", "", "native Weaver executable path")
	flags.StringVar(&sabPath, "sabnzbd", "", "native SABnzbd executable path")
	flags.StringVar(&nzbgetPath, "nzbget", "", "native NZBGet executable path")
	flags.StringVar(&dockerPath, "docker", "docker", "Docker executable for docker-linux")
	flags.StringVar(&chainPath, "chain", "", "chain description to take the raw stack from, so a check cannot describe a different stack than the run")
	flags.StringVar(&rawBinDir, "raw-bin-dir", "", "directory holding the staged e2e-nntp and nntpshaper executables")
	flags.StringVar(&rawDataDir, "raw-data-dir", "", "seeded article store for a raw stack")
	flags.StringVar(&rawPasswordFile, "raw-password-file", "", "NNTP password file for a raw stack")
	flags.StringVar(&rawHost, "raw-host", "", "address a raw stack binds (default 127.0.0.1)")
	var adaptersPath string
	flags.StringVar(&adaptersPath, "adapters", "", "adapter catalog to take the client executables from, so a check cannot name a client the run will not launch")
	if err := flags.Parse(args); err != nil {
		return err
	}
	descriptor, err := benchmark.DescribeExecutionTarget(benchmark.ExecutionTarget(targetText))
	if err != nil {
		return err
	}
	expectedHostOS := map[benchmark.ExecutionTarget]string{
		benchmark.DockerLinux:   runtime.GOOS,
		benchmark.MacOSNative:   "darwin",
		benchmark.WindowsNative: "windows",
	}[descriptor.ID]
	result := preflightResult{
		Target:      descriptor,
		HostOS:      runtime.GOOS,
		HostMatches: runtime.GOOS == expectedHostOS,
	}
	switch {
	case adaptersPath != "":
		if adapterPath != "" || weaverPath != "" || sabPath != "" || nzbgetPath != "" {
			return fmt.Errorf("--adapters already declares every executable the run launches; drop the per-client flags rather than naming them twice")
		}
		binaries, clients, err := preflightCatalogBinaries(adaptersPath, descriptor.ID)
		if err != nil {
			return err
		}
		result.Binaries = append(result.Binaries, binaries...)
		result.Clients = clients
		if descriptor.ID == benchmark.DockerLinux {
			result.Binaries = append(result.Binaries, inspectExecutable("docker", dockerPath))
		}
	case descriptor.ID == benchmark.DockerLinux:
		result.Binaries = append(result.Binaries, inspectExecutable("docker", dockerPath))
		if adapterPath != "" {
			result.Binaries = append(result.Binaries, inspectExecutable("clientadapter", adapterPath))
		}
	default:
		if descriptor.ID == benchmark.MacOSNative && sabPath == "" {
			sabPath = "/Applications/SABnzbd.app/Contents/MacOS/SABnzbd"
		}
		result.Binaries = append(result.Binaries,
			inspectExecutable("nativeadapter", adapterPath),
			inspectExecutable("weaver", weaverPath),
			inspectExecutable("sabnzbd", sabPath),
			inspectExecutable("nzbget", nzbgetPath),
		)
	}
	rawConfig, wanted, err := preflightRawStack(chainPath, rawBinDir, rawDataDir, rawPasswordFile, rawHost)
	if err != nil {
		return err
	}
	if wanted {
		if descriptor.ID == benchmark.DockerLinux {
			return fmt.Errorf("a raw stack is not part of the %s target; it is what the native lanes run instead of the Compose topology", benchmark.DockerLinux)
		}
		_, result.RawStack = rawstack.Preflight(rawConfig)
	}
	result.Ready = result.HostMatches
	for _, binary := range result.Binaries {
		if binary.Status != "present" {
			result.Ready = false
		}
	}
	for _, check := range result.RawStack {
		if check.Status != rawstack.CheckOK {
			result.Ready = false
		}
	}
	for _, check := range result.Clients {
		if check.Status != "present" {
			result.Ready = false
		}
	}
	if err := printJSON(result); err != nil {
		return err
	}
	if !result.Ready {
		return fmt.Errorf("preflight is not ready for target %q", descriptor.ID)
	}
	return nil
}

// preflightCatalogBinaries checks the executables the catalog actually
// launches. The products themselves are installed by hand, so all the harness
// needs is where they are -- and the catalog is where it is told. Checking the
// paths retyped on a command line instead would pass for a client the run
// never launches.
func preflightCatalogBinaries(path string, target benchmark.ExecutionTarget) ([]preflightBinary, []preflightClientCheck, error) {
	catalog, err := benchmark.LoadAdapterCatalog(path)
	if err != nil {
		return nil, nil, err
	}
	var matched []benchmark.Adapter
	for _, adapter := range catalog.SortedAdapters() {
		if adapter.Target == target {
			matched = append(matched, adapter)
		}
	}
	if len(matched) == 0 {
		return nil, nil, fmt.Errorf("adapter catalog %s declares no adapter for target %q", path, target)
	}
	binaries := inspectAdapterExecutables(matched)
	if target == benchmark.DockerLinux {
		return binaries, nil, nil
	}
	var clients []preflightClientCheck
	for _, adapter := range matched {
		binaries = append(binaries, inspectNativeClient(adapter))
		clients = append(clients, inspectClientRequirements(adapter)...)
	}
	return binaries, clients, nil
}

// inspectClientRequirements covers what a product needs from the host that
// installing it does not provide. Each of these turns a bare install into a
// run that fails, or worse hangs, well after the measurement has started.
func inspectClientRequirements(adapter benchmark.Adapter) []preflightClientCheck {
	name := string(adapter.Client)
	switch adapter.Client {
	case benchmark.NZBGet:
		// NZBGet ships neither unpacker and shells out to both by name. A
		// host without one does not fail: it skips the unpack and the run
		// fails output verification instead.
		var checks []preflightClientCheck
		for tool, lane := range map[string]string{
			nativeadapter.NZBGetUnrarCommand:    "every RAR fixture",
			nativeadapter.NZBGetSevenZipCommand: "every 7z fixture",
		} {
			check := preflightClientCheck{Client: name, Name: tool, Detail: tool, Status: "present"}
			resolved, err := exec.LookPath(tool)
			if err != nil {
				check.Status = "missing"
				check.Reason = fmt.Sprintf("NZBGet shells out to %q to unpack %s and does not ship it: %v", tool, lane, err)
			} else {
				check.Detail = resolved
			}
			checks = append(checks, check)
		}
		sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
		return checks
	case benchmark.Weaver:
		// Without a key of its own Weaver asks the OS keychain, and a run
		// started from a script waits on a prompt nobody answers.
		check := preflightClientCheck{Client: name, Name: "WEAVER_ENCRYPTION_KEY", Status: "present"}
		if strings.TrimSpace(adapter.Environment["WEAVER_ENCRYPTION_KEY"]) == "" {
			check.Status = "missing"
			check.Reason = "the catalog entry carries no WEAVER_ENCRYPTION_KEY; a native Weaver then waits on a keychain prompt instead of starting"
		}
		return []preflightClientCheck{check}
	default:
		return nil
	}
}

// inspectAdapterExecutables checks the adapter each entry runs. A catalog
// almost always points every client at the one staged launcher, so that case
// is reported once rather than once per client; entries that name different
// launchers are reported apart, because then they really are different files.
func inspectAdapterExecutables(adapters []benchmark.Adapter) []preflightBinary {
	shared := ""
	for index, adapter := range adapters {
		if len(adapter.Command) == 0 {
			shared = ""
			break
		}
		if index == 0 {
			shared = adapter.Command[0]
			continue
		}
		if adapter.Command[0] != shared {
			shared = ""
			break
		}
	}
	if shared != "" {
		return []preflightBinary{inspectExecutable("adapter", shared)}
	}
	binaries := make([]preflightBinary, 0, len(adapters))
	for _, adapter := range adapters {
		name := string(adapter.Client) + " adapter"
		if len(adapter.Command) == 0 {
			binaries = append(binaries, preflightBinary{Name: name, Status: "missing", Reason: "the catalog entry has no command"})
			continue
		}
		binaries = append(binaries, inspectExecutable(name, adapter.Command[0]))
	}
	return binaries
}

// inspectNativeClient resolves the product a native adapter launches. The
// executable is the first element of NATIVE_LAUNCH_COMMAND; the rest of the
// argv is templated per run and cannot be checked ahead of one.
func inspectNativeClient(adapter benchmark.Adapter) preflightBinary {
	name := string(adapter.Client)
	raw := adapter.Environment["NATIVE_LAUNCH_COMMAND"]
	if strings.TrimSpace(raw) == "" {
		return preflightBinary{Name: name, Status: "missing", Reason: "the catalog entry sets no NATIVE_LAUNCH_COMMAND"}
	}
	var argv []string
	if err := json.Unmarshal([]byte(raw), &argv); err != nil {
		return preflightBinary{Name: name, Status: "missing", Reason: fmt.Sprintf("NATIVE_LAUNCH_COMMAND is not a JSON argv array: %v", err)}
	}
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return preflightBinary{Name: name, Status: "missing", Reason: "NATIVE_LAUNCH_COMMAND names no program"}
	}
	return inspectExecutable(name, argv[0])
}

// preflightRawStack settles the stack to check. Taking it from the chain
// description is the form that cannot drift: a check against directories and
// ports retyped on a command line can pass for a stack the session will never
// run. The flags exist for the other case, staging a host before its session
// is written.
func preflightRawStack(chainPath, binDir, dataDir, passwordFile, host string) (rawstack.Config, bool, error) {
	byFlag := binDir != "" || dataDir != "" || passwordFile != "" || host != ""
	if chainPath == "" && !byFlag {
		return rawstack.Config{}, false, nil
	}
	if chainPath != "" && byFlag {
		return rawstack.Config{}, false, fmt.Errorf("--chain already declares the raw stack; drop the --raw-* flags rather than describing it twice")
	}
	if chainPath != "" {
		config, err := loadChainConfig(chainPath)
		if err != nil {
			return rawstack.Config{}, false, err
		}
		if config.Stack != ChainStackRaw {
			return rawstack.Config{}, false, fmt.Errorf("chain %s drives a %s stack, which has no local server side to check", chainPath, config.Stack)
		}
		settings, err := chainRawStackConfig(config)
		if err != nil {
			return rawstack.Config{}, false, err
		}
		return settings, true, nil
	}
	if binDir == "" || dataDir == "" {
		return rawstack.Config{}, false, fmt.Errorf("--raw-bin-dir and --raw-data-dir are both required to check a raw stack")
	}
	// The certificate and log directories are made at startup, so a check does
	// not need them to exist yet -- only to be named.
	return rawstack.Config{
		BinDir:       binDir,
		DataDir:      dataDir,
		CertDir:      filepath.Join(filepath.Dir(binDir), "certs"),
		LogDir:       filepath.Join(filepath.Dir(binDir), "logs"),
		Username:     "fixture-user",
		PasswordFile: passwordFile,
		Host:         host,
	}, true, nil
}

func inspectExecutable(name, path string) preflightBinary {
	result := preflightBinary{Name: name, Path: path}
	if strings.TrimSpace(path) == "" {
		result.Status = "missing"
		result.Reason = "path was not supplied"
		return result
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		result.Status = "missing"
		result.Reason = err.Error()
		return result
	}
	result.Path = resolved
	result.Status = "present"
	return result
}

func verifyOutput(args []string) error {
	flags := flag.NewFlagSet("verify-output", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var fixtureDir, outputDir string
	flags.StringVar(&fixtureDir, "fixture-dir", "", "generated fixture directory")
	flags.StringVar(&outputDir, "output-dir", "", "client completion directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if fixtureDir == "" || outputDir == "" {
		return fmt.Errorf("--fixture-dir and --output-dir are required")
	}
	result, err := benchmark.VerifyOutput(fixtureDir, outputDir)
	if err != nil {
		return err
	}
	return printJSON(result)
}

func run(args []string) error {
	return execute(args, "run")
}

func queue(args []string) error {
	return execute(args, "queue")
}

func sequential(args []string) error {
	return execute(args, "sequential")
}

func queueTransition(args []string) error {
	return execute(args, "queue-transition")
}

func execute(args []string, command string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	queueMode := command != "run"
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var planPath, adaptersPath, fixturesRoot, artifactsRoot, executionTarget, passwordFile string
	var config benchmark.RunConfig
	flags.StringVar(&planPath, "plan", "", "saved benchmark plan JSON")
	flags.StringVar(&adaptersPath, "adapters", "", "adapter catalog JSON")
	flags.StringVar(&fixturesRoot, "fixtures-root", "", "directory containing generated fixture directories")
	flags.StringVar(&artifactsRoot, "artifacts", "", "new benchmark artifact directory")
	flags.StringVar(&executionTarget, "target", "", "execution target from the saved plan: docker-linux, macos-native, or windows-native")
	flags.StringVar(&config.NNTPHost, "nntp-host", "", "NNTP hostname reachable by client adapters")
	flags.StringVar(&config.PlaintextPort, "nntp-port", "119", "plaintext NNTP port")
	flags.StringVar(&config.TLSPort, "nntp-tls-port", "563", "implicit TLS NNTP port")
	flags.StringVar(&config.TLSCAFile, "tls-ca-file", "", "PEM CA file mounted by verified-TLS adapters")
	flags.StringVar(&config.ShaperControlURL, "shaper-control-url", "", "nntpshaper control-plane base URL; required by shaped plans")
	flags.StringVar(&config.DockerBinary, "docker", "docker", "Docker executable used for shaped-storage lifecycle and attestation")
	flags.StringVar(&config.NFSContainer, "nfs-container", "", "shaped NFS server container; required by nfs storage plans")
	flags.StringVar(&config.NFSNetwork, "nfs-network", "", "Docker network carrying the shaped NFS path; required by nfs storage plans")
	flags.StringVar(&config.NFSHelperImage, "nfs-helper-image", benchmark.DefaultNFSImage, "locally built image used to mount, verify and clean the NFS export")
	flags.StringVar(&config.NFSVerifyBinary, "nfs-verify-binary", "", "nntpbench binary, built for the helper container's platform, that verifies and empties the NFS export; required by nfs storage plans")
	flags.StringVar(&config.NNTPUsername, "username", "", "NNTP username")
	flags.StringVar(&config.NNTPPassword, "password", "", "NNTP password")
	flags.StringVar(&passwordFile, "password-file", "", "file containing the NNTP password")
	flags.IntVar(&config.Connections, "connections", 8, "identical NNTP connection limit per client")
	flags.StringVar(&config.Profile, "profile", "", "must match the profile persisted in the plan (defaults to that profile)")
	timeoutDescription := "per-run client timeout"
	if queueMode {
		timeoutDescription = "per-suite client timeout"
	}
	flags.DurationVar(&config.Timeout, "timeout", 45*time.Minute, timeoutDescription)
	if err := flags.Parse(args); err != nil {
		return err
	}
	password, err := resolvePassword(config.NNTPPassword, passwordFile)
	if err != nil {
		return err
	}
	config.NNTPPassword = password
	if planPath == "" || adaptersPath == "" || fixturesRoot == "" || artifactsRoot == "" {
		return fmt.Errorf("--plan, --adapters, --fixtures-root, and --artifacts are required")
	}
	plan, catalog, planContents, adapterContents, err := loadExecutionInputs(planPath, adaptersPath)
	if err != nil {
		return err
	}
	config.Plan = plan
	config.Catalog = catalog
	config.Target = benchmark.ExecutionTarget(executionTarget)
	// Adapters run with the suite's artifact directory as their working
	// directory, so a relative fixtures root would be resolved from there and
	// point at nothing; the operator's path is anchored to this process's cwd.
	if fixturesRoot, err = filepath.Abs(fixturesRoot); err != nil {
		return fmt.Errorf("resolve --fixtures-root: %w", err)
	}
	config.FixtureRoot = fixturesRoot
	config.ArtifactRoot = artifactsRoot
	if config.Profile == "" {
		config.Profile = plan.Profile
	}
	if config.Target == "" && len(plan.ExecutionTargets) == 1 {
		config.Target = plan.ExecutionTargets[0]
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if err := writeExecutionManifest(artifactsRoot, command, planPath, adaptersPath, string(config.Target), config.Profile, args, planContents, adapterContents); err != nil {
		return err
	}
	if queueMode {
		var artifacts []benchmark.QueueArtifact
		var runErr error
		switch command {
		case "queue":
			artifacts, runErr = benchmark.ExecuteQueuePlan(ctx, config)
		case "sequential":
			artifacts, runErr = benchmark.ExecuteSequentialPlan(ctx, config)
		case "queue-transition":
			artifacts, runErr = benchmark.ExecuteQueueTransitionPlan(ctx, config)
		default:
			return fmt.Errorf("unsupported execution command %q", command)
		}
		if err := printJSON(artifacts); err != nil {
			return err
		}
		printStorageAttestations(artifacts)
		return runErr
	}
	artifacts, runErr := benchmark.ExecutePlan(ctx, config)
	if err := printJSON(artifacts); err != nil {
		return err
	}
	printRunStorageAttestations(artifacts)
	return runErr
}

// clientExclusionFlags parses repeated --exclude-client values of the form
// client:fixture-id:reason. Fixture ids never contain a colon; the reason is
// everything after the second one and may.
type clientExclusionFlags []benchmark.ClientExclusion

func (f *clientExclusionFlags) String() string {
	parts := make([]string, 0, len(*f))
	for _, exclusion := range *f {
		parts = append(parts, fmt.Sprintf("%s:%s:%s", exclusion.Client, exclusion.FixtureID, exclusion.Reason))
	}
	return strings.Join(parts, ",")
}

func (f *clientExclusionFlags) Set(value string) error {
	parts := strings.SplitN(value, ":", 3)
	if len(parts) != 3 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" || strings.TrimSpace(parts[2]) == "" {
		return fmt.Errorf("--exclude-client wants client:fixture-id:reason, got %q", value)
	}
	client, err := parseSingleClient(parts[0])
	if err != nil {
		return fmt.Errorf("--exclude-client %q: %w", value, err)
	}
	*f = append(*f, benchmark.ClientExclusion{Client: client, FixtureID: strings.TrimSpace(parts[1]), Reason: strings.TrimSpace(parts[2])})
	return nil
}

func splitCSV(value string) []string {

	var values []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			values = append(values, part)
		}
	}
	return values
}

func parseClients(value string) ([]benchmark.Client, error) {
	parts := splitCSV(value)
	clients := make([]benchmark.Client, len(parts))
	for index, part := range parts {
		clients[index] = benchmark.Client(part)
	}
	return clients, nil
}

func parseArchiveToolchains(value string) ([]benchmark.ArchiveToolchain, error) {
	parts := splitCSV(value)
	toolchains := make([]benchmark.ArchiveToolchain, len(parts))
	for index, part := range parts {
		toolchains[index] = benchmark.ArchiveToolchain(part)
	}
	return toolchains, nil
}

func parseTransports(value string) ([]benchmark.Transport, error) {
	parts := splitCSV(value)
	transports := make([]benchmark.Transport, len(parts))
	for index, part := range parts {
		transports[index] = benchmark.Transport(part)
	}
	return transports, nil
}

func parseExecutionTargets(value string) ([]benchmark.ExecutionTarget, error) {
	parts := splitCSV(value)
	targets := make([]benchmark.ExecutionTarget, len(parts))
	for index, part := range parts {
		target := benchmark.ExecutionTarget(part)
		if _, err := benchmark.DescribeExecutionTarget(target); err != nil {
			return nil, err
		}
		targets[index] = target
	}
	return targets, nil
}

func printJSON(value any) error {
	contents, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(contents))
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: nntpbench <command> [options]

Commands:
  seed           Post a generated fixture to an NNTP server and write its NZB
  seed-image     Cache, inspect, or restore a pre-seeded NNTP article store
  image build    Build the pinned local e2e-nntp image and save its provenance
  plan           Write a randomized, balanced benchmark plan
  server-env     Write an immutable server-side egress-shaper environment file
  storage-env    Write an immutable shaped-NFS server environment file
  run            Execute cold, one-NZB diagnostic runs through client adapters
  sequential     Run each persisted plan entry through a fresh isolated client
  queue           Execute each client lane as one uninterrupted multi-NZB queue (legacy)
  queue-transition Queue twenty forced duplicates of one direct fixture and report drain time
  pin            Pin a client image by digest in the adapter catalog and pre-pull it
  chain          Drive a whole declared session: shaper, phases and summaries
  summarize      Produce paired per-stratum statistics from verified sequential artifacts
  preflight      Check a host: target, client executables, and a raw stack
  verify-output  Verify a client completion directory against fixture hashes
  delete-output  Empty a verified client completion directory

Run "nntpbench <command> -h" for command-specific options.
`)
}
