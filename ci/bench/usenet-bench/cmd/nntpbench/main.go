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
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntp"
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

func plan(args []string) error {
	flags := flag.NewFlagSet("plan", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var fixturesCSV, corpusPath, clientsCSV, archiveToolchainsCSV, transportsCSV, targetsCSV, output, profile, serverLink string
	var storageProfileID, nfsLink string
	var repetitions int
	var seed int64
	var serverEgressBPS, serverBurstBytes uint64
	var exclusions clientExclusionFlags
	flags.StringVar(&fixturesCSV, "fixtures", "", "comma-separated generated fixture ids")
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
	link, err := benchmark.ResolveServerLinkProfile(serverLink, serverEgressBPS, serverBurstBytes)
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
	flags.StringVar(&profile, "server-link", benchmark.LinkUnlimited, "server aggregate egress profile: unlimited, 1gbit, 10gbit, or custom")
	flags.Uint64Var(&egressBPS, "server-egress-bps", 0, "required custom egress rate in bits per second")
	flags.Uint64Var(&burstBytes, "server-burst-bytes", 0, "required custom aggregate burst in bytes")
	flags.StringVar(&output, "output", "", "new Compose-compatible environment file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if output == "" {
		return fmt.Errorf("--output is required")
	}
	link, err := benchmark.ResolveServerLinkProfile(profile, egressBPS, burstBytes)
	if err != nil {
		return err
	}
	return benchmark.WriteServerLinkEnvironment(output, link)
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
	Ready       bool                       `json:"ready"`
}

func preflight(args []string) error {
	flags := flag.NewFlagSet("preflight", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var targetText, adapterPath, weaverPath, sabPath, nzbgetPath, dockerPath string
	flags.StringVar(&targetText, "target", "", "execution target: docker-linux, macos-native, or windows-native")
	flags.StringVar(&adapterPath, "adapter", "", "path to clientadapter or nativeadapter executable")
	flags.StringVar(&weaverPath, "weaver", "", "native Weaver executable path")
	flags.StringVar(&sabPath, "sabnzbd", "", "native SABnzbd executable path")
	flags.StringVar(&nzbgetPath, "nzbget", "", "native NZBGet executable path")
	flags.StringVar(&dockerPath, "docker", "docker", "Docker executable for docker-linux")
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
	if descriptor.ID == benchmark.DockerLinux {
		result.Binaries = append(result.Binaries, inspectExecutable("docker", dockerPath))
	} else {
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
	if adapterPath != "" && descriptor.ID == benchmark.DockerLinux {
		result.Binaries = append(result.Binaries, inspectExecutable("clientadapter", adapterPath))
	}
	result.Ready = result.HostMatches
	for _, binary := range result.Binaries {
		if binary.Status != "present" {
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
  summarize      Produce paired per-stratum statistics from verified sequential artifacts
  preflight      Check target host and native/Docker executable prerequisites
  verify-output  Verify a client completion directory against fixture hashes
  delete-output  Empty a verified client completion directory

Run "nntpbench <command> -h" for command-specific options.
`)
}
