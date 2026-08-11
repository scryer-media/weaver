package nativeadapter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func TestWeaverRenderUsesServiceLaunchAndStockExtractionDefault(t *testing.T) {
	cfg := testConfig(benchmark.Weaver)
	cfg.LaunchCommand = []string{"weaver", "--config", "{{config_dir}}", "serve", "--port", "{{api_port}}"}
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	command := strings.Join(spec.Command, "\n")
	for _, expected := range []string{
		"serve\n--port\n18080",
		"--config\n" + cfg.ConfigDir,
	} {
		if !strings.Contains(command, expected) {
			t.Fatalf("native Weaver command lacks %q:\n%s", expected, command)
		}
	}
	if strings.Contains(string(spec.Content), "WEAVER_MAX_CONCURRENT_EXTRACTIONS=") {
		t.Fatalf("native stock configuration must not override Weaver extraction concurrency:\n%s", spec.Content)
	}
}

func TestNativeSABAndNZBGetRenderEquivalentThroughputSettings(t *testing.T) {
	for _, client := range []benchmark.Client{benchmark.SABnzbd, benchmark.NZBGet} {
		t.Run(string(client), func(t *testing.T) {
			cfg := testConfig(client)
			cfg.Profile = benchmark.ProfileEquivalentThroughput
			spec, err := renderProduct(cfg)
			if err != nil {
				t.Fatal(err)
			}
			content := string(spec.Content)
			if client == benchmark.SABnzbd && !strings.Contains(content, "direct_unpack = 1") {
				t.Fatalf("SAB config lacks direct unpack:\n%s", content)
			}
			if client == benchmark.NZBGet && (!strings.Contains(content, "DirectWrite=yes") || !strings.Contains(content, "DirectUnpack=yes")) {
				t.Fatalf("NZBGet config lacks direct settings:\n%s", content)
			}
		})
	}
}

func TestNativeNZBGetVerifiedTLSEnablesCAValidation(t *testing.T) {
	cfg := testConfig(benchmark.NZBGet)
	cfg.Transport = benchmark.TLS
	cfg.NNTPUseTLS = true
	cfg.TLSValidation = benchmark.TLSCAVerified
	cfg.TransportLabel = "tls-ca-verified"
	cfg.NNTPCAFile = "/tmp/nntpbench-nativeadapter-test/ca.pem"
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	content := string(spec.Content)
	for _, expected := range []string{
		"Server1.Encryption=yes",
		"Server1.CertVerification=strict",
		"CertStore=" + cfg.NNTPCAFile,
		"CertCheck=yes",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("NZBGet config lacks %q:\n%s", expected, content)
		}
	}
}

func TestNativeNZBGetUsesTheDockerRepairDefaultsWithoutCacheOverride(t *testing.T) {
	cfg := testConfig(benchmark.NZBGet)
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	content := string(spec.Content)
	for _, expected := range []string{"ParCheck=auto", "ParRepair=yes", "Unpack=yes"} {
		if !strings.Contains(content, expected) {
			t.Fatalf("NZBGet config lacks %q:\n%s", expected, content)
		}
	}
	if strings.Contains(content, "ArticleCache=") {
		t.Fatalf("native stock config must not override ArticleCache:\n%s", content)
	}
}

func TestNativeSABLeavesAutoDisconnectAtItsStockDefault(t *testing.T) {
	cfg := testConfig(benchmark.SABnzbd)
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(spec.Content), "auto_disconnect =") {
		t.Fatalf("native stock config must not override auto_disconnect:\n%s", spec.Content)
	}
}

func TestNativeNZBGetRunCreatesRequiredWorkingDirectories(t *testing.T) {
	root := t.TempDir()
	cfg := testConfig(benchmark.NZBGet)
	cfg.FixtureDir = root
	cfg.ConfigDir = filepath.Join(root, "config")
	cfg.OutputDir = filepath.Join(root, "complete")
	cfg.ResultPath = filepath.Join(root, "adapter-result.json")
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeProductFiles(cfg, spec); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nzb", "queue", "tmp", "scripts"} {
		path := filepath.Join(cfg.ConfigDir, name)
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("required NZBGet directory %s was not created: %v", path, err)
		}
	}
}

func TestNativeAPIAddressOnlyAllowsLocalExplicitHTTPPorts(t *testing.T) {
	if _, port, err := nativeAPIAddress("http://127.0.0.1:18080"); err != nil || port != 18080 {
		t.Fatalf("valid local endpoint = port %d, err %v", port, err)
	}
	for _, endpoint := range []string{"https://127.0.0.1:18080", "http://example.test:18080", "http://127.0.0.1"} {
		if _, _, err := nativeAPIAddress(endpoint); err == nil {
			t.Fatalf("endpoint %q should be rejected", endpoint)
		}
	}
}

func TestNativeAPIClientVersionMustMatchTheDeclaredCatalogVersion(t *testing.T) {
	if err := reconcileAPIVersion("5.0.4", "5.0.4"); err != nil {
		t.Fatalf("matching API version rejected: %v", err)
	}
	if err := reconcileAPIVersion("5.0.4", "5.0.5"); err == nil {
		t.Fatal("mismatched API version should be rejected")
	}
	if got := recordedAPIVersion("declared", "actual"); got != "actual" {
		t.Fatalf("recorded API version = %q, want actual", got)
	}
}

func TestNativeSequentialQueueResultHasCompleteHonestTiming(t *testing.T) {
	started := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	accepted := started.Add(10 * time.Millisecond)
	lowerBound := accepted.Add(1900 * time.Millisecond)
	observed := lowerBound.Add(10 * time.Millisecond)
	metrics := benchmark.ResourceMetrics{
		CPUTimeNanoseconds:  benchmark.MeasuredMeasurement("client_process", "go-os-process-state", "darwin", 123),
		InstructionsRetired: benchmark.UnavailableMeasurement("client_process", "native-instructions", "darwin", "not available"),
	}
	result := benchmark.QueueAdapterResult{
		SchemaVersion:            5,
		SuiteID:                  "sequential-0001",
		SubmissionMode:           benchmark.SubmissionModeSequential,
		Client:                   benchmark.Weaver,
		ArchiveToolchain:         benchmark.VanillaArchiveToolchain,
		ArchiveToolchainIdentity: "stock",
		ExecutionTarget:          benchmark.MacOSNative,
		Transport:                benchmark.Plaintext,
		TLSValidation:            benchmark.TLSNotApplicable,
		TransportLabel:           "plaintext",
		ServerLink:               benchmark.DefaultServerLinkProfile(),
		QueueStartedAt:           started,
		QueueCompletedAt:         observed,
		StatusPollIntervalNanos:  int64(10 * time.Millisecond),
		ClientIdentity:           "sha256:test",
		ClientVersion:            "test",
		RenderedConfigSHA256:     strings.Repeat("a", 64),
		ResourceMetrics:          metrics,
		Jobs: []benchmark.QueueJobResult{{
			RunID:                           "run-0001",
			JobID:                           "native-run-0001",
			SubmissionStartedAt:             started,
			AcceptedAt:                      accepted,
			QueuedAt:                        accepted,
			FixtureWallClockNanoseconds:     observed.Sub(accepted).Nanoseconds(),
			TerminalStatus:                  "succeeded",
			ProcessingTimingError:           "native public API does not expose active-processing transitions",
			CompletionAt:                    observed,
			TerminalObservationLowerBound:   lowerBound,
			TerminalObservedAt:              observed,
			TerminalObservationUncertainty:  observed.Sub(lowerBound).Nanoseconds(),
			SubmissionToTerminalNanoseconds: observed.Sub(started).Nanoseconds(),
			ResourceMetrics:                 &metrics,
		}},
	}
	if err := validateNativeSequentialQueueResult(result); err != nil {
		t.Fatalf("complete native queue result rejected: %v", err)
	}
}

func TestNativeSequentialQueueRejectsMultipleJobs(t *testing.T) {
	cfg := testConfig(benchmark.Weaver)
	cfg.StartupTimeout = time.Second
	cfg.PollInterval = time.Millisecond
	cfg.QueueInput = &benchmark.QueueInput{
		SchemaVersion:  3,
		SuiteID:        "sequential-0001",
		SubmissionMode: benchmark.SubmissionModeSequential,
		Jobs: []benchmark.QueueInputJob{
			{RunID: "run-0001", FixtureID: "fixture-1", NZBPath: cfg.NZBPath},
			{RunID: "run-0002", FixtureID: "fixture-2", NZBPath: cfg.NZBPath},
		},
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "exactly one job") {
		t.Fatalf("multiple native sequential jobs should be rejected, got %v", err)
	}
}

func testConfig(client benchmark.Client) Config {
	root := filepath.Join("/tmp", "nntpbench-nativeadapter-test")
	return Config{
		RunID:            "run-0001",
		Client:           client,
		ArchiveToolchain: benchmark.VanillaArchiveToolchain,
		ExecutionTarget:  benchmark.MacOSNative,
		Transport:        benchmark.Plaintext,
		TransportLabel:   "plaintext",
		TLSValidation:    benchmark.TLSNotApplicable,
		ServerLink:       benchmark.DefaultServerLinkProfile(),
		FixtureDir:       root,
		NZBPath:          filepath.Join(root, "fixture.nzb"),
		OutputDir:        filepath.Join(root, "complete"),
		ConfigDir:        filepath.Join(root, "config"),
		ResultPath:       filepath.Join(root, "adapter-result.json"),
		NNTPHost:         "nntp",
		NNTPPort:         "119",
		NNTPUsername:     "user",
		NNTPPassword:     "password",
		Connections:      8,
		Profile:          benchmark.ProfileStock,
		LaunchCommand:    []string{"client", "--config", "{{config_dir}}"},
		APIEndpoint:      "http://127.0.0.1:18080",
		ClientVersion:    "test",
	}
}
