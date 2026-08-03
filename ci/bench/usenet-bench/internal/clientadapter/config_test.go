package clientadapter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func TestSABnzbdTLSAlwaysDisablesValidationAndIsMarkedUnverified(t *testing.T) {
	cfg := testConfig(t, benchmark.SABnzbd, benchmark.TLS, benchmark.TLSDisabled)
	cfg.TransportLabel = "tls-unverified"
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	content := string(spec.ConfigContent)
	if !strings.Contains(content, "ssl = 1\nssl_verify = 0") {
		t.Fatalf("SAB TLS config must be explicit ssl=1 ssl_verify=0:\n%s", content)
	}
	if spec.NeedsCAMount {
		t.Fatal("SAB unverified TLS must not claim a CA mount")
	}
	if !strings.Contains(string(spec.Rendered), "tls_validation=disabled") {
		t.Fatal("rendered config must preserve disabled SAB certificate validation")
	}
}

func TestWeaverUsesOneShotCLIWithTelemetryAcknowledgement(t *testing.T) {
	cfg := testConfig(t, benchmark.Weaver, benchmark.Plaintext, benchmark.TLSNotApplicable)
	cfg.ArchivePassword = "fixture-password"
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	if spec.ExposeAPI {
		t.Fatal("Weaver benchmark runs must not start the HTTP service")
	}
	if !spec.NeedsNZBMount {
		t.Fatal("Weaver CLI must receive the NZB through a read-only input mount")
	}
	if spec.CompletionReportName != weaverCLIReport || spec.CompletionAckName != weaverCLIReportAck {
		t.Fatalf("unexpected Weaver CLI report handshake: report=%q ack=%q", spec.CompletionReportName, spec.CompletionAckName)
	}
	command := strings.Join(spec.Command, "\n")
	for _, expected := range []string{
		"download\n/benchmark-input/fixture.nzb",
		"--report\n/config/" + weaverCLIReport,
		"--report-ack\n/config/" + weaverCLIReportAck,
		"--password\nfixture-password",
	} {
		if !strings.Contains(command, expected) {
			t.Fatalf("Weaver command lacks %q:\n%s", expected, command)
		}
	}
}

func TestVerifiedTLSClientMountsCAAndUsesStrictVerification(t *testing.T) {
	cfg := testConfig(t, benchmark.NZBGet, benchmark.TLS, benchmark.TLSCAVerified)
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	content := string(spec.ConfigContent)
	if !spec.NeedsCAMount {
		t.Fatal("verified TLS client must mount the generated CA")
	}
	for _, expected := range []string{
		"Server1.Encryption=yes",
		"Server1.CertVerification=strict",
		"CertStore=/benchmark-ca/nntp-ca.pem",
		"CertCheck=yes",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("NZBGet config lacks %q:\n%s", expected, content)
		}
	}
}

func TestEquivalentThroughputProfileOnlyChangesDeclaredDirectUnpack(t *testing.T) {
	cfg := testConfig(t, benchmark.SABnzbd, benchmark.Plaintext, benchmark.TLSNotApplicable)
	stock, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profile = benchmark.ProfileEquivalentThroughput
	equivalent, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stock.ConfigContent), "direct_unpack = 0") {
		t.Fatal("stock profile should leave SAB direct unpack disabled")
	}
	if !strings.Contains(string(equivalent.ConfigContent), "direct_unpack = 1") {
		t.Fatal("equivalent-throughput profile should declare SAB direct unpack")
	}
	if stock.ConfigSHA256 == equivalent.ConfigSHA256 {
		t.Fatal("rendered config hash must change when profile changes")
	}
}

func TestNZBGetRunCreatesRequiredWorkingDirectories(t *testing.T) {
	cfg := testConfig(t, benchmark.NZBGet, benchmark.Plaintext, benchmark.TLSNotApplicable)
	spec, err := cfg.RenderProductConfig()
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

func TestConfigRejectsUnpinnedImage(t *testing.T) {
	cfg := testConfig(t, benchmark.Weaver, benchmark.Plaintext, benchmark.TLSNotApplicable)
	cfg.Image = "ghcr.io/scryer-media/weaver:latest"
	if err := cfg.Validate(); err == nil {
		t.Fatal("floating image reference should be rejected")
	}
}

func TestParseTelemetryCounters(t *testing.T) {
	value, err := parseCgroupV2CPU("usage_usec 123\nuser_usec 80\nsystem_usec 43\n")
	if err != nil || value != 123 {
		t.Fatalf("parse cgroup v2 = %d, %v", value, err)
	}
	value, err = parseCgroupV1CPU("987654\n")
	if err != nil || value != 987654 {
		t.Fatalf("parse cgroup v1 = %d, %v", value, err)
	}
	instructions, err := parsePerfInstructions("123456; ;instructions;100.00;\n")
	if err != nil || instructions != 123456 {
		t.Fatalf("parse perf instructions = %d, %v", instructions, err)
	}
	if _, err := parsePerfInstructions("<not counted>; ;instructions;0;\n"); err == nil {
		t.Fatal("unavailable perf counter must not look like zero")
	}
}

func TestPublishedEndpointParsing(t *testing.T) {
	endpoint, err := endpointFromDockerPort("0.0.0.0:49153\n", "test")
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://127.0.0.1:49153" {
		t.Fatalf("endpoint = %q", endpoint)
	}
}

func testConfig(t *testing.T, client benchmark.Client, transport benchmark.Transport, validation benchmark.TLSValidation) Config {
	t.Helper()
	directory := t.TempDir()
	nzbPath := filepath.Join(directory, "fixture.nzb")
	if err := os.WriteFile(nzbPath, []byte("<nzb/>"), 0o600); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(caPath, []byte("test CA"), 0o600); err != nil {
		t.Fatal(err)
	}
	label := string(benchmark.Plaintext)
	useTLS := transport == benchmark.TLS
	if useTLS {
		label = "tls-ca-verified"
		if validation == benchmark.TLSDisabled {
			label = "tls-unverified"
		}
	}
	return Config{
		RunID:           "run-0001",
		Client:          client,
		ExecutionTarget: benchmark.DockerLinux,
		Transport:       transport,
		TransportLabel:  label,
		TLSValidation:   validation,
		ServerLink:      benchmark.DefaultServerLinkProfile(),
		FixtureDir:      directory,
		NZBPath:         nzbPath,
		OutputDir:       filepath.Join(directory, "complete"),
		ConfigDir:       filepath.Join(directory, "config"),
		ResultPath:      filepath.Join(directory, "adapter-result.json"),
		NNTPHost:        "nntp",
		NNTPPort:        "563",
		NNTPUsername:    "user",
		NNTPPassword:    "password",
		NNTPUseTLS:      useTLS,
		NNTPCAFile:      caPath,
		Connections:     8,
		Profile:         benchmark.ProfileStock,
		Image:           "example.test/client@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Network:         "nntp-bench",
		DockerBinary:    "docker",
		PerfBinary:      "perf",
		StartupTimeout:  1,
		PollInterval:    1,
	}
}
