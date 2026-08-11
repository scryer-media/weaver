package clientadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

func TestWeaverQueueUsesServiceAndPreservesControllerOwnership(t *testing.T) {
	t.Setenv("WEAVER_NNTP_TLS_BACKEND", "s2n")
	t.Setenv("RUST_LOG", "weaver_nntp=debug")
	cfg := testConfig(t, benchmark.Weaver, benchmark.Plaintext, benchmark.TLSNotApplicable)
	cfg.QueueInput = &benchmark.QueueInput{
		SchemaVersion: 1,
		SuiteID:       "queue-0001",
		Jobs: []benchmark.QueueInputJob{{
			RunID:     cfg.RunID,
			FixtureID: "fixture",
			NZBPath:   cfg.NZBPath,
		}},
	}
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !spec.ExposeAPI || spec.APIPort != 9090 || spec.NeedsNZBMount {
		t.Fatalf("queue Weaver spec = %#v", spec)
	}
	if got, want := strings.Join(spec.Command, " "), "--config /config serve --port 9090"; got != want {
		t.Fatalf("queue Weaver command = %q, want %q", got, want)
	}
	environment := strings.Join(spec.Environment, "\n")
	for _, key := range []string{"PUID=", "PGID="} {
		if !strings.Contains(environment, key) {
			t.Fatalf("queue Weaver environment lacks %s: %s", key, environment)
		}
	}
	if !strings.Contains(environment, "WEAVER_NNTP_TLS_BACKEND=s2n") {
		t.Fatalf("queue Weaver environment lacks TLS backend override: %s", environment)
	}
	if !strings.Contains(environment, "RUST_LOG=weaver_nntp=debug") {
		t.Fatalf("queue Weaver environment lacks diagnostic log filter: %s", environment)
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

func TestStockNZBGetKeepsItsBuiltInRepairPathEnabled(t *testing.T) {
	cfg := testConfig(t, benchmark.NZBGet, benchmark.Plaintext, benchmark.TLSNotApplicable)
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	content := string(spec.ConfigContent)
	for _, expected := range []string{"ParCheck=auto", "ParRepair=yes", "UnrarCmd=unrar"} {
		if !strings.Contains(content, expected) {
			t.Fatalf("stock NZBGet config lacks %q:\n%s", expected, content)
		}
	}
	if strings.Contains(content, "DaemonMode=") {
		t.Fatalf("current NZBGet image rejects legacy DaemonMode:\n%s", content)
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

func TestRarparVariantsRenderAnExplicitReplacementPath(t *testing.T) {
	for _, client := range []benchmark.Client{benchmark.SABnzbd, benchmark.NZBGet} {
		t.Run(string(client), func(t *testing.T) {
			cfg := testConfig(t, client, benchmark.Plaintext, benchmark.TLSNotApplicable)
			configureTestRarpar(t, &cfg)
			spec, err := cfg.RenderProductConfig()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(spec.Rendered), "archive_toolchain=rarpar") {
				t.Fatalf("rendered configuration does not record Rarpar:\n%s", spec.Rendered)
			}
			if client == benchmark.SABnzbd {
				if !strings.Contains(strings.Join(spec.Environment, "\n"), "PATH=/config/toolchain:/lsiopy/bin:") {
					t.Fatalf("SAB Rarpar lane does not prepend the staged toolchain to PATH: %#v", spec.Environment)
				}
				return
			}
			content := string(spec.ConfigContent)
			for _, expected := range []string{"Unpack=yes", "ParRepair=yes", "UnrarCmd=/config/toolchain/unrar", "Extensions="} {
				if !strings.Contains(content, expected) {
					t.Fatalf("NZBGet Rarpar config lacks %q:\n%s", expected, content)
				}
			}
			if strings.Contains(content, "rarpar-post") || len(spec.ExtraFiles) != 0 {
				t.Fatalf("NZBGet Rarpar lane must use its native pipeline, not a post-process script: %#v", spec)
			}
			if !strings.Contains(string(spec.Rendered), "UnRAR only; NZBGet built-in PAR2") {
				t.Fatalf("NZBGet Rarpar provenance must disclose its built-in PAR2 engine:\n%s", spec.Rendered)
			}
		})
	}
}

func TestNZBGetQueueParametersOnlyProvideTheArchivePassword(t *testing.T) {
	parameters := nzbgetPPParameters("fixture-password")
	if len(parameters) != 1 {
		t.Fatalf("parameters = %#v, want only the archive password", parameters)
	}
	if len(parameters[0]) != 1 || parameters[0]["*Unpack:Password"] != "fixture-password" {
		t.Fatalf("password parameter = %#v", parameters[0])
	}
	appendParameters := nzbgetAppendParameters("fixture.nzb", []byte("fixture"), parameters, "SCORE")
	if len(appendParameters) != 11 {
		t.Fatalf("append parameters = %#v, want 11 arguments", appendParameters)
	}
	if autoCategory, ok := appendParameters[9].(bool); !ok || autoCategory {
		t.Fatalf("AutoCategory argument = %#v, want false", appendParameters[9])
	}
	if got, ok := appendParameters[10].([]nzbgetPPParameter); !ok || len(got) != 1 || got[0]["*Unpack:Password"] != "fixture-password" {
		t.Fatalf("PPParameters argument = %#v, want %#v", appendParameters[10], parameters)
	}
	wire, err := json.Marshal(appendParameters)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wire), `{"*Unpack:Password":"fixture-password"}`) || strings.Contains(string(wire), `rarpar-post`) {
		t.Fatalf("append wire parameters must contain only the archive password: %s", wire)
	}
}

func TestRarparToolchainStagesOnlyVerifiedPublishedBinary(t *testing.T) {
	cfg := testConfig(t, benchmark.SABnzbd, benchmark.Plaintext, benchmark.TLSNotApplicable)
	configureTestRarpar(t, &cfg)
	if err := prepareRarparToolchain(cfg); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"rarpar", "unrar", "par2", "archive-passwords"} {
		if _, err := os.Stat(filepath.Join(cfg.ConfigDir, "toolchain", name)); err != nil {
			t.Fatalf("staged Rarpar toolchain is missing %s: %v", name, err)
		}
	}
	if got := cfg.archiveToolchainIdentity(); strings.Contains(got, cfg.RarparBinary) || !strings.Contains(got, "sha256:") {
		t.Fatalf("Rarpar identity leaks a path or omits a digest: %q", got)
	}
	staged, err := os.ReadFile(filepath.Join(cfg.ConfigDir, "toolchain", "rarpar"))
	if err != nil {
		t.Fatal(err)
	}
	actual := sha256.Sum256(staged)
	if got := hex.EncodeToString(actual[:]); got != cfg.RarparSHA256 {
		t.Fatalf("staged Rarpar hash = %s, want %s", got, cfg.RarparSHA256)
	}
}

func TestRarparUnrarShimExecutesOnlyTheStagedTool(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Rarpar shims are exercised only by Docker/Linux lanes")
	}
	cfg := testConfig(t, benchmark.SABnzbd, benchmark.Plaintext, benchmark.TLSNotApplicable)
	configureTestRarpar(t, &cfg)
	if err := prepareRarparToolchain(cfg); err != nil {
		t.Fatal(err)
	}
	shim := filepath.Join(cfg.ConfigDir, "toolchain", "unrar")
	output, err := exec.Command(shim, "x", "archive.rar").CombinedOutput()
	if err != nil {
		t.Fatalf("run unrar shim: %v: %s", err, output)
	}
	if got := string(output); !strings.Contains(got, "rarpar 0.2.5") {
		t.Fatalf("unrar shim did not execute staged Rarpar: %q", got)
	}
}

func TestRarparPAR2EntryPointIsVerifiedBinaryAndPreservesSABArguments(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Rarpar entry points are exercised only by Docker/Linux lanes")
	}
	cfg := testConfig(t, benchmark.SABnzbd, benchmark.Plaintext, benchmark.TLSNotApplicable)
	binary := filepath.Join(filepath.Dir(cfg.NZBPath), "rarpar")
	contents := []byte("#!/bin/sh\nprintf 'rarpar args:'\nprintf ' <%s>' \"$@\"\nprintf '\\n'\n")
	if err := os.WriteFile(binary, contents, 0o755); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	cfg.ArchiveToolchain = benchmark.RarparArchiveToolchain
	cfg.RarparBinary = binary
	cfg.RarparVersion = "0.2.5"
	cfg.RarparSHA256 = hex.EncodeToString(digest[:])
	if err := prepareRarparToolchain(cfg); err != nil {
		t.Fatal(err)
	}
	entryPoint := filepath.Join(cfg.ConfigDir, "toolchain", "par2")
	staged, err := os.ReadFile(entryPoint)
	if err != nil {
		t.Fatal(err)
	}
	if actual := sha256.Sum256(staged); actual != digest {
		t.Fatalf("SAB PAR2 entry point is not the verified Rarpar binary: got %x, want %x", actual, digest)
	}
	output, err := exec.Command(entryPoint, "r", "-B", "/downloads/incomplete/job", "repair.par2", "files/*.rar").CombinedOutput()
	if err != nil {
		t.Fatalf("run staged Rarpar PAR2 entry point: %v: %s", err, output)
	}
	if got, want := string(output), "rarpar args: <r> <-B> </downloads/incomplete/job> <repair.par2> <files/*.rar>\n"; got != want {
		t.Fatalf("SAB arguments changed before reaching Rarpar: got %q, want %q", got, want)
	}
}

func TestRarparConfigPathsKeepNZBGetBuiltInPAR2Explicit(t *testing.T) {
	cfg := testConfig(t, benchmark.NZBGet, benchmark.Plaintext, benchmark.TLSNotApplicable)
	configureTestRarpar(t, &cfg)
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	content := string(spec.ConfigContent)
	if !strings.Contains(content, "UnrarCmd=/config/toolchain/unrar") || strings.Contains(content, "/config/toolchain/par2") {
		t.Fatalf("NZBGet must replace only UnRAR, retaining built-in PAR2:\n%s", content)
	}
	if !strings.Contains(string(spec.Rendered), "UnRAR only; NZBGet built-in PAR2") {
		t.Fatalf("rendered Rarpar provenance must identify NZBGet's built-in PAR2:\n%s", spec.Rendered)
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
		RunID:            "run-0001",
		Client:           client,
		ArchiveToolchain: benchmark.VanillaArchiveToolchain,
		ExecutionTarget:  benchmark.DockerLinux,
		Transport:        transport,
		TransportLabel:   label,
		TLSValidation:    validation,
		ServerLink:       benchmark.DefaultServerLinkProfile(),
		FixtureDir:       directory,
		NZBPath:          nzbPath,
		OutputDir:        filepath.Join(directory, "complete"),
		ConfigDir:        filepath.Join(directory, "config"),
		ResultPath:       filepath.Join(directory, "adapter-result.json"),
		NNTPHost:         "nntp",
		NNTPPort:         "563",
		NNTPUsername:     "user",
		NNTPPassword:     "password",
		NNTPUseTLS:       useTLS,
		NNTPCAFile:       caPath,
		Connections:      8,
		Profile:          benchmark.ProfileStock,
		Image:            "example.test/client@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Network:          "nntp-bench",
		DockerBinary:     "docker",
		PerfBinary:       "perf",
		StartupTimeout:   1,
		PollInterval:     1,
	}
}

func configureTestRarpar(t *testing.T, cfg *Config) {
	t.Helper()
	binary := filepath.Join(filepath.Dir(cfg.NZBPath), "rarpar")
	contents := []byte("#!/bin/sh\necho 'rarpar 0.2.5'\n")
	if err := os.WriteFile(binary, contents, 0o755); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	cfg.ArchiveToolchain = benchmark.RarparArchiveToolchain
	cfg.RarparBinary = binary
	cfg.RarparVersion = "0.2.5"
	cfg.RarparSHA256 = hex.EncodeToString(digest[:])
}
