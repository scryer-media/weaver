package nativeadapter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func TestWeaverRenderUsesOneShotReportHandshake(t *testing.T) {
	cfg := testConfig(benchmark.Weaver)
	cfg.ArchivePassword = "fixture-password"
	cfg.LaunchCommand = []string{"weaver", "--config", "{{config_dir}}", "download", "{{nzb_path}}"}
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	command := strings.Join(spec.Command, "\n")
	for _, expected := range []string{
		"download\n" + cfg.NZBPath,
		"--report\n" + filepath.Join(cfg.ConfigDir, reportName),
		"--report-ack\n" + filepath.Join(cfg.ConfigDir, reportAckName),
		"--password\nfixture-password",
	} {
		if !strings.Contains(command, expected) {
			t.Fatalf("native Weaver command lacks %q:\n%s", expected, command)
		}
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

func testConfig(client benchmark.Client) Config {
	root := filepath.Join("/tmp", "nntpbench-nativeadapter-test")
	return Config{
		RunID:           "run-0001",
		Client:          client,
		ExecutionTarget: benchmark.MacOSNative,
		Transport:       benchmark.Plaintext,
		TransportLabel:  "plaintext",
		TLSValidation:   benchmark.TLSNotApplicable,
		ServerLink:      benchmark.DefaultServerLinkProfile(),
		FixtureDir:      root,
		NZBPath:         filepath.Join(root, "fixture.nzb"),
		OutputDir:       filepath.Join(root, "complete"),
		ConfigDir:       filepath.Join(root, "config"),
		ResultPath:      filepath.Join(root, "adapter-result.json"),
		NNTPHost:        "nntp",
		NNTPPort:        "119",
		NNTPUsername:    "user",
		NNTPPassword:    "password",
		Connections:     8,
		Profile:         benchmark.ProfileStock,
		LaunchCommand:   []string{"client", "--config", "{{config_dir}}"},
		APIEndpoint:     "http://127.0.0.1:18080",
		ClientVersion:   "test",
	}
}
