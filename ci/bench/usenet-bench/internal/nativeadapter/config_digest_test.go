package nativeadapter

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// suiteConfig is testConfig placed under one suite's artifact directory, the
// way the chain places every repetition of a stratum.
func suiteConfig(client benchmark.Client, suite string) Config {
	cfg := testConfig(client)
	root := filepath.Join("/tmp", "nntpbench-nativeadapter-test", suite)
	cfg.OutputDir = filepath.Join(root, "downloads", "complete")
	cfg.ConfigDir = filepath.Join(root, "config", "job-001")
	cfg.ResultPath = filepath.Join(cfg.ConfigDir, "adapter-result.json")
	cfg.WorkingDir = root
	return cfg
}

func digestOf(t *testing.T, cfg Config) string {
	t.Helper()
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatalf("render %s: %v", cfg.Client, err)
	}
	return spec.ConfigSHA256
}

// The summarizer refuses a stratum whose repetitions disagree on
// rendered_config_sha256. Repetitions differ only in which suite directory
// they were handed, which is not part of the product's configuration.
func TestTheConfigDigestIgnoresTheSuiteSandbox(t *testing.T) {
	for _, client := range []benchmark.Client{benchmark.Weaver, benchmark.SABnzbd, benchmark.NZBGet} {
		first := digestOf(t, suiteConfig(client, "sequential-0001"))
		second := digestOf(t, suiteConfig(client, "sequential-0199"))
		if first != second {
			t.Fatalf("%s digest changed with the suite directory: %s vs %s", client, first, second)
		}
	}
}

// Canonicalising the sandbox must not blunt the check it exists to serve.
func TestTheConfigDigestStillMovesForARealConfigurationChange(t *testing.T) {
	base := suiteConfig(benchmark.Weaver, "sequential-0001")
	baseline := digestOf(t, base)
	for name, mutate := range map[string]func(*Config){
		"connections":      func(cfg *Config) { cfg.Connections = cfg.Connections + 1 },
		"profile":          func(cfg *Config) { cfg.Profile = benchmark.ProfileEquivalentThroughput },
		"nntp port":        func(cfg *Config) { cfg.NNTPPort = "563" },
		"nntp password":    func(cfg *Config) { cfg.NNTPPassword = "different" },
		"archive password": func(cfg *Config) { cfg.ArchivePassword = "secret" },
		"launch command":   func(cfg *Config) { cfg.LaunchCommand = append(cfg.LaunchCommand, "--verbose") },
	} {
		changed := suiteConfig(benchmark.Weaver, "sequential-0001")
		mutate(&changed)
		if digestOf(t, changed) == baseline {
			t.Fatalf("changing the %s left the config digest unchanged", name)
		}
	}
}

// The digest is canonical; the audit file beside the run is not, because an
// operator reading it needs the paths the client was actually given.
func TestTheAuditRecordKeepsTheRealSandboxPaths(t *testing.T) {
	cfg := suiteConfig(benchmark.Weaver, "sequential-0199")
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(spec.Rendered), cfg.ConfigDir) {
		t.Fatalf("audit record dropped the real config directory %s", cfg.ConfigDir)
	}
	if !strings.Contains(string(spec.Rendered), cfg.OutputDir) {
		t.Fatalf("audit record dropped the real output directory %s", cfg.OutputDir)
	}
}

// On Windows the same directory appears three ways in one rendering: as the
// operating system writes it, with its separators doubled by encoding/json
// inside launch_command, and with forward slashes.
func TestTheDigestIgnoresEverySpellingOfAWindowsSandbox(t *testing.T) {
	cfg := testConfig(benchmark.Weaver)
	cfg.ConfigDir = `C:\bench\runs\series\sequential-0001\config\job-001`
	cfg.OutputDir = `C:\bench\runs\series\sequential-0001\downloads\complete`
	cfg.WorkingDir = ""
	cfg.ResultPath = ""
	rendered := strings.Join([]string{
		"WEAVER_DATA_DIR=" + cfg.ConfigDir,
		`launch_command=["weaver.exe","--config","` + strings.ReplaceAll(cfg.ConfigDir, `\`, `\\`) + `"]`,
		"complete_dir = " + strings.ReplaceAll(cfg.OutputDir, `\`, "/"),
	}, "\n")
	canonical := string(canonicalizeSandboxPaths(cfg, []byte(rendered)))
	if strings.Contains(canonical, "sequential-0001") {
		t.Fatalf("a suite path survived canonicalisation: %s", canonical)
	}
	if !strings.Contains(canonical, "{{suite_config_dir}}") || !strings.Contains(canonical, "{{suite_output_dir}}") {
		t.Fatalf("canonical form lost its placeholders: %s", canonical)
	}
}
