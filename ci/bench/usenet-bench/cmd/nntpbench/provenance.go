package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

type executionManifest struct {
	SchemaVersion    int             `json:"schema_version"`
	StartedAt        time.Time       `json:"started_at"`
	Command          string          `json:"command"`
	Arguments        []string        `json:"arguments"`
	ExecutionTarget  string          `json:"execution_target"`
	Profile          string          `json:"profile"`
	PlanPath         string          `json:"plan_path"`
	PlanSnapshotPath string          `json:"plan_snapshot_path"`
	PlanSHA256       string          `json:"plan_sha256"`
	AdapterPath      string          `json:"adapter_catalog_path"`
	AdapterSnapshot  string          `json:"adapter_catalog_snapshot_path"`
	AdapterSHA256    string          `json:"adapter_catalog_sha256"`
	ExecutablePath   string          `json:"harness_executable_path"`
	ExecutableSHA256 string          `json:"harness_executable_sha256"`
	Host             hostFingerprint `json:"host"`
}

type hostFingerprint struct {
	Hostname  string `json:"hostname"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	GoVersion string `json:"go_version"`
	NumCPU    int    `json:"logical_cpu_count"`
}

func loadExecutionInputs(planPath, adapterPath string) (benchmark.Plan, benchmark.AdapterCatalog, []byte, []byte, error) {
	planContents, err := os.ReadFile(planPath)
	if err != nil {
		return benchmark.Plan{}, benchmark.AdapterCatalog{}, nil, nil, fmt.Errorf("read benchmark plan: %w", err)
	}
	var plan benchmark.Plan
	if err := json.Unmarshal(planContents, &plan); err != nil {
		return benchmark.Plan{}, benchmark.AdapterCatalog{}, nil, nil, fmt.Errorf("decode benchmark plan: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return benchmark.Plan{}, benchmark.AdapterCatalog{}, nil, nil, err
	}
	adapterContents, err := os.ReadFile(adapterPath)
	if err != nil {
		return benchmark.Plan{}, benchmark.AdapterCatalog{}, nil, nil, fmt.Errorf("read adapter catalog: %w", err)
	}
	var catalog benchmark.AdapterCatalog
	if err := json.Unmarshal(adapterContents, &catalog); err != nil {
		return benchmark.Plan{}, benchmark.AdapterCatalog{}, nil, nil, fmt.Errorf("decode adapter catalog: %w", err)
	}
	return plan, catalog, planContents, adapterContents, nil
}

func writeExecutionManifest(artifactRoot, command, planPath, adapterPath, target, profile string, arguments []string, planContents, adapterContents []byte) error {
	absolutePlan, err := filepath.Abs(planPath)
	if err != nil {
		return fmt.Errorf("resolve plan path: %w", err)
	}
	absoluteAdapter, err := filepath.Abs(adapterPath)
	if err != nil {
		return fmt.Errorf("resolve adapter catalog path: %w", err)
	}
	planDigest := sha256Bytes(planContents)
	adapterDigest := sha256Bytes(adapterContents)
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve harness executable: %w", err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return fmt.Errorf("resolve harness executable symlinks: %w", err)
	}
	executableDigest, err := sha256File(executable)
	if err != nil {
		return fmt.Errorf("hash harness executable: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("read hostname: %w", err)
	}
	absoluteRoot, err := filepath.Abs(artifactRoot)
	if err != nil {
		return fmt.Errorf("resolve artifact root: %w", err)
	}
	if _, err := os.Stat(absoluteRoot); err == nil {
		return fmt.Errorf("artifact root already exists: %s", absoluteRoot)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect artifact root: %w", err)
	}
	parent := filepath.Dir(absoluteRoot)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("create artifact parent: %w", err)
	}
	stagingRoot, err := os.MkdirTemp(parent, ".nntpbench-execution-")
	if err != nil {
		return fmt.Errorf("create execution staging directory: %w", err)
	}
	defer os.RemoveAll(stagingRoot)
	planSnapshot := filepath.Join(stagingRoot, "plan.snapshot.json")
	adapterSnapshot := filepath.Join(stagingRoot, "adapter-catalog.snapshot.json")
	if err := writeBytesExclusive(planSnapshot, planContents); err != nil {
		return fmt.Errorf("snapshot plan: %w", err)
	}
	if err := writeBytesExclusive(adapterSnapshot, adapterContents); err != nil {
		return fmt.Errorf("snapshot adapter catalog: %w", err)
	}
	manifest := executionManifest{
		SchemaVersion:    1,
		StartedAt:        time.Now().UTC(),
		Command:          command,
		Arguments:        redactExecutionArguments(arguments),
		ExecutionTarget:  target,
		Profile:          profile,
		PlanPath:         absolutePlan,
		PlanSnapshotPath: filepath.Base(planSnapshot),
		PlanSHA256:       planDigest,
		AdapterPath:      absoluteAdapter,
		AdapterSnapshot:  filepath.Base(adapterSnapshot),
		AdapterSHA256:    adapterDigest,
		ExecutablePath:   executable,
		ExecutableSHA256: executableDigest,
		Host: hostFingerprint{
			Hostname:  hostname,
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			GoVersion: runtime.Version(),
			NumCPU:    runtime.NumCPU(),
		},
	}
	path := filepath.Join(stagingRoot, "execution-manifest.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create immutable execution manifest: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		_ = file.Close()
		return fmt.Errorf("write execution manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close execution manifest: %w", err)
	}
	if err := os.Rename(stagingRoot, absoluteRoot); err != nil {
		return fmt.Errorf("publish immutable execution root: %w", err)
	}
	return nil
}

func writeBytesExclusive(path string, contents []byte) error {
	destination, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := destination.Write(contents); err != nil {
		_ = destination.Close()
		return err
	}
	return destination.Close()
}

func sha256Bytes(contents []byte) string {
	digest := sha256.Sum256(contents)
	return hex.EncodeToString(digest[:])
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func redactExecutionArguments(arguments []string) []string {
	redacted := append([]string(nil), arguments...)
	for index := 0; index < len(redacted); index++ {
		argument := redacted[index]
		name := strings.TrimLeft(argument, "-")
		if name == "password" || name == "password-file" {
			if index+1 < len(redacted) {
				index++
				redacted[index] = "[REDACTED]"
			}
			continue
		}
		name, _, hasValue := strings.Cut(name, "=")
		if hasValue && (name == "password" || name == "password-file") {
			redacted[index] = strings.SplitN(argument, "=", 2)[0] + "=[REDACTED]"
		}
	}
	return redacted
}
