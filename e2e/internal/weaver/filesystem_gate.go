// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

const filesystemGateLabel = "org.scryer-media.weaver.fs-fixture-run"
const filesystemGateState = "filesystem-resource"

type filesystemEngineRun func(context.Context, bool, ...string) (string, error)

func runFilesystemEngine(ctx context.Context, capture bool, args ...string) (string, error) {
	cmd := containerengine.CommandContext(ctx, args...)
	cmd.Stderr = os.Stderr
	if capture {
		out, err := cmd.Output()
		return strings.TrimSpace(string(out)), err
	}
	cmd.Stdout = os.Stdout
	return "", cmd.Run()
}

func cmdFilesystem() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	return runFilesystemGate(ctx, filepath.Dir(e2eDir()), localRunDir(), runFilesystemEngine)
}

// Both refusal profiles run inside one isolated Linux container. Its exit code
// covers the controls and every selected storage/delivery regression.
func runFilesystemGate(ctx context.Context, root, runDir string, run filesystemEngineRun) (result error) {
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return err
	}
	scratch, err := os.MkdirTemp(runDir, "weaver-fs-fixture-")
	if err != nil {
		return err
	}
	defer os.Remove(scratch)
	runID := filepath.Base(scratch)
	ownershipFile, err := os.OpenFile(filepath.Join(runDir, filesystemGateState), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("reserve filesystem ownership record (use a separate E2E_RUN_DIR for concurrent runs): %w", err)
	}
	_, writeErr := ownershipFile.WriteString(runID)
	if err := errors.Join(writeErr, ownershipFile.Close()); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, cleanupFilesystemGate(runDir, run)) }()

	toolchain, err := os.ReadFile(filepath.Join(root, "rust-toolchain.toml"))
	if err != nil {
		return err
	}
	channel := parseRustToolchainChannel(string(toolchain))
	if channel == "" {
		return errors.New("filesystem gate requires the repository's pinned Rust toolchain")
	}
	image := "weaver-fs-fixture:" + runID
	if _, err := run(ctx, false, "buildx", "build", "--load", "--build-arg", "WEAVER_IMAGE=rust:"+channel+"-bookworm",
		"-t", image, filepath.Join(root, "e2e", "services", "filesystem")); err != nil {
		return fmt.Errorf("build filesystem fixture: %w", err)
	}
	if _, err := run(ctx, false, "create", "--name", runID, "--label", filesystemGateLabel+"="+runID,
		"--device", "/dev/fuse", "--cap-add", "SYS_ADMIN",
		"--security-opt", "apparmor=unconfined", "--security-opt", "label=disable",
		"--mount", "type=bind,source="+root+",target=/workspace,readonly",
		"--entrypoint", "python3", image, "/fs-fixture/regressions.py"); err != nil {
		return fmt.Errorf("create filesystem fixture: %w", err)
	}
	if _, err := run(ctx, false, "start", runID); err != nil {
		return fmt.Errorf("start filesystem fixture: %w", err)
	}
	if _, err := run(ctx, false, "logs", "--follow", runID); err != nil {
		return fmt.Errorf("stream filesystem regressions: %w", err)
	}
	state, err := run(ctx, true, "inspect", "--format", "{{.State.Running}} {{.State.ExitCode}}", runID)
	if err != nil {
		return fmt.Errorf("inspect filesystem result: %w", err)
	}
	if state != "false 0" {
		return fmt.Errorf("filesystem gate failed (running, exit code): %s", state)
	}
	return nil
}

func cleanupFilesystemGateAt(runDir string) error {
	return cleanupFilesystemGate(runDir, runFilesystemEngine)
}

// Use a fresh context for teardown after cancellation. Record ownership before
// creation so the parent can recover even if the child dies during create.
func cleanupFilesystemGate(runDir string, run filesystemEngineRun) error {
	statePath := filepath.Join(runDir, filesystemGateState)
	raw, err := os.ReadFile(statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	runID := string(raw)
	if !strings.HasPrefix(runID, "weaver-fs-fixture-") || filepath.Base(runID) != runID || strings.ContainsAny(runID, " \t\r\n") {
		return fmt.Errorf("invalid filesystem fixture ownership record %q", runID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ids, err := run(ctx, true, "ps", "-aq", "--filter", "label="+filesystemGateLabel+"="+runID)
	if err != nil {
		return fmt.Errorf("list owned filesystem fixtures: %w", err)
	}
	for _, id := range strings.Fields(ids) {
		owner, err := run(ctx, true, "inspect", "--format", "{{ index .Config.Labels \""+filesystemGateLabel+"\" }}", id)
		if err != nil {
			return err
		}
		if owner != runID {
			return fmt.Errorf("refusing to remove filesystem container %s without this run's ownership label", id)
		}
		if _, err := run(ctx, false, "rm", "-f", id); err != nil {
			return fmt.Errorf("remove filesystem fixture: %w", err)
		}
	}
	image := "weaver-fs-fixture:" + runID
	imageID, err := run(ctx, true, "image", "ls", "-q", image)
	if err != nil {
		return err
	}
	if imageID != "" {
		if _, err := run(ctx, false, "image", "rm", image); err != nil {
			return fmt.Errorf("remove filesystem image: %w", err)
		}
	}
	if err := os.Remove(filepath.Join(runDir, runID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Remove(statePath)
}
