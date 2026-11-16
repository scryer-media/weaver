// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFilesystemGateManifestRetainsCleanupKind(t *testing.T) {
	t.Setenv("E2E_KEEP_STACKS", "false")
	root := t.TempDir()
	bundle := filepath.Join(root, "weaver-e2e-full-filesystem")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(bundle, "full-run.json")
	phase := &fullPhaseContext{Name: "Filesystem Gate", Command: "filesystem", RunDir: filepath.Join(bundle, "run")}
	if err := writeFullRunManifest(path, bundle, []*fullPhaseContext{phase}); err != nil {
		t.Fatal(err)
	}
	called := false
	err := pruneFullRunBundles(root, func(int) bool { return false }, func(restored *fullPhaseContext) error {
		called = true
		if restored.Command != "filesystem" || restored.RunDir != phase.RunDir {
			t.Fatalf("lost fixture cleanup identity: %+v", restored)
		}
		return nil
	})
	if err != nil || !called {
		t.Fatalf("recover abandoned filesystem phase: called=%v error=%v", called, err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest fullRunManifest
	if err := json.Unmarshal(body, &manifest); err != nil || !manifest.StacksReleased {
		t.Fatalf("cleanup completion not recorded: %v", err)
	}
}

func TestFilesystemGatePreservesExistingRun(t *testing.T) {
	runDir := t.TempDir()
	path := filepath.Join(runDir, filesystemGateState)
	if err := os.WriteFile(path, []byte("weaver-fs-fixture-existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runFilesystemGate(context.Background(), t.TempDir(), runDir, func(context.Context, bool, ...string) (string, error) {
		t.Fatal("a concurrent run must not invoke the engine")
		return "", nil
	})
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("expected exclusive ownership failure: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "weaver-fs-fixture-existing" {
		t.Fatalf("overwrote existing ownership record: %q %v", body, err)
	}
}

func TestFilesystemGateRequiredInFull(t *testing.T) {
	t.Setenv("E2E_FULL_PHASE_JOBS", "")
	count := 0
	for _, def := range fullPhaseDefinitions {
		if def.command != "filesystem" {
			continue
		}
		count++
		phase := &fullPhaseContext{Command: def.command, SkipSeed: def.skipSeed, RunDir: t.TempDir()}
		if !def.skipSeed || fullPhaseNeedsLocalWeaverImage(phase) || len(fullPhaseFixtureProfiles([]*fullPhaseContext{phase})) != 0 {
			t.Fatal("filesystem gate must run without an NNTP corpus or application image")
		}
		if functionalFullPhase(def) {
			t.Fatal("filesystem gate belongs to full, not the functional-only loop")
		}
		slots := newFullPhaseSlots(len(fullPhaseDefinitions))
		if slots.slotFor(phase) != slots.general || cap(slots.general) != 3 || cap(slots.gate) != 1 {
			t.Fatal("filesystem gate must share the existing four-phase capacity")
		}
		// No ownership record means no resource was created. In particular,
		// cleanup must not invoke Compose or touch another running service.
		if err := cleanupFullPhaseContext(phase); err != nil || !phase.Cleaned {
			t.Fatalf("unstarted filesystem cleanup: %v", err)
		}
	}
	if count != 1 {
		t.Fatalf("full contains %d filesystem gates, want 1", count)
	}
}

func TestFilesystemGateResultAndCleanup(t *testing.T) {
	for _, scenario := range []string{"pass", "failed-tests", "still-running", "build", "create", "start", "logs", "inspect", "cancel", "cleanup", "foreign-owner"} {
		t.Run(scenario, func(t *testing.T) {
			root, runDir := t.TempDir(), t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "rust-toolchain.toml"), []byte("[toolchain]\nchannel = \"1.97.1\"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			created, removed, imageCreated, imageRemoved := false, false, false, false
			var runID string
			fault := errors.New("fixture failure")
			run := func(commandCtx context.Context, capture bool, args ...string) (string, error) {
				switch args[0] {
				case "buildx":
					if scenario == "build" {
						return "", fault
					}
					imageCreated = true
				case "create":
					created, runID = true, args[2]
					// Creation may finish server-side even if its client fails.
					if scenario == "create" {
						return "", fault
					}
					if !strings.Contains(strings.Join(args, " "), "target=/workspace,readonly") {
						t.Fatal("filesystem fixture must use a read-only checkout")
					}
				case "start":
					if scenario == "start" {
						return "", fault
					}
				case "logs":
					if scenario == "logs" {
						return "", fault
					}
					if scenario == "cancel" {
						cancel()
						return "", commandCtx.Err()
					}
				case "inspect":
					if strings.Contains(args[2], ".State.Running") {
						switch scenario {
						case "inspect":
							return "", fault
						case "failed-tests":
							return "false 1", nil
						case "still-running":
							return "true 0", nil
						}
						return "false 0", nil
					}
					if scenario == "foreign-owner" {
						return "another-run", nil
					}
					return runID, nil
				case "ps":
					if commandCtx.Err() != nil {
						t.Fatal("cleanup inherited the canceled phase context")
					}
					if created {
						return "owned-container", nil
					}
				case "rm":
					if scenario == "cleanup" {
						return "", fault
					}
					removed = true
				case "image":
					if args[1] == "ls" && imageCreated {
						return "owned-image", nil
					}
					if args[1] == "rm" {
						imageRemoved = true
					}
				default:
					t.Fatalf("unexpected engine command %v", args)
				}
				return "", nil
			}
			err := runFilesystemGate(ctx, root, runDir, run)
			if (err == nil) != (scenario == "pass") {
				t.Fatalf("gate result = %v", err)
			}
			if scenario == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			_, stateErr := os.Stat(filepath.Join(runDir, filesystemGateState))
			if scenario == "cleanup" || scenario == "foreign-owner" {
				if removed || stateErr != nil {
					t.Fatal("failed cleanup must preserve ownership for parent recovery")
				}
			} else if created != removed || imageCreated != imageRemoved || !os.IsNotExist(stateErr) {
				t.Fatalf("incomplete cleanup: container=%v/%v image=%v/%v state=%v", created, removed, imageCreated, imageRemoved, stateErr)
			}
		})
	}
}
