// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
	"testing"
)

type countingPlaywrightImage struct {
	fingerprints, labels, builds int
	labelled                     map[string]string
}

func (c *countingPlaywrightImage) ops(fingerprint string) weaverPlaywrightImageOps {
	return weaverPlaywrightImageOps{
		fingerprint: func() (string, error) { c.fingerprints++; return fingerprint, nil },
		label:       func(image string) string { c.labels++; return c.labelled[image] },
		build: func() error {
			c.builds++
			c.labelled[os.Getenv(weaverPlaywrightImageEnv)] = fingerprint
			return nil
		},
	}
}

func resetPlaywrightImageResolution(t *testing.T) {
	t.Helper()
	weaverPlaywrightImageReady = ""
	t.Cleanup(func() { weaverPlaywrightImageReady = "" })
	t.Setenv(weaverPlaywrightImageEnv, "")
	t.Setenv(weaverPlaywrightFingerprintEnv, "")
	t.Setenv(weaverPlaywrightForceRebuildEnv, "")
}

func TestPlaywrightImageResolvesOncePerProcess(t *testing.T) {
	resetPlaywrightImageResolution(t)
	const fingerprint = "0123456789abcdef0123"
	image := &countingPlaywrightImage{labelled: map[string]string{}}
	for flow := 0; flow < 5; flow++ {
		if err := resolveWeaverPlaywrightImage(image.ops(fingerprint)); err != nil {
			t.Fatal(err)
		}
	}
	if image.fingerprints != 1 || image.builds != 1 {
		t.Fatalf("five flows took %d fingerprints and %d builds, want 1 and 1", image.fingerprints, image.builds)
	}
	if got := os.Getenv(weaverPlaywrightImageEnv); got != "weaver-e2e-playwright:0123456789ab" {
		t.Fatalf("image = %q", got)
	}
}

func TestFlowProcessInheritsTheResolvedPlaywrightImage(t *testing.T) {
	resetPlaywrightImageResolution(t)
	t.Setenv(weaverPlaywrightImageEnv, "weaver-e2e-playwright:0123456789ab")
	t.Setenv(weaverPlaywrightFingerprintEnv, "0123456789abcdef0123")
	image := &countingPlaywrightImage{labelled: map[string]string{"weaver-e2e-playwright:0123456789ab": "0123456789abcdef0123"}}
	if err := resolveWeaverPlaywrightImage(image.ops("0123456789abcdef0123")); err != nil {
		t.Fatal(err)
	}
	if image.fingerprints != 0 || image.builds != 0 || image.labels != 1 {
		t.Fatalf("inherited image took %d fingerprints, %d builds, %d inspects", image.fingerprints, image.builds, image.labels)
	}

	// An inherited tag whose image no longer carries the fingerprint is
	// hashed and rebuilt, never trusted.
	resetPlaywrightImageResolution(t)
	t.Setenv(weaverPlaywrightImageEnv, "weaver-e2e-playwright:0123456789ab")
	t.Setenv(weaverPlaywrightFingerprintEnv, "0123456789abcdef0123")
	gone := &countingPlaywrightImage{labelled: map[string]string{}}
	if err := resolveWeaverPlaywrightImage(gone.ops("0123456789abcdef0123")); err != nil {
		t.Fatal(err)
	}
	if gone.fingerprints != 1 || gone.builds != 1 {
		t.Fatalf("missing image took %d fingerprints and %d builds, want 1 and 1", gone.fingerprints, gone.builds)
	}
}

func TestForcedPlaywrightRebuildIsSpentOnce(t *testing.T) {
	resetPlaywrightImageResolution(t)
	t.Setenv(weaverPlaywrightForceRebuildEnv, "1")
	image := &countingPlaywrightImage{labelled: map[string]string{}}
	if err := resolveWeaverPlaywrightImage(image.ops("0123456789abcdef0123")); err != nil {
		t.Fatal(err)
	}
	// A flow process starting now sees the parent's environment.
	weaverPlaywrightImageReady = ""
	if err := resolveWeaverPlaywrightImage(image.ops("0123456789abcdef0123")); err != nil {
		t.Fatal(err)
	}
	if image.builds != 1 || image.fingerprints != 1 {
		t.Fatalf("forced rebuild took %d builds and %d fingerprints, want 1 and 1", image.builds, image.fingerprints)
	}
}

func TestFlowProcessInheritsTheNNTPHealthBinary(t *testing.T) {
	built := nntpHealthBuilt
	t.Cleanup(func() { nntpHealthBuilt = built })
	nntpHealthBuilt = false
	// No Go toolchain or module lives in this directory, so a build here
	// would fail: only the inherited binary can satisfy the call.
	dir := t.TempDir()
	t.Setenv("E2E_DIR", dir)
	t.Setenv(nntpHealthBuiltEnv, "1")
	binary := filepath.Join(dir, ".bin", "nntp-health")
	if err := os.MkdirAll(filepath.Dir(binary), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureNNTPHealthBinary(); err != nil {
		t.Fatal(err)
	}
	if !nntpHealthBuilt {
		t.Fatal("inherited healthcheck binary was not taken")
	}
}
