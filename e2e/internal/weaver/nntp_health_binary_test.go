// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func useFakeNNTPHealthBuild(t *testing.T, content *string) {
	t.Helper()
	previous := nntpHealthBuild
	nntpHealthBuild = func(output string) error {
		return os.WriteFile(output, []byte(*content), 0o755)
	}
	t.Cleanup(func() { nntpHealthBuild = previous })
}

func TestNNTPHealthBinaryPublishedNameIsNeverReplaced(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".bin")
	content := "first build"
	useFakeNNTPHealthBuild(t, &content)

	first, err := publishNNTPHealthBinary(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A running container holds the published file open through its mount.
	mounted, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}

	again, err := publishNNTPHealthBinary(dir)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("same content published as %s, then %s", first, again)
	}

	content = "changed build"
	changed, err := publishNNTPHealthBinary(dir)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatalf("changed content reused the mounted name %s", first)
	}

	now, err := os.Stat(first)
	if err != nil {
		t.Fatalf("mounted binary is gone: %v", err)
	}
	if !os.SameFile(mounted, now) {
		t.Fatal("mounted binary's name now points at another file")
	}
	if body, _ := os.ReadFile(first); string(body) != "first build" {
		t.Fatalf("mounted binary was rewritten: %q", body)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".nntp-health-build-") {
			t.Fatalf("build scratch %s left behind", entry.Name())
		}
	}
}

func TestNNTPHealthBinaryPathReachesCompose(t *testing.T) {
	built := nntpHealthBuilt
	t.Cleanup(func() { nntpHealthBuilt = built })
	nntpHealthBuilt = false
	dir := t.TempDir()
	t.Setenv("E2E_DIR", dir)
	t.Setenv(nntpHealthBinaryEnv, "")
	content := "binary"
	useFakeNNTPHealthBuild(t, &content)

	if err := ensureNNTPHealthBinary(); err != nil {
		t.Fatal(err)
	}
	published := os.Getenv(nntpHealthBinaryEnv)
	if filepath.Dir(published) != filepath.Join(dir, ".bin") || !strings.HasPrefix(filepath.Base(published), "nntp-health-") {
		t.Fatalf("%s = %q, want a content name under %s", nntpHealthBinaryEnv, published, filepath.Join(dir, ".bin"))
	}
	if _, err := os.Stat(filepath.Join(dir, ".bin", "nntp-health")); !os.IsNotExist(err) {
		t.Fatalf("the shared unversioned path was written: %v", err)
	}
}

// Every mount of the healthcheck goes through the published path; none
// names the old shared file directly.
func TestComposeFilesMountThePublishedNNTPHealthBinary(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(e2eDir(), "docker-compose*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	mounts := 0
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.Contains(line, ":/nntp-health") {
				continue
			}
			mounts++
			if !strings.HasPrefix(strings.TrimSpace(line), "- ${"+nntpHealthBinaryEnv+":-") {
				t.Errorf("%s mounts the healthcheck without %s: %s", filepath.Base(file), nntpHealthBinaryEnv, strings.TrimSpace(line))
			}
		}
	}
	if mounts == 0 {
		t.Fatal("no healthcheck mount found in the Compose files")
	}
}
