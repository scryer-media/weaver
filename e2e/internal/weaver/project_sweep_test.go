// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestSweepTakesOnlyDeadOwnersProjectsInEngineOrder(t *testing.T) {
	now := time.Date(2026, 11, 3, 15, 0, 0, 0, time.UTC)
	recent := now.Add(-time.Hour)
	old := now.Add(-48 * time.Hour)
	ourDir := "/home/example/src/weaver/e2e"
	resources := []engineResource{
		// Claimed, owner alive: kept.
		{Kind: engineContainer, Name: "c-alive", Project: "weaver-e2e-1-alive", Created: old},
		{Kind: engineVolume, Name: "v-alive", Project: "weaver-e2e-1-alive", Created: old},
		// Claimed, owner dead: every kind removed, however recent.
		{Kind: engineVolume, Name: "v-dead", Project: "e2e-20261103-dead", Created: recent},
		{Kind: engineNetwork, Name: "n-dead", Project: "e2e-20261103-dead", Created: recent},
		{Kind: engineContainer, Name: "c-dead", Project: "e2e-20261103-dead", Workdir: ourDir, Created: recent},
		// Foreign: never touched, claimed by nobody and old.
		{Kind: engineContainer, Name: "c-foreign", Project: "docker", Workdir: "/elsewhere", Created: old},
		{Kind: engineVolume, Name: "v-foreign", Project: "e2e", Created: old},
		// Unclaimed and ours by name: removed only when all of it is old.
		{Kind: engineNetwork, Name: "n-legacy", Project: "weaver-e2e-9-legacy", Created: old},
		{Kind: engineNetwork, Name: "n-young", Project: "weaver-e2e-9-young", Created: recent},
		// Unclaimed and ours by working directory.
		{Kind: engineContainer, Name: "c-adv", Project: "adversarial-archive", Workdir: ourDir, Created: old},
		{Kind: engineNetwork, Name: "n-adv", Project: "adversarial-archive", Created: old},
	}
	owners := map[string]harnessOwner{
		"weaver-e2e-1-alive": {Project: "weaver-e2e-1-alive", PID: 10},
		"e2e-20261103-dead":  {Project: "e2e-20261103-dead", PID: 20},
	}
	doomed, projects := selectDeadHarnessResources(resources, owners, func(pid int) bool { return pid == 10 }, now)
	var names []string
	for _, resource := range doomed {
		names = append(names, resource.Kind.String()+":"+resource.Name)
	}
	want := []string{"container:c-adv", "container:c-dead", "network:n-adv", "network:n-dead", "network:n-legacy", "volume:v-dead"}
	if !slices.Equal(names, want) {
		t.Fatalf("doomed = %v, want %v", names, want)
	}
	if !slices.Equal(projects, []string{"adversarial-archive", "e2e-20261103-dead", "weaver-e2e-9-legacy"}) {
		t.Fatalf("projects = %v", projects)
	}
}

func TestEngineListingsReadPodmanAndDocker(t *testing.T) {
	podman := []byte(`[{"Id":"abc","Created":1790340356,"Labels":{"com.docker.compose.project":"weaver-e2e-1-x","com.docker.compose.project.working_dir":"/w/weaver/e2e"}}]`)
	got, err := parseEngineListing(engineContainer, podman)
	if err != nil || len(got) != 1 || got[0].Name != "abc" || got[0].Project != "weaver-e2e-1-x" ||
		got[0].Workdir != "/w/weaver/e2e" || got[0].Created.Unix() != 1790340356 {
		t.Fatalf("podman containers = %+v, %v", got, err)
	}
	podmanNetworks := []byte(`[{"name":"weaver-e2e-1-x_default","created":"2026-10-28T18:32:11.614321918Z","labels":{"com.docker.compose.project":"weaver-e2e-1-x"}}]`)
	got, err = parseEngineListing(engineNetwork, podmanNetworks)
	if err != nil || len(got) != 1 || got[0].Name != "weaver-e2e-1-x_default" || got[0].Created.IsZero() {
		t.Fatalf("podman networks = %+v, %v", got, err)
	}
	docker := []byte("{\"Name\":\"vol-a\",\"Labels\":\"com.docker.compose.project=weaver-e2e-2-y,com.docker.compose.volume=data\"}\n" +
		"{\"Name\":\"vol-b\",\"Labels\":\"\"}\n")
	got, err = parseEngineListing(engineVolume, docker)
	if err != nil || len(got) != 2 || got[0].Project != "weaver-e2e-2-y" || got[1].Project != "" || !got[0].Created.IsZero() {
		t.Fatalf("docker volumes = %+v, %v", got, err)
	}
}

func TestOldHarnessImagesRetireBeyondTheNewestPerFamily(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	at := func(hours int) time.Time { return base.Add(time.Duration(hours) * time.Hour) }
	listing := []map[string]any{
		{"Names": []string{"localhost/weaver-e2e-weaver:aaa"}, "Created": at(1).Unix()},
		{"Names": []string{"localhost/weaver-e2e-weaver:bbb"}, "Created": at(2).Unix()},
		{"Names": []string{"localhost/weaver-e2e-weaver:ccc"}, "Created": at(3).Unix()},
		{"Names": []string{"localhost/weaver-e2e-weaver:latest"}, "Created": at(0).Unix()},
		{"Names": []string{"docker.io/library/weaver-e2e-playwright:p1"}, "Created": at(5).Unix()},
		{"Names": []string{"localhost/weaver-e2e-nntp:corpus-functional-primary-h1"}, "Created": at(1).Unix()},
		{"Names": []string{"localhost/weaver-e2e-nntp:corpus-functional-primary-h2"}, "Created": at(2).Unix()},
		{"Names": []string{"localhost/weaver-e2e-nntp:corpus-chaos-primary-h1"}, "Created": at(0).Unix()},
		{"Names": []string{"localhost/weaver-e2e-rarlab:6.24"}, "Created": at(0).Unix()},
		{"Names": []string{"docker.io/library/postgres:17"}, "Created": at(0).Unix()},
	}
	body, err := json.Marshal(listing)
	if err != nil {
		t.Fatal(err)
	}
	images, err := parseHarnessImages(body)
	if err != nil {
		t.Fatal(err)
	}
	got := selectRetiredHarnessImages(images, 1)
	want := []string{
		"localhost/weaver-e2e-nntp:corpus-functional-primary-h1",
		"localhost/weaver-e2e-weaver:aaa",
		"localhost/weaver-e2e-weaver:bbb",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("retired = %v, want %v", got, want)
	}
	if got := selectRetiredHarnessImages(images, 3); len(got) != 0 {
		t.Fatalf("keeping three retires nothing here, got %v", got)
	}
}

func TestComposeProjectIsClaimedOnlyInsideAHarnessRun(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(harnessOwnersDirEnv, dir)
	t.Setenv(harnessOwnerPIDEnv, "")
	claimComposeProject("weaver-e2e-claim-none")
	if owners := readHarnessOwners(dir); len(owners) != 0 {
		t.Fatalf("claimed outside a run: %v", owners)
	}
	t.Setenv(harnessOwnerPIDEnv, "4242")
	t.Setenv(harnessOwnerRunEnv, "full-20261103-150000-4242")
	claimComposeProject("weaver-e2e-claim-one")
	owner, ok := readHarnessOwners(dir)["weaver-e2e-claim-one"]
	if !ok || owner.PID != 4242 || owner.Run != "full-20261103-150000-4242" {
		t.Fatalf("owner = %+v, %v", owner, ok)
	}
}

func TestRetainedBundleDropsWeaverDownloadsAndKeepsEvidence(t *testing.T) {
	root := t.TempDir()
	bundle := filepath.Join(root, "weaver-e2e-full-1")
	runDir := filepath.Join(bundle, "functional-sqlite", "run")
	for _, path := range []string{
		filepath.Join(runDir, "weaver", "complete", "show", "episode.mkv"),
		filepath.Join(runDir, "weaver", "intermediate", "job", "part01.rar"),
		filepath.Join(runDir, "weaver", "weaver.log"),
		filepath.Join(bundle, "functional-sqlite", "artifacts", "report.html"),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	manifest := fullRunManifest{
		OwnerPID: 7, StartedAt: time.Now(), StacksReleased: true,
		Phases: []fullRunPhaseEntry{{Name: "functional-sqlite", RunDir: runDir}},
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "full-run.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := pruneFullRunBundles(root, func(int) bool { return false }, func(*fullPhaseContext) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{"complete", "intermediate"} {
		if _, err := os.Stat(filepath.Join(runDir, "weaver", gone)); !os.IsNotExist(err) {
			t.Fatalf("%s still there: %v", gone, err)
		}
	}
	for _, kept := range []string{
		filepath.Join(runDir, "weaver", "weaver.log"),
		filepath.Join(bundle, "functional-sqlite", "artifacts", "report.html"),
	} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("%s removed: %v", kept, err)
		}
	}
}
