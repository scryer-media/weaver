package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteExecutionManifestIsImmutableAndRedactsSecrets(t *testing.T) {
	root := t.TempDir()
	planPath := filepath.Join(root, "plan.json")
	adapterPath := filepath.Join(root, "adapters.json")
	if err := os.WriteFile(planPath, []byte("plan"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(adapterPath, []byte("adapters"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifactRoot := filepath.Join(root, "artifacts")
	arguments := []string{"-password", "secret", "--password-file=/private/secret", "--target", "docker-linux"}
	if err := writeExecutionManifest(artifactRoot, "sequential", planPath, adapterPath, "docker-linux", "stock", arguments, []byte("plan"), []byte("adapters")); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(artifactRoot, "execution-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "secret") {
		t.Fatalf("execution manifest leaked a secret: %s", contents)
	}
	var manifest executionManifest
	if err := json.Unmarshal(contents, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 1 || len(manifest.PlanSHA256) != 64 || len(manifest.AdapterSHA256) != 64 || len(manifest.ExecutableSHA256) != 64 {
		t.Fatalf("incomplete execution manifest: %#v", manifest)
	}
	if snapshot, err := os.ReadFile(filepath.Join(artifactRoot, manifest.PlanSnapshotPath)); err != nil || string(snapshot) != "plan" {
		t.Fatalf("invalid plan snapshot: contents=%q error=%v", snapshot, err)
	}
	if snapshot, err := os.ReadFile(filepath.Join(artifactRoot, manifest.AdapterSnapshot)); err != nil || string(snapshot) != "adapters" {
		t.Fatalf("invalid adapter snapshot: contents=%q error=%v", snapshot, err)
	}
	if err := writeExecutionManifest(artifactRoot, "sequential", planPath, adapterPath, "docker-linux", "stock", arguments, []byte("plan"), []byte("adapters")); err == nil {
		t.Fatal("execution manifest overwrite unexpectedly succeeded")
	}
}

func TestRedactExecutionArgumentsDoesNotMutateInput(t *testing.T) {
	original := []string{"-password=secret", "--password-file", "/private/secret", "--profile", "stock"}
	redacted := redactExecutionArguments(original)
	if original[0] != "-password=secret" || strings.Contains(strings.Join(redacted, " "), "secret") {
		t.Fatalf("redaction failed: original=%v redacted=%v", original, redacted)
	}
}
