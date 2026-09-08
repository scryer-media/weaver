package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const templateCatalog = `{
  "schema_version": 3,
  "adapters": [
    {"client":"sabnzbd","kind":"docker","environment":{"CLIENT_IMAGE":"sab@sha256:aaa","EXTRA":"keep"}},
    {"client":"nzbget","kind":"docker","environment":{"CLIENT_IMAGE":"nzbget@sha256:bbb"}},
    {"client":"weaver","kind":"docker","environment":{"CLIENT_IMAGE":"weaver@sha256:old"}}
  ]
}`

func TestRewriteAdapterCatalogPinsOneClientAndKeepsTheRest(t *testing.T) {
	dir := t.TempDir()
	template := filepath.Join(dir, "adapters-next.json")
	destination := filepath.Join(dir, "adapters.json")
	if err := os.WriteFile(template, []byte(templateCatalog), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	pins, err := rewriteAdapterCatalog(template, destination, "weaver", "weaver@sha256:new")
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if len(pins) != 3 {
		t.Fatalf("reported %d pins, want 3: %v", len(pins), pins)
	}
	written, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var catalog struct {
		SchemaVersion int `json:"schema_version"`
		Adapters      []struct {
			Client      string            `json:"client"`
			Kind        string            `json:"kind"`
			Environment map[string]string `json:"environment"`
		} `json:"adapters"`
	}
	if err := json.Unmarshal(written, &catalog); err != nil {
		t.Fatalf("parse the rewritten catalog: %v", err)
	}
	if catalog.SchemaVersion != 3 {
		t.Fatalf("the rewrite dropped schema_version: %s", written)
	}
	byClient := map[string]map[string]string{}
	for _, adapter := range catalog.Adapters {
		if adapter.Kind != "docker" {
			t.Fatalf("the rewrite dropped adapter %s's kind", adapter.Client)
		}
		byClient[adapter.Client] = adapter.Environment
	}
	if got := byClient["weaver"]["CLIENT_IMAGE"]; got != "weaver@sha256:new" {
		t.Fatalf("weaver was pinned to %q", got)
	}
	if got := byClient["sabnzbd"]["CLIENT_IMAGE"]; got != "sab@sha256:aaa" {
		t.Fatalf("the rewrite changed sabnzbd's pin to %q", got)
	}
	// A field this build does not know about must survive the rewrite, or a
	// pin would silently discard part of the catalog.
	if got := byClient["sabnzbd"]["EXTRA"]; got != "keep" {
		t.Fatalf("the rewrite dropped an unknown environment entry: %q", got)
	}
}

func TestRewriteAdapterCatalogRefusesAnUndeclaredClient(t *testing.T) {
	dir := t.TempDir()
	template := filepath.Join(dir, "adapters.json")
	if err := os.WriteFile(template, []byte(templateCatalog), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := rewriteAdapterCatalog(template, filepath.Join(dir, "out.json"), "nzbhydra", "x@sha256:c")
	if err == nil || !strings.Contains(err.Error(), "nzbhydra") {
		t.Fatalf("expected the undeclared client to be named, got %v", err)
	}
	// A refused pin must leave nothing behind for a later session to pick up.
	if _, statErr := os.Stat(filepath.Join(dir, "out.json")); statErr == nil {
		t.Fatal("a refused pin wrote a catalog anyway")
	}
}

func TestRewriteAdapterCatalogRejectsAMissingImage(t *testing.T) {
	dir := t.TempDir()
	template := filepath.Join(dir, "adapters.json")
	contents := `{"adapters":[{"client":"weaver","environment":{}},{"client":"sabnzbd","environment":{"CLIENT_IMAGE":"s@sha256:a"}}]}`
	if err := os.WriteFile(template, []byte(contents), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Pinning weaver fills weaver's image, but sabnzbd's would still be read
	// from a catalog that never declared one.
	_, err := rewriteAdapterCatalog(template, filepath.Join(dir, "out.json"), "nzbget", "n@sha256:b")
	if err == nil {
		t.Fatal("a catalog missing a CLIENT_IMAGE was accepted")
	}
}

func TestBackupAdapterCatalogPreservesTheOriginal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "adapters.json")
	if err := os.WriteFile(path, []byte(templateCatalog), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	backup, err := backupAdapterCatalog(path)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	saved, err := os.ReadFile(backup)
	if err != nil {
		t.Fatalf("read the backup: %v", err)
	}
	if string(saved) != templateCatalog {
		t.Fatal("the backup does not match the original")
	}
	if !strings.HasPrefix(filepath.Base(backup), "adapters.json.bak-") {
		t.Fatalf("unexpected backup name %q", backup)
	}
}

func TestBackupAdapterCatalogToleratesAFirstPin(t *testing.T) {
	backup, err := backupAdapterCatalog(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil {
		t.Fatalf("backing up an absent catalog must be tolerated, got %v", err)
	}
	if backup != "" {
		t.Fatalf("an absent catalog produced backup %q", backup)
	}
}
