package fixturegen

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// ensureRoot writes the smallest harness checkout Ensure can load: a
// toolchain lock with one writer, a ledger with the given fixtures (all
// generated), a profile per scenario, and an unpinned lock.json.
func ensureRoot(t *testing.T, fixtures map[string]string) string {
	t.Helper()
	root := t.TempDir()
	const zero = "0000000000000000000000000000000000000000000000000000000000000000"
	writeEnsureJSON(t, filepath.Join(root, corpus.ToolchainsFile), map[string]any{
		"schema_version": 1,
		"docker_base":    "debian:bookworm-slim@sha256:" + zero,
		"rar_writers": []any{
			map[string]any{"id": "rarlab-7.23", "image": "weaver-e2e-rarlab:7.23"},
		},
		"par2_generator": map[string]any{"id": "par2cmdline-turbo-1.4.0"},
	})
	ledger := &corpus.Ledger{
		SchemaVersion: corpus.SchemaVersion,
		Toolchains:    corpus.ToolchainsFile,
		Generators: map[string]corpus.Generator{
			"fixturegen": {Path: "cmd/fixturegen", Toolchains: []string{"rarlab-7.23"}},
		},
	}
	profiles := map[string]any{}
	for path, contents := range fixtures {
		full := corpus.HostPath(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
		ledger.Files = append(ledger.Files, corpus.FileEntry{
			Path:   path,
			Size:   int64(len(contents)),
			BLAKE3: corpus.DigestBytes([]byte(contents)),
			Format: "bin",
			Source: corpus.Source{Kind: corpus.SourceGenerated, Generator: "fixturegen", Toolchains: []string{"rarlab-7.23"}},
		})
		slug := strings.Split(strings.TrimPrefix(path, "testdata/"), "/")[0]
		profiles[slug] = map[string]any{
			"include": []string{"testdata/" + slug + "/**"},
			"exclude": []string{"**/scenario.json"},
		}
	}
	if err := ledger.Save(root); err != nil {
		t.Fatal(err)
	}
	writeEnsureJSON(t, filepath.Join(root, corpus.ProfilesFile), map[string]any{
		"schema_version": corpus.SchemaVersion,
		"profiles":       profiles,
	})
	writeEnsureJSON(t, filepath.Join(root, corpus.LockFile), map[string]any{
		"schema_version": 1,
		"base_url":       "",
		"manifest":       map[string]string{"blake3": "", "url": ""},
		"signature": map[string]string{
			"bundle_url":              "",
			"certificate_identity":    corpus.PublishWorkflowIdentity,
			"certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
		},
		"provenance":     map[string]string{"blake3": "", "url": ""},
		"published_from": map[string]string{"commit": "", "run": ""},
	})
	return root
}

func writeEnsureJSON(t *testing.T, path string, document any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	contents, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(contents, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureReusesWhatIsPresentAndCorrect(t *testing.T) {
	root := ensureRoot(t, map[string]string{
		"testdata/alpha/archive.rar": "alpha bytes",
		"testdata/beta/archive.rar":  "beta bytes",
	})
	report, err := Ensure(context.Background(), EnsureConfig{
		Root: root, Profiles: []string{"alpha"}, Paths: []string{"testdata/beta/archive.rar"}, Digest: true, NoGenerate: true,
	})
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	want := []string{"testdata/alpha/archive.rar", "testdata/beta/archive.rar"}
	if !reflect.DeepEqual(report.Wanted, want) {
		t.Fatalf("wanted %v, got %v", want, report.Wanted)
	}
	if !reflect.DeepEqual(report.Present, want) {
		t.Fatalf("present %v, got %v", want, report.Present)
	}
	if len(report.Fetched) != 0 || len(report.Generated) != 0 || report.FetchSkipped != "" {
		t.Fatalf("nothing should have been fetched or generated: %+v", report)
	}
}

func TestEnsureTreatsWrongSizeAndWrongBytesAsMissing(t *testing.T) {
	root := ensureRoot(t, map[string]string{
		"testdata/alpha/archive.rar": "alpha bytes",
		"testdata/beta/archive.rar":  "beta bytes",
	})
	// Same size, different bytes: only the digest check notices.
	if err := os.WriteFile(corpus.HostPath(root, "testdata/alpha/archive.rar"), []byte("ALPHA BYTES"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Different size: the quick check notices.
	if err := os.WriteFile(corpus.HostPath(root, "testdata/beta/archive.rar"), []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Ensure(context.Background(), EnsureConfig{Root: root, Profiles: []string{"alpha", "beta"}, NoGenerate: true})
	if err == nil || !strings.Contains(err.Error(), "testdata/beta/archive.rar") || strings.Contains(err.Error(), "testdata/alpha/archive.rar") {
		t.Fatalf("size-only check should report beta and not alpha, got: %v", err)
	}
	report, err := Ensure(context.Background(), EnsureConfig{Root: root, Profiles: []string{"alpha", "beta"}, NoGenerate: true, Digest: true})
	if err == nil || !strings.Contains(err.Error(), "testdata/alpha/archive.rar") || !strings.Contains(err.Error(), "testdata/beta/archive.rar") {
		t.Fatalf("digest check should report both, got: %v", err)
	}
	if !strings.Contains(report.FetchSkipped, "nothing published") {
		t.Fatalf("an unpinned lock must be reported as the reason nothing was fetched, got %q", report.FetchSkipped)
	}
}

func TestEnsureRejectsAnUnledgeredPathAndAnUnknownProfile(t *testing.T) {
	root := ensureRoot(t, map[string]string{"testdata/alpha/archive.rar": "alpha bytes"})
	if _, err := Ensure(context.Background(), EnsureConfig{Root: root, Paths: []string{"testdata/alpha/nope.rar"}}); err == nil || !strings.Contains(err.Error(), "not a ledger path") {
		t.Fatalf("expected an unledgered-path error, got %v", err)
	}
	if _, err := Ensure(context.Background(), EnsureConfig{Root: root, Profiles: []string{"gamma"}}); err == nil || !strings.Contains(err.Error(), "gamma") {
		t.Fatalf("expected an unknown-profile error, got %v", err)
	}
	if _, err := Ensure(context.Background(), EnsureConfig{Root: root}); err == nil {
		t.Fatal("expected an error when nothing is selected")
	}
}

func TestSlugsOfMapsPathsToScenarioDirectories(t *testing.T) {
	got := slugsOf([]string{
		"testdata/rar5-multivolume/archive.part2.rar",
		"testdata/rar5-multivolume/archive.part1.rar",
		"testdata/shared/clip.mkv",
		"testdata/7z-encrypted/archive.7z",
	})
	want := []string{"7z-encrypted", "rar5-multivolume", "shared"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("slugsOf = %v, want %v", got, want)
	}
}

func TestEnsureLockSerialisesAndReleases(t *testing.T) {
	root := t.TempDir()
	release, err := lockEnsure(root)
	if err != nil {
		t.Fatal(err)
	}
	release()
	// A second acquisition after release must not block or fail.
	release, err = lockEnsure(root)
	if err != nil {
		t.Fatal(err)
	}
	release()
}
