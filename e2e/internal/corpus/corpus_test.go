package corpus

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// writeTree lays out a throwaway harness root: a toolchain lock with the two
// ids the tests name, plus whatever fixture bytes the case needs.
func writeTree(t *testing.T, fixtures map[string]string) string {
	t.Helper()
	root := t.TempDir()
	writeJSON(t, filepath.Join(root, ToolchainsFile), map[string]any{
		"schema_version": 1,
		"docker_base":    "debian:bookworm-slim@sha256:" + zeroDigest,
		"rar_writers": []any{
			map[string]any{"id": "rarlab-7.23", "image": "weaver-e2e-rarlab:7.23"},
		},
		"par2_generator": map[string]any{"id": "par2cmdline-turbo-1.4.0"},
	})
	for path, contents := range fixtures {
		writeFixture(t, root, path, contents)
	}
	return root
}

func writeFixture(t *testing.T, root, path, contents string) {
	t.Helper()
	full := HostPath(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeJSON(t *testing.T, path string, document any) {
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

const zeroDigest = "0000000000000000000000000000000000000000000000000000000000000000"

// entry is one ledger file entry for a fixture whose bytes are known.
func entry(path, contents string, source Source) FileEntry {
	return FileEntry{
		Path:   path,
		Size:   int64(len(contents)),
		BLAKE3: DigestBytes([]byte(contents)),
		Format: "bin",
		Source: source,
	}
}

func generatedSource() Source {
	return Source{Kind: SourceGenerated, Generator: "make.sh", Toolchains: []string{"rarlab-7.23"}}
}

func blockedSource(reason string) Source {
	return Source{Kind: SourceBlocked, Reason: reason}
}

func newLedger(files ...FileEntry) *Ledger {
	return &Ledger{
		SchemaVersion: SchemaVersion,
		Toolchains:    ToolchainsFile,
		Generators: map[string]Generator{
			"make.sh": {Path: "scripts/make.sh", Toolchains: []string{"rarlab-7.23"}},
		},
		Files: files,
	}
}

func loadToolchains(t *testing.T, root string) ToolchainLock {
	t.Helper()
	lock, err := LoadToolchainLock(HostPath(root, ToolchainsFile))
	if err != nil {
		t.Fatal(err)
	}
	return lock
}

func newProfiles(includes map[string][]string) *Profiles {
	profiles := &Profiles{SchemaVersion: SchemaVersion, Profiles: map[string]Profile{}}
	for name, include := range includes {
		profiles.Profiles[name] = Profile{Include: include, Exclude: []string{"**/scenario.json"}}
	}
	return profiles
}

func TestValidRelativePathRejectsEscapes(t *testing.T) {
	for _, path := range []string{"", "/absolute", "a//b", "../up", "a/../b", "a\\b", "./here"} {
		if ValidRelativePath(path) {
			t.Errorf("%q should not be a valid corpus path", path)
		}
	}
	for _, path := range []string{"testdata/x/a.rar", "test-corpus/sources.json"} {
		if !ValidRelativePath(path) {
			t.Errorf("%q should be a valid corpus path", path)
		}
	}
}

func TestDigestFileMatchesDigestBytes(t *testing.T) {
	root := writeTree(t, map[string]string{"testdata/x/a.bin": "hello corpus"})
	digest, err := DigestFile(HostPath(root, "testdata/x/a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if want := DigestBytes([]byte("hello corpus")); digest.BLAKE3 != want {
		t.Fatalf("streamed digest %s, in-memory digest %s", digest.BLAKE3, want)
	}
	if digest.Size != 12 {
		t.Fatalf("size %d, want 12", digest.Size)
	}
	if !IsDigest(digest.BLAKE3) {
		t.Fatalf("%q is not a well-formed digest", digest.BLAKE3)
	}
	for _, bad := range []string{"", "ABC", zeroDigest + "0", "g" + zeroDigest[1:]} {
		if IsDigest(bad) {
			t.Errorf("%q should not pass IsDigest", bad)
		}
	}
}

// A hydrated fixture is whole or absent: a write that fails part way leaves
// nothing at the destination.
func TestWriteFileAtomicLeavesNothingBehindOnFailure(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nested", "a.bin")
	err := WriteFileAtomic(path, func(writer io.Writer) error {
		_, _ = writer.Write([]byte("partial"))
		return errFailedWrite
	}, 0o644)
	if err == nil {
		t.Fatal("expected the write to fail")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("destination should not exist, got %v", err)
	}
	leftovers, _ := filepath.Glob(filepath.Join(root, "nested", ".*"))
	if len(leftovers) != 0 {
		t.Fatalf("temporary files left behind: %v", leftovers)
	}
}

var errFailedWrite = errors.New("write failed")
