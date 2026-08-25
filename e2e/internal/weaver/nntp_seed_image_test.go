package weaver

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestNntpSeedImageTagIncludesProfileRoleAndCorpusFingerprint(t *testing.T) {
	fingerprint := "0123456789abcdef"
	if got, want := nntpSeedImageTag("tcp-chaos", "backup", fingerprint), "weaver-e2e-nntp:corpus-tcp-chaos-backup-"+fingerprint; got != want {
		t.Fatalf("tag = %q, want %q", got, want)
	}
}

func TestNntpSeedFingerprintInputsAreLengthDelimited(t *testing.T) {
	first := sha256.New()
	writeNntpSeedFingerprintInput(first, "a", []byte("bc"))

	second := sha256.New()
	writeNntpSeedFingerprintInput(second, "ab", []byte("c"))

	if string(first.Sum(nil)) == string(second.Sum(nil)) {
		t.Fatal("distinct labeled inputs produced the same fingerprint input hash")
	}
}

func TestSnapshotSeededNZBsUsesExplicitFixtureRoot(t *testing.T) {
	const slug = "fixture-root-regression"
	ambientRoot := t.TempDir()
	phaseRoot := t.TempDir()
	contextRoot := t.TempDir()
	t.Setenv("FIXTURES_DIR", ambientRoot)

	for root, contents := range map[string]string{
		ambientRoot: "ambient fixture",
		phaseRoot:   "phase fixture",
	} {
		dir := filepath.Join(root, slug)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create fixture directory: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, slug+".nzb"), []byte(contents), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
	}

	if err := snapshotSeededNZBs(contextRoot, phaseRoot, []string{slug}); err != nil {
		t.Fatalf("snapshot seeded NZB: %v", err)
	}
	staged, err := os.ReadFile(filepath.Join(contextRoot, "e2e-seed-fixtures", slug, slug+".nzb"))
	if err != nil {
		t.Fatalf("read staged fixture: %v", err)
	}
	if got, want := string(staged), "phase fixture"; got != want {
		t.Fatalf("staged fixture = %q, want %q", got, want)
	}
}
