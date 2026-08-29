package benchmark

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

func TestDeleteOutputFilesRetainsOutputRoot(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "job", "movie.mkv")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nested, []byte("fixture"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := DeleteOutputFiles(root); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("output root still contains %d entries", len(entries))
	}
}

func TestHashFileUsesBLAKE3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := hashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const want = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
	if got != want {
		t.Fatalf("BLAKE3(abc) = %s, want %s", got, want)
	}
}

func TestVerifyOutputAllowsContentPreservingRename(t *testing.T) {
	fixtureDir := t.TempDir()
	outputDir := t.TempDir()
	contents := []byte("movie payload")
	actualPath := filepath.Join(outputDir, "release-name.mkv")
	if err := os.WriteFile(actualPath, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	digest, err := hashFile(actualPath)
	if err != nil {
		t.Fatal(err)
	}
	writeVerificationManifest(t, fixtureDir, []fixture.FileDigest{{
		Path:   "payload-01.mkv",
		Size:   int64(len(contents)),
		BLAKE3: digest,
	}})

	verification, err := VerifyOutput(fixtureDir, outputDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(verification.Files) != 1 || verification.Files[0].ActualPath != "release-name.mkv" {
		t.Fatalf("verification = %#v", verification)
	}
}

func TestVerifyOutputRejectsOneFlattenedFileForTwoExpectedMembers(t *testing.T) {
	fixtureDir := t.TempDir()
	outputDir := t.TempDir()
	contents := []byte("certificate")
	actualPath := filepath.Join(outputDir, "id.bdmv")
	if err := os.WriteFile(actualPath, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	digest, err := hashFile(actualPath)
	if err != nil {
		t.Fatal(err)
	}
	writeVerificationManifest(t, fixtureDir, []fixture.FileDigest{
		{Path: "CERTIFICATE/id.bdmv", Size: int64(len(contents)), BLAKE3: digest},
		{Path: "CERTIFICATE/BACKUP/id.bdmv", Size: int64(len(contents)), BLAKE3: digest},
	})

	if _, err := VerifyOutput(fixtureDir, outputDir); err == nil {
		t.Fatal("one flattened output file satisfied two expected members")
	}
}

func writeVerificationManifest(t *testing.T, fixtureDir string, expected []fixture.FileDigest) {
	t.Helper()
	manifest := fixture.GeneratedManifest{
		SchemaVersion: 3,
		Case:          fixture.ArchiveCase{ID: "verification"},
		ExpectedFiles: expected,
		ArchiveFiles:  []fixture.FileDigest{{Path: "archive/fixture.part01.rar", Size: 1, BLAKE3: "fixture"}},
	}
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixtureDir, "fixture-manifest.json"), contents, 0o644); err != nil {
		t.Fatal(err)
	}
}
