package benchmark

import (
	"os"
	"path/filepath"
	"testing"
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
