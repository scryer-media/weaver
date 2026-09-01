package fixturegen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

const saltedTestDigest = "1111111111111111111111111111111111111111111111111111111111111111"

// writeFixture drops bytes at a ledger-relative path under root.
func writeFixture(t *testing.T, root, rel, contents string) {
	t.Helper()
	host := corpus.HostPath(root, rel)
	if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func saltedEntry(path string) corpus.FileEntry {
	return corpus.FileEntry{Path: path, Salted: true}
}

func hashedEntry(path string, size int64) corpus.FileEntry {
	return corpus.FileEntry{Path: path, Size: size, BLAKE3: saltedTestDigest}
}

// (a) + (d): a salted fixture is satisfied by presence. Its bytes differ on
// every machine that generates them, so nothing may compare them — and nothing
// may declare it stale for differing.
func TestSaltedEntryIsSatisfiedByPresenceWhateverItsBytes(t *testing.T) {
	root := t.TempDir()
	ledger := &corpus.Ledger{Files: []corpus.FileEntry{saltedEntry("testdata/salty/archive.7z")}}
	wanted := []string{"testdata/salty/archive.7z"}

	// Absent: must be generated.
	missing, err := missingPaths(root, ledger, wanted, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 {
		t.Fatalf("an absent salted fixture must be reported missing, got %v", missing)
	}

	// Present with one set of bytes: satisfied.
	writeFixture(t, root, "testdata/salty/archive.7z", "salt-A-bytes")
	missing, err = missingPaths(root, ledger, wanted, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("a present salted fixture must be satisfied, got %v", missing)
	}

	// Regenerated with different bytes AND a different length: still satisfied.
	// This is the case that failed the operator's run before salting existed.
	writeFixture(t, root, "testdata/salty/archive.7z", "salt-B-completely-different-length")
	missing, err = missingPaths(root, ledger, wanted, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("a salted fixture must never be stale for its bytes, got %v", missing)
	}
}

// (b) The guard that just proved itself on 148 fixtures is untouched: a hashed
// entry whose bytes moved is still reported, which is what raises the
// "regenerated to different bytes" hard error.
func TestHashedEntryStillFailsOnAByteMismatch(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "testdata/pinned/archive.7z", "the wrong bytes")
	ledger := &corpus.Ledger{
		Files: []corpus.FileEntry{hashedEntry("testdata/pinned/archive.7z", int64(len("the wrong bytes")))},
	}

	missing, err := missingPaths(root, ledger, []string{"testdata/pinned/archive.7z"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 {
		t.Fatalf("a hashed entry whose digest disagrees must be reported, got %v", missing)
	}
}

// (c) Salting is an escape hatch from content verification, so it must not
// become the lazy way around a genuine reproducibility failure.
func TestSaltedEntryIsRefusedForAReproducibleRecipe(t *testing.T) {
	ledger := &corpus.Ledger{Files: []corpus.FileEntry{saltedEntry("testdata/reprod/archive.7z")}}
	recipes := []Recipe{{Slug: "reprod", ByteReproducible: true}}

	err := ValidateSaltedEntries(ledger, recipes)
	if err == nil {
		t.Fatal("a salted entry under a byte-reproducible recipe must be refused")
	}
	if !strings.Contains(err.Error(), "testdata/reprod/archive.7z") ||
		!strings.Contains(err.Error(), "reprod") {
		t.Fatalf("the error must name the path and the recipe: %v", err)
	}

	// The legal shape passes.
	recipes = []Recipe{{Slug: "reprod", ByteReproducible: false}}
	if err := ValidateSaltedEntries(ledger, recipes); err != nil {
		t.Fatalf("a salted entry under a non-reproducible recipe is legal: %v", err)
	}
}

// A salted entry's committed form must never move: sources.json is itself a
// fingerprint input, so a hash or a size would put the whole corpus identity
// back on a value that changes per machine.
func TestSaltedEntryMayNotCarryASizeOrDigest(t *testing.T) {
	for name, entry := range map[string]corpus.FileEntry{
		"digest": {Path: "testdata/salty/a.7z", Salted: true, BLAKE3: saltedTestDigest},
		"size":   {Path: "testdata/salty/a.7z", Salted: true, Size: 17},
	} {
		ledger := &corpus.Ledger{
			SchemaVersion: corpus.SchemaVersion,
			Toolchains:    "test-corpus/toolchains.json",
			Files:         []corpus.FileEntry{entry},
		}
		err := ledger.Validate(corpus.ToolchainLock{})
		if err == nil || !strings.Contains(err.Error(), "salted entries carry no") {
			t.Fatalf("%s: salted entries must carry neither, got %v", name, err)
		}
	}
}

// The committed ledger is the real subject: exactly the AES fixtures are
// salted, and every one of them belongs to a recipe that admits it cannot pin
// its bytes.
func TestCommittedSaltedEntriesAreOnlyTheSaltedWriters(t *testing.T) {
	ledger, _, err := corpus.LoadLedger(repoRoot)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	if err := ValidateSaltedEntries(ledger, Recipes()); err != nil {
		t.Fatalf("committed ledger violates the salting guard: %v", err)
	}
	for _, file := range ledger.Files {
		if !file.Salted {
			continue
		}
		if !strings.Contains(file.Path, "aes256") {
			t.Errorf("%s is salted but is not an AES fixture; salting is for writers that draw a salt", file.Path)
		}
	}
}
