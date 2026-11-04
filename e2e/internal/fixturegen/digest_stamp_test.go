// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// countCorpusHashes counts the files a digest check reads through BLAKE3.
func countCorpusHashes(t *testing.T) *int {
	t.Helper()
	count := 0
	previous := hashCorpusFile
	hashCorpusFile = func(path string) (corpus.FileDigest, error) {
		count++
		return previous(path)
	}
	t.Cleanup(func() { hashCorpusFile = previous })
	return &count
}

func stampedCorpus(t *testing.T) (root string, ledger *corpus.Ledger, wanted []string) {
	t.Helper()
	root = ensureRoot(t, map[string]string{
		"testdata/alpha/archive.rar": "alpha bytes",
		"testdata/beta/archive.rar":  "beta bytes",
	})
	wanted = []string{"testdata/alpha/archive.rar", "testdata/beta/archive.rar"}
	// Fixed times, so nothing depends on the filesystem's clock resolution.
	stamped := time.Date(2032, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, path := range wanted {
		if err := os.Chtimes(corpus.HostPath(root, path), stamped, stamped); err != nil {
			t.Fatal(err)
		}
	}
	ledger, _, err := corpus.LoadLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	return root, ledger, wanted
}

func checkDigests(t *testing.T, root string, ledger *corpus.Ledger, wanted []string) []string {
	t.Helper()
	missing, err := missingPaths(root, ledger, wanted, true)
	if err != nil {
		t.Fatal(err)
	}
	return missing
}

func TestDigestStampPassesAnUnchangedCorpusWithoutReadingIt(t *testing.T) {
	root, ledger, wanted := stampedCorpus(t)
	hashes := countCorpusHashes(t)

	if missing := checkDigests(t, root, ledger, wanted); len(missing) != 0 {
		t.Fatalf("missing = %v", missing)
	}
	if *hashes != 2 {
		t.Fatalf("first check hashed %d files, want both", *hashes)
	}
	if missing := checkDigests(t, root, ledger, wanted); len(missing) != 0 {
		t.Fatalf("missing = %v", missing)
	}
	if *hashes != 2 {
		t.Fatalf("second check hashed %d more files, want none", *hashes-2)
	}
}

func TestDigestStampRehashesAFileWhoseTimeMoved(t *testing.T) {
	root, ledger, wanted := stampedCorpus(t)
	checkDigests(t, root, ledger, wanted)
	hashes := countCorpusHashes(t)

	// Same size, new bytes, new time: only the digest can tell.
	alpha := corpus.HostPath(root, "testdata/alpha/archive.rar")
	if err := os.WriteFile(alpha, []byte("ALPHA BYTES"), 0o644); err != nil {
		t.Fatal(err)
	}
	moved := time.Date(2032, 6, 2, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(alpha, moved, moved); err != nil {
		t.Fatal(err)
	}

	missing := checkDigests(t, root, ledger, wanted)
	if len(missing) != 1 || missing[0] != "testdata/alpha/archive.rar" {
		t.Fatalf("missing = %v, want alpha", missing)
	}
	if *hashes != 1 {
		t.Fatalf("hashed %d files, want only alpha", *hashes)
	}
	// A file that failed stays unstamped: the next check reads it again.
	checkDigests(t, root, ledger, wanted)
	if *hashes != 2 {
		t.Fatalf("hashed %d files after the failure, want alpha again", *hashes)
	}
}

func TestDigestStampRehashesAFileWhoseSizeMoved(t *testing.T) {
	root, ledger, wanted := stampedCorpus(t)
	checkDigests(t, root, ledger, wanted)
	stamp := loadDigestStamp(root, mustLedgerDigest(t, ledger))
	hashes := countCorpusHashes(t)

	// The ledger and the file agree on a size the stamp did not record (the
	// file was replaced by one of the ledger's size since the stamp was
	// written): the stamp no longer vouches, so the file is read.
	alpha := corpus.HostPath(root, "testdata/alpha/archive.rar")
	info, err := os.Stat(alpha)
	if err != nil {
		t.Fatal(err)
	}
	entry := stamp.Entries["testdata/alpha/archive.rar"]
	entry.Size++
	stamp.Entries["testdata/alpha/archive.rar"] = entry
	want := corpus.DigestBytes([]byte("alpha bytes"))
	matches, err := stamp.matches("testdata/alpha/archive.rar", alpha, info, want)
	if err != nil || !matches {
		t.Fatalf("matches = %v, %v", matches, err)
	}
	if *hashes != 1 {
		t.Fatalf("hashed %d files, want alpha", *hashes)
	}
	if stamp.Entries["testdata/alpha/archive.rar"].Size != info.Size() {
		t.Fatal("the re-hash did not restamp the file's size")
	}
	var _ fs.FileInfo = info
}

func TestDigestStampRehashesEverythingWhenTheLedgerChanges(t *testing.T) {
	root, ledger, wanted := stampedCorpus(t)
	checkDigests(t, root, ledger, wanted)
	hashes := countCorpusHashes(t)

	// Any edit to the ledger, even one that leaves these entries alone,
	// voids the stamp.
	ledger.Files[0].Source.Notes = "edited"
	if missing := checkDigests(t, root, ledger, wanted); len(missing) != 0 {
		t.Fatalf("missing = %v", missing)
	}
	if *hashes != 2 {
		t.Fatalf("hashed %d files after the ledger changed, want both", *hashes)
	}
}

func mustLedgerDigest(t *testing.T, ledger *corpus.Ledger) string {
	t.Helper()
	digest, err := ledgerDigest(ledger)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
