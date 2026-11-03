// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// The digest stamp.
//
// A digest check reads every present corpus file through BLAKE3, gigabytes on
// every run, to learn what the last run already learned. The stamp remembers
// each file it hashed: its size and modification time then, and the digest
// that matched. A file whose size and modification time are what the stamp
// recorded, under the ledger the stamp was written against, passes without
// being read. Anything else is hashed as before: a new file, a file whose
// size or time moved, or every file once the ledger itself changed.
//
// The stamp lives under testdata/, which git ignores, so it is local to the
// checkout whose files it describes.
const digestStampFile = "testdata/.digest-stamp.json"

type digestStamp struct {
	// Ledger is the BLAKE3 of the ledger the entries were checked against.
	Ledger  string                      `json:"ledger"`
	Entries map[string]digestStampEntry `json:"entries"`
}

type digestStampEntry struct {
	Size    int64  `json:"size"`
	ModTime int64  `json:"mtime_ns"`
	BLAKE3  string `json:"blake3"`
}

// digestStampMu keeps two digest checks in one process from writing the
// stamp at once.
var digestStampMu sync.Mutex

// hashCorpusFile is the digest a check falls back to; tests count its calls.
var hashCorpusFile = corpus.DigestFile

// loadDigestStamp reads the stamp for root. A stamp that is missing, cannot
// be read, or was written against another ledger is empty: nothing in it
// vouches for anything.
func loadDigestStamp(root, ledgerDigest string) *digestStamp {
	empty := &digestStamp{Ledger: ledgerDigest, Entries: map[string]digestStampEntry{}}
	contents, err := os.ReadFile(corpus.HostPath(root, digestStampFile))
	if err != nil {
		return empty
	}
	var stamp digestStamp
	if json.Unmarshal(contents, &stamp) != nil || stamp.Ledger != ledgerDigest || stamp.Entries == nil {
		return empty
	}
	return &stamp
}

// matches reports whether the file at host is the one the ledger describes,
// by stat when the stamp vouches for it and by its digest otherwise. A digest
// that matches is stamped; one that does not is dropped from the stamp.
func (stamp *digestStamp) matches(path, host string, info fs.FileInfo, want string) (bool, error) {
	recorded, ok := stamp.Entries[path]
	if ok && recorded.Size == info.Size() && recorded.ModTime == info.ModTime().UnixNano() && recorded.BLAKE3 == want {
		return true, nil
	}
	actual, err := hashCorpusFile(host)
	if err != nil {
		return false, err
	}
	if actual.BLAKE3 != want {
		delete(stamp.Entries, path)
		return false, nil
	}
	stamp.Entries[path] = digestStampEntry{Size: info.Size(), ModTime: info.ModTime().UnixNano(), BLAKE3: actual.BLAKE3}
	return true, nil
}

// save writes the stamp whole or not at all. A stamp that cannot be written
// costs the next run a re-hash, never a wrong answer, so its error is only
// returned for the caller to report.
func (stamp *digestStamp) save(root string) error {
	contents, err := json.Marshal(stamp)
	if err != nil {
		return err
	}
	host := corpus.HostPath(root, digestStampFile)
	if _, err := os.Stat(corpus.HostPath(root, "testdata")); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return corpus.WriteFileAtomic(host, func(w io.Writer) error {
		_, err := w.Write(contents)
		return err
	}, 0o644)
}

// ledgerDigest is the BLAKE3 of the ledger a check runs against, as loaded.
func ledgerDigest(ledger *corpus.Ledger) (string, error) {
	contents, err := json.Marshal(ledger)
	if err != nil {
		return "", err
	}
	return corpus.DigestBytes(contents), nil
}
