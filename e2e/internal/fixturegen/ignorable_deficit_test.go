// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"crypto/md5"
	"path/filepath"
	"testing"
)

func TestIgnorableDeficitHasFiveDistinctMissingBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "info.nfo")
	if err := writeIgnorableSidecar(path); err != nil {
		t.Fatal(err)
	}
	clean := read(t, path)
	seen := map[[16]byte]bool{}
	for offset := 0; offset < len(clean); offset += ignorableSliceSize {
		hash := md5.Sum(clean[offset : offset+ignorableSliceSize])
		if seen[hash] {
			t.Fatalf("block at %d duplicates another block; PAR2 can copy it without parity", offset)
		}
		seen[hash] = true
	}
	if err := ZeroRange(path, ignorableSliceSize, 5*ignorableSliceSize); err != nil {
		t.Fatal(err)
	}
	damaged := read(t, path)
	missing := 0
	for hash := range seen {
		found := false
		for offset := 0; offset < len(damaged); offset += ignorableSliceSize {
			found = found || hash == md5.Sum(damaged[offset:offset+ignorableSliceSize])
		}
		if !found {
			missing++
		}
	}
	if missing != 5 || missing <= 3 {
		t.Fatalf("%d original blocks are unavailable, want five against three recovery blocks", missing)
	}
}
