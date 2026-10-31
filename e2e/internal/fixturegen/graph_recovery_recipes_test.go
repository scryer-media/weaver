// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

func TestUnrelatedPAR2XZDamagePreservesRecognitionAndRejectsHeader(t *testing.T) {
	header := []byte{'\xfd', '7', 'z', 'X', 'Z', 0, 0, 1, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(header[8:], crc32.ChecksumIEEE(header[6:8]))
	path := filepath.Join(t.TempDir(), "posted.xz")
	if err := os.WriteFile(path, header, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := corruptXZHeaderChecksum(path); err != nil {
		t.Fatal(err)
	}
	damaged := read(t, path)
	if !bytes.Equal(damaged[:8], header[:8]) {
		t.Fatal("fixture damage changed the XZ signature or stream flags")
	}
	if binary.LittleEndian.Uint32(damaged[8:]) == crc32.ChecksumIEEE(damaged[6:8]) {
		t.Fatal("fixture header still passes XZ integrity verification")
	}
	if err := corruptXZHeaderChecksum(path); err == nil {
		t.Fatal("an already damaged header must not be silently toggled back to valid")
	}
}

func TestRecoveryRecordCorpusAcceptsVariableArchivesAndPinsLooseFiles(t *testing.T) {
	ledger, _, err := corpus.LoadLedger(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	slugs := map[string]bool{
		"rar4-record-repair":            true,
		"rar4-record-and-volume-repair": true,
		"rar5-record-repair":            true,
		"rar5-record-and-volume-repair": true,
		"par2-unrelated-set":            true,
	}
	for _, recipe := range Recipes() {
		if !slugs[recipe.Slug] {
			continue
		}
		if recipe.ByteReproducible || recipe.ExpectedOutputs == nil {
			t.Errorf("%s must allow variable archive bytes and pin its usable outputs", recipe.Slug)
		}
		delete(slugs, recipe.Slug)
	}
	if len(slugs) != 0 {
		t.Fatalf("missing recovery record recipes: %v", slugs)
	}
	root := t.TempDir()
	var archives, pinned, wanted []string
	for _, entry := range ledger.Files {
		slug, _ := slugOfPath(entry.Path)
		if slug != "par2-unrelated-set" && !strings.HasSuffix(slug, "-record-repair") && !strings.HasSuffix(slug, "-record-and-volume-repair") {
			continue
		}
		variable := strings.HasSuffix(entry.Path, ".rar") || strings.HasSuffix(entry.Path, ".rev")
		if entry.Salted != variable {
			t.Errorf("%s: salted=%t, want %t", entry.Path, entry.Salted, variable)
		}
		wanted = append(wanted, entry.Path)
		if variable {
			archives = append(archives, entry.Path)
			continue
		}
		pinned = append(pinned, entry.Path)
		// Match the size so only the digest check can reject these bytes.
		writeFixture(t, root, entry.Path, strings.Repeat("x", int(entry.Size)))
	}
	if len(archives) != 9 || len(pinned) != 3 {
		t.Fatalf("recovery record corpus has %d variable archives and %d pinned files, want 9 and 3", len(archives), len(pinned))
	}
	missing, err := missingPaths(root, ledger, archives, true)
	if err != nil || len(missing) != len(archives) {
		t.Fatalf("absent recovery archives must still be missing: %v, %v", missing, err)
	}
	for _, contents := range []string{"first recovery record", "regenerated recovery data with a different size"} {
		for _, path := range archives {
			writeFixture(t, root, path, contents)
		}
		missing, err := missingPaths(root, ledger, wanted, true)
		if err != nil {
			t.Fatal(err)
		}
		if !equal(missing, pinned) {
			t.Fatalf("only the unrelated payload and PAR2 digests should mismatch: got %v, want %v", missing, pinned)
		}
	}
}

func TestUnicodeRecoveryRejectsGeneratorCrashArtifacts(t *testing.T) {
	env := &Env{Work: t.TempDir()}
	for _, name := range []string{"payload.bin", "回復.par3", "回復.vol0+4.par3"} {
		if err := os.WriteFile(env.OutputPath(name), []byte{1}, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateUnicodeRecoveryOutputs(env, 3, "payload.bin"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(env.OutputPath("qemu_par3_fixture.core"), []byte{1}, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateUnicodeRecoveryOutputs(env, 3, "payload.bin"); err == nil {
		t.Fatal("generator crash artifact was accepted as a posted source")
	}
}
