// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
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
