// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bytes"
	"testing"
)

func TestDecodeFileCoverageReadsWhatWeaverWrites(t *testing.T) {
	// rmp_serde's encoding of vec![(4u32, 2u32, 40u64, 60u64), (5, 1, 60, 70)].
	body := []byte{0x92, 0x94, 0x04, 0x02, 0x28, 0x3c, 0x94, 0x05, 0x01, 0x3c, 0x46}
	blob := append([]byte{'W', 'F', 'C', 'V', 0x01, 0x00}, body...)
	coverage, err := decodeFileCoverage(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []fileCoverageRun{{4, 2, 40, 60}, {5, 1, 60, 70}}
	if len(coverage.Runs) != len(want) {
		t.Fatalf("runs = %+v", coverage.Runs)
	}
	for i := range want {
		if coverage.Runs[i] != want[i] {
			t.Fatalf("run %d = %+v, want %+v", i, coverage.Runs[i], want[i])
		}
	}
	if got := coverage.CoveredBytes(); got != 30 {
		t.Fatalf("covered = %d", got)
	}
	if !coverage.Contains(5) || coverage.Contains(6) || coverage.Contains(3) {
		t.Fatalf("contains is wrong for %+v", coverage.Runs)
	}
	if again := encodeFileCoverage(coverage, fileCoverageSchemaVersion); !bytes.Equal(again, blob) {
		t.Fatalf("encode = %x, want %x", again, blob)
	}
}

func TestDecodeFileCoverageWideIntegersRoundTrip(t *testing.T) {
	coverage := fileCoverage{Runs: []fileCoverageRun{{0, 300, 0, 1 << 33}, {70000, 1, 1<<33 + 5, 1<<33 + 700000}}}
	decoded, err := decodeFileCoverage(encodeFileCoverage(coverage, fileCoverageSchemaVersion))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Runs) != 2 || decoded.Runs[0] != coverage.Runs[0] || decoded.Runs[1] != coverage.Runs[1] {
		t.Fatalf("round trip = %+v", decoded.Runs)
	}
}

func TestDecodeFileCoverageRefusesAnotherSchema(t *testing.T) {
	for name, blob := range map[string][]byte{
		"truncated": []byte("WFC"),
		"magic":     []byte("XXXX\x01\x00\x90"),
		"version":   encodeFileCoverage(fileCoverage{}, 2),
		"trailing":  append(encodeFileCoverage(fileCoverage{}, 1), 0x00),
	} {
		if _, err := decodeFileCoverage(blob); err == nil {
			t.Fatalf("%s: decoded", name)
		}
	}
}
