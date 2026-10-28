// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"testing"

	"github.com/zeebo/blake3"
)

func TestFeedVerdictOracleDerivesDamageFromPostedSizes(t *testing.T) {
	nzb := []byte(`<nzb><file><segments><segment bytes="5">a</segment><segment bytes="7">b</segment></segments></file><file><segments><segment bytes="19">c</segment></segments></file></nzb>`)
	length, expected, err := feedVerdictOutputOracle(nzb, "known-survivor-hash")
	if err != nil {
		t.Fatal(err)
	}
	zeroHash := fmt.Sprintf("%x", blake3.Sum256([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
	if length != 12 || len(expected) != 2 || expected["broken.bin"] != zeroHash || expected["survivor.bin"] != "known-survivor-hash" {
		t.Fatalf("input-derived oracle: length=%d, hashes=%v", length, expected)
	}
	proof := checkpointEvidence{
		Members:  []checkpointMember{{FileIndex: 0, HasFile: true, OutputName: "broken.bin"}},
		Backing:  map[string]checkpointBacking{"broken.bin": {Size: 12, BLAKE3: zeroHash}},
		Coverage: []fileCoverageRow{{FileIndex: 0, Length: &length, Coverage: fileCoverage{Runs: []fileCoverageRun{{Start: 0, End: 12, Provenance: "damaged"}}}}},
	}
	if err := requireFeedVerdictPlaceholder(proof, length, zeroHash); err != nil {
		t.Fatal(err)
	}
	wrong := proof.Backing["broken.bin"]
	wrong.BLAKE3 = fmt.Sprintf("%x", blake3.Sum256([]byte("actual wrong")))
	proof.Backing["broken.bin"] = wrong
	if err := requireFeedVerdictPlaceholder(proof, length, zeroHash); err == nil {
		t.Fatal("observed nonzero bytes were accepted as the expected placeholder")
	}
	wrong.BLAKE3 = zeroHash
	proof.Backing["broken.bin"] = wrong
	proof.Coverage[0].Coverage.Runs[0].Provenance = "fresh"
	if err := requireFeedVerdictPlaceholder(proof, length, zeroHash); err == nil {
		t.Fatal("healthy coverage was accepted as the abandoned-file verdict")
	}
}
