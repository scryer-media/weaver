// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFeedVerdictOracleDerivesLostBoundsFromPostedSizes(t *testing.T) {
	broken := `<file subject="&quot;broken.mkv&quot;"><segments><segment bytes="5">a</segment><segment bytes="7">b</segment></segments></file>`
	survivor := `<file subject="&quot;survivor.mkv&quot;"><segments><segment bytes="19">c</segment></segments></file>`
	for index, files := range []string{broken + survivor, survivor + broken} {
		nzb := []byte("<nzb>" + files + "</nzb>")
		gotIndex, length, expected, err := feedVerdictOutputOracle(nzb, "known-survivor-hash")
		if err != nil {
			t.Fatal(err)
		}
		if gotIndex != index || length != 12 || len(expected) != 1 || expected["broken.mkv"] != "" || expected["survivor.mkv"] != "known-survivor-hash" {
			t.Fatalf("input-derived oracle: index=%d, length=%d, hashes=%v", gotIndex, length, expected)
		}
	}
}

func TestFeedVerdictRequiresPersistedLossWithoutBacking(t *testing.T) {
	for _, index := range []int{0, 1} {
		for _, bad := range []string{"", "verdict", "wrong file", "member", "wrong name", "backing", "owned", "coverage", "bounds", "verified", "damaged"} {
			t.Run(fmt.Sprintf("%d/%s", index, bad), func(t *testing.T) {
				length := uint64(12)
				verdict := restartFeedVerdict{Version: 1, Reason: "missing", AbandonedFiles: []int{index}}
				proof := checkpointEvidence{
					Members:  []checkpointMember{{FileIndex: index, OutputName: "broken.mkv"}},
					Coverage: []fileCoverageRow{{FileIndex: index, Length: &length, Coverage: fileCoverage{Runs: []fileCoverageRun{{Start: 0, End: 12, Provenance: "lost"}}}}},
				}
				switch bad {
				case "verdict":
					verdict = restartFeedVerdict{}
				case "wrong file":
					verdict.AbandonedFiles = []int{1 - index}
				case "wrong name":
					proof.Members[0].OutputName = "survivor.mkv"
				case "member":
					proof.Members = nil
				case "backing":
					proof.Backing = map[string]checkpointBacking{"broken.mkv": {Size: 12}}
				case "owned":
					proof.Members[0].HasFile = true
				case "coverage":
					proof.Coverage = nil
				case "bounds":
					proof.Coverage[0].Coverage.Runs[0].End = 13
				case "verified":
					proof.Coverage[0].Coverage.Runs[0].Provenance = "fresh"
				case "damaged":
					proof.Coverage[0].Coverage.Runs[0].Provenance = "damaged"
				}
				err := requireFeedVerdictAbsence(proof, verdict, index, length)
				if (err == nil) != (bad == "") {
					t.Fatalf("case %q: %v", bad, err)
				}
			})
		}
	}
}

func TestFeedVerdictRejectsFabricatedOutput(t *testing.T) {
	dir := t.TempDir()
	if err := requireAbsentFeedOutput(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.mkv"), make([]byte, 12), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := requireAbsentFeedOutput(dir); err == nil {
		t.Fatal("fabricated zero-filled output was accepted")
	}
}
