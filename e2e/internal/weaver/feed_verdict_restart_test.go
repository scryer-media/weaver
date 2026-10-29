// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFeedVerdictOracleDerivesDamageFromPostedSizes(t *testing.T) {
	broken := `<file subject="&quot;broken.bin&quot;"><segments><segment bytes="5">a</segment><segment bytes="7">b</segment></segments></file>`
	survivor := `<file subject="&quot;survivor.bin&quot;"><segments><segment bytes="19">c</segment></segments></file>`
	for index, files := range []string{broken + survivor, survivor + broken} {
		nzb := []byte("<nzb>" + files + "</nzb>")
		gotIndex, length, expected, err := feedVerdictOutputOracle(nzb, "known-survivor-hash")
		if err != nil {
			t.Fatal(err)
		}
		if gotIndex != index || length != 12 || len(expected) != 1 || expected["broken.bin"] != "" || expected["survivor.bin"] != "known-survivor-hash" {
			t.Fatalf("input-derived oracle: index=%d, length=%d, hashes=%v", gotIndex, length, expected)
		}
	}
}

func TestFeedVerdictRequiresPersistedDamageWithoutBacking(t *testing.T) {
	for _, index := range []int{0, 1} {
		for _, bad := range []string{"", "verdict", "wrong file", "member", "wrong name", "backing", "owned", "coverage", "bounds", "verified"} {
			t.Run(fmt.Sprintf("%d/%s", index, bad), func(t *testing.T) {
				length := uint64(12)
				verdict := restartFeedVerdict{Version: 1, Reason: "missing", AbandonedFiles: []int{index}}
				proof := checkpointEvidence{
					Members:  []checkpointMember{{FileIndex: index, OutputName: "broken.bin"}},
					Coverage: []fileCoverageRow{{FileIndex: index, Length: &length, Coverage: fileCoverage{Runs: []fileCoverageRun{{Start: 0, End: 12, Provenance: "damaged"}}}}},
				}
				switch bad {
				case "verdict":
					verdict = restartFeedVerdict{}
				case "wrong file":
					verdict.AbandonedFiles = []int{1 - index}
				case "wrong name":
					proof.Members[0].OutputName = "survivor.bin"
				case "member":
					proof.Members = nil
				case "backing":
					proof.Backing = map[string]checkpointBacking{"broken.bin": {Size: 12}}
				case "owned":
					proof.Members[0].HasFile = true
				case "coverage":
					proof.Coverage = nil
				case "bounds":
					proof.Coverage[0].Coverage.Runs[0].End = 11
				case "verified":
					proof.Coverage[0].Coverage.Runs[0].Provenance = "fresh"
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
	if err := os.WriteFile(filepath.Join(dir, "broken.bin"), make([]byte, 12), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := requireAbsentFeedOutput(dir); err == nil {
		t.Fatal("fabricated zero-filled output was accepted")
	}
}
