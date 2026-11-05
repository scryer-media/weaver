// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import "testing"

func TestCompletedDownloadCheckpointRequiresEveryHealthyFile(t *testing.T) {
	length := uint64(12)
	segments := []nzbSegmentRef{{FileIndex: 0}, {FileIndex: 1}}
	rows := []fileCoverageRow{{FileIndex: 0, Length: &length, Coverage: fileCoverage{Runs: []fileCoverageRun{{Start: 0, End: 12, Provenance: "fresh"}}}}}
	if completedDownloadCheckpoint(segments, rows) {
		t.Fatal("one file does not make the post durable")
	}
	rows = append(rows, fileCoverageRow{FileIndex: 1, Length: &length, Coverage: fileCoverage{Runs: []fileCoverageRun{{Start: 0, End: 12, Provenance: "damaged"}}}})
	if completedDownloadCheckpoint(segments, rows) {
		t.Fatal("damaged coverage is not a completed download")
	}
	rows[1].Coverage.Runs[0].Provenance = "fresh"
	if !completedDownloadCheckpoint(segments, rows) {
		t.Fatal("whole healthy checkpoint was rejected")
	}
	rows[1].Coverage.Runs[0].End--
	if completedDownloadCheckpoint(segments, rows) || completedDownloadCheckpoint(nil, rows) {
		t.Fatal("incomplete checkpoint was accepted")
	}
}

func TestFeedVerdictAllowsUnsettledArticlesWithoutFabricatingCoverage(t *testing.T) {
	verdict := restartFeedVerdict{Version: 1, Reason: "missing", AbandonedFiles: []int{0}}
	proof := checkpointEvidence{Members: []checkpointMember{{FileIndex: 0, OutputName: "broken.mkv"}}, Coverage: []fileCoverageRow{{FileIndex: 0}}}
	if err := requireFeedVerdictAbsence(proof, verdict, 0, 12); err != nil {
		t.Fatal(err)
	}
	proof.Coverage[0].Coverage.Runs = []fileCoverageRun{{Start: 0, End: 5, Provenance: "lost"}}
	if err := requireFeedVerdictAbsence(proof, verdict, 0, 12); err != nil {
		t.Fatal(err)
	}
	proof.Coverage[0].Coverage.Runs[0].Provenance = "fresh"
	if err := requireFeedVerdictAbsence(proof, verdict, 0, 12); err == nil {
		t.Fatal("abandoned undecoded file claimed healthy bytes")
	}
}
