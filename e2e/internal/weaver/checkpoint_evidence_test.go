// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDirectDurabilityNeedsRecordedOwnershipHealthyCoverageAndBacking(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hasFile    bool
		provenance string
		size       int64
		problem    string
		onDisk     string
		want       bool
	}{
		{"committed", true, "fresh", 100, "", "", true},
		{"uncommitted envelope", false, "fresh", 100, "", "", false},
		{"damaged", true, "damaged", 100, "", "", false},
		{"repaired", true, "repaired", 100, "", "", true},
		{"short backing", true, "fresh", 99, "", "", false},
		{"deleted backing", true, "fresh", 100, "missing", "", false},
		{"committed move before acknowledgment", true, "fresh", 100, "", "old.bin", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := checkpointEvidence{
				Members:  []checkpointMember{{FileIndex: 0, HasFile: tc.hasFile, OutputName: "input.bin", OnDisk: tc.onDisk}},
				Coverage: []fileCoverageRow{{FileIndex: 0, Coverage: fileCoverage{Runs: []fileCoverageRun{{Start: 0, End: 100, Provenance: tc.provenance}}}}},
				Backing:  map[string]checkpointBacking{"input.bin": {Path: "input.bin", Size: tc.size, BLAKE3: "recorded digest", Problem: tc.problem}},
			}
			proven := directDurableArticles([]nzbSegmentRef{{FileIndex: 0, MessageID: "article"}}, map[string]decodedRange{"article": {Start: 0, End: 100}}, evidence)
			if proven["article"] != tc.want {
				t.Fatalf("proven=%v, want %v", proven, tc.want)
			}
		})
	}
}

func TestCheckpointEvidenceCapturesOnlyRecordedBackingFiles(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, string(weaverDatastoreSQLite))
	root := t.TempDir()
	dbPath := filepath.Join(root, "state.db")
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()
	mustExecWeaverStateSQL(t, db, `CREATE TABLE active_jobs (job_id INTEGER, output_dir TEXT)`)
	if _, err := db.Exec(`INSERT INTO active_jobs VALUES (7, ?)`, root); err != nil {
		t.Fatal(err)
	}
	extra := map[string]string{
		"graph_members":    ", node_id INTEGER, member_id INTEGER, name TEXT, output_name TEXT, length INTEGER, has_file BOOLEAN, on_disk TEXT",
		"graph_coverage":   ", node_id INTEGER, member_id INTEGER, range_start INTEGER, range_end INTEGER, provenance TEXT",
		"graph_envelopes":  ", node_id INTEGER, volume INTEGER, file TEXT",
		"graph_holds":      ", node_id INTEGER",
		"graph_held_parts": ", spool_path TEXT",
	}
	for _, table := range graphTables {
		mustExecWeaverStateSQL(t, db, fmt.Sprintf("CREATE TABLE %s (job_id INTEGER%s)", table, extra[table]))
	}
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_members VALUES (7, 0, 0, 'input.bin', 'input.bin', 5, 1, NULL)`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_members VALUES (7, 2, 0, 'child.bin', 'child.bin', 5, 1, 'old-child.bin')`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_members VALUES (7, 3, 0, 'unowned.bin', 'unowned.bin', 5, 0, NULL)`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_coverage VALUES (7, 0, 0, 0, 5, 'fresh')`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_envelopes VALUES (7, 2, 0, NULL)`)
	if err := os.Mkdir(filepath.Join(root, ".weaver-chunks"), 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"input.bin", "child.bin", "old-child.bin", "unowned.bin", ".weaver-chunks/graph-envelope-2-0", ".weaver-chunks/graph-envelope-99-0"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("bytes"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := readCheckpointEvidence(dbPath, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.Backing) != 4 || evidence.Backing["input.bin"].BLAKE3 == "" || evidence.Backing[".weaver-chunks/graph-envelope-2-0"].BLAKE3 == "" || evidence.Backing["child.bin"].BLAKE3 == "" || evidence.Backing["old-child.bin"].BLAKE3 == "" {
		t.Fatalf("recorded backing missing: %+v", evidence.Backing)
	}
	if _, adopted := evidence.Backing[".weaver-chunks/graph-envelope-99-0"]; adopted {
		t.Fatal("unrecorded scratch was adopted")
	}
	if _, adopted := evidence.Backing["unowned.bin"]; adopted {
		t.Fatal("a member without recorded file ownership was adopted")
	}
}
