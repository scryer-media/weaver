// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"path/filepath"
	"testing"
)

func TestFileCoverageCoversOnlyWhatItsRunsHold(t *testing.T) {
	coverage := fileCoverage{Runs: []fileCoverageRun{{60, 70, "fresh"}, {40, 60, "fresh"}, {100, 120, "damaged"}}}
	if got := coverage.CoveredBytes(); got != 50 {
		t.Fatalf("covered = %d", got)
	}
	for _, tc := range []struct {
		start, end uint64
		want       bool
	}{
		{40, 70, true},
		{45, 50, true},
		{100, 120, true},
		{30, 50, false},
		{60, 101, false},
		{70, 100, false},
		{50, 50, true},
	} {
		if got := coverage.Covers(tc.start, tc.end); got != tc.want {
			t.Fatalf("covers [%d, %d) = %t, want %t", tc.start, tc.end, got, tc.want)
		}
	}
}

func TestHealthyCoverageDoesNotCoverRecordedDamage(t *testing.T) {
	coverage := fileCoverage{Runs: []fileCoverageRun{
		{0, 10, "fresh"}, {10, 20, "damaged"}, {20, 30, "repaired"},
	}}
	healthy := coverage.Healthy()
	if !coverage.Covers(0, 30) || healthy.Covers(0, 30) || healthy.Covers(10, 20) {
		t.Fatal("recorded damage must occupy a range without satisfying healthy coverage")
	}
	if healthy.CoveredBytes() != 20 || !healthy.Covers(20, 30) {
		t.Fatalf("healthy coverage lost repaired input: %+v", healthy)
	}
}

func TestReadFileCoverageRowsReadsTheArticleRoot(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "weaver.db")
	db := openTestWeaverStateDB(t, dbPath)
	defer db.Close()

	// No graph tables: a database that has never held a graph checkpoint.
	rows, err := readFileCoverageRows(db, weaverDatastoreSQLite)
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows = %+v, err = %v", rows, err)
	}

	mustExecWeaverStateSQL(t, db, `CREATE TABLE graph_members (job_id INTEGER NOT NULL, node_id INTEGER NOT NULL, member_id INTEGER NOT NULL, name TEXT NOT NULL, length INTEGER)`)
	mustExecWeaverStateSQL(t, db, `CREATE TABLE graph_coverage (job_id INTEGER NOT NULL, node_id INTEGER NOT NULL, member_id INTEGER NOT NULL, range_start INTEGER NOT NULL, range_end INTEGER NOT NULL, provenance TEXT NOT NULL)`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_members VALUES
		(7, 0, 0, 'a.rar', 300),
		(7, 0, 1, 'b.rar', NULL),
		(7, 0, 2, 'c.rar', 100),
		(7, 3, 0, 'inner.mkv', 100),
		(8, 0, 0, 'a.rar', 100)`)
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_coverage VALUES
		(7, 0, 0, 200, 300, 'fresh'),
		(7, 0, 0, 0, 100, 'fresh'),
		(7, 0, 1, 0, 50, 'fresh'),
		(7, 3, 0, 0, 100, 'fresh'),
		(8, 0, 0, 0, 100, 'repaired')`)

	rows, err = readFileCoverageRows(db, weaverDatastoreSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("rows = %+v, want the four root members", rows)
	}
	first := rows[0]
	if first.JobID != 7 || first.FileIndex != 0 || first.Name != "a.rar" || first.Length == nil || *first.Length != 300 {
		t.Fatalf("first row = %+v", first)
	}
	if first.Coverage.CoveredBytes() != 200 || first.Whole() || first.Coverage.Covers(50, 250) {
		t.Fatalf("a member with a hole reads as whole: %+v", first)
	}
	if rows[1].Length != nil || rows[1].Whole() || rows[1].Coverage.CoveredBytes() != 50 {
		t.Fatalf("a member of unknown length = %+v", rows[1])
	}
	if rows[2].Coverage.CoveredBytes() != 0 || rows[2].Whole() {
		t.Fatalf("a member with no runs = %+v", rows[2])
	}
	if rows[3].JobID != 8 || !rows[3].Whole() || rows[3].Coverage.Runs[0].Provenance != "repaired" {
		t.Fatalf("another job's member = %+v", rows[3])
	}

	// Its last hole filled, the first member is whole.
	mustExecWeaverStateSQL(t, db, `INSERT INTO graph_coverage VALUES (7, 0, 0, 100, 200, 'fresh')`)
	rows, err = readFileCoverageRows(db, weaverDatastoreSQLite)
	if err != nil {
		t.Fatal(err)
	}
	if !rows[0].Whole() {
		t.Fatalf("first row = %+v, want whole", rows[0])
	}
}
