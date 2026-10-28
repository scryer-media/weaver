// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"sort"
)

// A posted file's restart checkpoint as the graph engine keeps it. The job's
// tree has the article root at node 0, and each posted file is one of the
// root's members, numbered by its index in the job. graph_coverage holds the
// decoded byte runs of each member that are on disk, and graph_members its
// length once the post has told it. The harness reads them to know exactly
// which bytes a restart may trust, so a case can name the articles a restart
// has to fetch again instead of guessing from byte counts.
const graphRootNode = 0

type fileCoverageRun struct {
	Start      uint64 `json:"start"`
	End        uint64 `json:"end"`
	Provenance string `json:"provenance"`
}

type fileCoverage struct {
	Runs []fileCoverageRun `json:"runs"`
}

// Healthy excludes recorded damage: damaged bytes occupy a range but cannot
// satisfy a restart assertion about input the downloader may keep.
func (coverage fileCoverage) Healthy() fileCoverage {
	var healthy fileCoverage
	for _, run := range coverage.Runs {
		if run.Provenance == "fresh" || run.Provenance == "repaired" {
			healthy.Runs = append(healthy.Runs, run)
		}
	}
	return healthy
}

func (coverage fileCoverage) CoveredBytes() uint64 {
	var total uint64
	for _, run := range coverage.Runs {
		if run.End > run.Start {
			total += run.End - run.Start
		}
	}
	return total
}

// Covers reports whether every byte of [start, end) is in a run.
func (coverage fileCoverage) Covers(start, end uint64) bool {
	runs := append([]fileCoverageRun(nil), coverage.Runs...)
	sort.Slice(runs, func(i, j int) bool { return runs[i].Start < runs[j].Start })
	cursor := start
	for _, run := range runs {
		if cursor >= end {
			break
		}
		if run.Start > cursor {
			return false
		}
		if run.End > cursor {
			cursor = run.End
		}
	}
	return cursor >= end
}

type fileCoverageRow struct {
	JobID     int    `json:"job_id"`
	FileIndex int    `json:"file_index"`
	Name      string `json:"name"`
	// The file's decoded length, nil until the post has told it.
	Length   *uint64      `json:"length,omitempty"`
	Coverage fileCoverage `json:"coverage"`
}

// Whole reports whether every byte of the file is on disk.
func (row fileCoverageRow) Whole() bool {
	return row.Length != nil && row.Coverage.Covers(0, *row.Length)
}

// readFileCoverageRows reads every job's posted files from the graph
// checkpoint in one statement, so a checkpoint written beside it is read
// whole or not at all. A database without the graph tables has none.
func readFileCoverageRows(db *sql.DB, datastore weaverDatastore) ([]fileCoverageRow, error) {
	for _, table := range []string{"graph_members", "graph_coverage"} {
		present, err := weaverStateTableExists(db, datastore, table)
		if err != nil || !present {
			return nil, err
		}
	}
	rows, err := db.Query(rebindWeaverSQL(datastore, `
		SELECT m.job_id, m.member_id, m.name, m.length, c.range_start, c.range_end, c.provenance
		FROM graph_members m
		LEFT JOIN graph_coverage c
		  ON c.job_id = m.job_id AND c.node_id = m.node_id AND c.member_id = m.member_id
		WHERE m.node_id = ?
		ORDER BY m.job_id, m.member_id, c.range_start`), graphRootNode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fileCoverageRow
	for rows.Next() {
		var (
			jobID, member int
			name          string
			length        sql.NullInt64
			start, end    sql.NullInt64
			provenance    sql.NullString
		)
		if err := rows.Scan(&jobID, &member, &name, &length, &start, &end, &provenance); err != nil {
			return nil, err
		}
		if n := len(out); n == 0 || out[n-1].JobID != jobID || out[n-1].FileIndex != member {
			row := fileCoverageRow{JobID: jobID, FileIndex: member, Name: name}
			if length.Valid && length.Int64 >= 0 {
				value := uint64(length.Int64)
				row.Length = &value
			}
			out = append(out, row)
		}
		if start.Valid && end.Valid {
			last := &out[len(out)-1]
			last.Coverage.Runs = append(last.Coverage.Runs, fileCoverageRun{
				Start:      uint64(start.Int64),
				End:        uint64(end.Int64),
				Provenance: provenance.String,
			})
		}
	}
	return out, rows.Err()
}

func readJobFileCoverage(dbPath string, jobID int) ([]fileCoverageRow, error) {
	available, err := weaverStateDBAvailable(dbPath)
	if err != nil || !available {
		return nil, err
	}
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer closeWeaverStateDB(db, datastore)
	all, err := readFileCoverageRows(db, datastore)
	if err != nil {
		return nil, err
	}
	var out []fileCoverageRow
	for _, row := range all {
		if row.JobID == jobID {
			out = append(out, row)
		}
	}
	return out, nil
}
