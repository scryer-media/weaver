// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

type checkpointBacking struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	BLAKE3  string `json:"blake3,omitempty"`
	Problem string `json:"problem,omitempty"`
}

type checkpointMember struct {
	FileIndex  int    `json:"file_index"`
	HasFile    bool   `json:"has_file"`
	OutputName string `json:"output_name"`
	OnDisk     string `json:"on_disk,omitempty"`
}

type checkpointEvidence struct {
	Tables   map[string][]map[string]any  `json:"tables"`
	Members  []checkpointMember           `json:"root_members"`
	Backing  map[string]checkpointBacking `json:"backing"`
	Coverage []fileCoverageRow            `json:"coverage"`
}

// Called after the process exits. Record only checkpoint-owned paths; an
// unrecorded scratch file is neither evidence nor an adoption candidate.
func readCheckpointEvidence(dbPath string, jobID int) (checkpointEvidence, error) {
	evidence := checkpointEvidence{Tables: map[string][]map[string]any{}, Backing: map[string]checkpointBacking{}}
	coverage, err := readJobFileCoverage(dbPath, jobID)
	if err != nil {
		return evidence, err
	}
	evidence.Coverage = coverage
	var workingDir string
	paths := map[string]bool{}
	err = withStateDB(dbPath, func(db *stateConn) error {
		if err := db.QueryRow(`SELECT output_dir FROM active_jobs WHERE job_id = ?`, jobID).Scan(&workingDir); err != nil {
			return err
		}
		for _, table := range graphTables {
			rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s WHERE job_id = ?`, table), jobID)
			if err != nil {
				return err
			}
			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				return err
			}
			for rows.Next() {
				values := make([]any, len(columns))
				dest := make([]any, len(columns))
				for i := range values {
					dest[i] = &values[i]
				}
				if err := rows.Scan(dest...); err != nil {
					rows.Close()
					return err
				}
				record := map[string]any{}
				for i, name := range columns {
					if bytes, ok := values[i].([]byte); ok {
						values[i] = append([]byte(nil), bytes...)
					}
					record[name] = values[i]
				}
				evidence.Tables[table] = append(evidence.Tables[table], record)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
		rows, err := db.Query(`SELECT member_id, has_file, output_name, on_disk FROM graph_members WHERE job_id = ? AND node_id = 0 ORDER BY member_id`, jobID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var member checkpointMember
			var onDisk sql.NullString
			if err := rows.Scan(&member.FileIndex, &member.HasFile, &member.OutputName, &onDisk); err != nil {
				rows.Close()
				return err
			}
			member.OnDisk = onDisk.String
			evidence.Members = append(evidence.Members, member)
			if member.HasFile {
				paths[member.OutputName] = true
				if member.OnDisk != "" {
					paths[member.OnDisk] = true
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = db.Query(`SELECT COALESCE(file, '.weaver-chunks/graph-envelope-' || CAST(node_id AS TEXT) || '-' || CAST(volume AS TEXT)) FROM graph_envelopes WHERE job_id = ?
			UNION SELECT '.weaver-chunks/graph-holds-' || CAST(node_id AS TEXT) || '.scratch' FROM graph_holds WHERE job_id = ?
			UNION SELECT spool_path FROM graph_held_parts WHERE job_id = ?
			UNION SELECT output_name FROM graph_members WHERE job_id = ? AND has_file = TRUE
			UNION SELECT on_disk FROM graph_members WHERE job_id = ? AND has_file = TRUE AND on_disk IS NOT NULL`, jobID, jobID, jobID, jobID, jobID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				return err
			}
			paths[path] = true
		}
		return rows.Err()
	})
	if err != nil {
		return evidence, err
	}
	for path := range paths {
		proof := checkpointBacking{Path: path}
		if !filepath.IsLocal(path) {
			proof.Problem = "non-local recorded path"
		} else {
			full := filepath.Join(workingDir, path)
			info, err := os.Lstat(full)
			if err != nil {
				proof.Problem = err.Error()
			} else if !info.Mode().IsRegular() {
				proof.Problem = "recorded path is not a regular file"
			} else {
				proof.Size = info.Size()
				proof.BLAKE3, err = fileBLAKE3(full)
				if err != nil {
					proof.Problem = err.Error()
				}
			}
		}
		evidence.Backing[path] = proof
	}
	return evidence, nil
}

// This is deliberately a conservative oracle for direct root files. The
// Rust restore regressions exercise envelope inverse maps and held parts;
// the Go harness records that evidence without reimplementing the graph.
func directDurableArticles(segments []nzbSegmentRef, ranges map[string]decodedRange, evidence checkpointEvidence) map[string]bool {
	proven := map[string]bool{}
	for _, member := range evidence.Members {
		if !member.HasFile {
			continue
		}
		path := member.OnDisk
		backing, exists := evidence.Backing[path]
		if path == "" || !exists || backing.Problem != "" {
			backing, exists = evidence.Backing[member.OutputName]
		}
		if !exists || backing.Problem != "" || backing.BLAKE3 == "" || backing.Size < 0 {
			continue
		}
		for _, row := range evidence.Coverage {
			if row.FileIndex != member.FileIndex {
				continue
			}
			for _, segment := range segments {
				decoded, known := ranges[segment.MessageID]
				if known && segment.FileIndex == member.FileIndex && decoded.End > decoded.Start && uint64(backing.Size) >= decoded.End && row.Coverage.Healthy().Covers(decoded.Start, decoded.End) {
					proven[segment.MessageID] = true
				}
			}
		}
	}
	return proven
}
