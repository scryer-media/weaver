// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func heldPartRestartCases() []restartCase {
	var cases []restartCase
	for _, shape := range []string{"plain", "rar", "stream"} {
		slug := "held-" + shape + "-uu"
		cases = append(cases, restartCase{
			Name:        "held_parts_" + shape,
			Description: "Committed UU held parts survive a paused restart without refetching",
			Slugs:       []string{slug},
			Run: func(ctx *restartCaseContext) (restartCaseResult, error) {
				return runHeldPartRestart(ctx, slug)
			},
		})
	}
	return cases
}

type heldRestartPart struct {
	FileIndex int    `json:"file_index"`
	Ordinal   uint32 `json:"ordinal"`
	Size      int64  `json:"decoded_len"`
	SpoolPath string `json:"spool_path"`
	BLAKE3    string `json:"blake3"`
	MessageID string `json:"message_id"`
}

type heldRestartEvidence struct {
	WorkingDir string            `json:"working_dir"`
	Parts      []heldRestartPart `json:"parts"`
}

// Only committed ownership rows authorize a spool read. Neither a directory
// scan nor root coverage can establish which out-of-order article is durable.
func readHeldRestartEvidence(dbPath string, jobID int, segments []nzbSegmentRef) (heldRestartEvidence, error) {
	var evidence heldRestartEvidence
	ids := map[[2]uint32]string{}
	for _, segment := range segments {
		ids[[2]uint32{uint32(segment.FileIndex), segment.Ordinal}] = strings.Trim(segment.MessageID, "<>")
	}
	err := withStateDB(dbPath, func(db *stateConn) error {
		if err := db.QueryRow(`SELECT output_dir FROM active_jobs WHERE job_id = ?`, jobID).Scan(&evidence.WorkingDir); err != nil {
			return err
		}
		rows, err := db.Query(`SELECT file_index, ordinal, decoded_len, spool_path FROM graph_held_parts WHERE job_id = ? ORDER BY file_index, ordinal`, jobID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var part heldRestartPart
			if err := rows.Scan(&part.FileIndex, &part.Ordinal, &part.Size, &part.SpoolPath); err != nil {
				return err
			}
			part.MessageID = ids[[2]uint32{uint32(part.FileIndex), part.Ordinal}]
			if part.Ordinal == 0 || part.MessageID == "" || part.Size <= 0 {
				return fmt.Errorf("unexpected held part identity: %+v", part)
			}
			if !filepath.IsLocal(part.SpoolPath) || filepath.Dir(part.SpoolPath) != ".weaver-chunks" || !strings.HasPrefix(filepath.Base(part.SpoolPath), "held-") {
				return fmt.Errorf("held part names an invalid spool path %q", part.SpoolPath)
			}
			full := filepath.Join(evidence.WorkingDir, part.SpoolPath)
			spoolDir, err := os.Lstat(filepath.Dir(full))
			if err != nil {
				return err
			}
			if !spoolDir.IsDir() || spoolDir.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("held spool directory is not a directory of its own")
			}
			info, err := os.Lstat(full)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() != part.Size {
				return fmt.Errorf("held spool %q has mode=%s bytes=%d, recorded=%d", part.SpoolPath, info.Mode(), info.Size(), part.Size)
			}
			part.BLAKE3, err = fileBLAKE3(full)
			if err != nil {
				return err
			}
			evidence.Parts = append(evidence.Parts, part)
		}
		return rows.Err()
	})
	return evidence, err
}

func requireHeldRestartParts(before, after heldRestartEvidence) error {
	if before.WorkingDir != after.WorkingDir {
		return fmt.Errorf("held working directory changed across restart")
	}
	byIdentity := map[[2]uint32]heldRestartPart{}
	for _, part := range after.Parts {
		byIdentity[[2]uint32{uint32(part.FileIndex), part.Ordinal}] = part
	}
	for _, part := range before.Parts {
		if retained, ok := byIdentity[[2]uint32{uint32(part.FileIndex), part.Ordinal}]; !ok || retained != part {
			return fmt.Errorf("held article %s lost its committed row or spool bytes", part.MessageID)
		}
	}
	return nil
}

func runHeldPartRestart(ctx *restartCaseContext, slug string) (restartCaseResult, error) {
	scenario := ctx.Scenarios[slug]
	if scenario == nil || len(scenario.ExpectedOutputBLAKE3) == 0 {
		return restartCaseResult{}, fmt.Errorf("%s has no output digest oracle", slug)
	}
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	var firstIDs []string
	for _, segment := range segments {
		if segment.Ordinal == 0 {
			firstIDs = append(firstIDs, segment.MessageID)
		}
	}
	if len(firstIDs) == 0 || len(segments) <= len(firstIDs) {
		return restartCaseResult{}, fmt.Errorf("%s is not a multipart fixture", slug)
	}
	if err := ctx.startWeaver(""); err != nil {
		return restartCaseResult{}, err
	}
	// The managed case config has only the primary server. Hold each file's
	// first article so every returned UU article must wait for its cursor.
	hold := "hold_ids=" + strings.Join(firstIDs, ";")
	if err := setNntpChaos(hold); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	// This process has exactly one job and no bytes preceding any held first
	// article. The existing buffered-write gauge therefore observes the park,
	// rather than merely a BODY request which could still be in flight.
	if err := ctx.pollFor("decoded UU part waiting for the held prefix", func() (bool, error) {
		response, err := weaverHTTPClient(ctx.weaverURL, ctx.Timeout).Get(adversarialMetricsURL(ctx.weaverURL))
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return false, err
		}
		if response.StatusCode != 200 {
			return false, fmt.Errorf("metrics returned %d", response.StatusCode)
		}
		return prometheusSamplesByLabel(body, "weaver_pipeline_write_buffered_segments", "")[""] > 0, nil
	}); err != nil {
		return restartCaseResult{}, err
	}
	if err := pauseQueueItemGraphQL(ctx.weaverURL, jobID); err != nil {
		return restartCaseResult{}, err
	}
	var paused heldRestartEvidence
	if err := ctx.pollFor("paused job with committed held spool bytes", func() (bool, error) {
		if status, ok := restartJobStatusFromDB(dbPath, jobID); !ok || status != "PAUSED" {
			return false, nil
		}
		paused, err = readHeldRestartEvidence(dbPath, jobID, segments)
		return len(paused.Parts) > 0, err
	}); err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "held_paused.json"), paused)
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	<-ctx.weaver.done
	committed, err := readHeldRestartEvidence(dbPath, jobID, segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "held_committed.json"), committed)
	if err := requireHeldRestartParts(paused, committed); err != nil {
		return restartCaseResult{}, err
	}
	checkpoint, err := readCheckpointEvidence(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "held_checkpoint.json"), checkpoint)
	var committedIDs []string
	for _, part := range committed.Parts {
		committedIDs = append(committedIDs, part.MessageID)
	}
	before, err := adversarialBodyCounts(committedIDs)
	if err != nil {
		return restartCaseResult{}, err
	}
	for _, id := range committedIDs {
		if before[id] == 0 {
			return restartCaseResult{}, fmt.Errorf("committed held article %s was never fetched", id)
		}
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "held_body_counts_before.json"), before)
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	if status, ok := restartJobStatusFromDB(dbPath, jobID); !ok || status != "PAUSED" {
		return restartCaseResult{}, fmt.Errorf("restored held job is %s, expected PAUSED", status)
	}
	restored, err := readHeldRestartEvidence(dbPath, jobID, segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "held_restored.json"), restored)
	if err := requireHeldRestartParts(committed, restored); err != nil {
		return restartCaseResult{}, err
	}
	// Starting Weaver clears fixture chaos; the persisted pause prevents work
	// during that interval. Restore the prefix barrier before explicit resume.
	if err := setNntpChaos(hold); err != nil {
		return restartCaseResult{}, err
	}
	if err := resumeQueueItemGraphQL(ctx.weaverURL, jobID); err != nil {
		return restartCaseResult{}, err
	}
	if err := liftPrimaryHold(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	after, err := adversarialBodyCounts(committedIDs)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "held_body_counts_after.json"), after)
	if !reflect.DeepEqual(before, after) {
		return restartCaseResult{}, fmt.Errorf("committed held articles refetched: before=%v after=%v", before, after)
	}
	if statuses[jobID] != "COMPLETE" {
		return restartCaseResult{}, fmt.Errorf("held job ended %s", statuses[jobID])
	}
	if err := assertExactOutputBLAKE3(dbPath, jobID, scenario.ExpectedOutputBLAKE3, false); err != nil {
		return restartCaseResult{}, err
	}
	outputDir, err := loadJobOutputDir(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := withStateDB(dbPath, func(db *stateConn) error {
		var retained int
		if err := db.QueryRow(`SELECT COUNT(*) FROM graph_held_parts WHERE job_id = ?`, jobID).Scan(&retained); err != nil {
			return err
		}
		if retained != 0 {
			return fmt.Errorf("completed job retained %d held-part rows", retained)
		}
		return nil
	}); err != nil {
		return restartCaseResult{}, err
	}
	for _, part := range committed.Parts {
		for _, dir := range []string{committed.WorkingDir, outputDir} {
			if _, err := os.Lstat(filepath.Join(dir, part.SpoolPath)); !os.IsNotExist(err) {
				return restartCaseResult{}, fmt.Errorf("consumed held spool %q remains or cannot be checked: %v", part.SpoolPath, err)
			}
		}
	}
	return restartCaseResult{Classification: restartPass, Summary: fmt.Sprintf("%s: %d committed held articles survived pause/restart/resume without refetch; exact output hashes and names match; spool consumed", slug, len(committed.Parts))}, nil
}
