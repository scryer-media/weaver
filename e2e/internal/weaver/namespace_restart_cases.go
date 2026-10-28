// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const namespaceRestartSlug = "par2-namespace-restart"
const namespaceChainSlug = "par3-rename-chain"
const namespaceRestartSentinel = "00-keep.bin"

func namespaceRestartCases() []restartCase {
	return []restartCase{
		{
			Name: "par2_moved_leaf", Description: "A PAR2 leaf moved after committed intent is adopted at its recorded destination",
			Slugs: []string{namespaceRestartSlug},
			Run: func(ctx *restartCaseContext) (restartCaseResult, error) {
				return runNamespaceRestart(ctx, namespaceRestartSlug, "a.bin", false, false, false)
			},
		},
		{
			Name: "par2_missing_moved_leaf", Description: "A missing moved PAR2 leaf is reconstructed without refetching surviving durable inputs",
			Slugs: []string{namespaceRestartSlug},
			Run: func(ctx *restartCaseContext) (restartCaseResult, error) {
				return runNamespaceRestart(ctx, namespaceRestartSlug, "a.bin", true, false, false)
			},
		},
		{
			Name: "par3_rename_chain", Description: "Committed PAR3 rename chains preserve both files across a case-folded name collision and crash",
			Slugs: []string{namespaceChainSlug},
			Run: func(ctx *restartCaseContext) (restartCaseResult, error) {
				return runNamespaceRestart(ctx, namespaceChainSlug, "lantern.bin", false, true, false)
			},
		},
		{
			Name: "par2_destination_collision", Description: "A committed move fails without replacing an unrelated destination or losing its source",
			Slugs: []string{namespaceRestartSlug},
			Run: func(ctx *restartCaseContext) (restartCaseResult, error) {
				return runNamespaceRestart(ctx, namespaceRestartSlug, "a.bin", false, false, true)
			},
		},
	}
}

type namespaceRestartMove struct {
	Node       int    `json:"node"`
	Member     int    `json:"member"`
	WorkingDir string `json:"working_dir"`
	OnDisk     string `json:"on_disk"`
	Wanted     string `json:"wanted"`
	BLAKE3     string `json:"blake3"`
}

// The failpoint alone is insufficient: require a committed ownership record
// naming both locations and the expected bytes at its desired location.
func readNamespaceIntent(dbPath string, jobID int, wanted string) (namespaceRestartMove, error) {
	var move namespaceRestartMove
	err := withStateDB(dbPath, func(db *stateConn) error {
		if err := db.QueryRow(`SELECT output_dir FROM active_jobs WHERE job_id = ?`, jobID).Scan(&move.WorkingDir); err != nil {
			return err
		}
		return db.QueryRow(`SELECT node_id, member_id, on_disk, output_name FROM graph_members
			WHERE job_id = ? AND has_file = TRUE AND output_name = ? AND on_disk IS NOT NULL AND on_disk <> output_name`, jobID, wanted).
			Scan(&move.Node, &move.Member, &move.OnDisk, &move.Wanted)
	})
	if err != nil {
		return move, fmt.Errorf("committed rename intent for %q: %w", wanted, err)
	}
	for _, path := range []string{move.OnDisk, move.Wanted} {
		if !filepath.IsLocal(path) || path == "." {
			return move, fmt.Errorf("non-local recorded move path %q", path)
		}
	}
	return move, nil
}

func readNamespaceMove(dbPath string, jobID int, wantedName, expected string) (namespaceRestartMove, error) {
	move, err := readNamespaceIntent(dbPath, jobID, wantedName)
	if err != nil {
		return move, err
	}
	if _, err := os.Lstat(filepath.Join(move.WorkingDir, move.OnDisk)); !os.IsNotExist(err) {
		return move, fmt.Errorf("old recorded location %q must be absent after the move, stat=%v", move.OnDisk, err)
	}
	wanted := filepath.Join(move.WorkingDir, move.Wanted)
	info, err := os.Lstat(wanted)
	if err != nil {
		return move, err
	}
	if !info.Mode().IsRegular() {
		return move, fmt.Errorf("recorded destination is not a regular leaf")
	}
	move.BLAKE3, err = fileBLAKE3(wanted)
	if err != nil {
		return move, err
	}
	if move.BLAKE3 != expected {
		return move, fmt.Errorf("moved %s digest %s, expected %s", move.Wanted, move.BLAKE3, expected)
	}
	return move, nil
}

// Binding order is concurrent: the first move may vacate the occupied name
// or place the other leaf at its temporary suffix. Inspect only committed
// intents and identify the actual moved payload by its fixture digest.
func readNamespaceChainMove(dbPath string, jobID int, expected map[string]string) (namespaceRestartMove, string, error) {
	var names []string
	err := withStateDB(dbPath, func(db *stateConn) error {
		rows, err := db.Query(`SELECT output_name FROM graph_members WHERE job_id = ? AND has_file = TRUE AND on_disk IS NOT NULL AND on_disk <> output_name ORDER BY node_id, member_id`, jobID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			names = append(names, name)
		}
		return rows.Err()
	})
	if err != nil {
		return namespaceRestartMove{}, "", err
	}
	var moved []namespaceRestartMove
	var identities []string
	for _, name := range names {
		intent, err := readNamespaceIntent(dbPath, jobID, name)
		if err != nil {
			return namespaceRestartMove{}, "", err
		}
		if _, err := os.Lstat(filepath.Join(intent.WorkingDir, intent.OnDisk)); err == nil {
			continue // This committed intent has not moved yet.
		} else if !os.IsNotExist(err) {
			return namespaceRestartMove{}, "", err
		}
		for _, identity := range []string{"beacon.bin", "lantern.bin"} {
			if expected[identity] == "" {
				return namespaceRestartMove{}, "", fmt.Errorf("rename chain has no %s digest", identity)
			}
			move, err := readNamespaceMove(dbPath, jobID, name, expected[identity])
			if err == nil {
				moved = append(moved, move)
				identities = append(identities, identity)
			}
		}
	}
	if len(moved) != 1 {
		return namespaceRestartMove{}, "", fmt.Errorf("first-move crash has %d proven moved chain payloads, expected one; recorded destinations=%v", len(moved), names)
	}
	return moved[0], identities[0], nil
}

// Recovery indexes remain available to open download admission. The posted
// leaves and recovery volumes wait while this immutable plain input is
// committed. That makes the surviving-input assertion independent of lanes,
// decode scheduling, or the speed of the later PAR2 rename.
func committedNamespaceSentinel(dbPath string, jobID, sentinelIndex int, expected string, segments []nzbSegmentRef, ranges map[string]decodedRange) (bool, error) {
	var workingDir, name string
	var hasFile bool
	err := withStateDB(dbPath, func(db *stateConn) error {
		return db.QueryRow(`SELECT j.output_dir, m.output_name, m.has_file FROM active_jobs j
			JOIN graph_members m ON m.job_id = j.job_id WHERE j.job_id = ? AND m.node_id = 0 AND m.member_id = ?`, jobID, sentinelIndex).
			Scan(&workingDir, &name, &hasFile)
	})
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if name != namespaceRestartSentinel {
		return false, fmt.Errorf("fixture sentinel is %q, expected %q", name, namespaceRestartSentinel)
	}
	if !hasFile {
		return false, nil
	}
	coverage, err := readJobFileCoverage(dbPath, jobID)
	if err != nil {
		return false, err
	}
	for _, segment := range segments {
		if segment.FileIndex != sentinelIndex {
			continue
		}
		decoded, ok := ranges[segment.MessageID]
		if !ok || decoded.End <= decoded.Start {
			return false, fmt.Errorf("sentinel article has no decoded range")
		}
		covered := false
		for _, row := range coverage {
			covered = covered || row.FileIndex == sentinelIndex && row.Coverage.Healthy().Covers(decoded.Start, decoded.End)
		}
		if !covered {
			return false, nil
		}
	}
	digest, err := fileBLAKE3(filepath.Join(workingDir, name))
	if err != nil {
		return false, err
	}
	return digest == expected, nil
}

func namespaceBootstrapFiles(subjects []string) (int, map[int]bool, error) {
	sentinel, indexes := -1, 0
	allowed := map[int]bool{}
	for index, subject := range subjects {
		if strings.Contains(subject, `"`+namespaceRestartSentinel+`"`) {
			if sentinel != -1 {
				return 0, nil, fmt.Errorf("namespace fixture has duplicate sentinels")
			}
			sentinel = index
			allowed[index] = true
		}
		if strings.Contains(subject, `"set.par2"`) || strings.Contains(subject, `"set.par3"`) {
			allowed[index] = true
			indexes++
		}
	}
	if sentinel == -1 || indexes != 1 {
		return 0, nil, fmt.Errorf("namespace fixture needs one named sentinel and one recovery index")
	}
	return sentinel, allowed, nil
}

func runNamespaceRestart(ctx *restartCaseContext, slug, movedName string, removeMoved, chain, collision bool) (restartCaseResult, error) {
	scenario := ctx.Scenarios[slug]
	if scenario == nil || scenario.ExpectedOutputBLAKE3[movedName] == "" || scenario.ExpectedOutputBLAKE3[namespaceRestartSentinel] == "" {
		return restartCaseResult{}, fmt.Errorf("namespace restart fixture has no exact output oracle")
	}
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ranges, err := segmentDecodedRanges(nntpHost(), nntpPort(), segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	subjects, err := nzbFileSubjects(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	sentinel, bootstrap, err := namespaceBootstrapFiles(subjects)
	if err != nil {
		return restartCaseResult{}, err
	}
	var held []string
	for _, segment := range segments {
		if !bootstrap[segment.FileIndex] {
			held = append(held, segment.MessageID)
		}
	}
	if len(held) == 0 || len(held) == len(segments) {
		return restartCaseResult{}, fmt.Errorf("namespace fixture must contain sentinel and recovery inputs")
	}
	if err := ctx.startWeaverWithOptions("extract.after_rename_before_record", 8); err != nil {
		return restartCaseResult{}, err
	}
	if err := setNntpChaos("hold_ids=" + strings.Join(held, ";")); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	if err := ctx.pollFor("committed namespace sentinel", func() (bool, error) {
		return committedNamespaceSentinel(dbPath, jobID, sentinel, scenario.ExpectedOutputBLAKE3[namespaceRestartSentinel], segments, ranges)
	}); err != nil {
		return restartCaseResult{}, err
	}
	if err := liftPrimaryHold(); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.waitForCrash(ctx.Timeout, jobID); err != nil {
		return restartCaseResult{}, err
	}
	var move namespaceRestartMove
	identity := movedName
	if chain {
		move, identity, err = readNamespaceChainMove(dbPath, jobID, scenario.ExpectedOutputBLAKE3)
	} else {
		move, err = readNamespaceMove(dbPath, jobID, movedName, scenario.ExpectedOutputBLAKE3[movedName])
	}
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "committed_namespace_move.json"), struct {
		Move     namespaceRestartMove `json:"move"`
		Identity string               `json:"payload_identity"`
	}{move, identity})
	before, err := readCheckpointEvidence(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_crash_checkpoint.json"), before)
	provenBefore := directDurableArticles(segments, ranges, before)
	proven := provenBefore
	var collisionExpected map[string]string
	if collision {
		// Recreate the pre-move state named by the committed intent, then put
		// unrelated bytes at its desired destination. Only recorded paths are
		// touched; the source must remain available for an explicit retry.
		if filepath.Dir(move.OnDisk) != "." {
			return restartCaseResult{}, fmt.Errorf("collision fixture source is not a posted leaf")
		}
		source, wanted := filepath.Join(move.WorkingDir, move.OnDisk), filepath.Join(move.WorkingDir, move.Wanted)
		if err := os.Rename(wanted, source); err != nil {
			return restartCaseResult{}, err
		}
		file, err := os.OpenFile(wanted, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return restartCaseResult{}, err
		}
		_, writeErr := file.WriteString("unrelated destination fixture\n")
		closeErr := file.Close()
		if writeErr != nil {
			return restartCaseResult{}, writeErr
		}
		if closeErr != nil {
			return restartCaseResult{}, closeErr
		}
		digest, err := fileBLAKE3(wanted)
		if err != nil {
			return restartCaseResult{}, err
		}
		collisionExpected = map[string]string{move.OnDisk: move.BLAKE3, move.Wanted: digest, namespaceRestartSentinel: scenario.ExpectedOutputBLAKE3[namespaceRestartSentinel]}
		writeRestartJSON(filepath.Join(ctx.CaseDir, "namespace_collision_expected.json"), collisionExpected)
		after, err := readCheckpointEvidence(dbPath, jobID)
		if err != nil {
			return restartCaseResult{}, err
		}
		writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_restart_collision_checkpoint.json"), after)
		proven = directDurableArticles(segments, ranges, after)
	}
	if removeMoved {
		for _, path := range []string{move.OnDisk, move.Wanted} {
			if err := os.Remove(filepath.Join(move.WorkingDir, path)); err != nil && !os.IsNotExist(err) {
				return restartCaseResult{}, err
			}
		}
		after, err := readCheckpointEvidence(dbPath, jobID)
		if err != nil {
			return restartCaseResult{}, err
		}
		writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_restart_missing_leaf_checkpoint.json"), after)
		proven = directDurableArticles(segments, ranges, after)
		if len(proven) >= len(provenBefore) {
			return restartCaseResult{}, fmt.Errorf("deleting recorded leaf did not remove any proven durable input")
		}
	}
	if len(proven) == 0 {
		return restartCaseResult{}, fmt.Errorf("namespace restart has no surviving proven durable inputs")
	}
	_, _, preMetrics, err := ctx.captureEvidence("pre_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	_, _, metrics, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	refetched := refetchedArticles(proven, preMetrics, metrics)
	outputErr := assertExactOutputBLAKE3(dbPath, jobID, scenario.ExpectedOutputBLAKE3, false)
	wantedStatus := "COMPLETE"
	operationalFailure := true
	if collision {
		wantedStatus = "FAILED"
		outputErr = assertOutputBLAKE3(dbPath, jobID, collisionExpected)
		if outputErr == nil {
			outputErr = assertForbiddenOutputPaths(dbPath, jobID, []string{".weaver-chunks"})
		}
		operationalFailure = logContains(ctx.caseLogPath, "graph job step failed")
	}
	detail := fmt.Sprintf("deleted_recorded_leaf=%t collision=%t durable_before=%d surviving_durable=%d refetched=%v output=%v operational_error=%t final=%s", removeMoved, collision, len(provenBefore), len(proven), refetched, outputErr, operationalFailure, statuses[jobID])
	pass := statuses[jobID] == wantedStatus && len(refetched) == 0 && outputErr == nil && operationalFailure
	return classifyRestartResult(ctx.Profile, pass, "recorded namespace recovery: "+detail, false, "", "recorded namespace recovery failed: "+detail), nil
}
