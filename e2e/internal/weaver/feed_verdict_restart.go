// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/zeebo/blake3"
)

func feedVerdictRestartCases() []restartCase {
	return []restartCase{{
		Name: "committed_feed_verdict", Slugs: []string{"feed-verdict-restart"},
		Description: "A committed abandoned-file verdict survives restart while healthy files finish",
		Run:         runCommittedFeedVerdictRestart,
	}}
}

type restartFeedVerdict struct {
	Version        int    `json:"version"`
	Reason         string `json:"reason"`
	AbandonedFiles []int  `json:"abandoned_files"`
}

func readRestartFeedVerdict(dbPath string, jobID int) (restartFeedVerdict, error) {
	var verdict restartFeedVerdict
	err := withStateDB(dbPath, func(db *stateConn) error {
		var value string
		if err := db.QueryRow(`SELECT COALESCE(feed_verdict, '') FROM active_jobs WHERE job_id = ?`, jobID).Scan(&value); err != nil {
			return err
		}
		if value == "" {
			return nil
		}
		return json.Unmarshal([]byte(value), &verdict)
	})
	return verdict, err
}

func runCommittedFeedVerdictRestart(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "feed-verdict-restart"
	scenario := ctx.Scenarios[slug]
	if scenario == nil || len(scenario.ExpectedOutputBLAKE3) != 1 || scenario.ExpectedOutputBLAKE3["survivor.bin"] == "" {
		return restartCaseResult{}, fmt.Errorf("feed verdict fixture requires the survivor digest")
	}
	nzb, err := os.ReadFile(filepath.Join(fixturesDir(), slug, slug+".nzb"))
	if err != nil {
		return restartCaseResult{}, err
	}
	missingLength, expected, err := feedVerdictOutputOracle(nzb, scenario.ExpectedOutputBLAKE3["survivor.bin"])
	if err != nil {
		return restartCaseResult{}, err
	}
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	// The recipe posts broken.bin before survivor.bin. Withhold the former,
	// and hold the latter so a committed verdict cannot race terminalization.
	var abandonedIDs, survivorIDs []string
	for _, segment := range segments {
		switch segment.FileIndex {
		case 0:
			abandonedIDs = append(abandonedIDs, segment.MessageID)
		case 1:
			survivorIDs = append(survivorIDs, segment.MessageID)
		default:
			return restartCaseResult{}, fmt.Errorf("unexpected feed fixture file %d", segment.FileIndex)
		}
	}
	if len(abandonedIDs) < 2 || len(survivorIDs) < 2 {
		return restartCaseResult{}, fmt.Errorf("feed fixture is not two multipart files")
	}
	putBack, err := withholdArticles(nntpHost(), nntpPort(), abandonedIDs)
	if err != nil {
		return restartCaseResult{}, err
	}
	restored := false
	defer func() {
		if !restored {
			_ = putBack()
		}
	}()
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	if err := setNntpChaos("hold_ids=" + strings.Join(survivorIDs, ";")); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	var committed restartFeedVerdict
	if err := ctx.pollFor("committed abandoned-file verdict", func() (bool, error) {
		committed, err = readRestartFeedVerdict(dbPath, jobID)
		return committed.Version == 1 && committed.Reason != "" && reflect.DeepEqual(committed.AbandonedFiles, []int{0}), err
	}); err != nil {
		return restartCaseResult{}, err
	}
	if err := pauseQueueItemGraphQL(ctx.weaverURL, jobID); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.pollFor("persisted pause after feed verdict", func() (bool, error) {
		status, ok := restartJobStatusFromDB(dbPath, jobID)
		return ok && status == "PAUSED", nil
	}); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	<-ctx.weaver.done
	proof, err := readCheckpointEvidence(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "feed_checkpoint.json"), proof)
	writeRestartJSON(filepath.Join(ctx.CaseDir, "feed_verdict.json"), committed)
	if err := requireFeedVerdictPlaceholder(proof, missingLength, expected["broken.bin"]); err != nil {
		return restartCaseResult{}, err
	}
	if err := withStateDB(dbPath, func(db *stateConn) error {
		var verified int
		if err := db.QueryRow(`SELECT COUNT(*) FROM graph_coverage WHERE job_id = ? AND node_id = 0 AND member_id = 0 AND verified = TRUE`, jobID).Scan(&verified); err != nil {
			return err
		}
		if verified != 0 {
			return fmt.Errorf("abandoned file has %d verified checkpoint runs", verified)
		}
		return nil
	}); err != nil {
		return restartCaseResult{}, err
	}
	// Make abandoned articles available again: a missing-file response after
	// restart must not hide a wrongly dispatched fetch.
	if err := putBack(); err != nil {
		return restartCaseResult{}, err
	}
	restored = true
	before, err := adversarialBodyCounts(abandonedIDs)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	afterRestart, err := readRestartFeedVerdict(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	if !reflect.DeepEqual(committed, afterRestart) {
		return restartCaseResult{}, fmt.Errorf("feed verdict changed across paused restart: before=%+v after=%+v", committed, afterRestart)
	}
	if err := resumeQueueItemGraphQL(ctx.weaverURL, jobID); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	after, err := adversarialBodyCounts(abandonedIDs)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "feed_body_counts.json"), map[string]any{"before": before, "after": after})
	if !reflect.DeepEqual(before, after) {
		return restartCaseResult{}, fmt.Errorf("abandoned files fetched after committed verdict: before=%v after=%v", before, after)
	}
	if statuses[jobID] != "PARTIAL" {
		return restartCaseResult{}, fmt.Errorf("feed verdict job ended %s, expected PARTIAL with whole survivor", statuses[jobID])
	}
	if err := assertExactOutputBLAKE3(dbPath, jobID, expected, false); err != nil {
		return restartCaseResult{}, err
	}
	return restartCaseResult{Classification: restartPass, Summary: "Committed feed verdict restored before dispatch; abandoned articles were not fetched after becoming available; healthy survivor delivered whole"}, nil
}

// No broken-file article is decoded, so its damaged placeholder is bounded
// by the posted NZB's encoded sizes. Derive its zero bytes from those inputs,
// never from a checkpoint or output that the assertion is meant to verify.
func feedVerdictOutputOracle(nzb []byte, survivorHash string) (uint64, map[string]string, error) {
	var post struct {
		Files []struct {
			Segments []struct {
				Bytes uint64 `xml:"bytes,attr"`
			} `xml:"segments>segment"`
		} `xml:"file"`
	}
	if err := xml.Unmarshal(nzb, &post); err != nil {
		return 0, nil, err
	}
	if len(post.Files) != 2 || len(post.Files[0].Segments) == 0 || survivorHash == "" {
		return 0, nil, fmt.Errorf("feed verdict oracle requires two files and a survivor digest")
	}
	var length uint64
	for _, segment := range post.Files[0].Segments {
		// This recipe contains a 64 KiB source; reject an unexpected oversized
		// fixture before allocating its independently computed zero image.
		if segment.Bytes == 0 || segment.Bytes > 1<<20 || length > 1<<20-segment.Bytes {
			return 0, nil, fmt.Errorf("unexpected feed fixture encoded size")
		}
		length += segment.Bytes
	}
	return length, map[string]string{
		"broken.bin":   fmt.Sprintf("%x", blake3.Sum256(make([]byte, length))),
		"survivor.bin": survivorHash,
	}, nil
}

func requireFeedVerdictPlaceholder(proof checkpointEvidence, length uint64, hash string) error {
	memberFound := false
	for _, member := range proof.Members {
		if member.FileIndex == 0 {
			memberFound = member.HasFile && member.OutputName == "broken.bin" && member.OnDisk == ""
		}
	}
	backing := proof.Backing["broken.bin"]
	if !memberFound || backing.Problem != "" || backing.Size != int64(length) || backing.BLAKE3 != hash {
		return fmt.Errorf("committed feed verdict lacks the expected recorded damaged placeholder: %+v", backing)
	}
	for _, row := range proof.Coverage {
		if row.FileIndex != 0 {
			continue
		}
		if row.Length == nil || *row.Length != length || !row.Coverage.Covers(0, length) {
			return fmt.Errorf("abandoned file does not cover its encoded placeholder bounds")
		}
		for _, run := range row.Coverage.Runs {
			if run.Provenance != "damaged" || run.End <= run.Start || run.End > length {
				return fmt.Errorf("abandoned file has non-damaged coverage: %+v", run)
			}
		}
		return nil
	}
	return fmt.Errorf("committed feed verdict has no abandoned-file coverage")
}
