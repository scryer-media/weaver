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
	abandonedIndex, missingLength, expected, err := feedVerdictOutputOracle(nzb, scenario.ExpectedOutputBLAKE3["survivor.bin"])
	if err != nil {
		return restartCaseResult{}, err
	}
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	// Nyuu may finish either file first and write its NZB entry first.
	// Withhold broken.bin by identity and hold survivor.bin so a committed
	// verdict cannot race terminalization.
	var abandonedIDs, survivorIDs []string
	for _, segment := range segments {
		switch segment.FileIndex {
		case abandonedIndex:
			abandonedIDs = append(abandonedIDs, segment.MessageID)
		case 1 - abandonedIndex:
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
		return committed.Version == 1 && committed.Reason != "" && reflect.DeepEqual(committed.AbandonedFiles, []int{abandonedIndex}), err
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
	persisted, err := readRestartFeedVerdict(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := requireFeedVerdictAbsence(proof, persisted, abandonedIndex, missingLength); err != nil {
		return restartCaseResult{}, err
	}
	if err := withStateDB(dbPath, func(db *stateConn) error {
		var verified int
		if err := db.QueryRow(`SELECT COUNT(*) FROM graph_coverage WHERE job_id = ? AND node_id = 0 AND member_id = ? AND verified = TRUE`, jobID, abandonedIndex).Scan(&verified); err != nil {
			return err
		}
		if verified != 0 {
			return fmt.Errorf("abandoned file has %d verified checkpoint runs", verified)
		}
		var outputDir string
		if err := db.QueryRow(`SELECT output_dir FROM active_jobs WHERE job_id = ?`, jobID).Scan(&outputDir); err != nil {
			return err
		}
		return requireAbsentFeedOutput(outputDir)
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

// No broken-file article is decoded. Its damaged coverage is bounded by the
// posted NZB's encoded sizes, but no physical file may be fabricated for it.
func feedVerdictOutputOracle(nzb []byte, survivorHash string) (int, uint64, map[string]string, error) {
	var post struct {
		Files []struct {
			Subject  string `xml:"subject,attr"`
			Segments []struct {
				Bytes uint64 `xml:"bytes,attr"`
			} `xml:"segments>segment"`
		} `xml:"file"`
	}
	if err := xml.Unmarshal(nzb, &post); err != nil {
		return 0, 0, nil, err
	}
	if len(post.Files) != 2 || survivorHash == "" {
		return 0, 0, nil, fmt.Errorf("feed verdict oracle requires two files and a survivor digest")
	}
	abandonedIndex, survivorIndex := -1, -1
	for index, file := range post.Files {
		switch {
		case strings.Contains(file.Subject, `"broken.bin"`) && abandonedIndex == -1:
			abandonedIndex = index
		case strings.Contains(file.Subject, `"survivor.bin"`) && survivorIndex == -1:
			survivorIndex = index
		default:
			return 0, 0, nil, fmt.Errorf("unexpected or duplicate feed fixture subject %q", file.Subject)
		}
	}
	if abandonedIndex < 0 || survivorIndex < 0 || len(post.Files[abandonedIndex].Segments) == 0 {
		return 0, 0, nil, fmt.Errorf("feed verdict oracle requires broken.bin and survivor.bin")
	}
	var length uint64
	for _, segment := range post.Files[abandonedIndex].Segments {
		// This recipe contains a 64 KiB source; reject an unexpected oversized
		// fixture before accepting its independently computed damaged bounds.
		if segment.Bytes == 0 || segment.Bytes > 1<<20 || length > 1<<20-segment.Bytes {
			return 0, 0, nil, fmt.Errorf("unexpected feed fixture encoded size")
		}
		length += segment.Bytes
	}
	return abandonedIndex, length, map[string]string{"survivor.bin": survivorHash}, nil
}

func requireFeedVerdictAbsence(proof checkpointEvidence, verdict restartFeedVerdict, index int, length uint64) error {
	if verdict.Version != 1 || verdict.Reason == "" || !reflect.DeepEqual(verdict.AbandonedFiles, []int{index}) {
		return fmt.Errorf("missing persisted abandoned-file verdict: %+v", verdict)
	}
	memberFound := false
	for _, member := range proof.Members {
		if member.FileIndex == index {
			if member.HasFile || member.OutputName != "broken.bin" || member.OnDisk != "" {
				return fmt.Errorf("abandoned file has unexpected backing ownership: %+v", member)
			}
			memberFound = true
		}
	}
	if _, backed := proof.Backing["broken.bin"]; !memberFound || backed {
		return fmt.Errorf("abandoned file must have a member and no backing file")
	}
	for _, row := range proof.Coverage {
		if row.FileIndex != index {
			continue
		}
		if row.Length == nil || *row.Length != length || !row.Coverage.Covers(0, length) {
			return fmt.Errorf("abandoned file does not cover its encoded damaged bounds")
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

func requireAbsentFeedOutput(outputDir string) error {
	_, err := os.Lstat(filepath.Join(outputDir, "broken.bin"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("abandoned file has an unexpected fabricated broken.bin")
}
