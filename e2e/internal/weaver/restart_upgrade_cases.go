// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// downloadThenInterrupt stops a single-file download once its checkpoint covers
// at least `floor` bytes, and returns the job and the ids served before the
// stop.
func (ctx *restartCaseContext) downloadThenInterrupt(slug string, floor uint64, extraEnv ...string) (int, restartNntpMetrics, error) {
	if err := ctx.startWeaverWithOptions("", 8, extraEnv...); err != nil {
		return 0, restartNntpMetrics{}, err
	}
	if err := setNntpChaos("slow_body=1"); err != nil {
		return 0, restartNntpMetrics{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return 0, restartNntpMetrics{}, err
	}
	dbPath := caseDBPath(ctx)
	deadline := time.Now().Add(ctx.Timeout)
	reached := false
	for time.Now().Before(deadline) && !reached {
		if status, ok := restartJobStatusFromDB(dbPath, jobID); ok && dbStatusTerminal(normalizeDBStatus(status)) {
			return 0, restartNntpMetrics{}, fmt.Errorf("job %d ended before its checkpoint covered %d bytes", jobID, floor)
		}
		if rows, err := readJobFileCoverage(dbPath, jobID); err == nil {
			for _, row := range rows {
				if row.Coverage.CoveredBytes() >= floor {
					reached = true
				}
			}
		}
		if !reached {
			time.Sleep(chaosPollInterval)
		}
	}
	if !reached {
		return 0, restartNntpMetrics{}, fmt.Errorf("job %d never covered %d bytes", jobID, floor)
	}
	if err := ctx.interruptWeaverAndWaitForExit(); err != nil {
		return 0, restartNntpMetrics{}, err
	}
	_ = setNntpChaos("off")
	_, _, pre, err := ctx.captureEvidence("pre_restart", "")
	return jobID, pre, err
}

// R4. A database last written before migration 65 holds a prefix floor, not
// coverage, and no graph checkpoint: the graph tables came later. The upgrade
// drops the floor, and with no checkpoint the job is admitted afresh, so the
// partly downloaded file downloads again from nothing: a floor that claims
// more than was written is never trusted.
func runPre65CheckpointRowRefused(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "single-mkv"
	const checkpointBytes = 4 * 1024 * 1024
	checkpointEnv := fmt.Sprintf("WEAVER_E2E_DOWNLOAD_RESTART_CHECKPOINT_BYTES=%d", checkpointBytes)
	jobID, pre, err := ctx.downloadThenInterrupt(slug, 4*checkpointBytes, checkpointEnv)
	if err != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", err.Error()), nil
	}
	source := filepath.Join(testdataDir(), slug, "test-media.mkv")
	info, err := os.Stat(source)
	if err != nil {
		return restartCaseResult{}, err
	}
	// The old shape, as migration 21 (SQLite) or the 25 baseline (Postgres)
	// created it and 64 left it, with a floor that claims all but the last
	// megabyte: far more than the stop wrote.
	wrongFloor := info.Size() - 1024*1024
	db, datastore, err := openWeaverStateDB(caseDBPath(ctx))
	if err != nil {
		return restartCaseResult{}, err
	}
	statements := []string{
		`DROP TABLE active_file_progress`,
		pre65ActiveFileProgressDDL(datastore),
		fmt.Sprintf(`INSERT INTO active_file_progress (job_id, file_index, contiguous_bytes_written) VALUES (%d, 0, %d)`, jobID, wrongFloor),
		`DELETE FROM _sqlx_migrations WHERE version = 65`,
	}
	// A database from before migration 65 holds no graph checkpoint, so the
	// job's is taken away with the rest of what came later.
	for _, table := range graphTables {
		present, err := weaverStateTableExists(db, datastore, table)
		if err != nil {
			closeWeaverStateDB(db, datastore)
			return restartCaseResult{}, err
		}
		if present {
			statements = append(statements, fmt.Sprintf(`DELETE FROM %s WHERE job_id = %d`, table, jobID))
		}
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			closeWeaverStateDB(db, datastore)
			return restartCaseResult{}, fmt.Errorf("plant the pre-65 row: %s: %w", statement, err)
		}
	}
	closeWeaverStateDB(db, datastore)

	if err := ctx.restartWeaverWithEnv(checkpointEnv); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, post, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	reapplied := false
	if db, datastore, err := openWeaverStateDB(caseDBPath(ctx)); err == nil {
		var count int
		if db.QueryRow(rebindWeaverSQL(datastore, `SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 65 AND success`)).Scan(&count) == nil {
			reapplied = count == 1
		}
		closeWeaverStateDB(db, datastore)
	}
	servedBefore, refetched := 0, 0
	for id, count := range pre.BodyCounts {
		if count == 0 {
			continue
		}
		servedBefore++
		if post.BodyCounts[id] > count {
			refetched++
		}
	}
	want, err := fileBLAKE3(source)
	if err != nil {
		return restartCaseResult{}, err
	}
	outputOK := false
	for _, output := range completedFilesBySize(info.Size()) {
		if got, err := fileBLAKE3(output); err == nil && got == want {
			outputOK = true
		}
	}
	detail := fmt.Sprintf("planted_floor=%d migration_65_reapplied=%t served_before=%d refetched_after=%d final=%s output_ok=%t",
		wrongFloor, reapplied, servedBefore, refetched, statuses[jobID], outputOK)
	pass := reapplied && servedBefore > 0 && refetched == servedBefore && statuses[jobID] == "COMPLETE" && outputOK
	return classifyRestartResult(ctx.Profile, pass, "a pre-65 floor was refused and the file downloaded again from nothing: "+detail, false, "", "pre-65 checkpoint row: "+detail), nil
}

// pre65ActiveFileProgressDDL is the prefix-floor table each engine held before
// migration 65: SQLite's from migration 21, Postgres's from its 25 baseline.
func pre65ActiveFileProgressDDL(datastore weaverDatastore) string {
	if datastore == weaverDatastorePostgres {
		return `CREATE TABLE active_file_progress (
    job_id                  BIGINT NOT NULL,
    file_index              BIGINT NOT NULL,
    contiguous_bytes_written BIGINT NOT NULL,
    PRIMARY KEY (job_id, file_index)
)`
	}
	return `CREATE TABLE active_file_progress (
    job_id                  INTEGER NOT NULL,
    file_index              INTEGER NOT NULL,
    contiguous_bytes_written INTEGER NOT NULL,
    PRIMARY KEY (job_id, file_index)
) WITHOUT ROWID`
}

var ypartLine = regexp.MustCompile(`^=ypart begin=(\d+) end=(\d+)`)

// replaceArticleExtent files the article again with its `=ypart end` moved by
// `delta`, and returns the call that files the original back.
func replaceArticleExtent(host, port, messageID string, delta int64) (func() error, error) {
	session, err := openNntpCommandSession(host, port, true)
	if err != nil {
		return nil, err
	}
	original, err := readStoredArticle(session, messageID)
	session.close()
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(original, []byte("\r\n"))
	moved := false
	for i, line := range lines {
		match := ypartLine.FindSubmatch(line)
		if match == nil {
			continue
		}
		end, _ := strconv.ParseInt(string(match[2]), 10, 64)
		lines[i] = []byte(fmt.Sprintf("=ypart begin=%s end=%d", match[1], end+delta))
		moved = true
		break
	}
	if !moved {
		return nil, fmt.Errorf("<%s> has no =ypart line", messageID)
	}
	if err := deleteArticleIDsAt(host, port, []string{messageID}); err != nil {
		return nil, err
	}
	poisoned := withheldArticle{messageID: messageID, wire: bytes.Join(lines, []byte("\r\n"))}
	if err := restoreWithheldArticles(host, port, []withheldArticle{poisoned}); err != nil {
		return nil, err
	}
	return func() error {
		if err := deleteArticleIDsAt(host, port, []string{messageID}); err != nil {
			return err
		}
		return restoreWithheldArticles(host, port, []withheldArticle{{messageID: messageID, wire: original}})
	}, nil
}

// R5. One article's yEnc extent runs into its neighbour. A Ctrl-C mid-file
// and a restart must keep that to the one article: its neighbours decode,
// the job ends repaired or partial, and no cascade of layout conflicts
// follows.
func runPoisonExtentDoesNotSpread(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "par2-small-repair"
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	subjects, err := nzbFileSubjects(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	payload := -1
	for index, subject := range subjects {
		if strings.Contains(subject, ".rar") {
			payload = index
		}
	}
	var inFile []nzbSegmentRef
	for _, segment := range segments {
		if segment.FileIndex == payload {
			inFile = append(inFile, segment)
		}
	}
	sort.Slice(inFile, func(i, j int) bool { return inFile[i].Number < inFile[j].Number })
	if len(inFile) < 6 {
		return restartCaseResult{}, fmt.Errorf("%s payload has %d segments; need at least 6", slug, len(inFile))
	}
	poison := inFile[2]
	restore, err := replaceArticleExtent(nntpHost(), nntpPort(), poison.MessageID, 4096)
	if err != nil {
		return restartCaseResult{}, err
	}
	defer func() { _ = restore() }()

	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	if err := setNntpChaos("slow_body=1"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ids := segmentIDs(inFile)
	if _, started, err := ctx.waitForBodies(jobID, ids, len(ids)/2); err != nil {
		return restartCaseResult{}, err
	} else if !started {
		return classifyRestartResult(ctx.Profile, false, "", false, "", "the job ended before the mid-file stop"), nil
	}
	if err := ctx.interruptWeaverAndWaitForExit(); err != nil {
		return restartCaseResult{}, err
	}
	_ = setNntpChaos("off")
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_restart", "")
	full, _ := os.ReadFile(ctx.caseLogPath)
	conflicts := 0
	named := map[string]bool{}
	segmentName := regexp.MustCompile(`segment (\S+) claims`)
	for _, line := range strings.Split(stripANSIEscapeSequences(string(full)), "\n") {
		if !strings.Contains(line, "yEnc layout conflict") {
			continue
		}
		conflicts++
		if match := segmentName.FindStringSubmatch(line); match != nil {
			named[match[1]] = true
		}
	}
	var namedList []string
	for name := range named {
		namedList = append(namedList, name)
	}
	sort.Strings(namedList)
	final := statuses[jobID]
	detail := fmt.Sprintf("poisoned_segment=%d of %d conflict_lines=%d segments_named=%v final=%s",
		poison.Number, len(inFile), conflicts, namedList, final)
	pass := len(named) <= 1 && (final == "COMPLETE" || final == "PARTIAL")
	return classifyRestartResult(ctx.Profile, pass, "a poisoned extent stayed with its own article across a restart: "+detail, false, "", "poison extent: "+detail), nil
}
