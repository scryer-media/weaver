// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The NNTP fault cases that pin the 0.16.5 field reports. They run on the
// restart suite's own Weaver process, so a case owns its signals, its log and
// its fixture faults: credentials refused mid-job, a Ctrl-C mid-batch, a
// direct-store set waiting on a volume, and an early gap in a plain file.

// chaosPollInterval paces the harness's reads of product state. Every wait
// ends on that state; the interval is only how often it is read.
const chaosPollInterval = 250 * time.Millisecond

// totalBodiesFor sums the BODY fetches the fixture served for the ids.
func totalBodiesFor(metrics restartNntpMetrics, ids []string) int {
	total := 0
	for _, id := range ids {
		total += metrics.BodyCounts[id]
	}
	return total
}

func segmentIDs(segments []nzbSegmentRef) []string {
	out := make([]string, 0, len(segments))
	for _, segment := range segments {
		out = append(out, segment.MessageID)
	}
	return out
}

// waitForBodies waits until the fixture has served at least `want` bodies of
// the job's articles, or the job ends first.
func (ctx *restartCaseContext) waitForBodies(jobID int, ids []string, want int) (int, bool, error) {
	deadline := time.Now().Add(ctx.Timeout)
	for time.Now().Before(deadline) {
		if status, ok := restartJobStatusFromDB(caseDBPath(ctx), jobID); ok && dbStatusTerminal(normalizeDBStatus(status)) {
			return 0, false, nil
		}
		metrics, err := fetchNntpBodyMetrics("")
		if err != nil {
			return 0, false, err
		}
		if got := totalBodiesFor(metrics, ids); got >= want {
			return got, true, nil
		}
		time.Sleep(chaosPollInterval)
	}
	return 0, false, fmt.Errorf("job %d never had %d bodies served", jobID, want)
}

// logLinesAfter returns the log lines after the first line holding `marker`
// that contain any needle.
func logLinesAfter(log, marker string, needles ...string) (bool, []string) {
	lines := strings.Split(stripANSIEscapeSequences(log), "\n")
	start := -1
	for i, line := range lines {
		if strings.Contains(line, marker) {
			start = i
			break
		}
	}
	if start < 0 {
		return false, nil
	}
	var out []string
	for _, line := range lines[start+1:] {
		for _, needle := range needles {
			if strings.Contains(line, needle) {
				out = append(out, strings.TrimSpace(line))
				break
			}
		}
	}
	return true, out
}

func countLogLines(log string, needles ...string) int {
	count := 0
	for _, line := range strings.Split(stripANSIEscapeSequences(log), "\n") {
		matched := true
		for _, needle := range needles {
			if !strings.Contains(line, needle) {
				matched = false
				break
			}
		}
		if matched {
			count++
		}
	}
	return count
}

// readServerPipeliningDepth reads the BODY depth Weaver persisted for the one
// configured server; "null" when it has not proven one.
func readServerPipeliningDepth(dbPath string) (string, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return "", err
	}
	defer closeWeaverStateDB(db, datastore)
	var depth sql.NullInt64
	if err := db.QueryRow(rebindWeaverSQL(datastore, `SELECT pipelining_depth FROM servers ORDER BY id LIMIT 1`)).Scan(&depth); err != nil {
		return "", err
	}
	if !depth.Valid {
		return "null", nil
	}
	return strconv.FormatInt(depth.Int64, 10), nil
}

// nzbFileSubjects lists the subject of every file in the slug's NZB, in the
// order nzbSegments numbers them.
func nzbFileSubjects(slug string) ([]string, error) {
	nzbData, err := os.ReadFile(filepath.Join(fixturesDir(), slug, slug+".nzb"))
	if err != nil {
		return nil, err
	}
	var doc struct {
		Files []struct {
			Subject string `xml:"subject,attr"`
		} `xml:"file"`
	}
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(doc.Files))
	for _, file := range doc.Files {
		out = append(out, file.Subject)
	}
	return out, nil
}

// C1. The news server starts refusing the account mid-job: every BODY asks
// for authentication again and every AUTHINFO is refused. With the only
// server refusing credentials, the queued articles are retired in one sweep
// and the job settles instead of requeueing.
func runCredentialsRefusedMidJobSettles(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "single-mkv"
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ids := segmentIDs(segments)
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
	served, started, err := ctx.waitForBodies(jobID, ids, 10)
	if err != nil {
		return restartCaseResult{}, err
	}
	if !started {
		return classifyRestartResult(ctx.Profile, false, "", false, "", "the job ended before the refusal could start"), nil
	}
	logBefore, _ := os.ReadFile(ctx.caseLogPath)
	// The refusal also refuses the harness, so the gate is set and cleared
	// over one control session that authenticated before it.
	releaseRefusal, err := holdNntpChaosOnServer(nntpHost(), nntpPort(), "slow_body=1,reauth_body=100,reject_auth=100")
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if releaseErr := releaseRefusal(); releaseErr != nil {
		return restartCaseResult{}, releaseErr
	}
	if err != nil {
		_, _, _, _ = ctx.captureEvidence("pre_timeout", "")
		return classifyRestartResult(ctx.Profile, false, "", false, "", fmt.Sprintf("the job never settled once credentials were refused: %v", err)), nil
	}
	_, _, post, err := ctx.captureEvidence("post_refusal", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	full, _ := os.ReadFile(ctx.caseLogPath)
	after := string(full[len(logBefore):])
	jobField := fmt.Sprintf("job_id=%d", jobID)
	sweeps := countLogLines(after, "no server may fetch these queued articles", jobField)
	refusals := countLogLines(after, "refusing_credentials=[0]")
	final := statuses[jobID]
	detail := fmt.Sprintf("bodies_before_refusal=%d bodies_total=%d of %d sweep_lines=%d lines_naming_the_refusing_server=%d final=%s",
		served, totalBodiesFor(post, ids), len(ids), sweeps, refusals, final)
	pass := sweeps == 1 && final != "COMPLETE" && final != ""
	return classifyRestartResult(ctx.Profile, pass, "credentials refused mid-job retired the queue in one sweep and the job settled: "+detail, false, "", "credentials refused mid-job: "+detail), nil
}

// C2. The operator stops Weaver while pipelined batches are in flight. The
// batches the shutdown cuts short are not the server's fault: no unclean
// batch is recorded, the persisted depth is untouched, and no stall dump runs.
func runShutdownMidBatchIsNotAnUncleanBatch(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "single-mkv"
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ids := segmentIDs(segments)
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
	if _, started, err := ctx.waitForBodies(jobID, ids, 30); err != nil {
		return restartCaseResult{}, err
	} else if !started {
		return classifyRestartResult(ctx.Profile, false, "", false, "", "the job ended before the shutdown point"), nil
	}
	dbPath := caseDBPath(ctx)
	depthBefore, err := readServerPipeliningDepth(dbPath)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.interruptWeaverAndWaitForExit(); err != nil {
		return restartCaseResult{}, err
	}
	firstRun, _ := os.ReadFile(ctx.caseLogPath)
	depthAfterExit, err := readServerPipeliningDepth(dbPath)
	if err != nil {
		return restartCaseResult{}, err
	}
	signalSeen, offending := logLinesAfter(string(firstRun), "received shutdown signal",
		"download pipeline batch was unclean",
		"server pinned to sequential BODY fetches",
		"stall diagnostics:")
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	depthAfterRestart, err := readServerPipeliningDepth(dbPath)
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_restart", "")
	detail := fmt.Sprintf("signal_logged=%t lines_after_signal=%d depth_before=%s depth_after_exit=%s depth_after_restart=%s final=%s",
		signalSeen, len(offending), depthBefore, depthAfterExit, depthAfterRestart, statuses[jobID])
	pass := signalSeen && len(offending) == 0 && depthAfterExit == depthBefore && depthAfterRestart == depthBefore && statuses[jobID] == "COMPLETE"
	failure := "shutdown mid-batch: " + detail
	if len(offending) > 0 {
		failure += fmt.Sprintf(" first lines: %q", offending[:min(len(offending), 3)])
	}
	return classifyRestartResult(ctx.Profile, pass, "a shutdown mid-batch recorded no unclean batch and kept the depth: "+detail, false, "", failure), nil
}

// C4. One article early in a plain file is missing. The rest of the file
// still downloads while it is missing: an early gap is no reason to hold the
// job back.
func runEarlyGapDoesNotThrottle(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "single-mkv"
	const gapNumber = 3
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	var gapID string
	var rest []string
	for _, segment := range segments {
		if segment.Number == gapNumber && segment.FileIndex == 0 {
			gapID = segment.MessageID
		} else {
			rest = append(rest, segment.MessageID)
		}
	}
	if gapID == "" {
		return restartCaseResult{}, fmt.Errorf("%s has no segment %d", slug, gapNumber)
	}
	putBack, err := withholdArticles(nntpHost(), nntpPort(), []string{gapID})
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
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	// Every other article of the file is served while the gap is still
	// missing, or the job ends first.
	deadline := time.Now().Add(ctx.Timeout)
	everyOther := false
	endedFirst := ""
	servedRest := 0
	for time.Now().Before(deadline) {
		metrics, err := fetchNntpBodyMetrics("")
		if err != nil {
			return restartCaseResult{}, err
		}
		servedRest = 0
		for _, id := range rest {
			if metrics.BodyCounts[id] > 0 {
				servedRest++
			}
		}
		if servedRest == len(rest) {
			everyOther = true
			break
		}
		if status, ok := restartJobStatusFromDB(caseDBPath(ctx), jobID); ok && dbStatusTerminal(normalizeDBStatus(status)) {
			endedFirst = normalizeDBStatus(status)
			break
		}
		time.Sleep(chaosPollInterval)
	}
	if err := putBack(); err != nil {
		return restartCaseResult{}, err
	}
	restored = true
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_gap", "")
	detail := fmt.Sprintf("served_past_gap=%d of %d ended_before_that=%q final=%s", servedRest, len(rest), endedFirst, statuses[jobID])
	return classifyRestartResult(ctx.Profile, everyOther, "the file downloaded past an early gap while the gap was missing: "+detail, false, "", "early gap throttled the job: "+detail), nil
}

// Q5. A damaged job needs its recovery data and the server is slow to send
// it. Once the payload is in, the item reads FETCHING_REPAIR_DATA while the
// recovery articles are in flight, not Queued or Downloading; with them in it
// repairs and completes. A withheld article cannot stand in for a slow one:
// the server answers it 430 and the job books it missing at once.
func runRepairDataWaitReadsFetchingRepairData(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "par2-small-repair"
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	// Weaver's start resets the server's chaos, so the slow server is set after it.
	if err := setNntpChaos("slow_body=1"); err != nil {
		return restartCaseResult{}, err
	}
	restored := false
	defer func() {
		if !restored {
			_ = setNntpChaos("off")
		}
	}()
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	var seen []string
	note := func(state string) {
		if len(seen) == 0 || seen[len(seen)-1] != state {
			seen = append(seen, state)
		}
	}
	deadline := time.Now().Add(ctx.Timeout)
	waiting := false
	for time.Now().Before(deadline) && !waiting {
		snapshot, err := fetchFacadeItemSnapshot(ctx.weaverURL, jobID)
		if err == nil && snapshot.Found {
			state := strings.ToUpper(snapshot.Status)
			note(state)
			if state == "FETCHING_REPAIR_DATA" {
				waiting = true
				break
			}
			if facadeTerminalStatus(state) || !snapshot.InQueue {
				break
			}
		}
		time.Sleep(chaosPollInterval)
	}
	if err := setNntpChaos("off"); err != nil {
		return restartCaseResult{}, err
	}
	restored = true
	beforeRelease := append([]string(nil), seen...)
	for time.Now().Before(deadline) {
		snapshot, err := fetchFacadeItemSnapshot(ctx.weaverURL, jobID)
		if err == nil && snapshot.Found {
			note(strings.ToUpper(snapshot.Status))
			if facadeTerminalStatus(snapshot.Status) || !snapshot.InQueue {
				break
			}
		}
		time.Sleep(chaosPollInterval)
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_release", "")
	repaired := false
	if kinds, err := jobEventKinds(caseDBPath(ctx), jobID); err == nil {
		for _, kind := range kinds {
			if kind == "RepairComplete" {
				repaired = true
			}
		}
	}
	detail := fmt.Sprintf("slow_body_ms_per_line=1 states_before_speedup=%v states_all=%v repaired=%t final=%s",
		beforeRelease, seen, repaired, statuses[jobID])
	pass := waiting && repaired && statuses[jobID] == "COMPLETE"
	return classifyRestartResult(ctx.Profile, pass, "a job waiting on recovery data read FETCHING_REPAIR_DATA and repaired once it came: "+detail, false, "", "repair-data wait: "+detail), nil
}
