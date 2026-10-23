// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zeebo/blake3"
)

// The restart cases that pin the 0.16.5 field reports: a restart after an
// early gap keeps what it had past the gap, an operator's Ctrl-C during
// verification refetches nothing, and a Ctrl-C during a PAR2 pass stops the
// pass before the process exits.

// interruptWeaverAndWaitForExit sends the operator's Ctrl-C and waits for the
// process to exit on its own. The case timeout only catches a hang.
func (ctx *restartCaseContext) interruptWeaverAndWaitForExit() error {
	if ctx.weaver == nil || ctx.weaver.cmd == nil || ctx.weaver.cmd.Process == nil {
		return fmt.Errorf("no managed weaver to interrupt")
	}
	if err := ctx.weaver.cmd.Process.Signal(os.Interrupt); err != nil {
		return fmt.Errorf("send SIGINT to weaver pid %d: %w", ctx.weaver.PID, err)
	}
	select {
	case <-ctx.weaver.done:
	case <-time.After(ctx.Timeout):
		return fmt.Errorf("weaver pid %d did not exit after SIGINT; log=%s%s", ctx.weaver.PID, ctx.caseLogPath, restartLogTail(ctx.caseLogPath, 20))
	}
	_ = os.Remove(localWeaverPIDPath())
	ctx.weaver = nil
	return nil
}

// A stored article the case took off the news server, in the wire form a POST
// gives back to it.
type withheldArticle struct {
	messageID string
	wire      []byte
}

// withholdArticles takes the articles off the news server, so every fetch of
// them answers 430, and returns the call that puts them back.
func withholdArticles(host, port string, messageIDs []string) (func() error, error) {
	session, err := openNntpCommandSession(host, port, true)
	if err != nil {
		return nil, err
	}
	var held []withheldArticle
	for _, messageID := range messageIDs {
		wire, err := readStoredArticle(session, messageID)
		if err != nil {
			session.close()
			return nil, err
		}
		held = append(held, withheldArticle{messageID: messageID, wire: wire})
	}
	session.close()
	if err := deleteArticleIDsAt(host, port, messageIDs); err != nil {
		return nil, err
	}
	return func() error { return restoreWithheldArticles(host, port, held) }, nil
}

func readStoredArticle(session *nntpCommandSession, messageID string) ([]byte, error) {
	status, err := session.send("ARTICLE <" + messageID + ">")
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(status, "220") {
		return nil, fmt.Errorf("ARTICLE <%s>: %s", messageID, status)
	}
	var lines [][]byte
	for {
		session.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		line, err := session.reader.ReadBytes('\n')
		if err != nil {
			return nil, fmt.Errorf("read ARTICLE <%s>: %w", messageID, err)
		}
		line = bytes.TrimRight(line, "\r\n")
		if string(line) == "." {
			break
		}
		lines = append(lines, line)
	}
	// The fixture stores most articles body-only; a POST needs the
	// Message-ID header to file the article under its id again.
	hasHeaders := len(lines) > 0 && !bytes.HasPrefix(lines[0], []byte("=ybegin"))
	if hasHeaders {
		hasHeaders = false
		for _, line := range lines {
			if len(line) == 0 {
				break
			}
			if bytes.HasPrefix(bytes.ToLower(line), []byte("message-id:")) {
				hasHeaders = true
				break
			}
		}
	}
	var wire bytes.Buffer
	if !hasHeaders {
		wire.WriteString("Message-ID: <" + messageID + ">\r\n\r\n")
	}
	for _, line := range lines {
		// Still dot-stuffed as the server sent it, which is what POST reads.
		wire.Write(line)
		wire.WriteString("\r\n")
	}
	wire.WriteString(".\r\n")
	return wire.Bytes(), nil
}

func restoreWithheldArticles(host, port string, held []withheldArticle) error {
	session, err := openNntpCommandSession(host, port, true)
	if err != nil {
		return err
	}
	defer session.close()
	for _, article := range held {
		status, err := session.send("POST")
		if err != nil {
			return err
		}
		if !strings.HasPrefix(status, "340") {
			return fmt.Errorf("POST <%s> refused: %s", article.messageID, status)
		}
		session.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := session.conn.Write(article.wire); err != nil {
			return fmt.Errorf("write <%s>: %w", article.messageID, err)
		}
		session.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		reply, err := session.reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read POST reply for <%s>: %w", article.messageID, err)
		}
		if !strings.HasPrefix(reply, "240") {
			return fmt.Errorf("POST <%s> failed: %s", article.messageID, strings.TrimSpace(reply))
		}
	}
	return nil
}

// nzbSegmentMessageIDs is every segment of the slug's NZB in file order, with
// its zero-based ordinal within its file.
type nzbSegmentRef struct {
	FileIndex int
	Ordinal   uint32
	Number    int
	MessageID string
}

func nzbSegments(slug string) ([]nzbSegmentRef, error) {
	nzbData, err := os.ReadFile(filepath.Join(fixturesDir(), slug, slug+".nzb"))
	if err != nil {
		return nil, err
	}
	files, err := parseNZBFileSegments(nzbData)
	if err != nil {
		return nil, err
	}
	var out []nzbSegmentRef
	for fileIndex, segments := range files {
		sort.Slice(segments, func(i, j int) bool { return segments[i].Number < segments[j].Number })
		for ordinal, segment := range segments {
			out = append(out, nzbSegmentRef{FileIndex: fileIndex, Ordinal: uint32(ordinal), Number: segment.Number, MessageID: segment.MessageID})
		}
	}
	return out, nil
}

func fileBLAKE3(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := blake3.New()
	if _, err := io.Copy(hasher, bufio.NewReaderSize(file, 1<<20)); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

// completedFilesBySize lists every file under the complete root of that size.
func completedFilesBySize(size int64) []string {
	var out []string
	root := filepath.Join(localWeaverDir(), "complete")
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() && info.Size() == size {
			out = append(out, path)
		}
		return nil
	})
	return out
}

// R1. One article early in a single plain file is missing while the rest of
// the file downloads, so the file's coverage has a hole near its start and
// most of its bytes past it. A restart must keep every covered ordinal and
// fetch only what the checkpoint did not cover, the hole among them.
func runEarlyGapRestartRefetchesOnlyTheGap(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "single-mkv"
	const checkpointBytes = 4 * 1024 * 1024
	checkpointEnv := fmt.Sprintf("WEAVER_E2E_DOWNLOAD_RESTART_CHECKPOINT_BYTES=%d", checkpointBytes)
	// Twelve barriers past the hole: three times the restart lead cap, so a
	// restart that fell back to the contiguous prefix could not hide inside
	// the cap's allowance.
	const coveredFloor = 12 * checkpointBytes
	const gapNumber = 3

	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	var gap *nzbSegmentRef
	for i := range segments {
		if segments[i].Number == gapNumber {
			gap = &segments[i]
		}
	}
	if gap == nil {
		return restartCaseResult{}, fmt.Errorf("%s has no segment %d", slug, gapNumber)
	}
	putBack, err := withholdArticles(nntpHost(), nntpPort(), []string{gap.MessageID})
	if err != nil {
		return restartCaseResult{}, err
	}
	restored := false
	defer func() {
		if !restored {
			_ = putBack()
		}
	}()

	if err := ctx.startWeaverWithOptions("", 8, checkpointEnv); err != nil {
		return restartCaseResult{}, err
	}
	// Slow every body so the restart point is reached with most of the file
	// still to come, whatever the machine's speed.
	if err := setNntpChaos("slow_body=1"); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}

	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	deadline := time.Now().Add(ctx.Timeout)
	reached := false
	var lastCoverage []fileCoverageRow
	for time.Now().Before(deadline) {
		status, _ := restartJobStatusFromDB(dbPath, jobID)
		if dbStatusTerminal(normalizeDBStatus(status)) {
			break
		}
		rows, err := readJobFileCoverage(dbPath, jobID)
		if err == nil {
			lastCoverage = rows
			for _, row := range rows {
				if row.FileIndex == gap.FileIndex && row.Err == nil && !row.Coverage.Contains(gap.Ordinal) && row.Coverage.CoveredBytes() >= coveredFloor {
					reached = true
				}
			}
		}
		if reached {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !reached {
		_, _, _, _ = ctx.captureEvidence("pre_timeout", "")
		return classifyRestartResult(ctx.Profile, false, "", false, "",
			fmt.Sprintf("the restart point was never reached: no checkpoint covered %d bytes past the missing ordinal %d (last coverage %+v)", coveredFloor, gap.Ordinal, lastCoverage)), nil
	}

	if err := ctx.interruptWeaverAndWaitForExit(); err != nil {
		return restartCaseResult{}, err
	}
	preDB, _, preNntp, err := ctx.captureEvidence("pre_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	preCoverage, err := readJobFileCoverage(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_restart_coverage.json"), preCoverage)
	if status := jobStatusFromDB(preDB, jobID); status == "" || dbStatusTerminal(status) {
		return classifyRestartResult(ctx.Profile, false, "", false, "",
			fmt.Sprintf("job was %q at shutdown, not an active download", status)), nil
	}
	covered := map[string]bool{}
	var coveredBytes uint64
	for _, row := range preCoverage {
		if row.Err != nil {
			return classifyRestartResult(ctx.Profile, false, "", false, "",
				fmt.Sprintf("file %d checkpoint does not decode: %v", row.FileIndex, row.Err)), nil
		}
		coveredBytes += row.Coverage.CoveredBytes()
		for _, segment := range segments {
			if segment.FileIndex == row.FileIndex && row.Coverage.Contains(segment.Ordinal) {
				covered[segment.MessageID] = true
			}
		}
	}
	if covered[gap.MessageID] {
		return classifyRestartResult(ctx.Profile, false, "", false, "",
			fmt.Sprintf("the checkpoint claims the missing ordinal %d", gap.Ordinal)), nil
	}

	if err := putBack(); err != nil {
		return restartCaseResult{}, err
	}
	restored = true
	if err := ctx.restartWeaverWithEnv(checkpointEnv); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, postNntp, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}

	var refetchedCovered []string
	gapFetched := postNntp.BodyCounts[gap.MessageID] > preNntp.BodyCounts[gap.MessageID]
	fetchedAfter := 0
	for id, count := range postNntp.BodyCounts {
		if count <= preNntp.BodyCounts[id] {
			continue
		}
		fetchedAfter++
		if covered[id] {
			refetchedCovered = append(refetchedCovered, id)
		}
	}
	sort.Strings(refetchedCovered)

	source := filepath.Join(testdataDir(), slug, "test-media.mkv")
	info, err := os.Stat(source)
	if err != nil {
		return restartCaseResult{}, err
	}
	want, err := fileBLAKE3(source)
	if err != nil {
		return restartCaseResult{}, err
	}
	outputs := completedFilesBySize(info.Size())
	outputOK := false
	got := ""
	for _, output := range outputs {
		if got, err = fileBLAKE3(output); err == nil && got == want {
			outputOK = true
		}
	}

	detail := fmt.Sprintf("covered_at_shutdown=%d bytes (%d of %d ordinals) gap_ordinal=%d fetched_after_restart=%d refetched_covered=%d gap_refetched=%t final=%s output_ok=%t",
		coveredBytes, len(covered), len(segments), gap.Ordinal, fetchedAfter, len(refetchedCovered), gapFetched, statuses[jobID], outputOK)
	pass := statuses[jobID] == "COMPLETE" && len(refetchedCovered) == 0 && gapFetched && outputOK
	failure := "early-gap restart: " + detail
	if len(refetchedCovered) > 0 {
		failure += fmt.Sprintf(" refetched covered ids: %v", refetchedCovered)
	}
	if !outputOK {
		failure += fmt.Sprintf(" output candidates=%v last_blake3=%s want=%s", outputs, got, want)
	}
	return classifyRestartResult(ctx.Profile, pass, "restart after an early gap fetched only what the checkpoint did not cover: "+detail, false, "", failure), nil
}

// R2. The operator stops Weaver while a job is verifying. Every article was
// already on disk, so the restart verifies again from the files and fetches
// nothing.
func runInterruptDuringVerifyingRefetchesNothing(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "par2-small-repair"
	const hook = "status.enter_verifying"
	delayEnv := "WEAVER_E2E_DELAY=" + hook + "=20000"
	if err := ctx.startWeaverWithOptions("", 8, delayEnv); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if _, err := waitForDelayHookRestartPoint(ctx, []int{jobID}, hook, ctx.Timeout); err != nil {
		_, _, _, _ = ctx.captureEvidence("pre_timeout", "")
		return classifyRestartResult(ctx.Profile, false, "", false, "", err.Error()), nil
	}
	if err := ctx.interruptWeaverAndWaitForExit(); err != nil {
		return restartCaseResult{}, err
	}
	preDB, _, preNntp, err := ctx.captureEvidence("pre_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	preStatus := jobStatusFromDB(preDB, jobID)
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, postNntp, err := ctx.captureEvidence("post_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	// An article first fetched after the restart (recovery data the repair
	// asks for) is not a refetch; one the first run already had is.
	var refetched []string
	firstFetches := 0
	for id, count := range postNntp.BodyCounts {
		if count <= preNntp.BodyCounts[id] {
			continue
		}
		if preNntp.BodyCounts[id] > 0 {
			refetched = append(refetched, id)
		} else {
			firstFetches++
		}
	}
	sort.Strings(refetched)
	detail := fmt.Sprintf("status_at_shutdown=%s final=%s refetched_after_restart=%d first_fetches_after_restart=%d refetched_ids=%v",
		preStatus, statuses[jobID], len(refetched), firstFetches, refetched)
	pass := statuses[jobID] == "COMPLETE" && len(refetched) == 0
	return classifyRestartResult(ctx.Profile, pass, "a Ctrl-C during verification restarted from the files without a fetch: "+detail, false, "", "Ctrl-C during verification: "+detail), nil
}

// The log lines a PAR2 pass writes while it scans or repairs. None may follow
// the drain's last line.
var par2PassLogNeedles = []string{
	"par2 damaged-path analysis",
	"par2 repair",
	"par2 scan",
	"par2 verify",
	"repairing",
}

// R3. The operator stops Weaver while a PAR2 repair pass is running on a
// damaged set. The pass stops, the drain finishes and the process exits; no
// pass writes a line after the drain. After a restart the job scans again and
// repairs.
func runInterruptDuringPar2PassStopsThePass(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "par2-heavy-damage"
	const hook = "repair.task_start"
	delayEnv := "WEAVER_E2E_DELAY=" + hook + "=20000"
	if err := ctx.startWeaverWithOptions("", 8, delayEnv); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if _, err := waitForDelayHookRestartPoint(ctx, []int{jobID}, hook, ctx.Timeout); err != nil {
		_, _, _, _ = ctx.captureEvidence("pre_timeout", "")
		return classifyRestartResult(ctx.Profile, false, "", false, "", err.Error()), nil
	}
	if err := ctx.interruptWeaverAndWaitForExit(); err != nil {
		return restartCaseResult{}, err
	}
	firstRun, err := os.ReadFile(ctx.caseLogPath)
	if err != nil {
		return restartCaseResult{}, err
	}
	preDB, _, _, err := ctx.captureEvidence("pre_restart", "")
	if err != nil {
		return restartCaseResult{}, err
	}
	preStatus := jobStatusFromDB(preDB, jobID)
	preKinds, _ := jobEventKinds(caseDBPath(ctx), jobID)

	lines := strings.Split(stripANSIEscapeSequences(string(firstRun)), "\n")
	stoppedAt := -1
	for i, line := range lines {
		if strings.Contains(line, "pipeline stopped") {
			stoppedAt = i
		}
	}
	signalAt := -1
	for i, line := range lines {
		if signalAt < 0 && strings.Contains(line, "received shutdown signal") {
			signalAt = i
		}
	}
	passLines := func(from int) []string {
		var out []string
		if from < 0 {
			return out
		}
		for _, line := range lines[from+1:] {
			lower := strings.ToLower(line)
			for _, needle := range par2PassLogNeedles {
				if strings.Contains(lower, needle) {
					out = append(out, strings.TrimSpace(line))
					break
				}
			}
		}
		return out
	}
	afterDrain := passLines(stoppedAt)
	// Evidence only: how much of the pass the drain waited out.
	afterSignal := passLines(signalAt)

	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	_, _, _, _ = ctx.captureEvidence("post_restart", "")
	full, err := os.ReadFile(ctx.caseLogPath)
	if err != nil {
		return restartCaseResult{}, err
	}
	secondRun := stripANSIEscapeSequences(string(full[len(firstRun):]))
	rescanned := strings.Contains(secondRun, "par2 damaged-path analysis started")
	repaired := false
	if kinds, err := jobEventKinds(caseDBPath(ctx), jobID); err == nil && len(kinds) >= len(preKinds) {
		for _, kind := range kinds[len(preKinds):] {
			if kind == "RepairComplete" {
				repaired = true
			}
		}
	}

	detail := fmt.Sprintf("status_at_shutdown=%s drain_finished=%t pass_lines_after_signal=%d pass_lines_after_drain=%d rescanned_after_restart=%t repaired_after_restart=%t final=%s",
		preStatus, stoppedAt >= 0, len(afterSignal), len(afterDrain), rescanned, repaired, statuses[jobID])
	pass := stoppedAt >= 0 && len(afterDrain) == 0 && rescanned && repaired && statuses[jobID] == "COMPLETE"
	failure := "Ctrl-C during a PAR2 pass: " + detail
	if len(afterSignal) > 0 {
		failure += fmt.Sprintf(" first pass lines after the signal: %q", afterSignal[:min(len(afterSignal), 3)])
	}
	if len(afterDrain) > 0 {
		failure += fmt.Sprintf(" first lines after the drain: %q", afterDrain[:min(len(afterDrain), 5)])
	}
	return classifyRestartResult(ctx.Profile, pass, "a Ctrl-C during a PAR2 pass stopped it before exit and the restart repaired: "+detail, false, "", failure), nil
}

func caseDBPath(ctx *restartCaseContext) string {
	return filepath.Join(ctx.CaseDir, "weaver.db")
}

type nzbParsedSegment struct {
	Number    int    `xml:"number,attr"`
	MessageID string `xml:",chardata"`
}

func parseNZBFileSegments(nzbData []byte) ([][]nzbParsedSegment, error) {
	var doc struct {
		Files []struct {
			Segments []nzbParsedSegment `xml:"segments>segment"`
		} `xml:"file"`
	}
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}
	out := make([][]nzbParsedSegment, 0, len(doc.Files))
	for _, file := range doc.Files {
		segments := make([]nzbParsedSegment, 0, len(file.Segments))
		for _, segment := range file.Segments {
			segment.MessageID = strings.Trim(strings.TrimSpace(segment.MessageID), "<>")
			segments = append(segments, segment)
		}
		out = append(out, segments)
	}
	return out, nil
}
