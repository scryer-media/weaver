// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bufio"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
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
	return ctx.waitForInterruptedWeaverExit()
}

// shutdownSignalLine is what Weaver logs once it has taken the operator's
// Ctrl-C and begun to shut down.
const shutdownSignalLine = "received shutdown signal"

// interruptHeldWeaverAndWaitForExit is the operator's Ctrl-C to a Weaver whose
// bodies the news server is holding. The hold is lifted once Weaver logs
// that it took the signal, after the log length read before sending it, so
// the bodies the signal caught mid-transfer finish at full speed while the
// shutdown drains them instead of waiting on the hold. `lift` replaces the
// hold.
func (ctx *restartCaseContext) interruptHeldWeaverAndWaitForExit(lift func() error) error {
	if ctx.weaver == nil || ctx.weaver.cmd == nil || ctx.weaver.cmd.Process == nil {
		return fmt.Errorf("no managed weaver to interrupt")
	}
	mark := int64(0)
	if info, err := os.Stat(ctx.caseLogPath); err == nil {
		mark = info.Size()
	}
	if err := ctx.weaver.cmd.Process.Signal(os.Interrupt); err != nil {
		return fmt.Errorf("send SIGINT to weaver pid %d: %w", ctx.weaver.PID, err)
	}
	done := ctx.weaver.done
	if err := ctx.pollFor("weaver to log the shutdown signal", func() (bool, error) {
		select {
		case <-done:
			return true, nil
		default:
		}
		return logHoldsAfter(ctx.caseLogPath, mark, shutdownSignalLine)
	}); err != nil {
		return err
	}
	if err := lift(); err != nil {
		return fmt.Errorf("lift the news server's hold after the shutdown signal: %w", err)
	}
	return ctx.waitForInterruptedWeaverExit()
}

// logHoldsAfter reports whether the log, past byte `mark`, holds `needle`.
func logHoldsAfter(path string, mark int64, needle string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	if _, err := file.Seek(mark, io.SeekStart); err != nil {
		return false, err
	}
	tail, err := io.ReadAll(file)
	if err != nil {
		return false, err
	}
	return strings.Contains(cleanWeaverLog(string(tail)), needle), nil
}

// waitForInterruptedWeaverExit waits for a Weaver sent the Ctrl-C to exit on
// its own. The case timeout only catches a hang.
func (ctx *restartCaseContext) waitForInterruptedWeaverExit() error {
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

// storedArticleHangGuard bounds each read of a stored article only so a dead
// news server cannot hang a case. The news server answers ARTICLE with no
// chaos, so any wait is load: a case reads hundreds of articles in a row on a
// runner busy with other suites, and a short deadline failed a case before
// Weaver had started. It is set far above any real answer.
const storedArticleHangGuard = 10 * time.Minute

func readStoredArticle(session *nntpCommandSession, messageID string) ([]byte, error) {
	cmd := "ARTICLE <" + messageID + ">"
	session.conn.SetWriteDeadline(time.Now().Add(storedArticleHangGuard))
	if _, err := session.conn.Write([]byte(cmd + "\r\n")); err != nil {
		return nil, fmt.Errorf("write command %q to %s: %w", cmd, session.addr, err)
	}
	session.conn.SetReadDeadline(time.Now().Add(storedArticleHangGuard))
	status, err := session.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read response for %q from %s: %w", cmd, session.addr, err)
	}
	status = strings.TrimSpace(status)
	if !strings.HasPrefix(status, "220") {
		return nil, fmt.Errorf("ARTICLE <%s>: %s", messageID, status)
	}
	var lines [][]byte
	for {
		session.conn.SetReadDeadline(time.Now().Add(storedArticleHangGuard))
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

// decodedRange is the bytes of its file an article decodes to, as [Start, End).
type decodedRange struct {
	Start uint64
	End   uint64
}

var ybeginSize = regexp.MustCompile(`^=ybegin .*\bsize=(\d+)`)

// articleDecodedRange reads the range an article decodes to from its yEnc
// header lines: `=ypart begin= end=` (one-based, inclusive) for a part of a
// multi-part file, or the whole of `=ybegin size=` for a file in one article.
func articleDecodedRange(wire []byte) (decodedRange, error) {
	var size uint64
	sawSize := false
	for _, line := range bytes.Split(wire, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if match := ypartLine.FindSubmatch(line); match != nil {
			begin, err := strconv.ParseUint(string(match[1]), 10, 64)
			if err != nil {
				return decodedRange{}, err
			}
			end, err := strconv.ParseUint(string(match[2]), 10, 64)
			if err != nil {
				return decodedRange{}, err
			}
			if begin == 0 || end < begin {
				return decodedRange{}, fmt.Errorf("=ypart begin=%d end=%d is not a range", begin, end)
			}
			return decodedRange{Start: begin - 1, End: end}, nil
		}
		if match := ybeginSize.FindSubmatch(line); match != nil && !sawSize {
			value, err := strconv.ParseUint(string(match[1]), 10, 64)
			if err != nil {
				return decodedRange{}, err
			}
			size, sawSize = value, true
		}
	}
	if !sawSize {
		return decodedRange{}, fmt.Errorf("no yEnc header")
	}
	return decodedRange{Start: 0, End: size}, nil
}

// segmentDecodedRanges reads, by message id, the range each segment decodes
// to, from the articles the news server holds. ARTICLE is not a BODY, so the
// server's body counts do not see it.
func segmentDecodedRanges(host, port string, segments []nzbSegmentRef) (map[string]decodedRange, error) {
	shapes, err := segmentArticleShapes(host, port, segments)
	if err != nil {
		return nil, err
	}
	out := make(map[string]decodedRange, len(shapes))
	for id, shape := range shapes {
		out[id] = shape.Range
	}
	return out, nil
}

// articleShape is what a case reads of one stored article before it runs:
// the range it decodes to, and how many body lines a BODY of it serves, the
// unit the news server's hold_body budget counts.
type articleShape struct {
	Range decodedRange
	Lines int
}

// segmentArticleShapes reads, by message id, each segment's shape from the
// articles the news server holds. ARTICLE is not a BODY: the server's body
// counts do not see it and it spends no hold.
func segmentArticleShapes(host, port string, segments []nzbSegmentRef) (map[string]articleShape, error) {
	session, err := openNntpCommandSession(host, port, true)
	if err != nil {
		return nil, err
	}
	defer session.close()
	out := make(map[string]articleShape, len(segments))
	for _, segment := range segments {
		wire, err := readStoredArticle(session, segment.MessageID)
		if err != nil {
			return nil, err
		}
		decoded, err := articleDecodedRange(wire)
		if err != nil {
			return nil, fmt.Errorf("<%s>: %w", segment.MessageID, err)
		}
		lines, err := articleBodyLines(wire)
		if err != nil {
			return nil, fmt.Errorf("<%s>: %w", segment.MessageID, err)
		}
		out[segment.MessageID] = articleShape{Range: decoded, Lines: lines}
	}
	return out, nil
}

// articleBodyLines counts the lines a BODY serves of an article in the wire
// form readStoredArticle returns: from its `=ybegin` line up to the
// terminating dot, which is where the news server's body starts and ends.
func articleBodyLines(wire []byte) (int, error) {
	start := bytes.Index(wire, []byte("\r\n=ybegin"))
	if start < 0 {
		return 0, fmt.Errorf("no =ybegin line")
	}
	body := wire[start+2:]
	if !bytes.HasSuffix(body, []byte("\r\n.\r\n")) {
		return 0, fmt.Errorf("article is not dot-terminated")
	}
	return bytes.Count(body, []byte("\r\n")) - 1, nil
}

// bodyHoldBudget is the hold_body line budget under which at least `need` of
// the bodies with these line counts complete, whatever order they are served
// in and however they spread over up to `sessions` connections: each
// connection serves one body at a time, so at most one body per connection
// is cut short by the hold, and the budget pays for the `need` longest
// bodies plus one longest body per connection. `most` is the most bodies the
// budget can complete: the shortest bodies, taken while they fit.
func bodyHoldBudget(lines []int, need, sessions int) (budget, most int, err error) {
	if need <= 0 || need > len(lines) || sessions <= 0 {
		return 0, 0, fmt.Errorf("a hold for %d of %d bodies over %d connections is not a hold", need, len(lines), sessions)
	}
	sorted := slices.Clone(lines)
	slices.Sort(sorted)
	for _, count := range sorted[len(sorted)-need:] {
		budget += count
	}
	budget += sessions * sorted[len(sorted)-1]
	spent := 0
	for _, count := range sorted {
		if spent+count > budget {
			break
		}
		spent += count
		most++
	}
	return budget, most, nil
}

// holdAfterBodies arms a hold on the primary news server that lets at least
// `need` of these articles' bodies complete over `sessions` connections and
// then holds every body, and fails when the hold could let every one of them
// complete, which would let the job finish before the case's stop.
func holdAfterBodies(shapes map[string]articleShape, ids []string, need, sessions int) (int, error) {
	lines := make([]int, 0, len(ids))
	for _, id := range ids {
		shape, ok := shapes[id]
		if !ok {
			return 0, fmt.Errorf("no shape read for <%s>", id)
		}
		lines = append(lines, shape.Lines)
	}
	budget, most, err := bodyHoldBudget(lines, need, sessions)
	if err != nil {
		return 0, err
	}
	if most >= len(ids) {
		return 0, fmt.Errorf("a hold for %d bodies over %d connections could complete all %d; the job could finish first", need, sessions, len(ids))
	}
	if err := setNntpChaosOnServer(nntpHost(), nntpPort(), fmt.Sprintf("hold_body=%d", budget)); err != nil {
		return 0, err
	}
	return budget, nil
}

// bodiesCovering is how many of these articles' bodies must complete for
// their decoded bytes to reach `bytes` whichever of them complete: the
// smallest articles, summed until they reach it.
func bodiesCovering(shapes map[string]articleShape, ids []string, bytes uint64) (int, error) {
	sizes := make([]uint64, 0, len(ids))
	for _, id := range ids {
		shape, ok := shapes[id]
		if !ok {
			return 0, fmt.Errorf("no shape read for <%s>", id)
		}
		sizes = append(sizes, shape.Range.End-shape.Range.Start)
	}
	slices.Sort(sizes)
	var total uint64
	for count := 1; count <= len(sizes); count++ {
		total += sizes[count-1]
		if total >= bytes {
			return count, nil
		}
	}
	return 0, fmt.Errorf("all %d articles decode to %d bytes, short of %d", len(sizes), total, bytes)
}

// coveredArticles names the articles whose decoded bytes the checkpoint rows
// hold whole: the articles a restart may never fetch again.
func coveredArticles(segments []nzbSegmentRef, ranges map[string]decodedRange, rows []fileCoverageRow) map[string]bool {
	covered := map[string]bool{}
	for _, row := range rows {
		for _, segment := range segments {
			decoded, known := ranges[segment.MessageID]
			if known && segment.FileIndex == row.FileIndex && row.Coverage.Covers(decoded.Start, decoded.End) {
				covered[segment.MessageID] = true
			}
		}
	}
	return covered
}

// refetchedArticles lists, sorted, the covered articles the server served
// again between the two readings of its body counts.
func refetchedArticles(covered map[string]bool, before, after restartNntpMetrics) []string {
	var out []string
	for id, count := range after.BodyCounts {
		if count > before.BodyCounts[id] && covered[id] {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
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
// most of its bytes past it. A restart must keep the healthy durable bytes
// and the definitive missing-article refusal. Restoring the provider's article
// does not turn a process restart into a new download attempt.
func runEarlyGapRestartPreservesBytesAndRefusal(ctx *restartCaseContext) (restartCaseResult, error) {
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
	// The checkpoint holds decoded bytes, so each article is named by the
	// range it decodes to, read with its body lines before the gap's article
	// is taken away.
	shapes, err := segmentArticleShapes(nntpHost(), nntpPort(), segments)
	if err != nil {
		return restartCaseResult{}, err
	}
	ranges := make(map[string]decodedRange, len(shapes))
	for id, shape := range shapes {
		ranges[id] = shape.Range
	}
	gapRange := ranges[gap.MessageID]
	var servable []string
	for _, segment := range segments {
		if segment.MessageID != gap.MessageID {
			servable = append(servable, segment.MessageID)
		}
	}
	// The stop needs the floor written past the hole, plus two barriers: the
	// checkpoint lags the bytes written by less than one, and one more keeps
	// the floor clear of that edge.
	need, err := bodiesCovering(shapes, servable, coveredFloor+2*checkpointBytes)
	if err != nil {
		return restartCaseResult{}, err
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

	const connections = 8
	if err := ctx.startWeaverWithOptions("", connections, checkpointEnv); err != nil {
		return restartCaseResult{}, err
	}
	// Let through enough bodies for the floor, then hold every body, so the
	// restart point is reached with the rest of the file still to come,
	// whatever the machine's speed.
	if _, err := holdAfterBodies(shapes, servable, need, connections); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}

	dbPath := filepath.Join(ctx.CaseDir, "weaver.db")
	var lastCoverage []fileCoverageRow
	if err := ctx.pollJob("the restart point", dbPath, jobID, func() (bool, error) {
		rows, err := readJobFileCoverage(dbPath, jobID)
		if err != nil {
			return false, nil
		}
		lastCoverage = rows
		for _, row := range rows {
			healthy := row.Coverage.Healthy()
			if row.FileIndex == gap.FileIndex && !healthy.Covers(gapRange.Start, gapRange.End) && healthy.CoveredBytes() >= coveredFloor {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		_, _, _, _ = ctx.captureEvidence("pre_timeout", "")
		coverageJSON, _ := json.Marshal(lastCoverage)
		return classifyRestartResult(ctx.Profile, false, "", false, "",
			fmt.Sprintf("the restart point was never reached: no checkpoint covered %d healthy bytes past the missing ordinal %d (%v; last coverage %s)", coveredFloor, gap.Ordinal, err, coverageJSON)), nil
	}

	if err := ctx.interruptHeldWeaverAndWaitForExit(liftPrimaryHold); err != nil {
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
	proof, err := readCheckpointEvidence(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "pre_restart_checkpoint.json"), proof)
	covered := directDurableArticles(segments, ranges, proof)
	if len(covered) == 0 {
		return restartCaseResult{}, fmt.Errorf("early-gap checkpoint contains no proven durable articles")
	}
	var coveredBytes uint64
	for _, row := range preCoverage {
		coveredBytes += row.Coverage.Healthy().CoveredBytes()
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

	refetchedCovered := refetchedArticles(covered, preNntp, postNntp)
	gapFetched := postNntp.BodyCounts[gap.MessageID] > preNntp.BodyCounts[gap.MessageID]
	fetchedAfter, _ := bodyFetchesSinceRestartPoint(preNntp, postNntp)

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
	// There is no recovery or other usable end file in this post. The persisted
	// refusal must therefore end Failed, even though the provider now has it.
	pass := statuses[jobID] == "FAILED" && len(refetchedCovered) == 0 && !gapFetched && !outputOK
	failure := "early-gap restart: " + detail
	if len(refetchedCovered) > 0 {
		failure += fmt.Sprintf(" refetched covered ids: %v", refetchedCovered)
	}
	if outputOK {
		failure += fmt.Sprintf(" output candidates=%v last_blake3=%s want=%s", outputs, got, want)
	}
	return classifyRestartResult(ctx.Profile, pass, "restart preserved healthy durable bytes and the missing-article refusal: "+detail, false, "", failure), nil
}

// R2. The operator stops Weaver while a job is verifying. Every article was
// already on disk, so the restart verifies again from the files and fetches
// nothing.
func runInterruptDuringVerifyingRefetchesNothing(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "par2-small-repair"
	const hook = "status.enter_verifying"
	holdEnv := "WEAVER_E2E_HOLD=" + hook
	if err := ctx.startWeaverWithOptions("", 8, holdEnv); err != nil {
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
	"graph repair worker",
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
	holdEnv := "WEAVER_E2E_HOLD=" + hook
	if err := ctx.startWeaverWithOptions("", 8, holdEnv); err != nil {
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

	lines := strings.Split(cleanWeaverLog(string(firstRun)), "\n")
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
	secondRun := cleanWeaverLog(string(full[len(firstRun):]))
	startedAgain := len(graphLogEvents(secondRun, jobID, "graph repair worker started")) > 0
	cancelled := false
	for _, started := range graphLogEvents(string(firstRun), jobID, "graph repair worker started") {
		for _, ended := range graphLogEvents(string(firstRun), jobID, "graph repair worker ended") {
			if started["node"] == ended["node"] && started["generation"] == ended["generation"] && ended["cancelled"] == "true" {
				cancelled = true
			}
		}
	}
	repaired := false
	if kinds, err := jobEventKinds(caseDBPath(ctx), jobID); err == nil && len(kinds) >= len(preKinds) {
		for _, kind := range kinds[len(preKinds):] {
			if kind == "RepairComplete" {
				repaired = true
			}
		}
	}

	if err := waitForJobArchived(caseDBPath(ctx), jobID); err != nil {
		return restartCaseResult{}, err
	}
	size, digest, err := singleMKVSource()
	if err != nil {
		return restartCaseResult{}, err
	}
	outputOK := deliveredCopy(completeDirOf(caseDBPath(ctx), jobID), size, digest)
	detail := fmt.Sprintf("status_at_shutdown=%s drain_finished=%t pass_lines_after_signal=%d pass_lines_after_drain=%d generation_cancelled=%t repair_started_after_restart=%t repaired_after_restart=%t output_verified=%t final=%s",
		preStatus, stoppedAt >= 0, len(afterSignal), len(afterDrain), cancelled, startedAgain, repaired, outputOK, statuses[jobID])
	pass := stoppedAt >= 0 && len(afterDrain) == 0 && cancelled && startedAgain && repaired && outputOK && statuses[jobID] == "COMPLETE"
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
