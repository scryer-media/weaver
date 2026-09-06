package weaver

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"html"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// --- verify ---

func cmdVerify() {
	// Collect NZBs and check articles exist via NNTP STAT
	addr := nntpHost() + ":" + nntpPort()
	log.Printf("verifying fixtures against NNTP at %s...", addr)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		log.Fatalf("nntp connect: %v", err)
	}
	defer conn.Close()

	// Read greeting
	buf := make([]byte, 512)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, _ := conn.Read(buf)
	greeting := string(buf[:n])
	if !strings.HasPrefix(greeting, "200") {
		log.Fatalf("unexpected greeting: %s", greeting)
	}

	totalChecked, totalFound := 0, 0
	for _, slug := range canonicalFixtureSlugs {
		nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
		nzbData, err := os.ReadFile(nzbPath)
		if err != nil {
			continue
		}

		// Extract message-IDs from the NZB (simple regex, not a full parser)
		msgIDs := extractMessageIDs(string(nzbData))
		missing := 0
		for _, msgID := range msgIDs {
			totalChecked++
			// Send STAT
			fmt.Fprintf(conn, "STAT <%s>\r\n", msgID)
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				log.Printf("  read error: %v", err)
				continue
			}
			resp := string(buf[:n])
			if strings.HasPrefix(resp, "223") {
				totalFound++
			} else {
				missing++
			}
		}

		if missing > 0 {
			log.Printf("  %s: %d/%d articles missing", slug, missing, len(msgIDs))
		} else {
			log.Printf("  %s: OK (%d articles)", slug, len(msgIDs))
		}
	}

	log.Printf("NNTP: %d/%d articles verified", totalFound, totalChecked)
}

// extractMessageIDs pulls message-IDs from NZB XML (simple string scan).
func extractMessageIDs(nzb string) []string {
	var ids []string
	const segmentStart = "<segment"
	rest := nzb
	for {
		idx := strings.Index(rest, segmentStart)
		if idx < 0 {
			break
		}
		rest = rest[idx+len(segmentStart):]
		// `<segment` is also a prefix of the `<segments>` container. Accept
		// only an actual segment element so the first article in every file is
		// not accidentally folded into its parent container tag.
		if len(rest) > 0 && rest[0] != '>' && rest[0] != ' ' && rest[0] != '\t' && rest[0] != '\r' && rest[0] != '\n' {
			continue
		}
		start := strings.Index(rest, ">")
		if start < 0 {
			break
		}
		rest = rest[start+1:]
		end := strings.Index(rest, "</segment>")
		if end < 0 {
			break
		}
		msgID := strings.TrimSpace(rest[:end])
		if msgID != "" {
			ids = append(ids, msgID)
		}
		rest = rest[end:]
	}
	return ids
}

// extractMessageIDsBySubjectContains collects the article ids of every NZB file
// whose subject matches one of the needles.
//
// `tailArticles` bounds it to the LAST n articles of each matching file, which
// is the difference between two very different kinds of damage. Deleting a
// whole volume makes it *missing*, and a missing volume is a different product
// path from a volume with *holes* in it. Deleting from the tail — never the
// head — is what keeps the hole in payload bytes: every RAR volume carries its
// own signature and headers in its first article, and a volume whose header is
// gone cannot be mapped at all. Zero or negative means every article, the
// original whole-file behaviour.
func extractMessageIDsBySubjectContains(nzbData []byte, needles []string, tailArticles int) ([]string, error) {
	type nzbSegment struct {
		MessageID string `xml:",chardata"`
	}
	type nzbFile struct {
		Subject  string       `xml:"subject,attr"`
		Segments []nzbSegment `xml:"segments>segment"`
	}
	type nzbDoc struct {
		Files []nzbFile `xml:"file"`
	}

	var doc nzbDoc
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}

	lowerNeedles := make([]string, 0, len(needles))
	for _, needle := range needles {
		needle = strings.ToLower(strings.TrimSpace(needle))
		if needle != "" {
			lowerNeedles = append(lowerNeedles, needle)
		}
	}
	if len(lowerNeedles) == 0 {
		return nil, nil
	}

	var ids []string
	for _, file := range doc.Files {
		subject := strings.ToLower(file.Subject)
		matched := false
		for _, needle := range lowerNeedles {
			if strings.Contains(subject, needle) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		segments := file.Segments
		if tailArticles > 0 && len(segments) > tailArticles {
			segments = segments[len(segments)-tailArticles:]
		}
		for _, segment := range segments {
			msgID := strings.TrimSpace(segment.MessageID)
			msgID = strings.TrimPrefix(msgID, "<")
			msgID = strings.TrimSuffix(msgID, ">")
			if msgID != "" {
				ids = append(ids, msgID)
			}
		}
	}

	return ids, nil
}

// extractMessageIDsBySegmentNumbers selects articles by their position within
// a file, which is what an interior-hole scenario needs: deleteSubjectContains
// picks whole files and deleteSubjectTailArticles only reaches the tail, so
// neither can name a segment in the middle. The subject needles stay optional
// and narrow which files are considered; with none, every file is.
func extractMessageIDsBySegmentNumbers(nzbData []byte, needles []string, numbers []int) ([]string, error) {
	type nzbSegment struct {
		Number    int    `xml:"number,attr"`
		MessageID string `xml:",chardata"`
	}
	type nzbFile struct {
		Subject  string       `xml:"subject,attr"`
		Segments []nzbSegment `xml:"segments>segment"`
	}
	type nzbDoc struct {
		Files []nzbFile `xml:"file"`
	}

	var doc nzbDoc
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}

	wanted := make(map[int]struct{}, len(numbers))
	for _, number := range numbers {
		wanted[number] = struct{}{}
	}
	if len(wanted) == 0 {
		return nil, nil
	}

	lowerNeedles := make([]string, 0, len(needles))
	for _, needle := range needles {
		if needle = strings.ToLower(strings.TrimSpace(needle)); needle != "" {
			lowerNeedles = append(lowerNeedles, needle)
		}
	}

	var ids []string
	for _, file := range doc.Files {
		if len(lowerNeedles) > 0 {
			subject := strings.ToLower(file.Subject)
			matched := false
			for _, needle := range lowerNeedles {
				if strings.Contains(subject, needle) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		for _, segment := range file.Segments {
			if _, ok := wanted[segment.Number]; !ok {
				continue
			}
			msgID := strings.TrimSpace(segment.MessageID)
			msgID = strings.TrimPrefix(msgID, "<")
			msgID = strings.TrimSuffix(msgID, ">")
			if msgID != "" {
				ids = append(ids, msgID)
			}
		}
	}

	return ids, nil
}

func extractAllMessageIDsFromNZB(nzbData []byte) ([]string, error) {
	type nzbSegment struct {
		MessageID string `xml:",chardata"`
	}
	type nzbFile struct {
		Segments []nzbSegment `xml:"segments>segment"`
	}
	type nzbDoc struct {
		Files []nzbFile `xml:"file"`
	}

	var doc nzbDoc
	if err := xml.Unmarshal(nzbData, &doc); err != nil {
		return nil, err
	}

	var ids []string
	for _, file := range doc.Files {
		for _, segment := range file.Segments {
			msgID := strings.TrimSpace(segment.MessageID)
			msgID = strings.TrimPrefix(msgID, "<")
			msgID = strings.TrimSuffix(msgID, ">")
			if msgID != "" {
				ids = append(ids, msgID)
			}
		}
	}
	return ids, nil
}

func rewriteNZBSegmentNumbers(nzbData []byte, numbers []int) ([]byte, error) {
	if len(numbers) == 0 {
		return nzbData, nil
	}

	matches := nzbSegmentNumberPattern.FindAllSubmatchIndex(nzbData, -1)
	if len(matches) != len(numbers) {
		return nil, fmt.Errorf(
			"segment number override count mismatch: got %d override(s) for %d segment(s)",
			len(numbers),
			len(matches),
		)
	}

	out := make([]byte, 0, len(nzbData)+len(numbers)*2)
	last := 0
	for index, match := range matches {
		number := numbers[index]
		if number <= 0 {
			return nil, fmt.Errorf("segment numbers must be positive, got %d at index %d", number, index)
		}
		out = append(out, nzbData[last:match[3]]...)
		out = strconv.AppendInt(out, int64(number), 10)
		out = append(out, nzbData[match[4]:match[5]]...)
		last = match[1]
	}
	out = append(out, nzbData[last:]...)
	return out, nil
}

// rewriteNZBSubjectFilenames makes the NZB's declared filename differ from
// the yEnc filename carried by the already-posted article. That distinction is
// present in real-world obfuscated posts and must survive seeding unchanged.
func rewriteNZBSubjectFilenames(nzbData []byte, overrides map[string]string) ([]byte, error) {
	if len(overrides) == 0 {
		return nzbData, nil
	}

	keys := make([]string, 0, len(overrides))
	for from := range overrides {
		keys = append(keys, from)
	}
	sort.Strings(keys)

	out := append([]byte(nil), nzbData...)
	for _, from := range keys {
		to := strings.TrimSpace(overrides[from])
		if strings.TrimSpace(from) == "" || to == "" {
			return nil, fmt.Errorf("NZB subject filename overrides require non-empty source and destination")
		}
		source := []byte("&quot;" + html.EscapeString(from) + "&quot;")
		replacement := []byte("&quot;" + html.EscapeString(to) + "&quot;")
		if count := bytes.Count(out, source); count != 1 {
			return nil, fmt.Errorf("NZB subject filename %q matched %d entries, want 1", from, count)
		}
		out = bytes.ReplaceAll(out, source, replacement)
	}
	return out, nil
}

func scenarioNZBSegmentNumbers(nzbData []byte, scenario *Scenario) ([]int, error) {
	if scenario == nil {
		return nil, nil
	}
	if len(scenario.NZBSegmentNumbers) > 0 {
		if scenario.NZBSegmentNumberStart != 0 || scenario.NZBSegmentNumberStep != 0 {
			return nil, fmt.Errorf("cannot combine explicit nzb_segment_numbers with nzb_segment_number_start/step")
		}
		return scenario.NZBSegmentNumbers, nil
	}
	if scenario.NZBSegmentNumberStart == 0 && scenario.NZBSegmentNumberStep == 0 {
		return nil, nil
	}
	if scenario.NZBSegmentNumberStep <= 0 {
		return nil, fmt.Errorf("nzb_segment_number_step must be positive")
	}

	segmentCount := len(nzbSegmentNumberPattern.FindAllSubmatchIndex(nzbData, -1))
	if segmentCount == 0 {
		return nil, fmt.Errorf("generated NZB contained no segments")
	}

	start := scenario.NZBSegmentNumberStart
	if start <= 0 {
		start = 1
	}

	numbers := make([]int, segmentCount)
	current := start
	for index := range numbers {
		numbers[index] = current
		current += scenario.NZBSegmentNumberStep
	}
	return numbers, nil
}

func healthProbeSampleIndices(totalSegs, probeRound int) []int {
	if totalSegs == 0 {
		return nil
	}

	probeCount := totalSegs * 8 / 100
	if probeCount < 10 {
		probeCount = 10
	}
	if probeCount > totalSegs {
		probeCount = totalSegs
	}

	stride := totalSegs / probeCount
	if stride < 1 {
		stride = 1
	}

	offset := 0
	if stride > 1 {
		offset = probeRound % stride
	}

	indices := make([]int, 0, probeCount)
	for i := offset; i < totalSegs; i += stride {
		indices = append(indices, i)
	}
	return indices
}

func extractFirstProbeSampleMessageIDs(nzbData []byte, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}

	ids, err := extractAllMessageIDsFromNZB(nzbData)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	probeIndices := healthProbeSampleIndices(len(ids), 0)
	if len(probeIndices) == 0 {
		return nil, nil
	}
	if count > len(probeIndices) {
		count = len(probeIndices)
	}

	selected := make([]string, 0, count)
	for _, idx := range probeIndices[:count] {
		selected = append(selected, ids[idx])
	}
	return selected, nil
}

func extractFirstMessageIDs(nzbData []byte, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}

	ids, err := extractAllMessageIDsFromNZB(nzbData)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if count > len(ids) {
		count = len(ids)
	}

	return append([]string(nil), ids[:count]...), nil
}

func deleteArticlesByMessageID(messageIDs []string) error {
	return deleteArticlesByMessageIDOnServers(messageIDs, true, true)
}

func deleteArticlesByMessageIDOnServers(messageIDs []string, includePrimary, includeBackup bool) error {
	if includePrimary {
		if err := deleteArticleIDsAt(nntpHost(), nntpPort(), messageIDs); err != nil {
			return err
		}
	}
	if includeBackup && backupNntpRunning() {
		if err := deleteArticleIDsAt(nntpHost(), backupNntpPort(), messageIDs); err != nil {
			return err
		}
	}
	return nil
}

func deleteArticleIDsAt(host, port string, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	greeting, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read greeting: %w", err)
	}
	if !strings.HasPrefix(greeting, "200") {
		return fmt.Errorf("unexpected greeting: %s", strings.TrimSpace(greeting))
	}
	if err := authenticateNNTPConnection(conn, r, addr); err != nil {
		return err
	}

	for _, messageID := range messageIDs {
		cmd := fmt.Sprintf("DELETEID <%s>\r\n", messageID)
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte(cmd)); err != nil {
			return fmt.Errorf("write DELETEID for <%s>: %w", messageID, err)
		}

		conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		resp, err := r.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read DELETEID response for <%s>: %w", messageID, err)
		}
		resp = strings.TrimSpace(resp)
		if !strings.HasPrefix(resp, "290") {
			return fmt.Errorf("DELETEID failed for <%s>: %s", messageID, resp)
		}
	}

	_, _ = conn.Write([]byte("QUIT\r\n"))
	return nil
}
