package weaver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
)

// Seeding a uuencoded release does not go through nyuu.
//
// Nyuu is a yEnc poster: it has no encoding selector, so there is no way to
// ask it for a uuencoded article. A uu scenario therefore ships its article
// bodies as corpus objects — encoded and split by the pinned UUDeview oracle
// at fixture-build time, see internal/fixturegen/uuencode.go — and the seeder
// posts those bytes verbatim over plain NNTP, then writes the NZB that
// describes them.
//
// A scenario can carry both: uu-mixed-yenc stages a media file for nyuu the
// way every other fixture does and posts a uu sidecar beside it, and the two
// sets of <file> elements are merged into one NZB.

const (
	uuPoster    = "e2e-test@example.invalid"
	uuNewsgroup = "alt.binaries.test"
)

// loadUUPlan reads a scenario's uu posting plan. A scenario without one is not
// a uu scenario, which is not an error.
func loadUUPlan(absDir string) (*fixturegen.UUPlan, error) {
	path := filepath.Join(absDir, filepath.FromSlash(fixturegen.UUPlanFile))
	contents, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var plan fixturegen.UUPlan
	if err := json.Unmarshal(contents, &plan); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if plan.SchemaVersion != fixturegen.UUPlanSchemaVersion {
		return nil, fmt.Errorf("%s has schema version %d, this harness reads %d",
			path, plan.SchemaVersion, fixturegen.UUPlanSchemaVersion)
	}
	if len(plan.Files) == 0 {
		return nil, fmt.Errorf("%s describes no files", path)
	}
	return &plan, nil
}

// scenarioStagesPostableFiles reports whether anything in this scenario goes
// to nyuu. A pure uu release has no staged file at all, and staging would fail
// rather than produce an empty set.
func scenarioStagesPostableFiles(absDir string, scenario *Scenario) (bool, error) {
	if len(scenario.SharedAssets) > 0 || len(scenario.FixtureAssets) > 0 {
		return true, nil
	}
	files, err := dataFiles(absDir)
	if err != nil {
		return false, err
	}
	return len(files) > 0, nil
}

// uuArticle is one article as it will be posted.
type uuArticle struct {
	messageID string
	subject   string
	body      []byte
}

// seedUUArticles posts every article the plan describes and returns the NZB
// <file> elements that describe them, along with the total decoded payload
// size the indexer is told about.
func seedUUArticles(
	host, port string,
	scenario *Scenario,
	absDir string,
	plan *fixturegen.UUPlan,
) ([]string, int64, error) {
	var (
		articles []uuArticle
		elements []string
		total    int64
	)

	for fileIndex, file := range plan.Files {
		if len(file.Parts) == 0 {
			return nil, 0, fmt.Errorf("uu file %q has no parts", file.Name)
		}
		total += file.Size

		segments := make([]string, 0, len(file.Parts))
		for _, part := range file.Parts {
			body, err := os.ReadFile(filepath.Join(absDir, filepath.FromSlash(part.Body)))
			if err != nil {
				return nil, 0, fmt.Errorf("read uu article body %s: %w", part.Body, err)
			}
			messageID := uuMessageID(scenario.Slug, fileIndex+1, part.Number)
			subject := uuSubject(file.Name, part.Number, len(file.Parts), file.Size)
			articles = append(articles, uuArticle{messageID: messageID, subject: subject, body: body})
			segments = append(segments, fmt.Sprintf(
				"\t\t\t<segment bytes=\"%d\" number=\"%d\">%s</segment>",
				part.Bytes, part.Number, xmlEscape(messageID)))
		}

		// The NZB carries the first article's subject at file level, which is
		// the convention every poster follows and what the harness's
		// subject-matching delete filters read.
		head := uuSubject(file.Name, 1, len(file.Parts), file.Size)
		elements = append(elements, strings.Join([]string{
			fmt.Sprintf("\t<file poster=\"%s\" date=\"%s\" subject=\"%s\">", uuPoster, stableNZBDate, xmlEscape(head)),
			"\t\t<groups>",
			"\t\t\t<group>" + uuNewsgroup + "</group>",
			"\t\t</groups>",
			"\t\t<segments>",
			strings.Join(segments, "\n"),
			"\t\t</segments>",
			"\t</file>",
		}, "\n"))
	}

	if err := postArticles(host, port, articles); err != nil {
		return nil, 0, err
	}
	return elements, total, nil
}

// uuMessageID keeps the prefix the harness's purge and percentage-delete
// controls match on, and separates the uu files from anything nyuu numbered in
// the same scenario.
func uuMessageID(slug string, fileNumber, part int) string {
	return fmt.Sprintf("e2e-%s-uu%d-%03d@e2e-test", slug, fileNumber, part)
}

// uuSubject is the old-style multi-part convention: a quoted filename and an
// (i/N) counter, with none of yEnc's marker token.
func uuSubject(name string, part, parts int, size int64) string {
	return fmt.Sprintf("%q (%d/%d) %d", name, part, parts, size)
}

func xmlEscape(value string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
		"'", "&apos;",
	).Replace(value)
}

// writeUUNZB writes the whole NZB for a scenario that has no yEnc files.
func writeUUNZB(path string, scenario *Scenario, elements []string) error {
	document := strings.Join(append([]string{
		`<?xml version="1.0" encoding="UTF-8"?>`,
		`<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">`,
		`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">`,
		"\t<head>",
		"\t\t<meta type=\"name\">" + xmlEscape(scenario.Title) + "</meta>",
		"\t\t<meta type=\"category\">" + xmlEscape(scenario.Category) + "</meta>",
		"\t</head>",
	}, elements...), "\n") + "\n</nzb>\n"
	return os.WriteFile(path, []byte(document), 0o644)
}

// spliceUUFilesIntoNZB adds the uu <file> elements to an NZB nyuu has already
// written for the same scenario's yEnc files.
func spliceUUFilesIntoNZB(path string, elements []string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	document := string(contents)
	closing := strings.LastIndex(document, "</nzb>")
	if closing < 0 {
		return fmt.Errorf("%s has no closing nzb element", path)
	}
	updated := document[:closing] + strings.Join(elements, "\n") + "\n" + document[closing:]
	return os.WriteFile(path, []byte(updated), 0o644)
}

// postArticles posts every article over one authenticated connection.
func postArticles(host, port string, articles []uuArticle) error {
	if len(articles) == 0 {
		return nil
	}
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("NNTP connection failed to %s: %w", addr, err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	greeting, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read greeting from %s: %w", addr, err)
	}
	if !strings.HasPrefix(greeting, "200") {
		return fmt.Errorf("unexpected greeting from %s: %s", addr, strings.TrimSpace(greeting))
	}
	if err := authenticateNNTPConnection(conn, reader, addr); err != nil {
		return err
	}

	for _, article := range articles {
		if err := postOneArticle(conn, reader, addr, article); err != nil {
			return err
		}
	}

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, _ = conn.Write([]byte("QUIT\r\n"))
	return nil
}

func postOneArticle(conn net.Conn, reader *bufio.Reader, addr string, article uuArticle) error {
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte("POST\r\n")); err != nil {
		return fmt.Errorf("write command to %s: %w", addr, err)
	}
	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read response from %s: %w", addr, err)
	}
	if !strings.HasPrefix(response, "340") {
		return fmt.Errorf("POST refused by %s for %s: %s", addr, article.messageID, strings.TrimSpace(response))
	}

	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := conn.Write(renderArticle(article)); err != nil {
		return fmt.Errorf("write article %s to %s: %w", article.messageID, addr, err)
	}

	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	response, err = reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read response from %s: %w", addr, err)
	}
	if !strings.HasPrefix(response, "240") {
		return fmt.Errorf("posting %s to %s failed: %s", article.messageID, addr, strings.TrimSpace(response))
	}
	return nil
}

// renderArticle builds the wire form: headers, the blank line, then the body
// with every line CRLF terminated and dot-stuffed.
//
// The dot-stuffing is not a formality here. A uuencoded line's first character
// encodes how many bytes it carries, and for a 14-byte line that character is
// '.' (0x20+14) — which is exactly the final short line of any payload whose
// length leaves 14 bytes over. Posting such a line unstuffed would end the
// article early.
func renderArticle(article uuArticle) []byte {
	var out strings.Builder
	for _, header := range []string{
		"From: " + uuPoster,
		"Newsgroups: " + uuNewsgroup,
		"Subject: " + article.subject,
		"Message-ID: <" + article.messageID + ">",
		"Date: " + uuArticleDate(),
	} {
		out.WriteString(header)
		out.WriteString("\r\n")
	}
	out.WriteString("\r\n")

	for _, line := range splitBodyLines(article.body) {
		if strings.HasPrefix(line, ".") {
			out.WriteByte('.')
		}
		out.WriteString(line)
		out.WriteString("\r\n")
	}
	out.WriteString(".\r\n")
	return []byte(out.String())
}

// splitBodyLines splits a stored article body into its lines. Bodies are
// written LF terminated on disk; the wire form is applied here, so a body that
// arrived with CRLF already is normalised rather than doubled.
func splitBodyLines(body []byte) []string {
	text := strings.ReplaceAll(string(body), "\r\n", "\n")
	lines := strings.Split(text, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// uuArticleDate is the fixed posting date. It is derived from the same stable
// timestamp the NZB dates are normalised to, rather than restated, so a reseed
// changes nothing and the two can never drift apart.
func uuArticleDate() string {
	seconds, err := strconv.ParseInt(stableNZBDate, 10, 64)
	if err != nil {
		// stableNZBDate is a compile-time constant in this package; if it ever
		// stops being a unix timestamp that is a bug to surface, not to paper
		// over with a silently different posting date.
		panic("stableNZBDate is not a unix timestamp: " + stableNZBDate)
	}
	return time.Unix(seconds, 0).UTC().Format(time.RFC1123Z)
}
