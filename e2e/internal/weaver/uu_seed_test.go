package weaver

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRenderArticleDotStuffsShortFinalLines is the one that matters most.
//
// A uuencoded line's first character is its byte count offset by 0x20, so a
// line carrying 14 bytes begins with '.' — and that is a real shape, not a
// contrived one: it is the final short line of any payload whose length leaves
// 14 bytes over. Posted unstuffed, that line would terminate the article and
// the rest of the file would be lost.
func TestRenderArticleDotStuffsShortFinalLines(t *testing.T) {
	body := ".&5L;&\\@=V]R;&0`\n`\nend\n"
	rendered := string(renderArticle(uuArticle{
		messageID: "e2e-uu-release-uu1-008@e2e-test",
		subject:   `"silver.horizon.s01e04.mkv" (8/8) 65536`,
		body:      []byte(body),
	}))

	headers, article, found := strings.Cut(rendered, "\r\n\r\n")
	if !found {
		t.Fatal("the article has no blank line between headers and body")
	}
	for _, header := range []string{
		"From: ", "Newsgroups: alt.binaries.test", "Subject: ", "Message-ID: <e2e-uu-release-uu1-008@e2e-test>", "Date: ",
	} {
		if !strings.Contains(headers, header) {
			t.Errorf("headers are missing %q:\n%s", header, headers)
		}
	}

	if !strings.HasPrefix(article, "..&5L") {
		t.Fatalf("the leading '.' of a 14-byte line was not stuffed: %q", article[:10])
	}
	if !strings.HasSuffix(article, "\r\n.\r\n") {
		t.Fatalf("the article is not terminated: %q", article[len(article)-10:])
	}
	// The terminator is the only bare dot line in the article.
	if got := strings.Count(article, "\r\n.\r\n"); got != 1 {
		t.Errorf("found %d bare dot lines, want only the terminator", got)
	}
}

func TestRenderArticleNormalisesLineEndings(t *testing.T) {
	rendered := string(renderArticle(uuArticle{
		messageID: "x@e2e-test", subject: "s", body: []byte("M0000\r\nM1111\n"),
	}))
	_, article, _ := strings.Cut(rendered, "\r\n\r\n")
	if article != "M0000\r\nM1111\r\n.\r\n" {
		t.Fatalf("body is %q; a CRLF already in the file must not be doubled", article)
	}
}

func TestUUSubjectIsOldStyleMultipart(t *testing.T) {
	got := uuSubject("silver.horizon.s01e04.mkv", 3, 8, 65536)
	want := `"silver.horizon.s01e04.mkv" (3/8) 65536`
	if got != want {
		t.Fatalf("uuSubject = %q, want %q", got, want)
	}
	if strings.Contains(got, "yEnc") {
		t.Error("a uu subject must not carry the yEnc marker")
	}
}

func TestUUMessageIDKeepsThePurgePrefix(t *testing.T) {
	got := uuMessageID("uu-release", 2, 7)
	if want := "e2e-uu-release-uu2-007@e2e-test"; got != want {
		t.Fatalf("uuMessageID = %q, want %q", got, want)
	}
	// purgeSeededArticles and the percentage delete both match on this prefix.
	if !strings.HasPrefix(got, "e2e-uu-release") {
		t.Error("the message id must keep the e2e-<slug> prefix the delete controls match on")
	}
}

// nzbDocument is the shape the assertions below read back.
type nzbDocument struct {
	Files []struct {
		Subject  string   `xml:"subject,attr"`
		Poster   string   `xml:"poster,attr"`
		Date     string   `xml:"date,attr"`
		Groups   []string `xml:"groups>group"`
		Segments []struct {
			Bytes     int64  `xml:"bytes,attr"`
			Number    int    `xml:"number,attr"`
			MessageID string `xml:",chardata"`
		} `xml:"segments>segment"`
	} `xml:"file"`
}

func uuTestElements() []string {
	return []string{strings.Join([]string{
		`	<file poster="e2e-test@example.invalid" date="1704067200" subject="&quot;amber.trail.s01e02.nfo&quot; (1/2) 20003">`,
		`		<groups>`,
		`			<group>alt.binaries.test</group>`,
		`		</groups>`,
		`		<segments>`,
		`			<segment bytes="12637" number="1">e2e-uu-mixed-yenc-uu1-001@e2e-test</segment>`,
		`			<segment bytes="7400" number="2">e2e-uu-mixed-yenc-uu1-002@e2e-test</segment>`,
		`		</segments>`,
		`	</file>`,
	}, "\n")}
}

func TestWriteUUNZBIsParseable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "uu-release.nzb")
	scenario := &Scenario{Slug: "uu-release", Title: "Silver.Horizon.S01E04", Category: "2000"}
	if err := writeUUNZB(path, scenario, uuTestElements()); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var document nzbDocument
	if err := xml.Unmarshal(contents, &document); err != nil {
		t.Fatalf("the generated NZB does not parse: %v\n%s", err, contents)
	}
	if len(document.Files) != 1 {
		t.Fatalf("got %d files, want 1", len(document.Files))
	}
	file := document.Files[0]
	if file.Subject != `"amber.trail.s01e02.nfo" (1/2) 20003` {
		t.Errorf("subject round-tripped as %q", file.Subject)
	}
	if len(file.Groups) != 1 || file.Groups[0] != "alt.binaries.test" {
		t.Errorf("groups = %v", file.Groups)
	}
	if len(file.Segments) != 2 {
		t.Fatalf("got %d segments, want 2", len(file.Segments))
	}
	if file.Segments[1].Number != 2 || file.Segments[1].Bytes != 7400 {
		t.Errorf("second segment = %+v", file.Segments[1])
	}
	if got := strings.TrimSpace(file.Segments[0].MessageID); got != "e2e-uu-mixed-yenc-uu1-001@e2e-test" {
		t.Errorf("message id = %q", got)
	}
	if !strings.Contains(string(contents), "<meta type=\"name\">Silver.Horizon.S01E04</meta>") {
		t.Error("the NZB head does not carry the scenario title")
	}
}

// TestSpliceUUFilesIntoNZB covers the mixed case: nyuu has already written the
// yEnc files, and the uu files join them in the same document.
func TestSpliceUUFilesIntoNZB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "uu-mixed-yenc.nzb")
	nyuu := `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
	<head>
		<meta type="name">Amber.Trail.S01E02</meta>
	</head>
	<file poster="p" date="1704067200" subject="&quot;amber.trail.s01e02.mkv&quot; yEnc (1/1) 65536">
		<groups>
			<group>alt.binaries.test</group>
		</groups>
		<segments>
			<segment bytes="67600" number="1">e2e-uu-mixed-yenc-1-001@e2e-test</segment>
		</segments>
	</file>
</nzb>
`
	if err := os.WriteFile(path, []byte(nyuu), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := spliceUUFilesIntoNZB(path, uuTestElements()); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var document nzbDocument
	if err := xml.Unmarshal(contents, &document); err != nil {
		t.Fatalf("the spliced NZB does not parse: %v\n%s", err, contents)
	}
	if len(document.Files) != 2 {
		t.Fatalf("got %d files, want the yEnc file and the uu file", len(document.Files))
	}
	if !strings.Contains(document.Files[0].Subject, "yEnc") {
		t.Errorf("the first file should still be the yEnc one, got %q", document.Files[0].Subject)
	}
	if strings.Contains(document.Files[1].Subject, "yEnc") {
		t.Errorf("the uu file must not carry the yEnc marker, got %q", document.Files[1].Subject)
	}
	if strings.Count(string(contents), "</nzb>") != 1 {
		t.Error("splicing duplicated the closing element")
	}
}

// TestExtractMessageIDsBySegmentNumbers is the interior-hole selector: neither
// deleteSubjectContains nor deleteSubjectTailArticles can name a segment in
// the middle of a file.
func TestExtractMessageIDsBySegmentNumbers(t *testing.T) {
	document := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
	<file subject="&quot;crimson.vale.s01e06.mkv&quot; (1/4) 65536">
		<segments>
			<segment bytes="10" number="1">a-1@e2e-test</segment>
			<segment bytes="10" number="2">a-2@e2e-test</segment>
			<segment bytes="10" number="3">a-3@e2e-test</segment>
			<segment bytes="10" number="4">a-4@e2e-test</segment>
		</segments>
	</file>
	<file subject="&quot;crimson.vale.s01e06.nfo&quot; (1/2) 2048">
		<segments>
			<segment bytes="10" number="1">b-1@e2e-test</segment>
			<segment bytes="10" number="2">b-2@e2e-test</segment>
		</segments>
	</file>
</nzb>
`)

	got, err := extractMessageIDsBySegmentNumbers(document, nil, []int{2})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a-2@e2e-test", "b-2@e2e-test"}; !equalStrings(got, want) {
		t.Errorf("with no subject filter got %v, want %v", got, want)
	}

	got, err = extractMessageIDsBySegmentNumbers(document, []string{".mkv"}, []int{2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a-2@e2e-test", "a-3@e2e-test"}; !equalStrings(got, want) {
		t.Errorf("narrowed by subject got %v, want %v", got, want)
	}

	got, err = extractMessageIDsBySegmentNumbers(document, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("with no numbers got %v, want nothing", got)
	}

	got, err = extractMessageIDsBySegmentNumbers(document, nil, []int{99})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("a number no segment carries selected %v", got)
	}
}

func TestLoadUUPlan(t *testing.T) {
	dir := t.TempDir()
	if plan, err := loadUUPlan(dir); err != nil || plan != nil {
		t.Fatalf("a directory with no plan should read as no plan, got %v, %v", plan, err)
	}

	uu := filepath.Join(dir, "uu")
	if err := os.MkdirAll(uu, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(body string) {
		if err := os.WriteFile(filepath.Join(uu, "plan.json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	write(`{"schema_version": 9999, "files": [{"name": "x", "size": 1, "parts": []}]}`)
	if _, err := loadUUPlan(dir); err == nil {
		t.Error("a plan from a newer schema must be refused, not guessed at")
	}

	write(`{"schema_version": 1, "files": []}`)
	if _, err := loadUUPlan(dir); err == nil {
		t.Error("a plan describing no files must be refused")
	}

	write(`{"schema_version": 1, "files": [{"name": "x.mkv", "size": 45, "parts": [{"number": 1, "body": "uu/x.mkv.001", "bytes": 63}]}]}`)
	plan, err := loadUUPlan(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Files) != 1 || plan.Files[0].Name != "x.mkv" || len(plan.Files[0].Parts) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestScenarioStagesPostableFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "scenario.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	// A pure uu release: scenario.json and a uu directory, nothing for nyuu.
	if err := os.MkdirAll(filepath.Join(dir, "uu"), 0o755); err != nil {
		t.Fatal(err)
	}
	staged, err := scenarioStagesPostableFiles(dir, &Scenario{})
	if err != nil {
		t.Fatal(err)
	}
	if staged {
		t.Error("a scenario whose only payload is uu articles has nothing to stage for nyuu")
	}

	staged, err = scenarioStagesPostableFiles(dir, &Scenario{FixtureAssets: []string{"single-mkv/test-media.mkv"}})
	if err != nil {
		t.Fatal(err)
	}
	if !staged {
		t.Error("a scenario staging another fixture's asset still goes through nyuu")
	}

	if err := os.WriteFile(filepath.Join(dir, "amber.trail.s01e02.mkv"), []byte("bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	staged, err = scenarioStagesPostableFiles(dir, &Scenario{})
	if err != nil {
		t.Fatal(err)
	}
	if !staged {
		t.Error("a top-level payload file is a yEnc file and must be staged")
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
