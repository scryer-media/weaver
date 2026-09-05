package nntp

import (
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
)

func testManifest() fixture.GeneratedManifest {
	return fixture.GeneratedManifest{
		SchemaVersion: fixture.GeneratedManifestSchemaVersion,
		Case:          fixture.ArchiveCase{ID: "bench-fixture", NZBOrder: fixture.ScatteredNZBOrder},
		ArchiveFiles: []fixture.FileDigest{
			{Path: "archive/fixture.part01.rar", Size: 100, BLAKE3: "a"},
			{Path: "archive/fixture.part03.rar", Size: 100, BLAKE3: "c"},
			{Path: "archive/fixture.par2", Size: 10, BLAKE3: "d"},
		},
		WithheldFiles: []fixture.FileDigest{
			{Path: "archive/fixture.part02.rar", Size: 100, BLAKE3: "b"},
		},
		NZBFileOrder: []string{
			"archive/fixture.part03.rar",
			"archive/fixture.par2",
			"archive/fixture.part02.rar",
			"archive/fixture.part01.rar",
		},
	}
}

func TestPostingPlanSplitsPostedFromWithheld(t *testing.T) {
	plan, err := newPostingPlan(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"archive/fixture.part03.rar", "archive/fixture.par2", "archive/fixture.part01.rar"}
	if len(plan.Posted) != len(want) {
		t.Fatalf("posted files = %v, want %v", plan.Posted, want)
	}
	for index := range want {
		if plan.Posted[index] != want[index] {
			t.Fatalf("posted files = %v, want %v", plan.Posted, want)
		}
	}
	if len(plan.Withheld) != 1 || plan.Withheld["archive/fixture.part02.rar"].Size != 100 {
		t.Fatalf("withheld set = %v", plan.Withheld)
	}
}

func TestPostingPlanRejectsAnOrderNamingAnUnknownFile(t *testing.T) {
	manifest := testManifest()
	manifest.NZBFileOrder[0] = "archive/fixture.part09.rar"
	if _, err := newPostingPlan(manifest); err == nil {
		t.Fatal("an order naming an unknown file was accepted")
	}
}

func subjectFor(name string, fileNum, files, part, parts int, size int64) string {
	return strings.Join([]string{
		"[" + itoa2(fileNum) + "/" + itoa2(files) + "] - \"" + name + "\" yEnc (" + itoa2(part) + "/" + itoa2(parts) + ") " + itoa64(size),
	}, "")
}

func itoa2(value int) string {
	if value < 10 {
		return "0" + string(rune('0'+value))
	}
	return string(rune('0'+value/10)) + string(rune('0'+value%10))
}

func itoa64(value int64) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}

func postedDocument() NZBDocument {
	return NZBDocument{Files: []NZBFile{
		{
			Poster:   "nntp-bench@example.invalid",
			Date:     1700000000,
			Subject:  subjectFor("fixture.part03.rar", 1, 3, 1, 2, 100),
			Groups:   []NZBGroup{"alt.binaries.test"},
			Segments: []NZBSegment{{Bytes: 52, Number: 1, MessageID: "a1"}, {Bytes: 51, Number: 2, MessageID: "a2"}},
		},
		{
			Poster:   "nntp-bench@example.invalid",
			Date:     1700000000,
			Subject:  subjectFor("fixture.par2", 2, 3, 1, 1, 10),
			Groups:   []NZBGroup{"alt.binaries.test"},
			Segments: []NZBSegment{{Bytes: 12, Number: 1, MessageID: "b1"}},
		},
		{
			Poster:   "nntp-bench@example.invalid",
			Date:     1700000000,
			Subject:  subjectFor("fixture.part01.rar", 3, 3, 1, 2, 100),
			Groups:   []NZBGroup{"alt.binaries.test"},
			Segments: []NZBSegment{{Bytes: 52, Number: 1, MessageID: "c1"}, {Bytes: 51, Number: 2, MessageID: "c2"}},
		},
	}}
}

func TestAssertNZBFileOrderAcceptsTheDeclaredOrder(t *testing.T) {
	plan, err := newPostingPlan(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err := assertNZBFileOrder(postedDocument(), plan.Posted); err != nil {
		t.Fatal(err)
	}
}

func TestAssertNZBFileOrderRejectsAReorderedDocument(t *testing.T) {
	plan, err := newPostingPlan(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	document := postedDocument()
	document.Files[0], document.Files[2] = document.Files[2], document.Files[0]
	err = assertNZBFileOrder(document, plan.Posted)
	if err == nil {
		t.Fatal("a reordered NZB was accepted")
	}
	if !strings.Contains(err.Error(), "posting order was not preserved") {
		t.Fatalf("unhelpful order failure: %v", err)
	}
}

func TestSpliceWithheldFilesRestoresTheFullDeclaredOrder(t *testing.T) {
	manifest := testManifest()
	plan, err := newPostingPlan(manifest)
	if err != nil {
		t.Fatal(err)
	}
	spliced, err := spliceWithheldFiles(postedDocument(), plan, "run-1", manifest.Case.ID, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertNZBFileOrder(spliced, plan.Order); err != nil {
		t.Fatal(err)
	}
	withheld := spliced.Files[2]
	name, err := nzbFileName(withheld.Subject)
	if err != nil {
		t.Fatal(err)
	}
	if name != "fixture.part02.rar" {
		t.Fatalf("spliced file 2 is %q", name)
	}
	if len(withheld.Segments) != 2 {
		t.Fatalf("withheld file has %d segments, want 2 for 100 bytes at 50 per article", len(withheld.Segments))
	}
	for _, segment := range withheld.Segments {
		if !strings.Contains(segment.MessageID, "withheld") {
			t.Fatalf("withheld segment %q does not use the never-posted identifier scheme", segment.MessageID)
		}
		if segment.Bytes <= 0 {
			t.Fatalf("withheld segment has no declared size: %#v", segment)
		}
	}
	if withheld.Poster != spliced.Files[0].Poster || len(withheld.Groups) != 1 {
		t.Fatalf("withheld file does not look like its neighbours: %#v", withheld)
	}
	for index, file := range spliced.Files {
		if !strings.Contains(file.Subject, "/4]") {
			t.Fatalf("file %d was not renumbered to the full posting: %q", index, file.Subject)
		}
	}
}

func TestSpliceWithheldFilesIsANoOpWithoutWithheldVolumes(t *testing.T) {
	manifest := testManifest()
	manifest.WithheldFiles = nil
	manifest.NZBFileOrder = []string{
		"archive/fixture.part03.rar",
		"archive/fixture.par2",
		"archive/fixture.part01.rar",
	}
	plan, err := newPostingPlan(manifest)
	if err != nil {
		t.Fatal(err)
	}
	document := postedDocument()
	spliced, err := spliceWithheldFiles(document, plan, "run-1", manifest.Case.ID, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(spliced.Files) != len(document.Files) {
		t.Fatalf("splice changed a document with nothing withheld")
	}
	if spliced.Files[0].Subject != document.Files[0].Subject {
		t.Fatalf("splice rewrote subjects with nothing withheld")
	}
}

func TestWithheldMessageIDsAreDeterministicAndDistinct(t *testing.T) {
	first := withheldMessageID("run-1", "bench-fixture", 3, 1)
	if first != withheldMessageID("run-1", "bench-fixture", 3, 1) {
		t.Fatal("withheld message ids are not deterministic")
	}
	if first == withheldMessageID("run-1", "bench-fixture", 3, 2) {
		t.Fatal("withheld message ids do not vary by part")
	}
	if first == withheldMessageID("run-2", "bench-fixture", 3, 1) {
		t.Fatal("withheld message ids do not vary by run")
	}
	if strings.Contains(MessageIDTemplate("run-1", "bench-fixture"), "withheld") {
		t.Fatal("the posted template can collide with the withheld namespace")
	}
}

func TestModalFullSegmentBytesIgnoresShortTails(t *testing.T) {
	if got := modalFullSegmentBytes(postedDocument()); got != 52 {
		t.Fatalf("modal full segment = %d, want 52", got)
	}
}
