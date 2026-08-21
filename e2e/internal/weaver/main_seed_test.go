package weaver

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestSeededArticleIDsAreScopedToExactFixtureNZB(t *testing.T) {
	root := t.TempDir()
	t.Setenv("FIXTURES_DIR", root)
	const slug = "rar5-multi-member"
	want := []string{
		"e2e-rar5-multi-member-1-001@e2e-test",
		"e2e-rar5-multi-member-1-002@e2e-test",
		"e2e-rar5-multi-member-2-1@e2e-test",
	}
	writeFile(t, filepath.Join(root, slug, slug+".nzb"), `<?xml version="1.0"?>
<nzb>
  <file><segments>
    <segment number="1">e2e-rar5-multi-member-1-001@e2e-test</segment>
    <segment number="2">e2e-rar5-multi-member-1-002@e2e-test</segment>
  </segments></file>
  <file><segments>
    <segment number="1">e2e-rar5-multi-member-2-1@e2e-test</segment>
  </segments></file>
</nzb>`)
	writeFile(t, filepath.Join(root, slug+"-encrypted", slug+"-encrypted.nzb"),
		`<nzb><file><segments><segment>e2e-rar5-multi-member-encrypted-1-001@e2e-test</segment></segments></file></nzb>`)

	messageIDs, err := seededArticleIDs(slug)
	if err != nil {
		t.Fatalf("seededArticleIDs(): %v", err)
	}
	if !reflect.DeepEqual(messageIDs, want) {
		t.Fatalf("seededArticleIDs() = %v, want %v", messageIDs, want)
	}
}

func TestExtractMessageIDsIncludesFirstArticleFromEverySegmentsContainer(t *testing.T) {
	nzb := `<nzb>
  <file><segments><segment number="1">first-file-first@e2e</segment><segment number="2">first-file-second@e2e</segment></segments></file>
  <file><segments><segment number="1">second-file-first@e2e</segment></segments></file>`
	want := []string{"first-file-first@e2e", "first-file-second@e2e", "second-file-first@e2e"}
	if got := extractMessageIDs(nzb); !reflect.DeepEqual(got, want) {
		t.Fatalf("extractMessageIDs() = %v, want %v", got, want)
	}
}

// deleteSubjectContains deletes whole files, so a scenario that wants one
// interior article of one named file kept out of NNTP — and the rest of that
// file kept in it — needs its own filter for the segment-number selector.
func TestSegmentDeleteNeedlesAreSeparateFromTheWholeFileFilter(t *testing.T) {
	nzb := []byte(`<nzb>
  <file subject="Cobalt.Harbor - test-media.mkv (1/2)"><segments>
    <segment number="1">payload-1@e2e</segment>
    <segment number="2">payload-2@e2e</segment>
  </segments></file>
  <file subject="Cobalt.Harbor - test-media.mkv.vol24+24.par2 (1/3)"><segments>
    <segment number="1">volume-1@e2e</segment>
    <segment number="2">volume-2@e2e</segment>
    <segment number="3">volume-3@e2e</segment>
  </segments></file>
</nzb>`)

	scenario := &Scenario{
		DeleteSegmentSubjectContains: []string{"test-media.mkv.vol24+24.par2"},
		DeleteSegmentNumbers:         []int{3},
	}
	if got := segmentDeleteNeedles(scenario); !reflect.DeepEqual(got, scenario.DeleteSegmentSubjectContains) {
		t.Fatalf("segmentDeleteNeedles() = %v, want its own filter", got)
	}
	ids, err := extractMessageIDsBySegmentNumbers(nzb, segmentDeleteNeedles(scenario), scenario.DeleteSegmentNumbers)
	if err != nil {
		t.Fatalf("extract segment ids: %v", err)
	}
	if want := []string{"volume-3@e2e"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("segment ids = %v, want %v", ids, want)
	}

	// With no filter of its own the field falls back to deleteSubjectContains,
	// which is what every scenario predating it relies on.
	fallback := &Scenario{DeleteSubjectContains: []string{"test-media.mkv.vol24+24.par2"}}
	if got := segmentDeleteNeedles(fallback); !reflect.DeepEqual(got, fallback.DeleteSubjectContains) {
		t.Fatalf("segmentDeleteNeedles() fallback = %v, want the whole-file filter", got)
	}
	if got := segmentDeleteNeedles(&Scenario{}); len(got) != 0 {
		t.Fatalf("segmentDeleteNeedles() with no filters = %v, want none", got)
	}
}
