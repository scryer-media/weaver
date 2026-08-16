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
