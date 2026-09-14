package weaver

import (
	"strings"
	"testing"
)

func TestOverrideNzbNewsgroupRewritesEveryGroupElement(t *testing.T) {
	nzb := []byte(`<nzb><file><groups>
	<group>alt.binaries.test</group>
</groups></file><file><groups><group>alt.binaries.test</group><group>alt.binaries.other</group></groups></file></nzb>`)

	rewritten, err := overrideNzbNewsgroup(nzb, "alt.binaries.split-7z")
	if err != nil {
		t.Fatalf("override: %v", err)
	}
	got := string(rewritten)
	if strings.Contains(got, "alt.binaries.test") || strings.Contains(got, "alt.binaries.other") {
		t.Fatalf("original newsgroups survived the override: %s", got)
	}
	if n := strings.Count(got, "<group>alt.binaries.split-7z</group>"); n != 3 {
		t.Fatalf("expected 3 rewritten group elements, got %d in %s", n, got)
	}
}

func TestOverrideNzbNewsgroupLeavesTheNzbAloneWithoutANewsgroup(t *testing.T) {
	nzb := []byte(`<nzb><file><groups><group>alt.binaries.test</group></groups></file></nzb>`)
	rewritten, err := overrideNzbNewsgroup(nzb, "  ")
	if err != nil {
		t.Fatalf("override: %v", err)
	}
	if string(rewritten) != string(nzb) {
		t.Fatalf("blank newsgroup changed the NZB: %s", rewritten)
	}
}

func TestOverrideNzbNewsgroupRejectsAnNzbWithoutGroups(t *testing.T) {
	if _, err := overrideNzbNewsgroup([]byte(`<nzb><file></file></nzb>`), "alt.binaries.x"); err == nil {
		t.Fatal("expected an error for an NZB without <group> elements")
	}
}
