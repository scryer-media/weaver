package weaver

import (
	"slices"
	"testing"
)

func TestCanonicalRunsLeaveDisabledFixturesOut(t *testing.T) {
	enabled := enabledCanonicalFixtureSlugs()
	disabled := 0
	for _, scenario := range loadScenariosForSlugs(canonicalFixtureSlugs) {
		if scenario.Disabled == "" {
			if !slices.Contains(enabled, scenario.Slug) {
				t.Errorf("enabled fixture %s left out of the canonical run", scenario.Slug)
			}
			continue
		}
		disabled++
		if slices.Contains(enabled, scenario.Slug) {
			t.Errorf("disabled fixture %s still runs", scenario.Slug)
		}
	}
	if len(enabled)+disabled != len(canonicalFixtureSlugs) {
		t.Fatalf("enabled=%d disabled=%d canonical=%d", len(enabled), disabled, len(canonicalFixtureSlugs))
	}
	for _, slug := range []string{"direct-store-par3-withheld-volume", "par3-rar5-withheld-volume", "par3-split-7z-withheld-part"} {
		if slices.Contains(enabled, slug) {
			t.Errorf("%s is expected to be disabled", slug)
		}
	}
}
