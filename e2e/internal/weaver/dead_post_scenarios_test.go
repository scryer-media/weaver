// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"slices"
	"strings"
	"testing"
)

func TestDeadPostFailureModesAreIsolated(t *testing.T) {
	// One dead post with nothing to recover from and one whose advertised
	// recovery is deleted with it. The server feed supplies the failure
	// reason; neither case may deliver usable output.
	scenarios := loadScenariosForSlugs([]string{"dead-post-first-articles-missing", "dead-post-recovery-unobtainable"})
	if len(scenarios) != 2 {
		t.Fatalf("expected both dead-post scenarios, got %d", len(scenarios))
	}
	for _, scenario := range scenarios {
		t.Run(scenario.Slug, func(t *testing.T) {
			if scenario.ExpectedOutcome != "health_failure" || !slices.Contains(scenario.DeleteSubjectContains, ".rar") {
				t.Fatal("both cases must delete their data articles and require a failure")
			}
			wantReason := "below critical"
			if scenario.Slug == "dead-post-first-articles-missing" {
				wantReason = "first articles are missing"
			}
			if scenario.ExpectedErrorContains != wantReason {
				t.Fatalf("%s must report %q, not %q", scenario.Slug, wantReason, scenario.ExpectedErrorContains)
			}
			par2 := false
			for _, asset := range scenario.FixtureAssets {
				par2 = par2 || strings.HasSuffix(asset, ".par2")
			}
			if scenario.Slug == "dead-post-first-articles-missing" {
				if len(scenario.FixtureAssets) != 10 || par2 {
					t.Fatal("the dead post without recovery stages ten RAR volumes and no PAR2")
				}
			} else if !par2 || !slices.Contains(scenario.DeleteSubjectContains, ".par2") {
				t.Fatal("the dead post with recovery advertises PAR2 and deletes it too")
			}
		})
	}
}
