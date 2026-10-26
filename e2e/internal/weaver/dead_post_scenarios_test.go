// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"slices"
	"strings"
	"testing"
)

func TestDeadPostFailureModesAreIsolated(t *testing.T) {
	// Keep these fixtures on opposite sides of FIRST_ARTICLE_GATE_MIN_FILES
	// (health.rs). Otherwise first-article and metadata failures can race.
	scenarios := loadScenariosForSlugs([]string{"dead-post-first-articles-missing", "dead-post-recovery-unobtainable"})
	if len(scenarios) != 2 {
		t.Fatalf("expected both dead-post scenarios, got %d", len(scenarios))
	}
	for _, scenario := range scenarios {
		t.Run(scenario.Slug, func(t *testing.T) {
			if scenario.ExpectedOutcome != "health_failure" || !slices.Contains(scenario.DeleteSubjectContains, ".rar") {
				t.Fatal("both cases must delete their data articles and require a health failure")
			}
			if scenario.Slug == "dead-post-first-articles-missing" {
				if len(scenario.FixtureAssets) != 10 || scenario.ExpectedErrorContains != "first articles are missing" {
					t.Fatal("first-article fixture must sample ten missing data files")
				}
				for _, asset := range scenario.FixtureAssets {
					if !strings.HasSuffix(asset, ".rar") {
						t.Fatalf("first-article fixture must not start PAR2 discovery: %s", asset)
					}
				}
			} else {
				if len(scenario.FixtureAssets) >= 10 || scenario.ExpectedErrorContains != "PAR2 metadata discovery exhausted" {
					t.Fatal("recovery fixture must stay below the first-article gate")
				}
				par2 := false
				for _, asset := range scenario.FixtureAssets {
					par2 = par2 || strings.HasSuffix(asset, ".par2")
				}
				if !par2 || !slices.Contains(scenario.DeleteSubjectContains, ".par2") {
					t.Fatal("recovery fixture requires PAR2 candidates")
				}
			}
		})
	}
}
