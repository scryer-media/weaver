// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"os"
	"slices"
)

// ArchiveMode checks a particular archive, including one inside another
// archive. A store binding followed by an engine binding must not satisfy a
// store-only contract just because the store was tried first.
type ScenarioArchiveModeAssertion struct {
	Archive     string   `json:"archive"`
	RequireKind string   `json:"requireKind"`
	ForbidKinds []string `json:"forbidKinds,omitempty"`
}

func assertArchiveModesScenario(jobID int, assertions []ScenarioArchiveModeAssertion) error {
	raw, err := os.ReadFile(localWeaverLogPath())
	if err != nil {
		return fmt.Errorf("read weaver log: %w", err)
	}
	return graphArchiveModesVerdict(string(raw), jobID, assertions)
}

func graphArchiveModesVerdict(raw string, jobID int, assertions []ScenarioArchiveModeAssertion) error {
	bindings := graphLogEvents(raw, jobID, graphObjectBoundMessage)
	for _, assertion := range assertions {
		if assertion.Archive == "" || !graphArchiveKinds[assertion.RequireKind] {
			return fmt.Errorf("invalid archive-mode assertion: archive=%q requireKind=%q", assertion.Archive, assertion.RequireKind)
		}
		for _, kind := range assertion.ForbidKinds {
			if !graphArchiveKinds[kind] || kind == assertion.RequireKind {
				return fmt.Errorf("invalid forbidden archive kind %q", kind)
			}
		}
		found := false
		for _, binding := range bindings {
			if binding["over"] != assertion.Archive {
				continue
			}
			kind := binding["kind"]
			if slices.Contains(assertion.ForbidKinds, kind) {
				return fmt.Errorf("archive %q bound forbidden kind %q", assertion.Archive, kind)
			}
			found = found || kind == assertion.RequireKind
		}
		if !found {
			return fmt.Errorf("archive %q never bound required kind %q", assertion.Archive, assertion.RequireKind)
		}
	}
	return nil
}
