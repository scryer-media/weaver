// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import "testing"

func TestTerminalWaitAcceptsPartialHistoryStatus(t *testing.T) {
	for _, state := range []string{"COMPLETE", "completed", "FAILED", "partial", " PARTIAL "} {
		if !facadeTerminalStatus(state) {
			t.Errorf("terminal state %q would keep waiting", state)
		}
	}
	for _, state := range []string{"", "QUEUED", "DOWNLOADING", "PAUSED", "EXTRACTING"} {
		if facadeTerminalStatus(state) {
			t.Errorf("active state %q ended the wait", state)
		}
	}
}
