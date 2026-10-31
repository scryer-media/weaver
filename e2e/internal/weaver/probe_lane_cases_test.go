// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"strings"
	"testing"
)

func TestProbeWaitingDistinguishesEachInvariant(t *testing.T) {
	if err := requireProbeWaiting(0, 0, 1, 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body, stat   int
		active, peak int64
		want         string
	}{
		{1, 0, 1, 1, "held BODY completed"},
		{0, 1, 1, 1, "early STAT"},
		{0, 0, 2, 2, "active data sessions"},
		{0, 0, 1, 2, "historical data-session peak"},
	} {
		if err := requireProbeWaiting(tc.body, tc.stat, tc.active, tc.peak); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}
