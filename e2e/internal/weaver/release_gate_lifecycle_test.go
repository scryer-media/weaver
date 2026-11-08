// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"errors"
	"sync"
	"testing"
)

func TestWeaverReleaseProjectHoldsAnyContainerOrVolume(t *testing.T) {
	cases := []struct {
		containers, volumes string
		holds               bool
	}{
		{"", "", false},
		{"\n", "  \n", false},
		{"3f2a9c\n", "", true},
		{"", "e2e-flow_weaver-clock\n", true},
	}
	for _, c := range cases {
		if got := weaverReleaseProjectHolds(c.containers, c.volumes); got != c.holds {
			t.Errorf("holds(%q, %q) = %v, want %v", c.containers, c.volumes, got, c.holds)
		}
	}
}

func TestOnlyAFlowsFirstStackSetsItsOwnClock(t *testing.T) {
	t.Setenv("E2E_WEAVER_MODE", "1")
	t.Setenv(weaverE2EClockInitialEnv, weaverE2EClockStart)
	if !weaverReleaseStackSetsItsOwnClock(1) {
		t.Fatal("the first stack leaves the clock to the service")
	}
	if weaverReleaseStackSetsItsOwnClock(2) {
		t.Fatal("a later stack resets the clock explicitly")
	}
	t.Setenv(weaverE2EClockInitialEnv, "")
	if weaverReleaseStackSetsItsOwnClock(1) {
		t.Fatal("without the initial clock the stack resets it explicitly")
	}
	t.Setenv(weaverE2EClockInitialEnv, weaverE2EClockStart)
	t.Setenv("E2E_WEAVER_MODE", "0")
	if weaverReleaseStackSetsItsOwnClock(1) {
		t.Fatal("outside e2e mode there is no clock to set")
	}
}

// Every pull is started before any returns, and the errors come back in
// the order the pulls are listed, though they finish in reverse.
func TestEvidencePullsRunSideBySideAndJoinInOrder(t *testing.T) {
	const count = 4
	var started sync.WaitGroup
	started.Add(count)
	finished := make([]chan struct{}, count)
	for index := range finished {
		finished[index] = make(chan struct{})
	}
	pulls := make([]func() error, count)
	for index := range pulls {
		pulls[index] = func() error {
			started.Done()
			started.Wait()
			if index+1 < count {
				<-finished[index+1]
			}
			close(finished[index])
			if index%2 == 0 {
				return errors.New("pull " + string(rune('a'+index)))
			}
			return nil
		}
	}
	err := runWeaverReleasePulls(pulls)
	if err == nil || err.Error() != "pull a\npull c" {
		t.Fatalf("joined errors = %v, want pull a then pull c", err)
	}
}
