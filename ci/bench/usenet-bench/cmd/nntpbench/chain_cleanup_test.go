package main

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type cleanupTestStack struct {
	calls          []ChainPhase
	fail           string
	failure        error
	restoreFailure bool
}

func (*cleanupTestStack) preflight(func(string, ...any)) error { return nil }
func (*cleanupTestStack) stop(func(string, ...any))            {}
func (s *cleanupTestStack) apply(_ ChainConfig, phase ChainPhase, _ func(string, ...any)) error {
	s.calls = append(s.calls, phase)
	if phase.Name == s.fail {
		return s.failure
	}
	if phase.Name == "restore" && s.restoreFailure {
		return errors.New("restore failed")
	}
	return nil
}

func TestChainRestoresAfterFailedTransition(t *testing.T) {
	for _, tc := range []struct {
		name, fail, restore     string
		restoreFailure          bool
		wantCalls, wantMeasured []string
	}{
		{"success", "", "unlimited", false, []string{"first", "second", "restore"}, []string{"first", "same-link", "second"}},
		{"later failure", "second", "unlimited", false, []string{"first", "second", "restore"}, []string{"first", "same-link"}},
		{"restore failure keeps original error", "second", "unlimited", true, []string{"first", "second", "restore"}, []string{"first", "same-link"}},
		{"first failure", "first", "unlimited", false, []string{"first"}, nil},
		{"restore disabled", "second", "", false, []string{"first", "second"}, []string{"first", "same-link"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := errors.New("transition failed")
			stack := &cleanupTestStack{fail: tc.fail, failure: failure, restoreFailure: tc.restoreFailure}
			var measured, logs []string
			config := ChainConfig{RestoreServerLink: tc.restore, RestoreServerRTT: "0ms"}
			err := runChainPhases(config, []ChainPhase{
				{Name: "first", ServerLink: "1gbit"},
				{Name: "same-link", ServerLink: "1gbit"},
				{Name: "second", ServerLink: "100mbit"},
			}, stack, 0, func(format string, args ...any) {
				logs = append(logs, fmt.Sprintf(format, args...))
			}, func(phase ChainPhase) { measured = append(measured, phase.Name) })
			if tc.fail == "" && err != nil || tc.fail != "" && !errors.Is(err, failure) {
				t.Fatalf("unexpected phase error: %v", err)
			}
			var calls []string
			for _, phase := range stack.calls {
				calls = append(calls, phase.Name)
				if phase.Name == "restore" && (phase.ServerLink != tc.restore || phase.ServerRTT != "0ms") {
					t.Fatalf("wrong restore conditions: %+v", phase)
				}
			}
			if !reflect.DeepEqual(calls, tc.wantCalls) || !reflect.DeepEqual(measured, tc.wantMeasured) {
				t.Fatalf("calls %v, measured %v; want %v, %v", calls, measured, tc.wantCalls, tc.wantMeasured)
			}
			if tc.restoreFailure && !strings.Contains(strings.Join(logs, "\n"), "could not restore the shaper") {
				t.Fatal("restore failure was not reported")
			}
		})
	}
}
