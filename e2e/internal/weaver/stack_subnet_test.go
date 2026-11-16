// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/e2e/internal/composeutil"
)

func overrideSubnet(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	match := networkOverrideSubnetPattern.FindSubmatch(body)
	if match == nil {
		t.Fatalf("no subnet in %s", body)
	}
	return string(match[1])
}

func subnetTakenError(subnet string) error {
	return errors.New("docker compose up: exit status 1\n\nLast command output:\nError: subnet " + subnet + " is already used on the host or by another config")
}

func TestStackUpMovesToAnotherSubnetWhenTheEngineSaysItIsUsed(t *testing.T) {
	override := filepath.Join(t.TempDir(), "network.compose.override.yml")
	if err := composeutil.WriteNetworkOverride(override, "10.250.177.0/24"); err != nil {
		t.Fatal(err)
	}
	// Another run holds 10.250.177.0/24 and the next candidate as well.
	taken := map[string]bool{"10.250.177.0/24": true}
	var attempts []string
	live := func() ([]string, error) {
		var out []string
		for subnet := range taken {
			out = append(out, subnet)
		}
		return out, nil
	}
	up := func() error {
		subnet := overrideSubnet(t, override)
		attempts = append(attempts, subnet)
		if taken[subnet] {
			return subnetTakenError(subnet)
		}
		return nil
	}
	if err := upWithStackSubnetRepick(up, override, "weaver-release-x", live); err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0] != "10.250.177.0/24" {
		t.Fatalf("attempts = %v, want the taken subnet then one other", attempts)
	}
	moved := netip.MustParsePrefix(attempts[1])
	if moved.Overlaps(netip.MustParsePrefix("10.250.177.0/24")) || !strings.HasPrefix(attempts[1], "10.") {
		t.Fatalf("moved to %s, want another private /24", attempts[1])
	}
}

func TestStackSubnetRepicksAreBounded(t *testing.T) {
	override := filepath.Join(t.TempDir(), "network.compose.override.yml")
	if err := composeutil.WriteNetworkOverride(override, "198.18.4.0/24"); err != nil {
		t.Fatal(err)
	}
	var attempts []string
	up := func() error {
		subnet := overrideSubnet(t, override)
		attempts = append(attempts, subnet)
		// The engine refuses every subnet, the way a racing run could.
		return errors.New("Error response from daemon: invalid pool request: Pool overlaps with other one on this address space")
	}
	err := upWithStackSubnetRepick(up, override, "weaver-release-y", func() ([]string, error) { return nil, nil })
	if err == nil || !isComposeSubnetCollision(err) {
		t.Fatalf("err = %v, want the engine's answer", err)
	}
	if len(attempts) != maxStackSubnetRepicks+1 {
		t.Fatalf("stack-up ran %d times, want %d", len(attempts), maxStackSubnetRepicks+1)
	}
	seen := map[string]bool{}
	fetchable := netip.MustParsePrefix("198.18.0.0/15")
	for _, subnet := range attempts {
		if seen[subnet] {
			t.Fatalf("subnet %s tried twice in %v", subnet, attempts)
		}
		seen[subnet] = true
		if !fetchable.Overlaps(netip.MustParsePrefix(subnet)) {
			t.Fatalf("subnet %s left the fetchable range", subnet)
		}
	}
}

func TestStackUpWithoutAPlainOverrideKeepsTheEnginesAnswer(t *testing.T) {
	calls := 0
	want := subnetTakenError("10.249.3.0/24")
	err := upWithStackSubnetRepick(func() error { calls++; return want }, "", "p", func() ([]string, error) {
		t.Fatal("listed live subnets for a stack that cannot move")
		return nil, nil
	})
	if !errors.Is(err, want) || calls != 1 {
		t.Fatalf("err = %v after %d calls, want the engine's answer after one", err, calls)
	}
	other := errors.New("docker compose up: exit status 1: no such image")
	calls = 0
	override := filepath.Join(t.TempDir(), "network.compose.override.yml")
	if err := composeutil.WriteNetworkOverride(override, "10.249.3.0/24"); err != nil {
		t.Fatal(err)
	}
	if err := upWithStackSubnetRepick(func() error { calls++; return other }, override, "p", nil); !errors.Is(err, other) || calls != 1 {
		t.Fatalf("err = %v after %d calls, want an unrelated failure returned at once", err, calls)
	}
}

func TestOnlyPlainReleaseFlowOverridesMayMove(t *testing.T) {
	phase := &weaverReleasePhase{Flow: "ui-queue", ComposeOverride: "/run/network.compose.override.yml"}
	if got := phase.env()[stackNetworkOverrideEnv]; got != phase.ComposeOverride {
		t.Fatalf("default layout flow: %s = %q, want its override", stackNetworkOverrideEnv, got)
	}
	proxy := &weaverReleasePhase{Flow: "proxy-routing", ComposeOverride: "/run/network.compose.override.yml"}
	if got, ok := proxy.env()[stackNetworkOverrideEnv]; ok {
		t.Fatalf("proxy-routing places fixed addresses in its subnet, yet %s = %q", stackNetworkOverrideEnv, got)
	}
}
