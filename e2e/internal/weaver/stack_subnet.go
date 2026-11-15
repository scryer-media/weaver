// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"os"
	"regexp"
	"strings"

	"github.com/scryer-media/weaver/e2e/internal/composeutil"
)

// stackNetworkOverrideEnv names a release flow's plain network override: the
// stack's default network on one subnet, with nothing else built from that
// subnet. Only a stack with such an override can move to another subnet at
// stack-up; a layout that places fixed addresses in its subnet cannot.
const stackNetworkOverrideEnv = "E2E_STACK_NETWORK_OVERRIDE"

// maxStackSubnetRepicks bounds how often one stack-up moves to another
// subnet after the engine answers that the one it asked for is taken.
const maxStackSubnetRepicks = 3

// The gate picks every flow's subnet when it starts, and a flow can start an
// hour later. Another stack can take that subnet in between; the engine then
// refuses the network. Docker says "Pool overlaps with other one on this
// address space"; Podman says "subnet ... is already used".
func isComposeSubnetCollision(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "pool overlaps with other one on this address space") {
		return true
	}
	return strings.Contains(message, "subnet") && strings.Contains(message, "is already used")
}

var networkOverrideSubnetPattern = regexp.MustCompile(`subnet:\s*"([^"]+)"`)

// upWithStackSubnetRepick runs up and, while the engine answers that the
// stack's subnet is taken, rewrites the stack's network override with a
// subnet no live network overlaps and runs up again, a bounded number of
// times. A stack without a plain override gets the engine's answer as is.
func upWithStackSubnetRepick(
	up func() error,
	overridePath string,
	project string,
	liveSubnets func() ([]string, error),
) error {
	var tried []string
	for repicks := 0; ; repicks++ {
		err := up()
		if err == nil || overridePath == "" || !isComposeSubnetCollision(err) {
			return err
		}
		if repicks == maxStackSubnetRepicks {
			return fmt.Errorf("%w (the stack moved to another subnet %d times)", err, repicks)
		}
		previous, next, repickErr := repickStackSubnet(overridePath, project, tried, liveSubnets)
		if repickErr != nil {
			return fmt.Errorf("%w; pick another subnet: %v", err, repickErr)
		}
		tried = append(tried, previous)
		log.Printf("stack subnet %s is taken; moving the stack to %s (attempt %d/%d)", previous, next, repicks+1, maxStackSubnetRepicks)
	}
}

// repickStackSubnet moves the override to the first candidate in the same
// range as its current subnet that neither a live network nor an earlier
// try overlaps.
func repickStackSubnet(
	overridePath string,
	project string,
	tried []string,
	liveSubnets func() ([]string, error),
) (string, string, error) {
	body, err := os.ReadFile(overridePath)
	if err != nil {
		return "", "", err
	}
	match := networkOverrideSubnetPattern.FindSubmatch(body)
	if match == nil {
		return "", "", fmt.Errorf("no subnet in network override %s", overridePath)
	}
	current := string(match[1])
	prefix, err := netip.ParsePrefix(current)
	if err != nil {
		return "", "", fmt.Errorf("network override %s: %w", overridePath, err)
	}
	live, err := liveSubnets()
	if err != nil {
		return "", "", err
	}
	seed := []*weaverReleasePhase{{Project: project}}
	candidates := weaverReleaseNetworkCandidates(seed)
	if netip.MustParsePrefix("198.18.0.0/15").Overlaps(prefix) {
		candidates = weaverReleaseFetchableNetworkCandidates(seed)
	}
	avoid := append(append(append([]string(nil), live...), tried...), current)
	picked, err := composeutil.SelectNonOverlappingSubnets(1, candidates, avoid)
	if err != nil {
		return "", "", err
	}
	if err := composeutil.WriteNetworkOverride(overridePath, picked[0]); err != nil {
		return "", "", err
	}
	return current, picked[0], nil
}

func liveEngineSubnets() ([]string, error) {
	return composeutil.ListNetworkSubnets(context.Background(), e2eDir())
}
