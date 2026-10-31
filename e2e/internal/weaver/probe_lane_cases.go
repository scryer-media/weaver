// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/scryer-media/weaver/e2e/internal/nntphealth"
)

func probeLaneRestartCases() []restartCase {
	return []restartCase{{
		Name: "health_probe_waits_for_owned_lane", Slugs: []string{"held-plain-uu"},
		Description: "A health STAT probe waits for the occupied BODY lane and reuses its one connection seat",
		Run:         runHealthProbeWaitsForOwnedLane,
	}}
}

func runHealthProbeWaitsForOwnedLane(ctx *restartCaseContext) (restartCaseResult, error) {
	const slug = "held-plain-uu"
	segments, err := nzbSegments(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	// More than the largest ring keeps a refill ahead of completion: the
	// waiting probe must take the yielded lane, not disappear at final drain.
	if len(segments) <= 8 || segments[0].Ordinal != 0 {
		return restartCaseResult{}, fmt.Errorf("probe fixture needs more than eight ordered articles")
	}
	var heldIDs []string
	for _, segment := range segments[1:] {
		if segment.FileIndex != 0 {
			return restartCaseResult{}, fmt.Errorf("probe fixture must have exactly one source file")
		}
		heldIDs = append(heldIDs, segment.MessageID)
	}
	wanted := make(map[string]bool, len(segments))
	for _, segment := range segments {
		wanted[strings.Trim(segment.MessageID, "<>")] = true
	}
	statCount := func(metrics restartNntpMetrics) int {
		total := 0
		for id, count := range metrics.StatCounts {
			if wanted[strings.Trim(id, "<>")] {
				total += count
			}
		}
		return total
	}
	if err := ctx.startWeaverWithConnections("", 1); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	<-ctx.weaver.done
	if _, err := waitForFreeNntpConnectionMetrics(nntpHost(), nntpPort()); err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	// Seed the fixture server's existing persisted capability while this
	// isolated process is stopped. The initial known-capable lease is depth
	// two, and the owned worker fills that ring before reading its first reply.
	// Thus the held second BODY is issued before the missing first BODY can
	// arm a probe; no warm-up timing or scheduler ordering is assumed.
	if err := withStateDB(dbPath, func(db *stateConn) error {
		var servers int
		if err := db.QueryRow(`SELECT COUNT(*) FROM servers`).Scan(&servers); err != nil {
			return err
		}
		if servers != 1 {
			return fmt.Errorf("probe case has %d servers, expected one", servers)
		}
		_, err := db.Exec(`UPDATE servers SET supports_pipelining = TRUE, pipelining_depth = 2, connections = 1`)
		return err
	}); err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "probe_server_seed.json"), map[string]any{
		"supports_pipelining": true, "pipelining_depth": 2, "connections": 1,
	})
	if err := resetNntpMetricsOn(nntpHost(), nntpPort()); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	if err := withStateDB(dbPath, func(db *stateConn) error {
		var capable bool
		var depth, connections int
		if err := db.QueryRow(`SELECT supports_pipelining, pipelining_depth, connections FROM servers`).Scan(&capable, &depth, &connections); err != nil {
			return err
		}
		if !capable || depth != 2 || connections != 1 {
			return fmt.Errorf("fixture server lost seeded capability: pipelining=%t depth=%d seats=%d", capable, depth, connections)
		}
		return nil
	}); err != nil {
		return restartCaseResult{}, err
	}
	putBack, err := withholdArticles(nntpHost(), nntpPort(), []string{segments[0].MessageID})
	if err != nil {
		return restartCaseResult{}, err
	}
	restored := false
	defer func() {
		if !restored {
			_ = putBack()
		}
	}()
	if err := setNntpChaos("hold_ids=" + strings.Join(heldIDs, ";")); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	jobID, err := ctx.submitSlug(slug)
	if err != nil {
		return restartCaseResult{}, err
	}
	jobField := regexp.MustCompile(fmt.Sprintf(`\bjob_id=%d\b`, jobID))
	if err := ctx.pollFor("this job's armed health probe behind the held second BODY", func() (bool, error) {
		log, err := os.ReadFile(ctx.caseLogPath)
		if err != nil {
			return false, err
		}
		armed := false
		for _, line := range strings.Split(stripANSIEscapeSequences(string(log)), "\n") {
			if jobField.MatchString(line) && strings.Contains(line, "health probe armed: STAT sampling") {
				armed = true
				break
			}
		}
		return armed, nil
	}); err != nil {
		return restartCaseResult{}, err
	}
	// The armed message follows handling the first 430. Ring issuance has
	// therefore already reserved the second request on this owned lane.
	// Fixture BODY counters record completed transfers, not requests; the
	// held second response must still have zero completions at this barrier.
	// Run the actual healthcheck protocol while that data seat is occupied.
	// Completion is the barrier; no timer or scheduler ordering is assumed.
	if err := nntphealth.CheckAddress(net.JoinHostPort(nntpHost(), fmt.Sprint(nntpPort())), 0); err != nil {
		return restartCaseResult{}, fmt.Errorf("overlapping control-aware healthcheck: %w", err)
	}
	bodies, err := adversarialBodyCounts([]string{heldIDs[0]})
	if err != nil {
		return restartCaseResult{}, err
	}
	blocked, err := fetchNntpStatMetricsFrom(nntpHost(), nntpPort(), "")
	if err != nil {
		return restartCaseResult{}, err
	}
	connections, err := fetchNntpConnectionMetricsFrom(nntpHost(), nntpPort())
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "probe_waiting.json"), map[string]any{
		"job_id": jobID, "held_message_id": heldIDs[0], "body_completions": bodies, "stat": blocked, "connections": connections,
	})
	if err := requireProbeWaiting(bodies[heldIDs[0]], statCount(blocked), connections.Active, connections.PeakActive); err != nil {
		return restartCaseResult{}, err
	}
	if err := liftPrimaryHold(); err != nil {
		return restartCaseResult{}, err
	}
	var released restartNntpMetrics
	if err := ctx.pollFor("waiting probe using the released lane", func() (bool, error) {
		released, err = fetchNntpStatMetricsFrom(nntpHost(), nntpPort(), "")
		return statCount(released) > 0, err
	}); err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	connections, err = fetchNntpConnectionMetricsFrom(nntpHost(), nntpPort())
	if err != nil {
		return restartCaseResult{}, err
	}
	writeRestartJSON(filepath.Join(ctx.CaseDir, "probe_released.json"), map[string]any{
		"stat": released, "connections": connections, "status": statuses[jobID],
	})
	if connections.PeakActive > 1 || statuses[jobID] != "FAILED" {
		return restartCaseResult{}, fmt.Errorf("probe release: peak=%d final=%s, expected one seat and FAILED without the UU prefix", connections.PeakActive, statuses[jobID])
	}
	if err := putBack(); err != nil {
		return restartCaseResult{}, err
	}
	restored = true
	return restartCaseResult{Classification: restartPass, Summary: "Armed health probe waited for held BODY, then issued STAT through the released lane; provider peak stayed at one; missing UU prefix failed without usable output"}, nil
}

func requireProbeWaiting(bodies, stats int, active, peak int64) error {
	var problems []string
	if bodies != 0 {
		problems = append(problems, fmt.Sprintf("held BODY completed %d time(s)", bodies))
	}
	if stats != 0 {
		problems = append(problems, fmt.Sprintf("early STAT requests=%d", stats))
	}
	if active != 1 {
		problems = append(problems, fmt.Sprintf("active data sessions=%d, want 1", active))
	}
	if peak > 1 {
		problems = append(problems, fmt.Sprintf("historical data-session peak=%d, want <=1", peak))
	}
	if len(problems) > 0 {
		return fmt.Errorf("probe occupied-lane assertion: %s", strings.Join(problems, "; "))
	}
	return nil
}
