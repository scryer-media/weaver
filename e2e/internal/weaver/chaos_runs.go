package weaver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// cmdChaosTest runs the full test suite cleanly first (baseline), then repeats
// it 5 times with different chaos configurations active. This verifies weaver
// can recover from transient NNTP failures and still complete all downloads.
func cmdChaosTest() {
	ensureStandardDockerInfrastructure()
	if err := ensureBackupNntpReady(); err != nil {
		log.Fatalf("prepare backup NNTP for chaos test: %v", err)
	}
	if err := ensureStandardManagedWeaver(); err != nil {
		log.Fatalf("restart managed weaver for chaos test: %v", err)
	}
	weaverURL := defaultWeaverURL()
	prepareStandardTestRun(weaverURL, true)
	defer func() {
		if err := ensureNntpChaosOff(); err != nil {
			log.Printf("warning: final NNTP chaos reset failed: %v", err)
		}
	}()

	// Load the success workload from the canonical fixture set, but load the
	// explicit STAT probe helper directly by slug because it is intentionally
	// seeded for chaos only and must not join the canonical success workload.
	allScenarios := loadCanonicalScenarios()
	statProbeScenario, err := loadScenario(filepath.Join(testdataDir(), chaosStatProbeSlug))
	if err != nil {
		log.Fatalf("load NNTP chaos STAT probe scenario %q: %v", chaosStatProbeSlug, err)
	}
	var scenarios []*Scenario
	for _, s := range allScenarios {
		// Only use scenarios that expect success — skip error/failure scenarios
		if s.ExpectedOutcome == "success" || s.ExpectedOutcome == "repair_then_success" {
			scenarios = append(scenarios, s)
		}
	}

	scenarios = filterChaosScenarios(scenarios)
	log.Printf("loaded %d scenarios for chaos testing", len(scenarios))

	type chaosRound struct {
		name             string
		config           string
		requireStatChaos bool
		// providerCap is the primary server's connection cap for the round,
		// set below the lanes weaver is configured to open. The round then
		// asserts the provider refused connects and that the cap held.
		//
		// The provider counts every session, including this harness's own
		// control connection, and weaver keeps its accepted lanes cached
		// between jobs. Weaver is therefore stopped before the cap is lifted
		// and started fresh before it is applied, so no round inherits the
		// previous round's open sockets and the control commands always get a
		// slot.
		providerCap int
		// newsgroupPerJob submits each job for its own newsgroup, so every job
		// boundary hands a cached connection work for a different group. With
		// providerCap this reproduces the stall where connections opened for
		// one job were dropped at the next job and no replacement was admitted.
		newsgroupPerJob bool
	}

	rounds := []chaosRound{
		{"baseline (no chaos)", "", false, 0, false},
		{"201 greetings on all connects", "greet_201=100", false, 0, false},
		{"400 greetings on 30% of connects", "greet_400=30", false, 0, false},
		{"drop 30% connections", "drop_conn=30", false, 0, false},
		{"reject 50% auth", "reject_auth=50", false, 0, false},
		{"force BODY re-auth on 50% of requests", "reauth_body=50", false, 0, false},
		{"split BODY terminator on all requests", "split_term=100", false, 0, false},
		{"drop 10% of BODY responses mid-transfer", "drop_mid_body=10", false, 0, false},
		{"malformed terminator on 10% of BODY responses", "bad_term=10", false, 0, false},
		{"corrupt 5% bodies", "corrupt_body=5", false, 0, false},
		{"timeout 10% bodies", "timeout_body=10", false, 0, false},
		{"STAT bad code on all requests", "stat_bad_code=100", true, 0, false},
		{"STAT short response on all requests", "stat_short=100", true, 0, false},
		{"combined: STAT bad code 100% + drop mid-body 5%", "stat_bad_code=100,drop_mid_body=5", true, 0, false},
		{"combined: reauth 30% + drop mid-body 5% + slow 5ms", "reauth_body=30,drop_mid_body=5,slow_body=5", false, 0, false},
		{
			name:            "provider cap below configured lanes, new newsgroup per job",
			config:          "max_conns=4,slow_body=5",
			providerCap:     4,
			newsgroupPerJob: true,
		},
	}

	onlyRound := 0
	if value := os.Getenv("CHAOS_ONLY_ROUND"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > len(rounds) {
			log.Fatalf("invalid CHAOS_ONLY_ROUND=%q (expected 1-%d)", value, len(rounds))
		}
		onlyRound = parsed
		log.Printf("running only chaos round %d: %s", parsed, rounds[parsed-1].name)
	}
	totalRounds := len(rounds)
	if onlyRound != 0 {
		totalRounds = 1
	}
	phaseTotal := len(scenarios) * totalRounds
	overallResolved := 0
	emitProgressEvent(progressEvent{Kind: "phase_total", Total: phaseTotal, Detail: "NNTP chaos"})

	totalPass := 0
	totalFail := 0
	executedRounds := 0
	chaosRunRoot := filepath.Join(localRunDir(), "nntp-chaos", time.Now().Format("20060102-150405"))
	if err := os.MkdirAll(chaosRunRoot, 0o755); err != nil {
		log.Fatalf("create NNTP chaos artifact dir: %v", err)
	}

	for roundIdx, round := range rounds {
		if onlyRound != 0 && roundIdx+1 != onlyRound {
			continue
		}
		executedRounds++
		fmt.Printf("\n=== ROUND %d/%d: %s ===\n", roundIdx+1, len(rounds), round.name)
		emitProgressEvent(progressEvent{Kind: "phase_note", Detail: round.name})

		// Keep control-plane operations out of the chaos blast radius. The
		// workload below still runs with the round's chaos enabled.
		if round.providerCap > 0 {
			killWeaver()
		}
		if err := ensureNntpChaosOff(); err != nil {
			log.Fatalf("reset NNTP chaos before round %q: %v", round.name, err)
		}
		if err := resetNntpMetrics(); err != nil {
			log.Fatalf("reset NNTP metrics before round %q: %v", round.name, err)
		}
		if round.config != "" {
			if err := setNntpChaos(round.config); err != nil {
				log.Fatalf("enable NNTP chaos for round %q: %v", round.name, err)
			}
			log.Printf("chaos enabled: %s", round.config)
		}
		if round.providerCap > 0 {
			if err := restartStandardManagedWeaverPreservingState(); err != nil {
				log.Fatalf("start managed weaver under provider cap for round %q: %v", round.name, err)
			}
		}

		// Submit in small batches so NNTP chaos exercises recovery behavior
		// instead of turning into a queue-length timeout test.
		type job struct {
			slug   string
			jobID  int
			status string
		}
		var jobs []job

		timeout := 3 * time.Minute
		if round.config != "" {
			timeout = 5 * time.Minute
		}
		const chaosBatchSize = 4
		for batchStart := 0; batchStart < len(scenarios); batchStart += chaosBatchSize {
			batchEnd := batchStart + chaosBatchSize
			if batchEnd > len(scenarios) {
				batchEnd = len(scenarios)
			}
			batch := scenarios[batchStart:batchEnd]
			batchJobs := make([]job, 0, len(batch))

			for _, s := range batch {
				options := submitNZBOptions{force: roundIdx > 0}
				if round.newsgroupPerJob {
					options.newsgroup = "alt.binaries." + s.Slug
				}
				jobID, err := submitOneNZBWithOptions(weaverURL, s, options)
				if err != nil {
					log.Printf("  %s: submit error: %v", s.Slug, err)
					batchJobs = append(batchJobs, job{slug: s.Slug, status: "ERROR"})
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "error",
						Detail:  s.Slug,
					})
					continue
				}
				batchJobs = append(batchJobs, job{slug: s.Slug, jobID: jobID})
			}

			deadline := time.Now().Add(timeout)
			pending := 0
			for _, j := range batchJobs {
				if j.status == "" {
					pending++
				}
			}

			for pending > 0 && time.Now().Before(deadline) {
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("NNTP chaos round %d batch polling", roundIdx+1))
				for i := range batchJobs {
					if batchJobs[i].status != "" {
						continue
					}
					s := pollJobOnce(weaverURL, batchJobs[i].jobID)
					if s == "COMPLETE" || s == "FAILED" {
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							s,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			if pending > 0 {
				remainingIDs := make([]int, 0, pending)
				for _, job := range batchJobs {
					if job.status == "" && job.jobID > 0 {
						remainingIDs = append(remainingIDs, job.jobID)
					}
				}
				if len(remainingIDs) > 0 {
					log.Printf("reconciling %d unresolved NNTP chaos job(s) before timeout scoring", len(remainingIDs))
					reconciled := reconcileTerminalSnapshots(
						weaverURL,
						remainingIDs,
						20*time.Second,
						fmt.Sprintf("NNTP chaos round %d final reconciliation", roundIdx+1),
					)
					for i := range batchJobs {
						if batchJobs[i].status != "" {
							continue
						}
						snapshot, ok := reconciled[batchJobs[i].jobID]
						if !ok {
							continue
						}
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							snapshot.Status,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			timedOutJobs := 0
			for i := range batchJobs {
				if batchJobs[i].status == "" {
					batchJobs[i].status = "TIMEOUT"
					timedOutJobs++
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "timeout",
						Detail:  batchJobs[i].slug,
					})
				}
			}

			if timedOutJobs > 0 {
				log.Printf("canceling %d timed out job(s) before next NNTP chaos batch", timedOutJobs)
				for _, j := range batchJobs {
					if j.status != "TIMEOUT" || j.jobID == 0 {
						continue
					}
					if err := cancelJobGraphQL(weaverURL, j.jobID); err != nil {
						log.Printf("  WARNING: cancel timed out job %s (%d): %v", j.slug, j.jobID, err)
						continue
					}
					if err := waitForJobCancelSettledGraphQL(
						weaverURL,
						j.jobID,
						weaverCancelSettleTimeout,
						weaverCancelSettlePollInterval,
					); err != nil {
						log.Printf("  queue snapshot after failed cancel settle: %s", describeJobsGraphQL(weaverURL))
						log.Printf("  Weaver log tail after failed cancel settle:\n%s", localWeaverLogTail(40))
						log.Fatalf("timed out NNTP chaos job %s (%d) did not settle after cancel: %v", j.slug, j.jobID, err)
					}
				}
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("NNTP chaos round %d batch cleanup", roundIdx+1))
			}

			jobs = append(jobs, batchJobs...)
		}

		// Score
		roundPass := 0
		roundFail := 0
		for _, j := range jobs {
			if j.status == "COMPLETE" {
				roundPass++
			} else {
				roundFail++
				log.Printf("  FAIL: %s = %s", j.slug, j.status)
			}
		}
		totalPass += roundPass
		totalFail += roundFail
		fmt.Printf("  Round result: %d/%d passed\n", roundPass, len(jobs))

		if round.requireStatChaos {
			metrics, err := fetchNntpStatMetrics("")
			if err != nil {
				log.Printf("  FAIL: fetch STAT metrics for round %q: %v", round.name, err)
				roundFail++
				totalFail++
			} else if metrics.StatChaosHits <= 0 {
				probeStatus, probeErr := runChaosStatProbeScenario(
					weaverURL,
					statProbeScenario,
					round.name,
					statOnlyChaosConfig(round.config),
				)
				if probeErr != nil {
					log.Printf("  FAIL: round %q STAT probe scenario %q: %v", round.name, statProbeScenario.Slug, probeErr)
					roundFail++
					totalFail++
				} else {
					log.Printf(
						"  STAT probe scenario %s reached terminal status %s",
						statProbeScenario.Slug,
						probeStatus,
					)
					metrics, err = fetchNntpStatMetrics("")
					if err != nil {
						log.Printf("  FAIL: fetch STAT metrics after probe scenario for round %q: %v", round.name, err)
						roundFail++
						totalFail++
					} else if metrics.StatChaosHits <= 0 {
						log.Printf("  FAIL: round %q recorded no STAT chaos hits after probe scenario %q", round.name, statProbeScenario.Slug)
						roundFail++
						totalFail++
					} else {
						log.Printf(
							"  STAT metrics: %d requests, %d chaos hits after probe scenario %s",
							len(metrics.StatCounts),
							metrics.StatChaosHits,
							statProbeScenario.Slug,
						)
					}
				}
			} else {
				log.Printf("  STAT metrics: %d requests, %d chaos hits", len(metrics.StatCounts), metrics.StatChaosHits)
			}
		}

		if round.providerCap > 0 {
			// Weaver's cached lanes hold the capped slots; stop it so the
			// provider answers the harness again, then read the round's
			// connection counters before the cap is lifted.
			killWeaver()
			connections, err := fetchNntpConnectionMetricsFrom(nntpHost(), nntpPort())
			if err != nil {
				log.Printf("  FAIL: fetch connection metrics for round %q: %v", round.name, err)
				roundFail++
				totalFail++
			} else {
				log.Printf(
					"  connection metrics: limit %d, attempted %d, accepted %d, rejected %d, peak active %d",
					connections.ConfiguredLimit,
					connections.Attempted,
					connections.Accepted,
					connections.Rejected,
					connections.PeakActive,
				)
				if connections.ConfiguredLimit != round.providerCap {
					log.Printf("  FAIL: round %q provider cap was %d, expected %d", round.name, connections.ConfiguredLimit, round.providerCap)
					roundFail++
					totalFail++
				}
				if connections.Rejected <= 0 {
					log.Printf("  FAIL: round %q provider refused no connects, so the cap was never hit", round.name)
					roundFail++
					totalFail++
				}
				if connections.PeakActive > int64(round.providerCap) {
					log.Printf("  FAIL: round %q provider held %d connections over its cap of %d", round.name, connections.PeakActive, round.providerCap)
					roundFail++
					totalFail++
				}
			}
		}

		sendNntpCommand("CHAOS off")

		if round.providerCap > 0 {
			if err := restartStandardManagedWeaverPreservingState(); err != nil {
				log.Fatalf("restart managed weaver after provider cap round %q: %v", round.name, err)
			}
		}

		// Post-round diagnostics
		var diagIDs []int
		var diagStatuses []string
		roundArtifacts := make([]chaosRoundJobArtifact, 0, len(jobs))
		for _, j := range jobs {
			diagIDs = append(diagIDs, j.jobID)
			diagStatuses = append(diagStatuses, j.status)
			roundArtifacts = append(roundArtifacts, chaosRoundJobArtifact{
				Slug:   j.slug,
				JobID:  j.jobID,
				Status: j.status,
			})
		}
		printRoundDiagnostics(weaverURL, diagIDs, diagStatuses, true)
		writeChaosRoundArtifacts(chaosRunRoot, roundIdx+1, round.name, round.config, roundArtifacts, weaverURL)

		// Clean up weaver state between rounds — cancel active jobs and
		// delete history so stale jobs don't clog the pipeline.
		for _, j := range jobs {
			cancelJobGraphQL(weaverURL, j.jobID)
		}
		deleteAllHistoryGraphQL(weaverURL)
		time.Sleep(3 * time.Second)
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("CHAOS TEST TOTAL: %d passed, %d failed across %d rounds\n", totalPass, totalFail, executedRounds)

	if totalFail > 0 {
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "fail"})
		os.Exit(1)
	}
	emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "pass"})
}

// cmdTcpChaosTest runs tests through toxiproxy, injecting real TCP-level
// failures: latency, connection resets, bandwidth limits, and timeouts.
// Manages the full lifecycle: starts toxiproxy, restarts weaver with
// toxiproxy ports, runs chaos rounds, then restores original config.
func cmdTcpChaosTest() {
	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	weaverURL := fmt.Sprintf("http://localhost:%s", weaverPort)
	configPath := localWeaverConfigPath()

	// Ensure toxiproxy and the clean backup NNTP server are running.
	log.Println("starting toxiproxy and backup NNTP containers...")
	if err := dockerComposeUp("nntp", "nntp2", "toxiproxy"); err != nil {
		log.Fatalf("failed to start tcp-chaos infrastructure: %v", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 15*time.Second)
	waitForHTTP(toxiproxyURL()+"/version", 15*time.Second)
	if err := ensureNntpChaosOff(); err != nil {
		log.Fatalf("reset NNTP chaos before tcp-chaos: %v", err)
	}
	syncArticlesToBackup()

	// Verify toxiproxy proxies are configured
	resp, err := http.Get(toxiproxyURL() + "/proxies")
	if err != nil {
		log.Fatalf("toxiproxy not reachable at %s: %v", toxiproxyURL(), err)
	}
	resp.Body.Close()
	removeAllToxics()
	log.Println("cleared existing toxiproxy toxics before startup")

	// Kill any existing weaver
	killWeaver()

	// Write weaver config pointing through toxiproxy
	port1 := mustPortInt("TOXIPROXY_NNTP1_PORT", toxiproxyNntp1Port())
	port2 := mustPortInt("TOXIPROXY_NNTP2_PORT", toxiproxyNntp2Port())
	log.Printf("configuring weaver to use toxiproxy ports (%d/%d)...", port1, port2)
	writeWeaverConfig(configPath, port1, port2)

	// Clean weaver state
	cleanWeaverState()

	// Start weaver
	log.Println("starting weaver...")
	weaverCmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	weaverCmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug")
	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFile, _ := os.Create(localWeaverLogPath())
	weaverCmd.Stdout = logFile
	weaverCmd.Stderr = logFile
	if err := weaverCmd.Start(); err != nil {
		log.Fatalf("failed to start weaver: %v", err)
	}
	_ = os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(weaverCmd.Process.Pid)+"\n"), 0o644)
	defer func() {
		weaverCmd.Process.Kill()
		weaverCmd.Wait()
		_ = os.Remove(localWeaverPIDPath())
		logFile.Close()
	}()
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	log.Println("weaver ready")

	// Load canonical success-path scenarios
	var scenarios []*Scenario
	for _, s := range loadCanonicalScenarios() {
		if s.ExpectedOutcome == "success" || s.ExpectedOutcome == "repair_then_success" {
			scenarios = append(scenarios, s)
		}
	}
	scenarios = filterTcpChaosScenarios(scenarios)
	log.Printf("loaded %d scenarios for TCP chaos testing", len(scenarios))

	type tcpChaosRound struct {
		name  string
		setup func() // add toxics
	}

	rounds := []tcpChaosRound{
		{
			name: "200ms latency + 50ms jitter on primary",
			setup: func() {
				addToxic("nntp1", "latency", "latency", "downstream", map[string]interface{}{
					"latency": 200, "jitter": 50,
				})
			},
		},
		{
			name: "reset 20% connections on primary",
			setup: func() {
				addToxic("nntp1", "reset", "reset_peer", "downstream", map[string]interface{}{
					"timeout": 500,
				})
			},
		},
		{
			name: "1MB/s bandwidth limit on primary",
			setup: func() {
				addToxic("nntp1", "bandwidth", "bandwidth", "downstream", map[string]interface{}{
					"rate": 1024, // KB/s = 1MB/s
				})
			},
		},
		{
			name: "30s timeout on primary, clean backup",
			setup: func() {
				// Toxiproxy cuts connection after 30s of data transfer.
				// Weaver's command_timeout is 60s, so this simulates a server
				// that starts responding then dies mid-transfer.
				addToxic("nntp1", "timeout", "timeout", "downstream", map[string]interface{}{
					"timeout": 30000,
				})
			},
		},
		{
			name: "combined: 100ms latency + 500KB/s limit on primary",
			setup: func() {
				addToxic("nntp1", "latency", "latency", "downstream", map[string]interface{}{
					"latency": 100, "jitter": 30,
				})
				addToxic("nntp1", "bandwidth", "bandwidth", "downstream", map[string]interface{}{
					"rate": 512, // KB/s
				})
			},
		},
		{
			// A far server with plenty of bandwidth: each article costs far
			// less on the wire than the round trip to ask for it, which is the
			// shape that drives the BODY depth explorer to its deepest rung.
			// No bandwidth toxic on purpose — capping the link would make
			// transfer dominate again and hold the depth shallow.
			name: "900ms latency, uncapped bandwidth on primary (deep BODY pipelining)",
			setup: func() {
				addToxic("nntp1", "latency", "latency", "downstream", map[string]interface{}{
					"latency": 900, "jitter": 40,
				})
			},
		},
	}

	onlyRound := 0
	if value := os.Getenv("TCP_CHAOS_ONLY_ROUND"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > len(rounds) {
			log.Fatalf("invalid TCP_CHAOS_ONLY_ROUND=%q (expected 1-%d)", value, len(rounds))
		}
		onlyRound = parsed
		log.Printf("running only TCP chaos round %d: %s", parsed, rounds[parsed-1].name)
	}
	totalRounds := len(rounds)
	if onlyRound != 0 {
		totalRounds = 1
	}
	phaseTotal := len(scenarios) * totalRounds
	overallResolved := 0
	emitProgressEvent(progressEvent{Kind: "phase_total", Total: phaseTotal, Detail: "TCP chaos"})

	totalPass := 0
	totalFail := 0

	for roundIdx, round := range rounds {
		if onlyRound != 0 && roundIdx+1 != onlyRound {
			continue
		}
		fmt.Printf("\n=== TCP CHAOS ROUND %d/%d: %s ===\n", roundIdx+1, len(rounds), round.name)
		emitProgressEvent(progressEvent{Kind: "phase_note", Detail: round.name})

		// Clean slate
		removeAllToxics()
		round.setup()
		log.Printf("toxics configured for round %d", roundIdx+1)

		// Submit in small batches so TCP chaos exercises failover behavior
		// instead of turning into a queue-length timeout test.
		type job struct {
			slug   string
			jobID  int
			status string
		}
		var jobs []job
		const tcpChaosBatchSize = 4
		for batchStart := 0; batchStart < len(scenarios); batchStart += tcpChaosBatchSize {
			batchEnd := batchStart + tcpChaosBatchSize
			if batchEnd > len(scenarios) {
				batchEnd = len(scenarios)
			}
			batch := scenarios[batchStart:batchEnd]
			batchJobs := make([]job, 0, len(batch))

			for _, s := range batch {
				jobID, err := submitOneNZBWithOptions(weaverURL, s, submitNZBOptions{force: roundIdx > 0})
				if err != nil {
					log.Printf("  %s: submit error: %v", s.Slug, err)
					batchJobs = append(batchJobs, job{slug: s.Slug, status: "ERROR"})
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "error",
						Detail:  s.Slug,
					})
					continue
				}
				batchJobs = append(batchJobs, job{slug: s.Slug, jobID: jobID})
			}

			// Keep the timeout generous enough to allow real failover work to
			// finish, but scope it to the active batch instead of the whole round.
			deadline := time.Now().Add(10 * time.Minute)
			pending := 0
			for _, j := range batchJobs {
				if j.status == "" {
					pending++
				}
			}

			for pending > 0 && time.Now().Before(deadline) {
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("TCP chaos round %d batch polling", roundIdx+1))
				for i := range batchJobs {
					if batchJobs[i].status != "" {
						continue
					}
					s := pollJobOnce(weaverURL, batchJobs[i].jobID)
					if s == "COMPLETE" || s == "FAILED" {
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							s,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			if pending > 0 {
				remainingIDs := make([]int, 0, pending)
				for _, job := range batchJobs {
					if job.status == "" && job.jobID > 0 {
						remainingIDs = append(remainingIDs, job.jobID)
					}
				}
				if len(remainingIDs) > 0 {
					log.Printf("reconciling %d unresolved TCP chaos job(s) before timeout scoring", len(remainingIDs))
					reconciled := reconcileTerminalSnapshots(
						weaverURL,
						remainingIDs,
						20*time.Second,
						fmt.Sprintf("TCP chaos round %d final reconciliation", roundIdx+1),
					)
					for i := range batchJobs {
						if batchJobs[i].status != "" {
							continue
						}
						snapshot, ok := reconciled[batchJobs[i].jobID]
						if !ok {
							continue
						}
						batchJobs[i].status, _ = applyTerminalStateCheck(
							localWeaverDBPath(),
							batchJobs[i].jobID,
							batchJobs[i].slug,
							snapshot.Status,
						)
						pending--
						overallResolved++
						emitProgressEvent(progressEvent{
							Kind:    "phase_progress",
							Current: overallResolved,
							Total:   phaseTotal,
							Status:  strings.ToLower(batchJobs[i].status),
							Detail:  batchJobs[i].slug,
						})
					}
				}
			}

			timedOutJobs := 0
			for i := range batchJobs {
				if batchJobs[i].status == "" {
					batchJobs[i].status = "TIMEOUT"
					timedOutJobs++
					overallResolved++
					emitProgressEvent(progressEvent{
						Kind:    "phase_progress",
						Current: overallResolved,
						Total:   phaseTotal,
						Status:  "timeout",
						Detail:  batchJobs[i].slug,
					})
				}
			}

			if timedOutJobs > 0 {
				log.Printf("canceling %d timed out job(s) before next TCP chaos batch", timedOutJobs)
				for _, j := range batchJobs {
					if j.status != "TIMEOUT" || j.jobID == 0 {
						continue
					}
					if err := cancelJobGraphQL(weaverURL, j.jobID); err != nil {
						log.Printf("  WARNING: cancel timed out job %s (%d): %v", j.slug, j.jobID, err)
						continue
					}
					if err := waitForJobCancelSettledGraphQL(
						weaverURL,
						j.jobID,
						weaverCancelSettleTimeout,
						weaverCancelSettlePollInterval,
					); err != nil {
						log.Printf("  queue snapshot after failed cancel settle: %s", describeJobsGraphQL(weaverURL))
						log.Printf("  Weaver log tail after failed cancel settle:\n%s", localWeaverLogTail(40))
						log.Fatalf("timed out TCP chaos job %s (%d) did not settle after cancel: %v", j.slug, j.jobID, err)
					}
				}
				mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("TCP chaos round %d batch cleanup", roundIdx+1))
			}

			jobs = append(jobs, batchJobs...)
		}

		// Remove toxics before scoring
		removeAllToxics()

		// Score
		roundPass := 0
		for _, j := range jobs {
			if j.status == "COMPLETE" {
				roundPass++
			} else {
				log.Printf("  FAIL: %s = %s", j.slug, j.status)
			}
		}
		totalPass += roundPass
		totalFail += len(jobs) - roundPass
		fmt.Printf("  Round result: %d/%d passed\n", roundPass, len(jobs))

		// Post-round diagnostics
		var diagIDs []int
		var diagStatuses []string
		for _, j := range jobs {
			diagIDs = append(diagIDs, j.jobID)
			diagStatuses = append(diagStatuses, j.status)
		}
		printRoundDiagnostics(weaverURL, diagIDs, diagStatuses, false)

		// Cleanup between rounds
		for _, j := range jobs {
			cancelJobGraphQL(weaverURL, j.jobID)
		}
		deleteAllHistoryGraphQL(weaverURL)
		time.Sleep(3 * time.Second)
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("TCP CHAOS TEST TOTAL: %d passed, %d failed across %d rounds\n", totalPass, totalFail, totalRounds)

	if totalFail > 0 {
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "fail"})
		os.Exit(1)
	}
	emitProgressEvent(progressEvent{Kind: "phase_done", Current: phaseTotal, Total: phaseTotal, Status: "pass"})
}

// cmdAdaptiveDispatchTest verifies that Weaver's latency-aware server ordering
// prefers the lower-latency server within a priority group.
func cmdAdaptiveDispatchTest() {
	scenarioSlug := strings.TrimSpace(env("ADAPTIVE_DISPATCH_SCENARIO", "large-segments"))
	latencyMs := envInt("ADAPTIVE_DISPATCH_LATENCY_MS", 75)
	jitterMs := envInt("ADAPTIVE_DISPATCH_JITTER_MS", 10)
	connections := envInt("ADAPTIVE_DISPATCH_CONNECTIONS", 8)
	minDirectPct := envInt("ADAPTIVE_DISPATCH_MIN_DIRECT_PCT", 60)
	sampleIntervalMs := envInt("ADAPTIVE_DISPATCH_SAMPLE_MS", 250)
	timeoutSec := envInt("ADAPTIVE_DISPATCH_TIMEOUT_SEC", 300)
	if scenarioSlug == "" {
		log.Fatalf("ADAPTIVE_DISPATCH_SCENARIO must not be empty")
	}
	if latencyMs <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_LATENCY_MS must be positive")
	}
	if jitterMs < 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_JITTER_MS must not be negative")
	}
	if connections <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_CONNECTIONS must be positive")
	}
	if minDirectPct < 1 || minDirectPct > 100 {
		log.Fatalf("ADAPTIVE_DISPATCH_MIN_DIRECT_PCT must be between 1 and 100")
	}
	if sampleIntervalMs <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_SAMPLE_MS must be positive")
	}
	if timeoutSec <= 0 {
		log.Fatalf("ADAPTIVE_DISPATCH_TIMEOUT_SEC must be positive")
	}

	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	weaverURL := fmt.Sprintf("http://localhost:%s", weaverPort)
	configPath := localWeaverConfigPath()
	outputDir := filepath.Join(localRunDir(), "adaptive-dispatch")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create adaptive-dispatch output dir: %v", err)
	}

	log.Println("starting adaptive-dispatch infrastructure...")
	if err := ensureSeedingInfrastructureErr(); err != nil {
		log.Fatalf("start seeding infrastructure: %v", err)
	}
	if err := dockerComposeUp("nntp2", "toxiproxy"); err != nil {
		log.Fatalf("start adaptive-dispatch backup/proxy infrastructure: %v", err)
	}
	if err := refreshRuntimePortEnvFromRunningStack(); err != nil {
		log.Fatalf("refresh runtime ports after starting adaptive-dispatch infrastructure: %v", err)
	}
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	waitForTCP("localhost:"+backupNntpPort(), 30*time.Second)
	waitForHTTP(toxiproxyURL()+"/version", 15*time.Second)
	if err := ensureNntpChaosOff(); err != nil {
		log.Fatalf("reset NNTP chaos before adaptive-dispatch: %v", err)
	}
	removeAllToxics()
	defer removeAllToxics()

	scenarioDir := filepath.Join(testdataDir(), scenarioSlug)
	scenario, err := loadScenario(scenarioDir)
	if err != nil {
		log.Fatalf("load adaptive-dispatch scenario %s: %v", scenarioSlug, err)
	}
	if scenario.ExpectedOutcome != "success" {
		log.Fatalf("adaptive-dispatch scenario %s must be a success-path fixture, got %q", scenario.Slug, scenario.ExpectedOutcome)
	}
	if err := seedFixtureWithRetry(scenarioDir, envInt("ADAPTIVE_DISPATCH_SEED_RETRIES", 3)); err != nil {
		log.Fatalf("seed adaptive-dispatch scenario %s: %v", scenario.Slug, err)
	}
	syncArticlesToBackup()

	killWeaver()
	cleanWeaverState()

	latentPort := mustPortInt("TOXIPROXY_NNTP1_PORT", toxiproxyNntp1Port())
	directPort := mustPortInt("NNTP_BACKUP_PORT", backupNntpPort())
	writeAdaptiveDispatchWeaverConfig(configPath, latentPort, directPort, connections)
	if err := addToxic("nntp1", "adaptive-latency", "latency", "downstream", map[string]interface{}{
		"latency": latencyMs,
		"jitter":  jitterMs,
	}); err != nil {
		log.Fatalf("add adaptive-dispatch latency toxic: %v", err)
	}
	log.Printf(
		"adaptive-dispatch config: server1=toxiproxy:%d +%dms/%dms jitter, server2=nntp2:%d direct, connections=%d",
		latentPort,
		latencyMs,
		jitterMs,
		directPort,
		connections,
	)

	log.Println("starting weaver for adaptive-dispatch...")
	weaverCmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	weaverCmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug,weaver_nntp=debug")
	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFile, err := os.Create(localWeaverLogPath())
	if err != nil {
		log.Fatalf("create managed weaver log: %v", err)
	}
	weaverCmd.Stdout = logFile
	weaverCmd.Stderr = logFile
	if err := weaverCmd.Start(); err != nil {
		_ = logFile.Close()
		log.Fatalf("start adaptive-dispatch weaver: %v", err)
	}
	_ = os.MkdirAll(filepath.Dir(localWeaverPIDPath()), 0o755)
	_ = os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(weaverCmd.Process.Pid)+"\n"), 0o644)
	defer func() {
		stopManagedWeaverCommand(weaverCmd, 30*time.Second)
		_ = os.Remove(localWeaverPIDPath())
		_ = logFile.Close()
	}()

	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	prepareStandardTestRun(weaverURL, true)
	if err := resetNntpMetrics(); err != nil {
		log.Fatalf("reset NNTP metrics before adaptive-dispatch workload: %v", err)
	}

	run := runDownloadBenchIteration(
		weaverURL,
		scenario,
		1,
		outputDir,
		time.Duration(sampleIntervalMs)*time.Millisecond,
		time.Duration(timeoutSec)*time.Second,
	)
	log.Printf(
		"adaptive-dispatch %s: status=%s duration=%s first_byte=%s all_bytes=%s",
		run.Scenario,
		run.Status,
		formatMilliseconds(run.DurationMs),
		formatOptionalMilliseconds(run.TimeToFirstByteMs),
		formatOptionalMilliseconds(run.TimeToAllBytesMs),
	)
	if run.Status != "COMPLETE" {
		log.Printf("Weaver log tail after adaptive-dispatch failure:\n%s", localWeaverLogTail(60))
		os.Exit(1)
	}

	metricsPrefix := strings.TrimSpace(env("ADAPTIVE_DISPATCH_MESSAGE_PREFIX", fmt.Sprintf("e2e-%s-", scenario.Slug)))
	latentMetrics, err := fetchNntpBodyMetricsFrom(nntpHost(), nntpPort(), metricsPrefix)
	if err != nil {
		log.Fatalf("fetch latent NNTP BODY metrics: %v", err)
	}
	directMetrics, err := fetchNntpBodyMetricsFrom(nntpHost(), backupNntpPort(), metricsPrefix)
	if err != nil {
		log.Fatalf("fetch direct NNTP BODY metrics: %v", err)
	}
	latentFetches := totalBodyFetches(latentMetrics)
	directFetches := totalBodyFetches(directMetrics)
	totalFetches := latentFetches + directFetches
	directPct := 0.0
	if totalFetches > 0 {
		directPct = float64(directFetches) * 100.0 / float64(totalFetches)
	}

	summaryPath := filepath.Join(outputDir, "adaptive-dispatch-summary.json")
	summary := map[string]interface{}{
		"scenario":          scenario.Slug,
		"message_prefix":    metricsPrefix,
		"latency_ms":        latencyMs,
		"jitter_ms":         jitterMs,
		"connections":       connections,
		"latent_body_count": latentFetches,
		"direct_body_count": directFetches,
		"total_body_count":  totalFetches,
		"direct_pct":        directPct,
		"min_direct_pct":    minDirectPct,
		"run":               run,
	}
	if data, err := json.MarshalIndent(summary, "", "  "); err != nil {
		log.Printf("warning: marshal adaptive-dispatch summary: %v", err)
	} else if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		log.Printf("warning: write adaptive-dispatch summary: %v", err)
	} else {
		log.Printf("adaptive-dispatch summary written to %s", summaryPath)
	}

	fmt.Printf(
		"\nADAPTIVE DISPATCH: latent=%d direct=%d total=%d direct=%.1f%% (minimum %d%%)\n",
		latentFetches,
		directFetches,
		totalFetches,
		directPct,
		minDirectPct,
	)
	if totalFetches < 20 {
		log.Printf("FAIL: only %d filtered BODY fetches were observed for prefix %q", totalFetches, metricsPrefix)
		os.Exit(1)
	}
	if directFetches <= latentFetches || directPct < float64(minDirectPct) {
		log.Printf(
			"FAIL: non-latent server was not materially preferred (latent=%d direct=%d direct=%.1f%%)",
			latentFetches,
			directFetches,
			directPct,
		)
		log.Printf("Weaver log tail after adaptive-dispatch preference failure:\n%s", localWeaverLogTail(80))
		os.Exit(1)
	}
}

func totalBodyFetches(metrics restartNntpMetrics) int {
	total := 0
	for _, count := range metrics.BodyCounts {
		total += count
	}
	return total
}

// cmdTlsTest exercises weaver's TLS NNTP path with a custom CA cert.
// Runs a small subset of scenarios (single-mkv, rar5-single, 7z-encrypted,
// gzip-single, zip-encrypted) over the TLS port to verify negotiation works.
func cmdTlsTest() {
	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	weaverURL := fmt.Sprintf("http://localhost:%s", weaverPort)
	configPath := localWeaverConfigPath()
	caPath := filepath.Join(localWeaverDir(), "nntp-ca.pem")

	ensureStandardDockerInfrastructure()

	// Extract CA cert from NNTP container
	log.Println("extracting NNTP CA cert...")
	containerID, err := dockerComposeServiceContainerID("nntp")
	if err != nil {
		log.Fatalf("resolve NNTP container: %v", err)
	}
	_ = os.MkdirAll(filepath.Dir(caPath), 0o755)
	extractCA := exec.Command("docker", "cp", containerID+":/certs/ca.pem", caPath)
	if err := extractCA.Run(); err != nil {
		log.Fatalf("failed to extract CA cert: %v", err)
	}
	caPem, err := os.ReadFile(caPath)
	if err != nil {
		log.Fatalf("read extracted CA cert: %v", err)
	}
	log.Printf("  CA cert written to %s (%d bytes)", caPath, len(caPem))

	// Kill existing weaver, clean state
	killWeaver()
	cleanWeaverState()

	// Write config with TLS server on the runtime-assigned host TLS port.
	root := localWeaverDir()
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	tlsPort := nntpTLSPort()
	tlsConfig := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true

[[servers]]
id = 1
host = "localhost"
port = %s
tls = true
tls_ca_cert = "%s"
username = %q
password = %q
connections = 4
active = true
priority = 0

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), tlsPort, caPath, env("E2E_NNTP_USERNAME", "e2e-user"), env("E2E_NNTP_PASSWORD", "e2e-pass"))
	os.WriteFile(configPath, []byte(tlsConfig), 0o600)

	// Start weaver
	log.Println("starting weaver with TLS NNTP config...")
	weaverCmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	weaverCmd.Env = managedWeaverEnv(os.Environ(), localRunDir(), "info,weaver::pipeline=debug,weaver_nntp=debug")
	_ = os.MkdirAll(filepath.Dir(localWeaverLogPath()), 0o755)
	logFile, _ := os.Create(localWeaverLogPath())
	weaverCmd.Stdout = logFile
	weaverCmd.Stderr = logFile
	if err := weaverCmd.Start(); err != nil {
		log.Fatalf("failed to start weaver: %v", err)
	}
	_ = os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(weaverCmd.Process.Pid)+"\n"), 0o644)
	defer func() {
		weaverCmd.Process.Kill()
		weaverCmd.Wait()
		_ = os.Remove(localWeaverPIDPath())
		logFile.Close()
	}()
	waitForGraphQL(graphqlURL(weaverURL), 30*time.Second)
	log.Println("weaver ready (TLS mode)")

	// Small subset of scenarios to exercise TLS
	tlsSlugs := []string{"single-mkv", "rar5-single", "7z-encrypted", "gzip-single", "zip-encrypted"}
	if configured := strings.TrimSpace(os.Getenv("E2E_TLS_SCENARIOS")); configured != "" {
		tlsSlugs = nil
		for _, slug := range strings.Split(configured, ",") {
			if slug = strings.TrimSpace(slug); slug != "" {
				tlsSlugs = append(tlsSlugs, slug)
			}
		}
		if len(tlsSlugs) == 0 {
			log.Fatal("E2E_TLS_SCENARIOS did not contain a scenario slug")
		}
	}

	var scenarios []*Scenario
	for _, slug := range tlsSlugs {
		s, err := loadScenario(filepath.Join(testdataDir(), slug))
		if err != nil {
			log.Printf("  WARNING: scenario %s not found: %v", slug, err)
			continue
		}
		scenarios = append(scenarios, s)
	}
	log.Printf("running %d scenarios over TLS...", len(scenarios))

	// Submit all
	type job struct {
		slug   string
		jobID  int
		status string
	}
	var jobs []job
	for _, s := range scenarios {
		jobID, err := submitOneNZB(weaverURL, s)
		if err != nil {
			log.Printf("  %s: submit error: %v", s.Slug, err)
			jobs = append(jobs, job{slug: s.Slug, status: "ERROR"})
			continue
		}
		jobs = append(jobs, job{slug: s.Slug, jobID: jobID})
	}

	// Poll
	deadline := time.Now().Add(120 * time.Second)
	pending := 0
	for _, j := range jobs {
		if j.status == "" {
			pending++
		}
	}
	for pending > 0 && time.Now().Before(deadline) {
		mustSleepWithSuspendDetection(2*time.Second, "TLS test polling")
		for i := range jobs {
			if jobs[i].status != "" {
				continue
			}
			s := pollJobOnce(weaverURL, jobs[i].jobID)
			if s == "COMPLETE" || s == "FAILED" {
				jobs[i].status, _ = applyTerminalStateCheck(localWeaverDBPath(), jobs[i].jobID, jobs[i].slug, s)
				pending--
			}
		}
	}
	for i := range jobs {
		if jobs[i].status == "" {
			jobs[i].status = "TIMEOUT"
		}
	}

	// Score
	passed := 0
	for _, j := range jobs {
		if j.status == "COMPLETE" {
			passed++
			log.Printf("  PASS: %s", j.slug)
		} else {
			log.Printf("  FAIL: %s = %s", j.slug, j.status)
		}
	}

	fmt.Printf("\nTLS TEST: %d/%d passed\n", passed, len(jobs))
	if passed != len(jobs) {
		os.Exit(1)
	}
}
