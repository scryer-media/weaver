// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// --- test-all ---

type testJob struct {
	slug     string
	scenario *Scenario
	jobID    int
	status   string // terminal status or "submit_error"/"skip"
	errMsg   string
	// Wall-clock bounds of the fixture's run: stamped at submission and at
	// the moment its terminal snapshot is taken, so the summary can show
	// where the phase's time went instead of only whether it passed.
	submittedAt                        time.Time
	resolvedAt                         time.Time
	fileIdentityRewriteObserved        bool
	fileIdentityRewriteLastObservation fileIdentityRewriteObservation
	fileIdentityRewriteLastQueryError  string
}

type chaosRoundJobArtifact struct {
	Slug            string  `json:"slug"`
	JobID           int     `json:"job_id"`
	Status          string  `json:"status"`
	Found           bool    `json:"found,omitempty"`
	Error           string  `json:"error,omitempty"`
	ProgressPercent float64 `json:"progress_percent,omitempty"`
	Health          int     `json:"health,omitempty"`
}

var functionalRewritePollInterval = 250 * time.Millisecond
var functionalNormalPollInterval = 500 * time.Millisecond

const (
	functionalFastStatusPollBatchSize = 16
	functionalRegularBatchSize        = 8
)

func countResolvedTestJobs(jobs []testJob) int {
	resolved := 0
	for _, job := range jobs {
		if job.status != "" && job.status != "queued_exclusive" && job.status != "queued_regular" {
			resolved++
		}
	}
	return resolved
}

// testJobDuration is how long the fixture spent between submission and its
// terminal snapshot; zero when either bound was never stamped (a fixture
// that never submitted, or one resolved through a path that does not take a
// snapshot).
func testJobDuration(job testJob) time.Duration {
	if job.submittedAt.IsZero() || job.resolvedAt.IsZero() || job.resolvedAt.Before(job.submittedAt) {
		return 0
	}
	return job.resolvedAt.Sub(job.submittedAt)
}

func testJobDurationLabel(job testJob) string {
	d := testJobDuration(job)
	if d == 0 {
		return "-"
	}
	return d.Round(100 * time.Millisecond).String()
}

// printSlowestTestJobs names the fixtures that dominated the phase's wall
// time. The pass/fail table says nothing about where the time went, and a
// run that "felt slow" is otherwise diagnosed by hand from timestamps.
func printSlowestTestJobs(jobs []testJob, limit int) {
	timed := make([]testJob, 0, len(jobs))
	for _, job := range jobs {
		if testJobDuration(job) > 0 {
			timed = append(timed, job)
		}
	}
	if len(timed) == 0 {
		return
	}
	sort.SliceStable(timed, func(a, b int) bool {
		return testJobDuration(timed[a]) > testJobDuration(timed[b])
	})
	if limit > len(timed) {
		limit = len(timed)
	}
	total := time.Duration(0)
	for _, job := range timed {
		total += testJobDuration(job)
	}
	fmt.Printf("Slowest fixtures (submit to terminal; %d timed, %s summed):\n", len(timed), total.Round(time.Second))
	for _, job := range timed[:limit] {
		fmt.Printf("  %-8s %-40s %s\n", testJobDurationLabel(job), job.slug, job.status)
	}
}

func observeRuntimeFileIdentityRewrite(job *testJob, queryContext string) {
	if !testJobNeedsRuntimeFileIdentityRewrite(job) {
		return
	}
	observer, err := openFileIdentityRewriteObserver()
	if err != nil {
		job.fileIdentityRewriteLastQueryError = err.Error()
		log.Printf("  %s: runtime file-identity rewrite query error%s: %v", job.slug, queryContext, err)
		return
	}
	defer observer.Close()

	observeRuntimeFileIdentityRewriteWithObserver(job, observer, queryContext)
}

func observeRuntimeFileIdentityRewriteWithObserver(
	job *testJob,
	observer *fileIdentityRewriteObserver,
	queryContext string,
) {
	assertion := job.scenario.fileIdentityRewriteAssertion()
	if assertion == nil {
		return
	}
	observation, observeErr := observer.Observe(job.jobID, assertion)
	if observeErr != nil {
		job.fileIdentityRewriteLastQueryError = observeErr.Error()
		log.Printf("  %s: runtime file-identity rewrite query error%s: %v", job.slug, queryContext, observeErr)
	} else {
		job.fileIdentityRewriteLastObservation = observation
		job.fileIdentityRewriteLastQueryError = ""
		job.fileIdentityRewriteObserved = observation.Observed
		if observation.Observed {
			log.Printf("  %s: observed runtime file-identity rewrite", job.slug)
		}
	}
}

func testJobNeedsRuntimeFileIdentityRewrite(job *testJob) bool {
	if job == nil || job.status != "" || job.fileIdentityRewriteObserved {
		return false
	}
	return job.scenario != nil && job.scenario.fileIdentityRewriteAssertion() != nil
}

func functionalHasPendingFileIdentityRewrite(jobs []testJob) bool {
	for i := range jobs {
		if testJobNeedsRuntimeFileIdentityRewrite(&jobs[i]) {
			return true
		}
	}
	return false
}

func functionalRegularBatches(jobs []testJob, batchSize int) [][]int {
	if batchSize <= 0 {
		return nil
	}

	regular := make([]int, 0, len(jobs))
	for i := range jobs {
		if jobs[i].status == "queued_regular" {
			regular = append(regular, i)
		}
	}

	batches := make([][]int, 0, (len(regular)+batchSize-1)/batchSize)
	for start := 0; start < len(regular); start += batchSize {
		end := min(start+batchSize, len(regular))
		batches = append(batches, regular[start:end])
	}
	return batches
}

func functionalQueueLivenessProbeIndex(jobs []testJob, blockerIndex int) (int, error) {
	if blockerIndex < 0 || blockerIndex >= len(jobs) {
		return -1, fmt.Errorf("queue-liveness blocker index %d is out of range", blockerIndex)
	}
	if jobs[blockerIndex].scenario == nil {
		return -1, fmt.Errorf("queue-liveness fixture %q has no scenario", jobs[blockerIndex].slug)
	}
	assertion := jobs[blockerIndex].scenario.queueLivenessAssertion()
	if assertion == nil {
		return -1, fmt.Errorf("fixture %q has no queue-liveness assertion", jobs[blockerIndex].slug)
	}
	probeSlug := strings.TrimSpace(assertion.ProbeSlug)
	if probeSlug == "" {
		return -1, fmt.Errorf("fixture %q has an empty queue-liveness probe slug", jobs[blockerIndex].slug)
	}

	probeIndex := -1
	for i := range jobs {
		if jobs[i].slug != probeSlug {
			continue
		}
		if i == blockerIndex {
			return -1, fmt.Errorf("queue-liveness fixture %q cannot probe itself", probeSlug)
		}
		if probeIndex >= 0 {
			return -1, fmt.Errorf("queue-liveness probe %q appears more than once", probeSlug)
		}
		probeIndex = i
	}
	if probeIndex < 0 {
		return -1, fmt.Errorf("queue-liveness probe %q is not in the functional corpus", probeSlug)
	}
	if jobs[probeIndex].status != "queued_regular" {
		return -1, fmt.Errorf(
			"queue-liveness probe %q must be a regular fixture, got %q",
			probeSlug,
			jobs[probeIndex].status,
		)
	}
	return probeIndex, nil
}

func functionalStatusPollIndexes(jobs []testJob, fastRewritePolling bool, cursor *int) []int {
	pending := make([]int, 0, len(jobs))
	for i := range jobs {
		if jobs[i].status == "" {
			pending = append(pending, i)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if !fastRewritePolling || len(pending) <= functionalFastStatusPollBatchSize {
		if cursor != nil {
			*cursor = 0
		}
		return pending
	}

	start := 0
	if cursor != nil && *cursor > 0 {
		start = *cursor % len(pending)
	}
	count := functionalFastStatusPollBatchSize
	indexes := make([]int, 0, count)
	for offset := 0; offset < count; offset++ {
		indexes = append(indexes, pending[(start+offset)%len(pending)])
	}
	if cursor != nil {
		*cursor = (start + count) % len(pending)
	}
	return indexes
}

func finalizeTestJobFromSnapshot(job *testJob, dbPath string, snapshot facadeItemSnapshot, queryContext string) {
	job.resolvedAt = time.Now()
	assertion := job.scenario.fileIdentityRewriteAssertion()
	observeRuntimeFileIdentityRewrite(job, queryContext)

	if overrideStatus, overrideErrMsg, overridden := applyRuntimeFileIdentityRewriteTerminalCheck(
		snapshot.Status,
		assertion,
		job.fileIdentityRewriteObserved,
		job.fileIdentityRewriteLastObservation,
		job.fileIdentityRewriteLastQueryError,
	); overridden {
		job.status = overrideStatus
		job.errMsg = overrideErrMsg
	} else {
		// The orphan sweep is database-wide, not about this job, so runTests
		// runs it once when the phase ends instead of after every fixture.
		job.status, job.errMsg = applyTerminalStateCheckSweeping(dbPath, job.jobID, job.slug, snapshot.Status, false)
		if job.errMsg == "" {
			job.errMsg = snapshot.Error
		}
	}
}

// fileCompleteQuery asks whether a job has every byte of a posted file. Each
// posted file is checkpointed as a root member (node 0) with the byte runs it
// holds of it, so the file is complete once its length is known and its runs
// cover all of it.
const fileCompleteQuery = `SELECT COUNT(*) FROM graph_members m
	WHERE m.job_id = ? AND m.node_id = 0 AND m.name = ? AND m.length IS NOT NULL
	AND m.length <= (
		SELECT COALESCE(SUM(c.range_end - c.range_start), 0) FROM graph_coverage c
		WHERE c.job_id = m.job_id AND c.node_id = m.node_id AND c.member_id = m.member_id
	)`

// fileComplete reports whether weaver holds every byte of the job's posted
// file.
func fileComplete(db *sql.DB, datastore weaverDatastore, jobID int, filename string) (bool, error) {
	var count int
	err := db.QueryRow(rebindWeaverSQL(datastore, fileCompleteQuery), jobID, filename).Scan(&count)
	return count > 0, err
}

func waitForActiveFileComplete(dbPath string, jobID int, filename string, timeout time.Duration) error {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return err
	}
	defer closeWeaverStateDB(db, datastore)

	deadline := time.Now().Add(timeout)
	for {
		complete, err := fileComplete(db, datastore, jobID, filename)
		if err == nil && complete {
			return nil
		}
		if err != nil && !isTransientSQLiteBusy(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("job %d did not complete file %q within %s", jobID, filename, timeout)
		}
		mustSleepWithSuspendDetection(50*time.Millisecond, fmt.Sprintf("job %d file-completion gate", jobID))
	}
}

func runExclusiveFunctionalJob(
	weaverURL, dbPath string,
	job *testJob,
	queueLivenessProbe *testJob,
) {
	config := strings.TrimSpace(job.scenario.PrimaryChaosConfig)
	backupGateFilename := strings.TrimSpace(job.scenario.BackupUnavailableUntilFileComplete)
	var releaseBackupGate func() error
	if err := ensureNntpChaosOff(); err != nil {
		job.status = "setup_error"
		job.errMsg = err.Error()
		return
	}
	defer func() {
		if releaseBackupGate != nil {
			if err := releaseBackupGate(); err != nil {
				log.Printf("warning: release backup gate after exclusive scenario %s: %v", job.slug, err)
			}
		}
		if err := ensureNntpChaosOff(); err != nil {
			log.Printf("warning: reset NNTP chaos after exclusive scenario %s: %v", job.slug, err)
		}
	}()

	if config != "" {
		if err := setNntpChaosOnServer(nntpHost(), nntpPort(), config); err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		log.Printf("  %s: primary NNTP chaos enabled: %s", job.slug, config)
	}
	backupGateForJob := job.scenario.BackupUnavailableUntilJobTerminal
	if backupGateFilename != "" && backupGateForJob {
		job.status = "setup_error"
		job.errMsg = "backupUnavailableUntilFileComplete and backupUnavailableUntilJobTerminal are mutually exclusive"
		return
	}
	if backupGateFilename != "" && len(job.scenario.PrimaryDeleteSubjectContains) == 0 {
		job.status = "setup_error"
		job.errMsg = "backup-unavailable gate requires primaryDeleteSubjectContains"
		return
	}
	if backupGateFilename != "" || backupGateForJob {
		if !backupNntpRunning() {
			job.status = "setup_error"
			job.errMsg = "backup-unavailable gate requires the backup NNTP server"
			return
		}
		// Refuse new sessions and make any pooled session re-authenticate on
		// BODY, so earlier scenarios cannot leave a connection around the gate.
		release, err := holdNntpChaosOnServer(
			nntpHost(),
			backupNntpPort(),
			"greet_400=100,reauth_body=100",
		)
		if err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		releaseBackupGate = release
		if backupGateForJob {
			// Released by the deferred cleanup once the job is terminal.
			log.Printf("  %s: backup NNTP unavailable for the whole job", job.slug)
		} else {
			log.Printf("  %s: backup NNTP unavailable until %s completes", job.slug, backupGateFilename)
		}
	}

	jobID, err := submitOneNZB(weaverURL, job.scenario)
	if err != nil {
		job.status = "submit_error"
		job.errMsg = err.Error()
		return
	}
	job.jobID = jobID
	job.submittedAt = time.Now()
	log.Printf("  %s: submitted exclusive job=%d", job.slug, jobID)
	if backupGateFilename != "" {
		if err := waitForActiveFileComplete(dbPath, jobID, backupGateFilename, 60*time.Second); err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		release := releaseBackupGate
		releaseBackupGate = nil
		if err := release(); err != nil {
			job.status = "setup_error"
			job.errMsg = err.Error()
			return
		}
		log.Printf("  %s: backup NNTP released after %s completed", job.slug, backupGateFilename)
	}
	if assertion := job.scenario.queueLivenessAssertion(); assertion != nil {
		if delay, enabled := configuredDirectPostRepairVerificationDelay(); enabled {
			runDirectPostRepairQueueLiveness(
				weaverURL,
				dbPath,
				job,
				queueLivenessProbe,
				assertion,
				delay,
			)
			return
		}
	}

	deadline := time.Now().Add(180 * time.Second)
	firstPoll := true
	for time.Now().Before(deadline) {
		if !firstPoll {
			mustSleepWithSuspendDetection(2*time.Second, fmt.Sprintf("exclusive test %s polling", job.slug))
		}
		firstPoll = false

		snapshot, err := fetchFacadeItemSnapshot(weaverURL, job.jobID)
		if err != nil || !snapshot.Found || !facadeTerminalStatus(snapshot.Status) {
			continue
		}
		finalizeTestJobFromSnapshot(job, dbPath, snapshot, " during exclusive polling")
		log.Printf(
			"  %s: %s (health=%.1f%% err=%s)",
			job.slug,
			job.status,
			float64(snapshot.Health)/10,
			job.errMsg,
		)
		return
	}

	reconciled := reconcileTerminalSnapshots(
		weaverURL,
		[]int{job.jobID},
		15*time.Second,
		fmt.Sprintf("exclusive test %s final reconciliation", job.slug),
	)
	if snapshot, ok := reconciled[job.jobID]; ok {
		finalizeTestJobFromSnapshot(job, dbPath, snapshot, " during exclusive reconciliation")
		log.Printf(
			"  %s: %s after reconciliation (health=%.1f%% err=%s)",
			job.slug,
			job.status,
			float64(snapshot.Health)/10,
			job.errMsg,
		)
		return
	}

	job.status = "timeout"
	job.errMsg = "exclusive scenario timed out after 180s"
	log.Printf("  %s: TIMEOUT after 180s", job.slug)
}

const directPostRepairVerificationHook = "direct_store.post_repair_verify"

func configuredDirectPostRepairVerificationDelay() (time.Duration, bool) {
	raw := strings.TrimSpace(os.Getenv("WEAVER_E2E_DELAY"))
	name, millis, ok := strings.Cut(raw, "=")
	if !ok || strings.TrimSpace(name) != directPostRepairVerificationHook {
		return 0, false
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(millis), 10, 64)
	if err != nil || parsed <= 0 {
		return 0, false
	}
	return time.Duration(parsed) * time.Millisecond, true
}

func waitForJobEvents(dbPath string, jobID int, required []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events, err := jobEventKinds(dbPath, jobID)
		if err == nil && len(missingJobEvents(events, required)) == 0 {
			return nil
		}
		if err != nil && !isTransientSQLiteBusy(err) {
			return fmt.Errorf("load job events for job %d: %w", jobID, err)
		}
		mustSleepWithSuspendDetection(50*time.Millisecond, fmt.Sprintf("job %d event gate", jobID))
	}
	return fmt.Errorf("job %d did not emit required event(s) %s within %s", jobID, strings.Join(required, ", "), timeout)
}

func waitForLocalWeaverDelayHook(hook string, timeout time.Duration) error {
	needle := fmt.Sprintf("hook=\"%s\"", hook)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if logContains(localWeaverLogPath(), needle) {
			return nil
		}
		mustSleepWithSuspendDetection(50*time.Millisecond, fmt.Sprintf("%s delay hook", hook))
	}
	return fmt.Errorf("did not observe %s delay hook within %s", hook, timeout)
}

func runDirectPostRepairQueueLiveness(
	weaverURL, dbPath string,
	blocker *testJob,
	probe *testJob,
	assertion *ScenarioQueueLivenessAssertion,
	verificationDelay time.Duration,
) {
	if verificationDelay < 5*time.Second {
		blocker.status = "setup_error"
		blocker.errMsg = fmt.Sprintf("%s delay must be at least 5s, got %s", directPostRepairVerificationHook, verificationDelay)
		return
	}
	if err := waitForJobEvents(dbPath, blocker.jobID, []string{"RepairComplete"}, 90*time.Second); err != nil {
		blocker.status = "timeout"
		blocker.errMsg = err.Error()
		return
	}
	if err := waitForLocalWeaverDelayHook(directPostRepairVerificationHook, 30*time.Second); err != nil {
		blocker.status = "timeout"
		blocker.errMsg = err.Error()
		return
	}

	probeSlug := strings.TrimSpace(assertion.ProbeSlug)
	if probe == nil || probe.slug != probeSlug || probe.scenario == nil {
		blocker.status = "setup_error"
		blocker.errMsg = fmt.Sprintf("queue-liveness probe %q is not a canonical queued fixture", probeSlug)
		return
	}
	probe.status = ""
	probeID, err := submitOneNZB(weaverURL, probe.scenario)
	if err != nil {
		probe.status = "submit_error"
		probe.errMsg = err.Error()
		blocker.status = "submit_error"
		blocker.errMsg = fmt.Sprintf("submit queue-liveness probe %q: %v", probeSlug, err)
		return
	}
	probe.jobID = probeID
	log.Printf("  %s: submitted queue-liveness probe %s as job=%d", blocker.slug, probeSlug, probeID)

	// Leave a little margin for observing the terminal façade update. The
	// verifier remains blocked for the full configured delay, so a completion
	// here proves the scheduler actor continued to service another job.
	probeDeadline := time.Now().Add(verificationDelay - 2*time.Second)
	for time.Now().Before(probeDeadline) {
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, probeID)
		if err == nil && snapshot.Found && facadeTerminalStatus(snapshot.Status) {
			finalizeTestJobFromSnapshot(probe, dbPath, snapshot, " during queue-liveness polling")
			if probe.status != "COMPLETE" {
				blocker.status = probe.status
				blocker.errMsg = fmt.Sprintf("queue-liveness probe %s ended %s: %s", probeSlug, probe.status, probe.errMsg)
				return
			}
			break
		}
		mustSleepWithSuspendDetection(100*time.Millisecond, "queue-liveness probe polling")
	}
	if probe.status != "COMPLETE" {
		probe.status = "timeout"
		probe.errMsg = fmt.Sprintf("did not complete before %s elapsed", verificationDelay)
		blocker.status = "timeout"
		blocker.errMsg = fmt.Sprintf("queue-liveness probe %s did not complete before %s elapsed", probeSlug, verificationDelay)
		return
	}

	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := fetchFacadeItemSnapshot(weaverURL, blocker.jobID)
		if err == nil && snapshot.Found && facadeTerminalStatus(snapshot.Status) {
			finalizeTestJobFromSnapshot(blocker, dbPath, snapshot, " after queue-liveness probe")
			log.Printf("  %s: %s after queue-liveness probe", blocker.slug, blocker.status)
			return
		}
		mustSleepWithSuspendDetection(250*time.Millisecond, "queue-liveness blocker polling")
	}
	blocker.status = "timeout"
	blocker.errMsg = "blocker did not complete after queue-liveness probe"
}

func cmdTest(targets []string) {
	// Validate all targets have NZBs
	var slugs []string
	for _, slug := range targets {
		nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
		if _, err := os.Stat(nzbPath); err != nil {
			log.Fatalf("no NZB for %q — run '%s seed %s' first", slug, cliProgramName, filepath.Join(testdataDir(), slug))
		}
		slugs = append(slugs, slug)
	}
	runTests(slugs)
}

func cmdTestAll() {
	slugs := enabledCanonicalFixtureSlugs()

	if len(slugs) == 0 {
		log.Fatal("no canonical fixtures configured")
	}
	runTests(slugs)
}

func runTests(slugs []string) {
	ensureStandardDockerInfrastructure()
	weaverURL := defaultWeaverURL()
	prepareStandardTestRun(weaverURL, true)
	emitProgressEvent(progressEvent{Kind: "phase_total", Total: len(slugs), Detail: "functional fixtures"})

	log.Printf("submitting %d fixtures to weaver at %s...", len(slugs), weaverURL)

	jobs := make([]testJob, 0, len(slugs))
	for i, slug := range slugs {
		scenarioDir := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(scenarioDir)
		if err != nil {
			scenario = &Scenario{Slug: slug, ExpectedOutcome: "success"}
		}

		job := testJob{slug: slug, scenario: scenario}
		if scenarioUsesExclusiveNntpState(scenario) {
			job.status = "queued_exclusive"
			log.Printf("[%d/%d] %s — queued exclusive NNTP scenario", i+1, len(slugs), slug)
			jobs = append(jobs, job)
			continue
		}

		job.status = "queued_regular"
		log.Printf("[%d/%d] %s — queued regular scenario", i+1, len(slugs), slug)
		jobs = append(jobs, job)
	}

	dbPath := localWeaverDBPath()
	// The queue-liveness fixture must consume the one-shot delay before normal
	// functional batches can run. It is otherwise indistinguishable from a
	// normal direct-store repair fixture, so keep it in the functional corpus
	// but execute its assertion first and in isolation.
	for i := range jobs {
		if jobs[i].status != "queued_exclusive" || jobs[i].scenario.queueLivenessAssertion() == nil {
			continue
		}
		log.Printf("running exclusive queue-liveness scenario %s...", jobs[i].slug)
		probeIndex, err := functionalQueueLivenessProbeIndex(jobs, i)
		if err != nil {
			jobs[i].status = "setup_error"
			jobs[i].errMsg = err.Error()
		} else {
			runExclusiveFunctionalJob(weaverURL, dbPath, &jobs[i], &jobs[probeIndex])
		}
		emitProgressEvent(progressEvent{
			Kind:    "phase_progress",
			Current: countResolvedTestJobs(jobs),
			Total:   len(jobs),
			Status:  strings.ToLower(jobs[i].status),
			Detail:  jobs[i].slug,
		})
	}
	// Regular fixtures run through a sliding window rather than in fixed
	// batches. A batch waited for its slowest member before submitting the
	// next eight, so one 12 s PAR2 fixture idled seven slots for its whole
	// duration and every batch paid at least one poll interval even when all
	// eight finished in a hundred milliseconds. Here a fixture is submitted
	// the moment a slot frees, and each fixture carries its own completion
	// deadline from the moment it was submitted. The order is the batch order
	// the unit tests pin, flattened.
	queue := make([]int, 0, len(jobs))
	for _, batch := range functionalRegularBatches(jobs, functionalRegularBatchSize) {
		queue = append(queue, batch...)
	}
	weaverDiedMidRun := false
	completionTimeout := functionalCompletionTimeout()
	inFlight := 0
	nextQueued := 0
	log.Printf(
		"submitting %d regular functional fixture(s) through a window of %d; each has %s to complete",
		len(queue), functionalRegularBatchSize, completionTimeout,
	)
	submitUpToWindow := func() {
		for inFlight < functionalRegularBatchSize && nextQueued < len(queue) && !weaverDiedMidRun {
			i := queue[nextQueued]
			nextQueued++
			jobs[i].status = ""
			jobID, err := submitOneNZB(weaverURL, jobs[i].scenario)
			if err != nil {
				log.Printf("[%d/%d] %s — submit error: %v", i+1, len(jobs), jobs[i].slug, err)
				jobs[i].status = "submit_error"
				jobs[i].errMsg = err.Error()
				emitProgressEvent(progressEvent{
					Kind:    "phase_progress",
					Current: countResolvedTestJobs(jobs),
					Total:   len(jobs),
					Status:  "submit_error",
					Detail:  jobs[i].slug,
				})
				if died, waitErr := managedWeaverDied(); died {
					weaverDiedMidRun = true
					log.Printf("FATAL: managed weaver exited during fixture submission (%v)", waitErr)
					log.Printf("weaver died here:\n%s", managedWeaverDeathReport())
					return
				}
				continue
			}

			jobs[i].jobID = jobID
			jobs[i].submittedAt = time.Now()
			inFlight++
			log.Printf(
				"[%d/%d] %s — submitted job=%d (%d in flight, %d queued)",
				i+1, len(jobs), jobs[i].slug, jobID, inFlight, len(queue)-nextQueued,
			)
		}
	}
	submitUpToWindow()

	firstPoll := true
	statusPollCursor := 0
	for inFlight > 0 {
		// Fail fast on a dead server. Polling on would score every remaining
		// job as `timeout` — a label that describes the corpse, not the cause —
		// and burn the whole completion budget doing it.
		if died, waitErr := managedWeaverDied(); died {
			weaverDiedMidRun = true
			log.Printf(
				"FATAL: managed weaver exited mid-run (%v) with %d job(s) still in flight; "+
					"abandoning the wait — these are not timeouts, there is no server to answer them",
				waitErr, inFlight,
			)
			log.Printf("weaver died here:\n%s", managedWeaverDeathReport())
			break
		}
		if !firstPoll {
			mustSleepWithSuspendDetection(functionalPollInterval(jobs), "functional test polling")
		}
		firstPoll = false

		fastRewritePolling := functionalHasPendingFileIdentityRewrite(jobs)
		if fastRewritePolling {
			observer, err := openFileIdentityRewriteObserver()
			if err != nil {
				for i := range jobs {
					if testJobNeedsRuntimeFileIdentityRewrite(&jobs[i]) {
						jobs[i].fileIdentityRewriteLastQueryError = err.Error()
					}
				}
				log.Printf("  runtime file-identity rewrite query error: %v", err)
			} else {
				for i := range jobs {
					if testJobNeedsRuntimeFileIdentityRewrite(&jobs[i]) {
						observeRuntimeFileIdentityRewriteWithObserver(&jobs[i], observer, "")
					}
				}
				_ = observer.Close()
			}
		}

		for _, i := range functionalStatusPollIndexes(jobs, fastRewritePolling, &statusPollCursor) {
			snapshot, err := fetchFacadeItemSnapshot(weaverURL, jobs[i].jobID)
			if err != nil {
				continue
			}
			if !snapshot.Found {
				continue
			}
			s := snapshot.Status

			if s == "COMPLETE" || s == "FAILED" {
				finalizeTestJobFromSnapshot(&jobs[i], dbPath, snapshot, "")
				inFlight--
				resolved := countResolvedTestJobs(jobs)
				log.Printf("  %s: %s (health=%.1f%% err=%s) [%d in flight, %d queued]",
					jobs[i].slug, jobs[i].status, float64(snapshot.Health)/10, jobs[i].errMsg,
					inFlight, len(queue)-nextQueued)
				emitProgressEvent(progressEvent{
					Kind:    "phase_progress",
					Current: resolved,
					Total:   len(jobs),
					Status:  strings.ToLower(jobs[i].status),
					Detail:  jobs[i].slug,
				})
			}
		}

		// Per-fixture deadlines. A fixture past its budget gets one last
		// reconciliation query before it is scored as a timeout, then is
		// cancelled and settled so its slot is genuinely free and the
		// exclusive scenarios later find a quiet queue.
		expired := make([]int, 0)
		for i := range jobs {
			if jobs[i].status == "" && jobs[i].jobID > 0 && !jobs[i].submittedAt.IsZero() &&
				time.Since(jobs[i].submittedAt) > completionTimeout {
				expired = append(expired, i)
			}
		}
		if len(expired) > 0 {
			expiredIDs := make([]int, 0, len(expired))
			for _, i := range expired {
				expiredIDs = append(expiredIDs, jobs[i].jobID)
			}
			log.Printf("reconciling %d functional job(s) past their %s budget before timeout scoring", len(expiredIDs), completionTimeout)
			reconciled := reconcileTerminalSnapshots(weaverURL, expiredIDs, 15*time.Second, "functional test final reconciliation")
			timedOut := make([]int, 0, len(expired))
			for _, i := range expired {
				if snapshot, ok := reconciled[jobs[i].jobID]; ok {
					finalizeTestJobFromSnapshot(&jobs[i], dbPath, snapshot, " during reconciliation")
					inFlight--
					log.Printf("  %s: %s after reconciliation (health=%.1f%% err=%s) [%d in flight, %d queued]",
						jobs[i].slug, jobs[i].status, float64(snapshot.Health)/10, jobs[i].errMsg,
						inFlight, len(queue)-nextQueued)
				} else {
					jobs[i].status = "timeout"
					inFlight--
					timedOut = append(timedOut, i)
					log.Printf("  %s: TIMEOUT after %s", jobs[i].slug, completionTimeout)
				}
				emitProgressEvent(progressEvent{
					Kind:    "phase_progress",
					Current: countResolvedTestJobs(jobs),
					Total:   len(jobs),
					Status:  strings.ToLower(jobs[i].status),
					Detail:  jobs[i].slug,
				})
			}
			if len(timedOut) > 0 {
				log.Printf("canceling %d timed out regular job(s) before refilling the window", len(timedOut))
				for _, i := range timedOut {
					j := &jobs[i]
					if err := cancelJobGraphQL(weaverURL, j.jobID); err != nil {
						log.Printf("  WARNING: cancel timed out job %s (%d): %v", j.slug, j.jobID, err)
					}
					if err := waitForJobCancelSettledGraphQL(
						weaverURL,
						j.jobID,
						weaverCancelSettleTimeout,
						weaverCancelSettlePollInterval,
					); err != nil {
						log.Printf("  queue snapshot after failed cancel settle: %s", describeJobsGraphQL(weaverURL))
						log.Printf("  WARNING: timed out job %s (%d) did not settle before the window refilled: %v", j.slug, j.jobID, err)
					}
				}
			}
		}

		submitUpToWindow()
	}

	if weaverDiedMidRun {
		// Jobs that were in flight when the server died are not timeouts: the
		// scenario never had a server to complete against. Keep the status
		// string — scoring and reconciliation elsewhere key off "timeout" —
		// but do not let the report imply the scenario was given its full
		// budget and failed to finish. There is nothing to cancel on a process
		// that has exited, so no cancel/settle either.
		for i := range jobs {
			if jobs[i].status != "" || jobs[i].jobID == 0 {
				continue
			}
			jobs[i].status = "timeout"
			jobs[i].errMsg = "weaver exited mid-run; scenario never had a server to complete against"
			log.Printf("  %s: NOT RUN (weaver exited mid-run)", jobs[i].slug)
			emitProgressEvent(progressEvent{
				Kind:    "phase_progress",
				Current: countResolvedTestJobs(jobs),
				Total:   len(jobs),
				Status:  "timeout",
				Detail:  jobs[i].slug,
			})
		}
	}

	if weaverDiedMidRun {
		for i := range jobs {
			if jobs[i].status != "queued_regular" {
				continue
			}
			jobs[i].status = "timeout"
			jobs[i].errMsg = "weaver exited before this fixture was submitted"
			log.Printf("  %s: NOT RUN (weaver exited before submission)", jobs[i].slug)
			emitProgressEvent(progressEvent{
				Kind:    "phase_progress",
				Current: countResolvedTestJobs(jobs),
				Total:   len(jobs),
				Status:  "timeout",
				Detail:  jobs[i].slug,
			})
		}
	}

	for i := range jobs {
		if jobs[i].status != "queued_exclusive" {
			continue
		}
		if weaverDiedMidRun {
			jobs[i].status = "timeout"
			jobs[i].errMsg = "weaver exited before the exclusive scenario ran"
			log.Printf("  %s: NOT RUN (weaver exited before exclusive scenario)", jobs[i].slug)
			emitProgressEvent(progressEvent{
				Kind:    "phase_progress",
				Current: countResolvedTestJobs(jobs),
				Total:   len(jobs),
				Status:  "timeout",
				Detail:  jobs[i].slug,
			})
			continue
		}
		log.Printf("running exclusive NNTP scenario %s...", jobs[i].slug)
		runExclusiveFunctionalJob(weaverURL, dbPath, &jobs[i], nil)
		emitProgressEvent(progressEvent{
			Kind:    "phase_progress",
			Current: countResolvedTestJobs(jobs),
			Total:   len(jobs),
			Status:  strings.ToLower(jobs[i].status),
			Detail:  jobs[i].slug,
		})
	}

	// Summary
	fmt.Println()
	fmt.Printf("%-25s %-22s %-12s %-8s %s\n", "FIXTURE", "EXPECTED", "ACTUAL", "TIME", "RESULT")
	fmt.Println(strings.Repeat("-", 78))
	passCount, failCount := 0, 0
	for _, j := range jobs {
		passed := false
		switch j.scenario.expectedOutcome() {
		case "success", "repair_then_success":
			passed = j.status == "COMPLETE"
		case "health_failure", "encryption_unsupported", "repair_failure", "extraction_failure", outcomePartialDelivery:
			passed = j.status == "FAILED"
		case "nested_depth_exceeded":
			// Completes but output is still an archive (not the final media)
			passed = j.status == "COMPLETE"
		default:
			passed = j.status == "COMPLETE"
		}

		// A scenario that names the failure it is testing has to get that
		// failure: "FAILED" on its own is also what a broken fixture looks like.
		mismatch := ""
		if expected := j.scenario.expectedErrorContains(); passed && expected != "" {
			if !strings.Contains(strings.ToLower(j.errMsg), strings.ToLower(expected)) {
				passed = false
				mismatch = fmt.Sprintf(
					"  %s: expected error containing %q, got %q\n",
					j.slug, expected, j.errMsg,
				)
			}
		}

		label := "PASS"
		if !passed {
			label = "FAIL"
			if mismatch != "" {
				fmt.Print(mismatch)
			}
			failCount++
		} else {
			passCount++
		}
		fmt.Printf("%-25s %-22s %-12s %-8s %s\n", j.slug, j.scenario.expectedOutcome(), j.status, testJobDurationLabel(j), label)
	}
	fmt.Println(strings.Repeat("-", 78))
	fmt.Printf("Total: %d passed, %d failed out of %d\n", passCount, failCount, len(jobs))
	printSlowestTestJobs(jobs, 8)

	// Every fixture skipped the orphan sweep at its terminal check; this is
	// the one sweep that covers them all. Weaver has not restarted since the
	// phase began, so its startup pruning cannot have hidden anything.
	//
	// One look settles it. Every terminal check has already seen its job's
	// history row, and the archive that writes it deletes the job's active
	// rows in the same transaction; orphans are only pruned at startup. What
	// the sweep finds now is what the phase left behind, and waiting cannot
	// change it.
	if !weaverDiedMidRun {
		if err := assertNoOrphanActiveStatePath(dbPath); err != nil {
			fmt.Printf("ACTIVE-STATE ORPHAN SWEEP FAILED: %v\n", err)
			emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(jobs), Total: len(jobs), Status: "fail"})
			os.Exit(1)
		}
	}

	if err := assertDirectStoreEngagement(jobs); err != nil {
		fmt.Printf("DIRECT-STORE ASSERTION FAILED: %v\n", err)
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(jobs), Total: len(jobs), Status: "fail"})
		os.Exit(1)
	}

	if failCount > 0 {
		emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(jobs), Total: len(jobs), Status: "fail"})
		os.Exit(1)
	}

	emitProgressEvent(progressEvent{Kind: "phase_done", Current: len(jobs), Total: len(jobs), Status: "pass"})
	stopManagedWeaverAfterProfileCollection()
}

// assertDirectStoreEngagement checks that the phase's store archives were read
// in place. Stored members produce the same bytes either way, so a run that
// never bound a store object passes every `expectedOutputBLAKE3` and every
// event assertion in the corpus; the graph's lines are the only witness (see
// graphDirectStoreEngagementVerdict).
func assertDirectStoreEngagement(jobs []testJob) error {
	raw, err := os.ReadFile(localWeaverLogPath())
	if err != nil {
		return fmt.Errorf("could not read the weaver log to check store engagement: %w", err)
	}
	var jobIDs []int
	for _, job := range jobs {
		if job.jobID > 0 {
			jobIDs = append(jobIDs, job.jobID)
		}
	}
	return graphDirectStoreEngagementVerdict(string(raw), jobIDs)
}

// weaverDebugRingTarget is the log target weaver writes a job's debug ring
// under when it reports a stall. Each of those records replays an earlier
// event of the same job — its message and its fields, `job_id` included — so a
// scan for an event's message would count the replay as a second occurrence.
const weaverDebugRingTarget = "weaver_server_core::runtime::job_debug_ring:"

// isWeaverDebugRingReplay reports whether a weaver log line is one of the
// stall-diagnostics records the debug ring writes rather than an event in its
// own right. Every scan that looks for an event's message skips these.
func isWeaverDebugRingReplay(line string) bool {
	return strings.Contains(ansiEscape.ReplaceAllString(line, ""), weaverDebugRingTarget)
}

func directLogJobID(line string) string {
	clean := ansiEscape.ReplaceAllString(line, "")
	start := strings.Index(clean, "job_id=")
	if start < 0 {
		return ""
	}
	rest := clean[start+len("job_id="):]
	end := strings.IndexAny(rest, " \t")
	if end < 0 {
		return strings.TrimSpace(rest)
	}
	return rest[:end]
}

// ansiEscape matches the SGR sequences weaver's tracing writer emits around
// every field name and separator.
var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

func functionalPollInterval(jobs []testJob) time.Duration {
	if functionalHasPendingFileIdentityRewrite(jobs) {
		return functionalRewritePollInterval
	}
	return functionalNormalPollInterval
}

func functionalCompletionTimeout() time.Duration {
	if weaverUsesPostgresDatastore() {
		return 8 * time.Minute
	}
	return 180 * time.Second
}

type downloadBenchSnapshot struct {
	Job struct {
		Status                          string  `json:"status"`
		Progress                        float64 `json:"progress"`
		Health                          int     `json:"health"`
		TotalBytes                      uint64  `json:"totalBytes"`
		DownloadedBytes                 uint64  `json:"downloadedBytes"`
		OptionalRecoveryBytes           uint64  `json:"optionalRecoveryBytes"`
		OptionalRecoveryDownloadedBytes uint64  `json:"optionalRecoveryDownloadedBytes"`
		FailedBytes                     uint64  `json:"failedBytes"`
		Error                           *string `json:"error"`
	} `json:"job"`
	Metrics struct {
		CurrentDownloadSpeed uint64  `json:"currentDownloadSpeed"`
		ArticlesPerSec       float64 `json:"articlesPerSec"`
		DecodeRateMbps       float64 `json:"decodeRateMbps"`
		BytesDownloaded      uint64  `json:"bytesDownloaded"`
		BytesDecoded         uint64  `json:"bytesDecoded"`
		BytesCommitted       uint64  `json:"bytesCommitted"`
		SegmentsDownloaded   uint64  `json:"segmentsDownloaded"`
		SegmentsDecoded      uint64  `json:"segmentsDecoded"`
		SegmentsCommitted    uint64  `json:"segmentsCommitted"`
	} `json:"metrics"`
}

type downloadBenchSample struct {
	ElapsedMs                       int64   `json:"elapsed_ms"`
	Status                          string  `json:"status"`
	Progress                        float64 `json:"progress"`
	Health                          int     `json:"health"`
	TotalBytes                      uint64  `json:"total_bytes"`
	DownloadedBytes                 uint64  `json:"downloaded_bytes"`
	OptionalRecoveryBytes           uint64  `json:"optional_recovery_bytes"`
	OptionalRecoveryDownloadedBytes uint64  `json:"optional_recovery_downloaded_bytes"`
	FailedBytes                     uint64  `json:"failed_bytes"`
	CurrentDownloadSpeed            uint64  `json:"current_download_speed"`
	ArticlesPerSec                  float64 `json:"articles_per_sec"`
	DecodeRateMbps                  float64 `json:"decode_rate_mbps"`
	BytesDownloaded                 uint64  `json:"bytes_downloaded_total"`
	BytesDecoded                    uint64  `json:"bytes_decoded_total"`
	BytesCommitted                  uint64  `json:"bytes_committed_total"`
	SegmentsDownloaded              uint64  `json:"segments_downloaded_total"`
	SegmentsDecoded                 uint64  `json:"segments_decoded_total"`
	SegmentsCommitted               uint64  `json:"segments_committed_total"`
	Error                           string  `json:"error,omitempty"`
}

type downloadBenchRun struct {
	Scenario               string   `json:"scenario"`
	Iteration              int      `json:"iteration"`
	JobID                  int      `json:"job_id"`
	Status                 string   `json:"status"`
	Error                  string   `json:"error,omitempty"`
	TotalBytes             uint64   `json:"total_bytes"`
	DownloadedBytes        uint64   `json:"downloaded_bytes"`
	FailedBytes            uint64   `json:"failed_bytes"`
	LowestHealth           int      `json:"lowest_health"`
	DurationMs             int64    `json:"duration_ms"`
	TimeToFirstByteMs      *int64   `json:"time_to_first_byte_ms,omitempty"`
	TimeToAllBytesMs       *int64   `json:"time_to_all_bytes_ms,omitempty"`
	AvgEndToEndBytesPerSec float64  `json:"avg_end_to_end_bytes_per_sec"`
	AvgActiveBytesPerSec   float64  `json:"avg_active_bytes_per_sec"`
	PeakDownloadSpeed      uint64   `json:"peak_download_speed"`
	PeakArticlesPerSec     float64  `json:"peak_articles_per_sec"`
	PeakDecodeRateMbps     float64  `json:"peak_decode_rate_mbps"`
	SampleCount            int      `json:"sample_count"`
	SampleFile             string   `json:"sample_file,omitempty"`
	StatusesSeen           []string `json:"statuses_seen,omitempty"`
}

type downloadBenchAggregate struct {
	Scenario               string  `json:"scenario"`
	Runs                   int     `json:"runs"`
	Successes              int     `json:"successes"`
	AvgDurationMs          float64 `json:"avg_duration_ms"`
	AvgTimeToFirstByteMs   float64 `json:"avg_time_to_first_byte_ms"`
	AvgTimeToAllBytesMs    float64 `json:"avg_time_to_all_bytes_ms"`
	AvgEndToEndBytesPerSec float64 `json:"avg_end_to_end_bytes_per_sec"`
	AvgActiveBytesPerSec   float64 `json:"avg_active_bytes_per_sec"`
	AvgPeakDownloadSpeed   float64 `json:"avg_peak_download_speed"`
	AvgPeakArticlesPerSec  float64 `json:"avg_peak_articles_per_sec"`
	AvgPeakDecodeRateMbps  float64 `json:"avg_peak_decode_rate_mbps"`
	LowestObservedHealth   int     `json:"lowest_observed_health"`
}

type downloadBenchSummary struct {
	GeneratedAt        string                   `json:"generated_at"`
	ManagedLocalWeaver bool                     `json:"managed_local_weaver"`
	WeaverURL          string                   `json:"weaver_url"`
	OutputDir          string                   `json:"output_dir"`
	Iterations         int                      `json:"iterations"`
	SampleIntervalMs   int                      `json:"sample_interval_ms"`
	TimeoutSec         int                      `json:"timeout_sec"`
	Runs               []downloadBenchRun       `json:"runs"`
	Aggregates         []downloadBenchAggregate `json:"aggregates"`
}

type managedWeaverSession struct {
	URL     string
	LogPath string
	PID     int
	cmd     *exec.Cmd
	exit    *childExitWatch
	logFile *os.File
}

func cmdDownloadBench(args []string) {
	slugs := args
	if len(slugs) == 0 {
		envSlugs := strings.TrimSpace(env("DOWNLOAD_BENCH_SLUGS", ""))
		if envSlugs != "" {
			for _, slug := range strings.Split(envSlugs, ",") {
				slug = strings.TrimSpace(slug)
				if slug != "" {
					slugs = append(slugs, slug)
				}
			}
		}
	}
	if len(slugs) == 0 {
		slugs = []string{"single-mkv", "large-segments"}
	}

	iterations := envInt("DOWNLOAD_BENCH_ITERATIONS", 3)
	sampleIntervalMs := envInt("DOWNLOAD_BENCH_SAMPLE_MS", 250)
	timeoutSec := envInt("DOWNLOAD_BENCH_TIMEOUT_SEC", 300)
	managedLocalWeaver := envBool("DOWNLOAD_BENCH_LOCAL_WEAVER", false)
	outputDir := downloadBenchOutputDir()
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create download bench output dir: %v", err)
	}

	var (
		weaverURL string
		session   *managedWeaverSession
	)
	if managedLocalWeaver {
		var err error
		session, err = startManagedDownloadBenchWeaver(outputDir)
		if err != nil {
			log.Fatalf("start managed weaver: %v", err)
		}
		defer session.Close()
		weaverURL = session.URL
		log.Printf("managed weaver ready: pid=%d url=%s log=%s", session.PID, session.URL, session.LogPath)
	} else {
		weaverURL = defaultWeaverURL()
	}

	prepareStandardTestRun(weaverURL, true)

	scenarios := make([]*Scenario, 0, len(slugs))
	for _, slug := range slugs {
		scenarioPath := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(scenarioPath)
		if err != nil {
			log.Fatalf("load scenario %s: %v", slug, err)
		}
		nzbPath := filepath.Join(fixturesDir(), slug, slug+".nzb")
		if _, err := os.Stat(nzbPath); err != nil {
			log.Fatalf("no NZB for %q — run '%s seed %s' first", slug, cliProgramName, scenarioPath)
		}
		scenarios = append(scenarios, scenario)
	}

	sampleInterval := time.Duration(sampleIntervalMs) * time.Millisecond
	timeout := time.Duration(timeoutSec) * time.Second
	summary := downloadBenchSummary{
		GeneratedAt:        time.Now().Format(time.RFC3339),
		ManagedLocalWeaver: managedLocalWeaver,
		WeaverURL:          weaverURL,
		OutputDir:          outputDir,
		Iterations:         iterations,
		SampleIntervalMs:   sampleIntervalMs,
		TimeoutSec:         timeoutSec,
	}

	for _, scenario := range scenarios {
		for iteration := 1; iteration <= iterations; iteration++ {
			prepareStandardTestRun(weaverURL, true)
			run := runDownloadBenchIteration(weaverURL, scenario, iteration, outputDir, sampleInterval, timeout)
			summary.Runs = append(summary.Runs, run)
			log.Printf(
				"download-bench %s #%d: status=%s duration=%s first_byte=%s all_bytes=%s avg_active=%s peak=%s",
				run.Scenario,
				run.Iteration,
				run.Status,
				formatMilliseconds(run.DurationMs),
				formatOptionalMilliseconds(run.TimeToFirstByteMs),
				formatOptionalMilliseconds(run.TimeToAllBytesMs),
				formatBytesPerSecond(run.AvgActiveBytesPerSec),
				formatBytesPerSecond(float64(run.PeakDownloadSpeed)),
			)
		}
	}

	summary.Aggregates = buildDownloadBenchAggregates(summary.Runs)
	summaryPath := filepath.Join(outputDir, "summary.json")
	summaryJSON, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		log.Fatalf("marshal download bench summary: %v", err)
	}
	if err := os.WriteFile(summaryPath, summaryJSON, 0o644); err != nil {
		log.Fatalf("write download bench summary: %v", err)
	}

	printDownloadBenchSummary(summary)
	log.Printf("download bench artifacts written to %s", outputDir)

	for _, run := range summary.Runs {
		if run.Status != "COMPLETE" {
			os.Exit(1)
		}
	}
}

func cmdPgo(args []string) {
	profileDir := weaverProfileDir()
	outputDir := weaverPgoOutputDir()
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		log.Fatalf("create Weaver PGO profile dir: %v", err)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		log.Fatalf("create Weaver PGO output dir: %v", err)
	}
	beforeCount, err := countProfrawFiles(profileDir)
	if err != nil {
		log.Fatalf("scan Weaver PGO profile dir: %v", err)
	}

	if strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")) == "" {
		setEnv("E2E_WEAVER_PROFILE_DIR", profileDir)
	}
	if strings.TrimSpace(os.Getenv("DOWNLOAD_BENCH_LOCAL_WEAVER")) == "" {
		setEnv("DOWNLOAD_BENCH_LOCAL_WEAVER", "1")
	}
	if strings.TrimSpace(os.Getenv("DOWNLOAD_BENCH_ITERATIONS")) == "" {
		setEnv("DOWNLOAD_BENCH_ITERATIONS", "2")
	}
	if strings.TrimSpace(os.Getenv("DOWNLOAD_BENCH_OUTPUT_DIR")) == "" {
		setEnv("DOWNLOAD_BENCH_OUTPUT_DIR", filepath.Join(outputDir, "download-bench"))
	}

	log.Printf("collecting Weaver LLVM profiles into %s", profileDir)
	log.Printf("PGO run artifacts: %s", outputDir)
	log.Printf("managed Weaver binary: %s", findWeaverBin())

	log.Printf("phase 1/3: baseline suite")
	cmdTestAll()

	log.Printf("phase 2/3: TLS subset")
	cmdTlsTest()

	log.Printf("phase 3/3: download hotpath benchmark")
	cmdDownloadBench(args)

	afterCount, err := countProfrawFiles(profileDir)
	if err != nil {
		log.Fatalf("scan Weaver PGO profile dir after run: %v", err)
	}
	if afterCount <= beforeCount {
		log.Fatalf("no new .profraw files were written under %s; rerun with an instrumented Weaver binary", profileDir)
	}

	log.Printf(
		"collected %d new LLVM raw profile(s) (%d total) under %s",
		afterCount-beforeCount,
		afterCount,
		profileDir,
	)
}

func downloadBenchOutputDir() string {
	if value := strings.TrimSpace(env("DOWNLOAD_BENCH_OUTPUT_DIR", "")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join(os.TempDir(), "e2e-download-bench-"+time.Now().Format("20060102-150405"))
}

func startManagedDownloadBenchWeaver(outputDir string) (*managedWeaverSession, error) {
	waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)

	weaverBin := env("WEAVER_BIN", findWeaverBin())
	weaverPort := localWeaverPort()
	configPath := filepath.Join(outputDir, "weaver.toml")
	logPath := filepath.Join(outputDir, "weaver.log")
	pidPath := filepath.Join(outputDir, "weaver.pid")

	killWeaver()
	cleanWeaverState()
	writeDownloadBenchWeaverConfig(
		configPath,
		envInt("DOWNLOAD_BENCH_NNTP_PORT", mustPortInt("DOWNLOAD_BENCH_NNTP_PORT", nntpPort())),
		envInt("DOWNLOAD_BENCH_CONNECTIONS", 8),
	)

	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("create weaver log: %w", err)
	}

	cmd := exec.Command(weaverBin, "--config", configPath, "serve", "--port", weaverPort)
	cmd.Env = managedWeaverEnv(os.Environ(), outputDir, env("DOWNLOAD_BENCH_RUST_LOG", "warn"))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start weaver: %w", err)
	}
	exit := watchChildExit(cmd, logPath)

	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
		log.Printf("warning: write weaver pid file: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(localWeaverPIDPath()), 0o755); err == nil {
		if err := os.WriteFile(localWeaverPIDPath(), []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
			log.Printf("warning: write local weaver pid file: %v", err)
		}
	}

	url := fmt.Sprintf("http://localhost:%s", weaverPort)
	waitForGraphQL(graphqlURL(url), exit.Probe)

	return &managedWeaverSession{
		URL:     url,
		LogPath: logPath,
		PID:     cmd.Process.Pid,
		cmd:     cmd,
		exit:    exit,
		logFile: logFile,
	}, nil
}

func (s *managedWeaverSession) Close() {
	if s == nil {
		return
	}
	switch {
	case s.cmd != nil && s.cmd.Process != nil && s.exit != nil:
		stopWatchedChild(s.cmd, s.exit, 30*time.Second)
	case s.cmd != nil && s.cmd.Process != nil:
		stopManagedWeaverCommand(s.cmd, 30*time.Second)
	}
	_ = os.Remove(localWeaverPIDPath())
	if s.logFile != nil {
		_ = s.logFile.Close()
	}
}

func writeDownloadBenchWeaverConfig(path string, nntpPort, connections int) {
	root := localWeaverDir()
	os.MkdirAll(filepath.Dir(path), 0o755)
	os.MkdirAll(filepath.Join(root, "intermediate"), 0o755)
	os.MkdirAll(filepath.Join(root, "complete"), 0o755)
	config := fmt.Sprintf(`data_dir = %q
intermediate_dir = %q
complete_dir = %q
cleanup_after_extract = true
max_retries = 3

[[servers]]
id = 1
host = "localhost"
port = %d
tls = false
username = "e2e-user"
password = "e2e-pass"
connections = %d
active = true
priority = 0

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "series"
`, root, filepath.Join(root, "intermediate"), filepath.Join(root, "complete"), nntpPort, connections)
	_ = os.WriteFile(path, []byte(config), 0o644)
}

// managedWeaverExtractionMemoryBytes is the extraction memory budget every
// natively launched server gets: 6 GiB.
const managedWeaverExtractionMemoryBytes = "6442450944"

func managedWeaverEnv(base []string, runRoot, rustLog string) []string {
	if strings.TrimSpace(runRoot) == "" {
		runRoot = localRunDir()
	}
	homeDir := filepath.Join(runRoot, ".weaver-home")
	cacheDir := filepath.Join(homeDir, ".cache")
	configDir := filepath.Join(homeDir, ".config")
	dataDir := filepath.Join(homeDir, ".local", "share")
	_ = os.MkdirAll(cacheDir, 0o755)
	_ = os.MkdirAll(configDir, 0o755)
	_ = os.MkdirAll(dataDir, 0o755)

	env := append([]string(nil), base...)
	env = append(env,
		"HOME="+homeDir,
		"XDG_CACHE_HOME="+cacheDir,
		"XDG_CONFIG_HOME="+configDir,
		"XDG_DATA_HOME="+dataDir,
		"WEAVER_FORCE_KEY_FILE=1",
		// The managed server is reached only through its loopback port by the
		// E2E client. Trusting loopback keeps it loginless for those tests
		// without weakening non-local browser administration.
		"WEAVER_TRUSTED_CIDRS=127.0.0.1/32,::1/128",
	)
	// Several of these servers run side by side on one host, and each one's
	// extraction memory budget otherwise defaults to half the host's RAM. The
	// largest decoder the corpus declares needs a few tens of MiB, so a fixed
	// budget keeps the lanes from promising each other the same memory.
	env = appendOrReplaceEnv(env, "WEAVER_EXTRACTION_MAX_MEMORY_BYTES", managedWeaverExtractionMemoryBytes)
	if weaverUsesPostgresDatastore() {
		env = appendOrReplaceEnv(env, "WEAVER_DATABASE_URL", weaverPostgresURL())
	}
	if strings.TrimSpace(rustLog) != "" {
		env = append(env, "RUST_LOG="+rustLog)
	}
	if profileDir := strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")); profileDir != "" {
		profileDir = absolutePath(profileDir)
		if err := os.MkdirAll(profileDir, 0o755); err != nil {
			log.Fatalf("create managed Weaver profile dir: %v", err)
		}
		env = append(env, "LLVM_PROFILE_FILE="+filepath.Join(profileDir, managedWeaverProfileStem(runRoot)+"-%m-%p.profraw"))
	}
	return env
}

func weaverProfileDir() string {
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_PROFILE_DIR")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join(localRunDir(), "pgo", "profraw")
}

func weaverPgoOutputDir() string {
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_PGO_OUTPUT_DIR")); value != "" {
		return absolutePath(value)
	}
	return filepath.Join(localRunDir(), "pgo")
}

func countProfrawFiles(dir string) (int, error) {
	matches, err := filepath.Glob(filepath.Join(absolutePath(dir), "*.profraw"))
	if err != nil {
		return 0, err
	}
	return len(matches), nil
}

func managedWeaverProfileStem(runRoot string) string {
	stem := strings.TrimSpace(strings.ToLower(filepath.Base(runRoot)))
	if stem == "" || stem == "." || stem == string(filepath.Separator) {
		return "weaver"
	}
	replacer := strings.NewReplacer(
		"/", "-",
		"\\", "-",
		" ", "-",
		".", "-",
		":", "-",
		"@", "-",
		"%", "-",
		"+", "-",
		"=", "-",
	)
	stem = strings.Trim(replacer.Replace(stem), "-_")
	if stem == "" {
		return "weaver"
	}
	return stem
}

func runDownloadBenchIteration(
	weaverURL string,
	scenario *Scenario,
	iteration int,
	outputDir string,
	sampleInterval time.Duration,
	timeout time.Duration,
) downloadBenchRun {
	run := downloadBenchRun{
		Scenario:     scenario.Slug,
		Iteration:    iteration,
		Status:       "SUBMIT_ERROR",
		LowestHealth: 1000,
		StatusesSeen: make([]string, 0, 8),
	}

	started := time.Now()
	jobID, err := submitOneNZB(weaverURL, scenario)
	if err != nil {
		run.Error = err.Error()
		return run
	}
	run.JobID = jobID

	samplePath := filepath.Join(outputDir, fmt.Sprintf("%s-run%d-samples.json", scenario.Slug, iteration))
	deadline := started.Add(timeout)
	samples := make([]downloadBenchSample, 0, 256)
	statusSeen := map[string]bool{}
	var firstByteMs *int64
	var allBytesMs *int64

	for {
		snapshot, err := queryDownloadBenchSnapshot(weaverURL, jobID)
		if err == nil {
			elapsedMs := time.Since(started).Milliseconds()
			sample := snapshotToDownloadBenchSample(elapsedMs, snapshot)
			samples = append(samples, sample)

			if !statusSeen[sample.Status] {
				statusSeen[sample.Status] = true
				run.StatusesSeen = append(run.StatusesSeen, sample.Status)
			}
			if sample.Health < run.LowestHealth {
				run.LowestHealth = sample.Health
			}
			if sample.CurrentDownloadSpeed > run.PeakDownloadSpeed {
				run.PeakDownloadSpeed = sample.CurrentDownloadSpeed
			}
			if sample.ArticlesPerSec > run.PeakArticlesPerSec {
				run.PeakArticlesPerSec = sample.ArticlesPerSec
			}
			if sample.DecodeRateMbps > run.PeakDecodeRateMbps {
				run.PeakDecodeRateMbps = sample.DecodeRateMbps
			}

			processedBytes := processedDownloadBytes(sample.DownloadedBytes, sample.FailedBytes)
			if firstByteMs == nil && processedBytes > 0 {
				value := elapsedMs
				firstByteMs = &value
			}
			if allBytesMs == nil && (sample.Progress >= 0.999 || sample.Status == "COMPLETE" || sample.Status == "FAILED") {
				value := elapsedMs
				allBytesMs = &value
			}

			if sample.Status == "COMPLETE" || sample.Status == "FAILED" {
				run.Status, run.Error = applyTerminalStateCheck(localWeaverDBPath(), jobID, scenario.Slug, sample.Status)
				if run.Error == "" {
					run.Error = sample.Error
				}
				run.TotalBytes = sample.TotalBytes
				run.DownloadedBytes = sample.DownloadedBytes
				run.FailedBytes = sample.FailedBytes
				break
			}
		}

		if time.Now().After(deadline) {
			run.Status = "TIMEOUT"
			run.Error = fmt.Sprintf("timed out after %s", timeout)
			break
		}
		time.Sleep(sampleInterval)
	}

	run.SampleCount = len(samples)
	run.SampleFile = samplePath
	run.DurationMs = time.Since(started).Milliseconds()
	run.TimeToFirstByteMs = firstByteMs
	run.TimeToAllBytesMs = allBytesMs

	if len(samples) > 0 {
		last := samples[len(samples)-1]
		run.TotalBytes = last.TotalBytes
		run.DownloadedBytes = last.DownloadedBytes
		run.FailedBytes = last.FailedBytes
		if last.Health < run.LowestHealth {
			run.LowestHealth = last.Health
		}
		run.AvgEndToEndBytesPerSec = bytesPerSecond(processedDownloadBytes(last.DownloadedBytes, last.FailedBytes), run.DurationMs)
		if allBytesMs != nil {
			activeMs := *allBytesMs
			if firstByteMs != nil && *allBytesMs > *firstByteMs {
				activeMs = *allBytesMs - *firstByteMs
			}
			run.AvgActiveBytesPerSec = bytesPerSecond(processedDownloadBytes(last.DownloadedBytes, last.FailedBytes), activeMs)
		}
	}

	sampleJSON, err := json.MarshalIndent(samples, "", "  ")
	if err != nil {
		log.Printf("warning: marshal download bench samples for %s #%d: %v", scenario.Slug, iteration, err)
	} else if err := os.WriteFile(samplePath, sampleJSON, 0o644); err != nil {
		log.Printf("warning: write download bench samples for %s #%d: %v", scenario.Slug, iteration, err)
	}

	return run
}

func queryDownloadBenchSnapshot(weaverURL string, jobID int) (downloadBenchSnapshot, error) {
	var result downloadBenchSnapshot
	item, err := fetchFacadeItemSnapshot(weaverURL, jobID)
	if err != nil {
		return result, err
	}
	if !item.Found {
		return result, fmt.Errorf("item %d not found", jobID)
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"query": `query {
			metrics {
				currentDownloadSpeed
				articlesPerSec
				decodeRateMbps
				bytesDownloaded
				bytesDecoded
				bytesCommitted
				segmentsDownloaded
				segmentsDecoded
				segmentsCommitted
			}
		}`,
	})
	resp, err := postGraphQL(weaverURL, payload)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	var gqlResp struct {
		Data struct {
			Metrics struct {
				CurrentDownloadSpeed uint64  `json:"currentDownloadSpeed"`
				ArticlesPerSec       float64 `json:"articlesPerSec"`
				DecodeRateMbps       float64 `json:"decodeRateMbps"`
				BytesDownloaded      uint64  `json:"bytesDownloaded"`
				BytesDecoded         uint64  `json:"bytesDecoded"`
				BytesCommitted       uint64  `json:"bytesCommitted"`
				SegmentsDownloaded   uint64  `json:"segmentsDownloaded"`
				SegmentsDecoded      uint64  `json:"segmentsDecoded"`
				SegmentsCommitted    uint64  `json:"segmentsCommitted"`
			} `json:"metrics"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		return result, err
	}
	if len(gqlResp.Errors) > 0 {
		return result, fmt.Errorf("%s", gqlResp.Errors[0].Message)
	}

	result.Job.Status = item.Status
	result.Job.Progress = item.ProgressPercent
	result.Job.Health = item.Health
	result.Job.TotalBytes = item.TotalBytes
	result.Job.DownloadedBytes = item.DownloadedBytes
	result.Job.OptionalRecoveryBytes = item.OptionalRecoveryBytes
	result.Job.OptionalRecoveryDownloadedBytes = item.OptionalRecoveryDownloadedBytes
	result.Job.FailedBytes = item.FailedBytes
	if item.Error != "" {
		errMsg := item.Error
		result.Job.Error = &errMsg
	}
	result.Metrics = gqlResp.Data.Metrics
	return result, nil
}

func snapshotToDownloadBenchSample(elapsedMs int64, snapshot downloadBenchSnapshot) downloadBenchSample {
	sample := downloadBenchSample{
		ElapsedMs:                       elapsedMs,
		Status:                          snapshot.Job.Status,
		Progress:                        snapshot.Job.Progress,
		Health:                          snapshot.Job.Health,
		TotalBytes:                      snapshot.Job.TotalBytes,
		DownloadedBytes:                 snapshot.Job.DownloadedBytes,
		OptionalRecoveryBytes:           snapshot.Job.OptionalRecoveryBytes,
		OptionalRecoveryDownloadedBytes: snapshot.Job.OptionalRecoveryDownloadedBytes,
		FailedBytes:                     snapshot.Job.FailedBytes,
		CurrentDownloadSpeed:            snapshot.Metrics.CurrentDownloadSpeed,
		ArticlesPerSec:                  snapshot.Metrics.ArticlesPerSec,
		DecodeRateMbps:                  snapshot.Metrics.DecodeRateMbps,
		BytesDownloaded:                 snapshot.Metrics.BytesDownloaded,
		BytesDecoded:                    snapshot.Metrics.BytesDecoded,
		BytesCommitted:                  snapshot.Metrics.BytesCommitted,
		SegmentsDownloaded:              snapshot.Metrics.SegmentsDownloaded,
		SegmentsDecoded:                 snapshot.Metrics.SegmentsDecoded,
		SegmentsCommitted:               snapshot.Metrics.SegmentsCommitted,
	}
	if snapshot.Job.Error != nil {
		sample.Error = *snapshot.Job.Error
	}
	return sample
}

func buildDownloadBenchAggregates(runs []downloadBenchRun) []downloadBenchAggregate {
	type accumulator struct {
		aggregate       downloadBenchAggregate
		durationSum     float64
		firstByteSum    float64
		firstByteCount  int
		allBytesSum     float64
		allBytesCount   int
		endToEndSum     float64
		activeSum       float64
		peakSpeedSum    float64
		peakArticlesSum float64
		peakDecodeSum   float64
	}

	byScenario := make(map[string]*accumulator)
	for _, run := range runs {
		acc := byScenario[run.Scenario]
		if acc == nil {
			acc = &accumulator{aggregate: downloadBenchAggregate{
				Scenario:             run.Scenario,
				LowestObservedHealth: 1000,
			}}
			byScenario[run.Scenario] = acc
		}

		acc.aggregate.Runs++
		if run.Status == "COMPLETE" {
			acc.aggregate.Successes++
		}
		acc.durationSum += float64(run.DurationMs)
		acc.endToEndSum += run.AvgEndToEndBytesPerSec
		acc.activeSum += run.AvgActiveBytesPerSec
		acc.peakSpeedSum += float64(run.PeakDownloadSpeed)
		acc.peakArticlesSum += run.PeakArticlesPerSec
		acc.peakDecodeSum += run.PeakDecodeRateMbps
		if run.LowestHealth < acc.aggregate.LowestObservedHealth {
			acc.aggregate.LowestObservedHealth = run.LowestHealth
		}
		if run.TimeToFirstByteMs != nil {
			acc.firstByteSum += float64(*run.TimeToFirstByteMs)
			acc.firstByteCount++
		}
		if run.TimeToAllBytesMs != nil {
			acc.allBytesSum += float64(*run.TimeToAllBytesMs)
			acc.allBytesCount++
		}
	}

	out := make([]downloadBenchAggregate, 0, len(byScenario))
	for _, scenario := range sortedKeys(byScenario) {
		acc := byScenario[scenario]
		runsCount := float64(acc.aggregate.Runs)
		acc.aggregate.AvgDurationMs = acc.durationSum / runsCount
		acc.aggregate.AvgEndToEndBytesPerSec = acc.endToEndSum / runsCount
		acc.aggregate.AvgActiveBytesPerSec = acc.activeSum / runsCount
		acc.aggregate.AvgPeakDownloadSpeed = acc.peakSpeedSum / runsCount
		acc.aggregate.AvgPeakArticlesPerSec = acc.peakArticlesSum / runsCount
		acc.aggregate.AvgPeakDecodeRateMbps = acc.peakDecodeSum / runsCount
		if acc.firstByteCount > 0 {
			acc.aggregate.AvgTimeToFirstByteMs = acc.firstByteSum / float64(acc.firstByteCount)
		}
		if acc.allBytesCount > 0 {
			acc.aggregate.AvgTimeToAllBytesMs = acc.allBytesSum / float64(acc.allBytesCount)
		}
		out = append(out, acc.aggregate)
	}
	return out
}

func sortedKeys[T any](items map[string]T) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func printDownloadBenchSummary(summary downloadBenchSummary) {
	fmt.Println()
	fmt.Printf("%-18s %-5s %-10s %-11s %-11s %-12s %-12s %-10s %-8s\n",
		"SCENARIO", "RUN", "STATUS", "TOTAL", "1ST BYTE", "ALL BYTES", "AVG ACTIVE", "PEAK", "HEALTH")
	fmt.Println(strings.Repeat("-", 112))
	for _, run := range summary.Runs {
		fmt.Printf("%-18s %-5d %-10s %-11s %-11s %-12s %-12s %-10s %-8.1f\n",
			run.Scenario,
			run.Iteration,
			run.Status,
			formatMilliseconds(run.DurationMs),
			formatOptionalMilliseconds(run.TimeToFirstByteMs),
			formatOptionalMilliseconds(run.TimeToAllBytesMs),
			formatBytesPerSecond(run.AvgActiveBytesPerSec),
			formatBytesPerSecond(float64(run.PeakDownloadSpeed)),
			float64(run.LowestHealth)/10.0,
		)
	}
	fmt.Println(strings.Repeat("-", 112))
	fmt.Println("AVERAGES")
	for _, aggregate := range summary.Aggregates {
		fmt.Printf("  %-18s runs=%d ok=%d total=%s first_byte=%s all_bytes=%s avg_active=%s peak=%s decode=%.1f health=%.1f%%\n",
			aggregate.Scenario,
			aggregate.Runs,
			aggregate.Successes,
			formatMilliseconds(int64(aggregate.AvgDurationMs)),
			formatFloatMilliseconds(aggregate.AvgTimeToFirstByteMs),
			formatFloatMilliseconds(aggregate.AvgTimeToAllBytesMs),
			formatBytesPerSecond(aggregate.AvgActiveBytesPerSec),
			formatBytesPerSecond(aggregate.AvgPeakDownloadSpeed),
			aggregate.AvgPeakDecodeRateMbps,
			float64(aggregate.LowestObservedHealth)/10.0,
		)
	}
}

func processedDownloadBytes(downloadedBytes, failedBytes uint64) uint64 {
	return downloadedBytes + failedBytes
}

func bytesPerSecond(bytes uint64, durationMs int64) float64 {
	if durationMs <= 0 {
		return 0
	}
	return float64(bytes) / (float64(durationMs) / 1000.0)
}

func formatMilliseconds(ms int64) string {
	if ms <= 0 {
		return "0ms"
	}
	return (time.Duration(ms) * time.Millisecond).Round(10 * time.Millisecond).String()
}

func formatOptionalMilliseconds(ms *int64) string {
	if ms == nil {
		return "n/a"
	}
	return formatMilliseconds(*ms)
}

func formatFloatMilliseconds(ms float64) string {
	if ms <= 0 {
		return "n/a"
	}
	return formatMilliseconds(int64(ms))
}

func formatBytesPerSecond(bytesPerSecond float64) string {
	if bytesPerSecond <= 0 {
		return "0 B/s"
	}
	units := []string{"B/s", "KiB/s", "MiB/s", "GiB/s"}
	value := bytesPerSecond
	unit := units[0]
	for i := 1; i < len(units) && value >= 1024; i++ {
		value /= 1024
		unit = units[i]
	}
	if value >= 10 {
		return fmt.Sprintf("%.0f %s", value, unit)
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}
