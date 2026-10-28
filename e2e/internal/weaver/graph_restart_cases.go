// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// The graph engine's cases: the NZB admission cap, backfill held to the
// articles every fill server lacks, the seat cap across a dial burst and
// redials, job directories that sanitise alike, a job with no checkpoint
// after a restart, upgrades from a 0.16.5 database and from one written
// before the graph engine became the only engine, a row start-up cannot
// archive, a RAR4 trial key condemned on its whole-file checksum, and a zip's
// directories and code page 437 names.

const (
	graphCaseSlug     = "single-mkv"
	rar4TrialKeySlug  = "direct-store-rar4-encrypted"
	zipDirectorySlug  = "zip-cp437-empty-dirs"
	admissionCap      = 2
	admissionJobs     = 5
	seatCap           = 4
	seatChaos         = "drop_mid_body=10"
	refusedArchiveLog = "failed to archive a job during startup recovery"
)

func graphRestartCases() []restartCase {
	return []restartCase{
		{
			Name:        "admission_cap_keeps_the_rest_queued",
			Description: "More NZBs than the admission cap are submitted while the news server holds every body; no sample admits more than the cap, the rest wait without failing, every job completes and admission follows submission order.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     10 * time.Minute,
			Run:         runAdmissionCapKeepsTheRestQueued,
		},
		{
			Name:        "backfill_serves_only_what_the_fill_tier_lacks",
			Description: "The fill server lacks three articles a backfill server has; the job completes and the backfill server serves exactly those three.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     5 * time.Minute,
			Run:         runBackfillServesOnlyWhatTheFillTierLacks,
		},
		{
			Name:        "backfill_is_no_backup_for_refused_credentials",
			Description: "The fill server refuses credentials while a backfill server has every article; the backfill server serves nothing and the job does not complete from it.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     5 * time.Minute,
			Run:         runBackfillIsNoBackupForRefusedCredentials,
		},
		{
			Name:        "lower_priority_server_covers_a_down_primary",
			Description: "The priority-0 server refuses every greeting; a priority-1 fill server has every article and the job completes from it.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     5 * time.Minute,
			Run:         runLowerPriorityServerCoversADownPrimary,
		},
		{
			Name:        "warm_dial_never_exceeds_the_seat_cap",
			Description: "With four connections configured, the news server never sees more than four of Weaver's at once, through the dial burst at start and the redials after dropped bodies.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     5 * time.Minute,
			Run:         runWarmDialNeverExceedsTheSeatCap,
		},
		{
			Name:        "alike_job_names_get_their_own_directories",
			Description: "Two jobs whose names sanitise to the same directory name each deliver into a directory of their own, both whole.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     5 * time.Minute,
			Run:         runAlikeJobNamesGetTheirOwnDirectories,
		},
		{
			Name:        "job_without_checkpoint_admitted_afresh",
			Description: "A job stopped before its first checkpoint was written is admitted afresh after the restart and completes, rather than being refused.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     5 * time.Minute,
			Run:         runJobWithoutCheckpointAdmittedAfresh,
		},
		{
			Name:        "upgrade_from_0_16_5_database",
			Description: "A 0.16.5 database with two active jobs, rows in classic-only tables and two history rows upgrades: every migration applies, both jobs complete, history reads back and the classic rows are gone.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     10 * time.Minute,
			Run:         runUpgradeFrom0165Database,
		},
		{
			Name:        "upgrade_from_engine_stamped_database",
			Description: "A database written before the graph engine became the only engine, with an active job stamped classic, one stamped graph, history rows with both stamps and rows in classic-only tables, upgrades: both jobs complete and history reads back.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     10 * time.Minute,
			Run:         runUpgradeFromEngineStampedDatabase,
		},
		{
			Name:        "unarchivable_row_does_not_stop_startup",
			Description: "A terminal active row whose archive the database refuses is warned about once and left active; start-up goes on and the healthy job beside it completes, and the next start archives it.",
			Slugs:       []string{graphCaseSlug},
			Timeout:     10 * time.Minute,
			Run:         runUnarchivableRowDoesNotStopStartup,
		},
		{
			Name:        "rar4_trial_key_condemned_on_whole_file_checksum",
			Description: "A RAR4 store set with readable headers is opened by the operator's archive passwords, the wrong one first; the wrong key fails the whole-file checksum, the right one opens the same bytes, and no article is fetched twice.",
			Slugs:       []string{rar4TrialKeySlug},
			Timeout:     5 * time.Minute,
			Run:         runRAR4TrialKeyCondemnedOnWholeFileChecksum,
		},
		{
			Name:        "zip_directories_and_cp437_names_delivered",
			Description: "A zip's directory records, two of them empty, are delivered as directories, and a code page 437 name and an unflagged UTF-8 name come out as the entries declare them.",
			Slugs:       []string{zipDirectorySlug},
			Timeout:     5 * time.Minute,
			Run:         runZipDirectoriesAndCP437NamesDelivered,
		},
	}
}

// G1. The admission cap holds at every sample, the jobs past it wait queued,
// and they are admitted in the order they were submitted.
func runAdmissionCapKeepsTheRestQueued(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	if err := setMaxConcurrentNzbsGraphQL(ctx.weaverURL, admissionCap); err != nil {
		return restartCaseResult{}, err
	}
	if err := holdPrimaryBodies(); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	ids, err := ctx.submitSlugNTimes(graphCaseSlug, admissionJobs)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	trace := newAdmissionTrace(admissionCap, ids)
	sample := 0
	var waitingFailed []string
	observe := func() (map[int]admissionRow, []int, error) {
		rows, err := readAdmissionRows(dbPath, ids)
		if err != nil {
			return nil, nil, err
		}
		var admitted, finished []int
		for _, id := range ids {
			row, active := rows[id]
			switch {
			case active && row.Admitted:
				admitted = append(admitted, id)
			case active && (row.Status == "failed" || row.Status == "cancelled"):
				waitingFailed = append(waitingFailed, fmt.Sprintf("job %d %s: %s", id, row.Status, row.Error))
			case !active:
				if archived, err := jobArchived(dbPath, id); err == nil && archived {
					finished = append(finished, id)
				}
			}
		}
		trace.observe(sample, admitted, finished)
		sample++
		return rows, finished, nil
	}

	// With every body held, the cap is full once two jobs are admitted, and
	// the three behind them stay queued for as long as the hold lasts.
	var held map[int]admissionRow
	if err := ctx.pollFor("the admission cap filled", func() (bool, error) {
		rows, _, err := observe()
		if err != nil {
			return false, err
		}
		held = rows
		admitted := 0
		for _, row := range rows {
			if row.Admitted {
				admitted++
			}
		}
		return admitted >= admissionCap, nil
	}); err != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", err.Error()), nil
	}
	waiting := 0
	for _, id := range ids {
		if row, ok := held[id]; ok && !row.Admitted && row.Status != "failed" {
			waiting++
		}
	}
	if err := liftPrimaryHold(); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.pollFor("every admitted job finished", func() (bool, error) {
		_, finished, err := observe()
		return len(finished) == len(ids), err
	}); err != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", err.Error()+"; "+trace.summary()), nil
	}
	statuses, err := ctx.waitForAllTerminal(ids, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	completed := 0
	for _, id := range ids {
		if statuses[id] == "COMPLETE" {
			completed++
		}
	}
	verdict := trace.verdict()
	detail := fmt.Sprintf("%s waiting_while_held=%d completed=%d/%d waiting_failed=%v", trace.summary(), waiting, completed, len(ids), waitingFailed)
	pass := verdict == nil && waiting == admissionJobs-admissionCap && completed == len(ids) && len(waitingFailed) == 0
	failure := detail
	if verdict != nil {
		failure = verdict.Error() + "; " + detail
	}
	return classifyRestartResult(ctx.Profile, pass, "the admission cap held and the queue drained in order: "+detail, false, "", failure), nil
}

// backfillSetup starts Weaver with the primary as its fill server, brings the
// backup news server up with every article, and adds it as a second server.
func (ctx *restartCaseContext) backfillSetup(priority int, backfill bool) error {
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return err
	}
	if err := ensureBackupNntpReady(); err != nil {
		return err
	}
	if _, err := addNewsServerGraphQL(ctx.weaverURL, backupNntpPort(), 8, priority, backfill); err != nil {
		return err
	}
	return resetNntpMetrics()
}

// G2a. A backfill server fills exactly what the fill tier lacks.
func runBackfillServesOnlyWhatTheFillTierLacks(ctx *restartCaseContext) (restartCaseResult, error) {
	segments, err := nzbSegments(graphCaseSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	if len(segments) < 8 {
		return restartCaseResult{}, fmt.Errorf("%s has %d articles; the case withholds three apart", graphCaseSlug, len(segments))
	}
	if err := ctx.backfillSetup(0, true); err != nil {
		return restartCaseResult{}, err
	}
	withheld := []string{
		segments[len(segments)/4].MessageID,
		segments[len(segments)/2].MessageID,
		segments[3*len(segments)/4].MessageID,
	}
	putBack, err := withholdArticles(nntpHost(), nntpPort(), withheld)
	if err != nil {
		return restartCaseResult{}, err
	}
	defer putBack()
	jobID, err := ctx.submitSlug(graphCaseSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	backup, err := fetchNntpBodyMetricsFrom(nntpHost(), backupNntpPort(), "")
	if err != nil {
		return restartCaseResult{}, err
	}
	var served []string
	for id, count := range backup.BodyCounts {
		if count > 0 {
			served = append(served, id)
		}
	}
	sort.Strings(served)
	want := append([]string(nil), withheld...)
	sort.Strings(want)
	size, digest, err := singleMKVSource()
	if err != nil {
		return restartCaseResult{}, err
	}
	outputOK := statuses[jobID] == "COMPLETE" && deliveredCopy(completeDirOf(caseDBPath(ctx), jobID), size, digest)
	detail := fmt.Sprintf("final=%s output_ok=%t backfill_served=%v withheld=%v", statuses[jobID], outputOK, served, want)
	pass := outputOK && strings.Join(served, ",") == strings.Join(want, ",")
	return classifyRestartResult(ctx.Profile, pass, "the backfill server served exactly the articles the fill server lacked: "+detail, false, "", detail), nil
}

// G2b. A fill server that refuses credentials does not unlock backfill.
func runBackfillIsNoBackupForRefusedCredentials(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.backfillSetup(0, true); err != nil {
		return restartCaseResult{}, err
	}
	// The refusal refuses the harness too, so it is set and cleared over one
	// control session that authenticated before it.
	release, err := holdNntpChaosOnServer(nntpHost(), nntpPort(), "reauth_body=100,reject_auth=100")
	if err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(graphCaseSlug)
	if err != nil {
		_ = release()
		return restartCaseResult{}, err
	}
	statuses, waitErr := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err := release(); err != nil {
		return restartCaseResult{}, err
	}
	if waitErr != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", "the job never settled with credentials refused: "+waitErr.Error()), nil
	}
	backup, err := fetchNntpBodyMetricsFrom(nntpHost(), backupNntpPort(), "")
	if err != nil {
		return restartCaseResult{}, err
	}
	backfillBodies := 0
	for _, count := range backup.BodyCounts {
		backfillBodies += count
	}
	final := statuses[jobID]
	detail := fmt.Sprintf("final=%s backfill_bodies=%d", final, backfillBodies)
	pass := backfillBodies == 0 && final != "COMPLETE"
	// The code today unlocks backfill once every fill server is excluded,
	// refused credentials among the reasons, so the backfill server serving
	// the job is the known gap the ruling removes.
	gap := backfillBodies > 0
	return classifyRestartResult(ctx.Profile, pass,
		"refused credentials left the backfill server idle and the job unfinished: "+detail,
		gap, "refused credentials unlocked the backfill server: "+detail,
		"refused credentials: "+detail), nil
}

// G2c. Priority levels are the backup: a lower-priority fill server covers a
// primary that is down.
func runLowerPriorityServerCoversADownPrimary(ctx *restartCaseContext) (restartCaseResult, error) {
	segments, err := nzbSegments(graphCaseSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	ids := segmentIDs(segments)
	if err := ctx.backfillSetup(1, false); err != nil {
		return restartCaseResult{}, err
	}
	release, err := holdNntpChaosOnServer(nntpHost(), nntpPort(), "greet_400=100")
	if err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(graphCaseSlug)
	if err != nil {
		_ = release()
		return restartCaseResult{}, err
	}
	statuses, waitErr := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err := release(); err != nil {
		return restartCaseResult{}, err
	}
	if waitErr != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", "the job never settled with the primary down: "+waitErr.Error()), nil
	}
	primary, err := fetchNntpBodyMetricsFrom(nntpHost(), nntpPort(), "")
	if err != nil {
		return restartCaseResult{}, err
	}
	backup, err := fetchNntpBodyMetricsFrom(nntpHost(), backupNntpPort(), "")
	if err != nil {
		return restartCaseResult{}, err
	}
	coveredByBackup := 0
	for _, id := range ids {
		if backup.BodyCounts[id] > 0 {
			coveredByBackup++
		}
	}
	size, digest, err := singleMKVSource()
	if err != nil {
		return restartCaseResult{}, err
	}
	outputOK := statuses[jobID] == "COMPLETE" && deliveredCopy(completeDirOf(caseDBPath(ctx), jobID), size, digest)
	detail := fmt.Sprintf("final=%s output_ok=%t primary_bodies=%d backup_covered=%d/%d",
		statuses[jobID], outputOK, totalBodiesFor(primary, ids), coveredByBackup, len(ids))
	pass := outputOK && coveredByBackup == len(ids) && totalBodiesFor(primary, ids) == 0
	return classifyRestartResult(ctx.Profile, pass, "the priority-1 server covered the down primary: "+detail, false, "", detail), nil
}

// G3. The seat cap holds from the first dial to the last redial. The
// harness reaches the news server over control channels, which the server
// leaves out of its session counts, so the peak is Weaver's alone and the
// allowance is the configured count exactly.
func runWarmDialNeverExceedsTheSeatCap(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", seatCap); err != nil {
		return restartCaseResult{}, err
	}
	// Dropped bodies force redials across the whole job; each one is retried.
	if err := setNntpChaosOnServer(nntpHost(), nntpPort(), seatChaos); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	// From here the only harness connection the server sees is the one
	// reading the counters at the end.
	if err := resetNntpMetrics(); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(graphCaseSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	connections, err := waitForFreeNntpConnectionMetrics(nntpHost(), nntpPort())
	if err != nil {
		return restartCaseResult{}, err
	}
	if err := liftPrimaryHold(); err != nil {
		return restartCaseResult{}, err
	}
	const harnessReads = 1
	weaverDials := connections.Attempted - harnessReads
	// More dials than seats means some seat was dialled twice: the redials
	// the dropped bodies forced are in the window the peak covers.
	redialed := weaverDials > int64(seatCap)
	detail := fmt.Sprintf("final=%s weaver_dials=%d accepted=%d rejected=%d peak_sessions=%d seats=%d",
		statuses[jobID], weaverDials, connections.Accepted, connections.Rejected, connections.PeakActive, seatCap)
	pass := statuses[jobID] == "COMPLETE" && redialed && connections.PeakActive <= int64(seatCap)
	failure := detail
	if !redialed {
		failure = "no redial happened, so the case proved only the first burst: " + detail
	}
	return classifyRestartResult(ctx.Profile, pass, "the news server never held more sessions than the seat cap: "+detail, false, "", failure), nil
}

// G4. Two names that sanitise to one directory name each get their own.
func runAlikeJobNamesGetTheirOwnDirectories(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	first, err := ctx.submitScenarioAs(graphCaseSlug, "E2E Alike: One", false)
	if err != nil {
		return restartCaseResult{}, err
	}
	second, err := ctx.submitScenarioAs(graphCaseSlug, "E2E Alike? One", true)
	if err != nil {
		return restartCaseResult{}, err
	}
	ids := []int{first, second}
	statuses, err := ctx.waitForAllTerminal(ids, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	size, digest, err := singleMKVSource()
	if err != nil {
		return restartCaseResult{}, err
	}
	dirs := make([]string, 0, 2)
	whole := 0
	for _, id := range ids {
		if err := waitForJobArchived(dbPath, id); err != nil {
			return restartCaseResult{}, err
		}
		dir, err := historyOutputDir(dbPath, id)
		if err != nil {
			return restartCaseResult{}, err
		}
		dirs = append(dirs, dir)
		if statuses[id] == "COMPLETE" && deliveredCopy(dir, size, digest) {
			whole++
		}
	}
	detail := fmt.Sprintf("statuses=%v output_dirs=%q whole=%d/2", statuses, dirs, whole)
	pass := dirs[0] != dirs[1] && whole == 2
	return classifyRestartResult(ctx.Profile, pass, "names that sanitise alike delivered into their own directories: "+detail, false, "", detail), nil
}

// G8. A job whose checkpoint never landed is admitted afresh after a restart.
func runJobWithoutCheckpointAdmittedAfresh(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	if err := holdPrimaryBodies(); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	jobID, err := ctx.submitSlug(graphCaseSlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	// Admission writes the first checkpoint; once it has, removing its rows
	// leaves the job exactly as a stop before that write would have.
	if err := ctx.pollFor(fmt.Sprintf("job %d's first checkpoint", jobID), func() (bool, error) {
		rows, err := graphNodeRows(dbPath, jobID)
		return rows > 0, err
	}); err != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", err.Error()), nil
	}
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	if err := deleteGraphCheckpoint(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	if err := liftPrimaryHold(); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	blocked := ""
	if err := ctx.pollFor(fmt.Sprintf("job %d finished or refused", jobID), func() (bool, error) {
		if status, ok := restartJobStatusFromDB(dbPath, jobID); ok && dbStatusTerminal(normalizeDBStatus(status)) {
			return true, nil
		}
		rows, err := readAdmissionRows(dbPath, []int{jobID})
		if err != nil {
			return false, err
		}
		if row, ok := rows[jobID]; ok && strings.Contains(row.Error, "recovery blocked") {
			blocked = row.Error
			return true, nil
		}
		return false, nil
	}); err != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", err.Error()), nil
	}
	if blocked != "" {
		// Leave no active row behind for the suite's state checks.
		_ = cancelJobGraphQL(ctx.weaverURL, jobID)
		_ = waitForJobCancelSettledGraphQL(ctx.weaverURL, jobID, ctx.Timeout, chaosPollInterval)
		detail := fmt.Sprintf("the restart refused the job: %s", blocked)
		return classifyRestartResult(ctx.Profile, false, "", true, detail, detail), nil
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	size, digest, err := singleMKVSource()
	if err != nil {
		return restartCaseResult{}, err
	}
	outputOK := statuses[jobID] == "COMPLETE" && deliveredCopy(completeDirOf(dbPath, jobID), size, digest)
	detail := fmt.Sprintf("final=%s output_ok=%t", statuses[jobID], outputOK)
	return classifyRestartResult(ctx.Profile, outputOK, "a job with no checkpoint was admitted afresh and completed: "+detail, false, "", detail), nil
}

// upgradeSeed is what an upgrade case leaves in the database before the
// rewind: two history rows and two active jobs held mid-download.
type upgradeSeed struct {
	history []int
	active  []int
}

// seedUpgradeJobs completes one job, copies its history row, and stops
// Weaver with two more jobs active. A job named in `checkpointed` is left
// with its graph checkpoint written.
func (ctx *restartCaseContext) seedUpgradeJobs(checkpointed bool) (upgradeSeed, error) {
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return upgradeSeed{}, err
	}
	finished, err := ctx.submitSlug(graphCaseSlug)
	if err != nil {
		return upgradeSeed{}, err
	}
	if _, err := ctx.waitForAllTerminal([]int{finished}, ctx.Timeout); err != nil {
		return upgradeSeed{}, err
	}
	dbPath := caseDBPath(ctx)
	if err := waitForJobArchived(dbPath, finished); err != nil {
		return upgradeSeed{}, err
	}
	clone := finished + 100000
	if err := execAll(dbPath, cloneHistoryRow(finished, clone)); err != nil {
		return upgradeSeed{}, err
	}
	if err := holdPrimaryBodies(); err != nil {
		return upgradeSeed{}, err
	}
	active, err := ctx.submitSlugNTimes(graphCaseSlug, 2)
	if err != nil {
		return upgradeSeed{}, err
	}
	if checkpointed {
		for _, id := range active {
			if err := ctx.pollFor(fmt.Sprintf("job %d's first checkpoint", id), func() (bool, error) {
				rows, err := graphNodeRows(dbPath, id)
				return rows > 0, err
			}); err != nil {
				return upgradeSeed{}, err
			}
		}
	}
	if err := ctx.killWeaverForRestart(); err != nil {
		return upgradeSeed{}, err
	}
	if err := liftPrimaryHold(); err != nil {
		return upgradeSeed{}, err
	}
	return upgradeSeed{history: []int{finished, clone}, active: active}, nil
}

// judgeUpgrade restarts on the rewound database and checks what the upgrade
// owes: every migration recorded, both active jobs complete, the history
// readable through the API, and nothing left of the planted classic rows.
func (ctx *restartCaseContext) judgeUpgrade(seed upgradeSeed, from int) (restartCaseResult, error) {
	if err := ctx.restartWeaver(); err != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", "the upgraded database did not start: "+err.Error()), nil
	}
	statuses, err := ctx.waitForAllTerminal(seed.active, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	for _, id := range seed.active {
		if err := waitForJobArchived(dbPath, id); err != nil {
			return restartCaseResult{}, err
		}
	}
	versions, err := migrationVersions(dbPath)
	if err != nil {
		return restartCaseResult{}, err
	}
	var missing []int
	for version := from; version <= graphEngineMigration; version++ {
		if !versions[version] {
			missing = append(missing, version)
		}
	}
	listed, err := listJobsGraphQL(ctx.weaverURL)
	if err != nil {
		return restartCaseResult{}, err
	}
	seen := map[int]bool{}
	for _, job := range listed {
		seen[job.ID] = true
	}
	historyRead := 0
	for _, id := range seed.history {
		if seen[id] {
			historyRead++
		}
	}
	size, digest, err := singleMKVSource()
	if err != nil {
		return restartCaseResult{}, err
	}
	completed := 0
	for _, id := range seed.active {
		if statuses[id] == "COMPLETE" && deliveredCopy(completeDirOf(dbPath, id), size, digest) {
			completed++
		}
	}
	left, err := plantedRowsLeft(dbPath, seed.active)
	if err != nil {
		return restartCaseResult{}, err
	}
	leftTotal := 0
	for _, count := range left {
		leftTotal += count
	}
	detail := fmt.Sprintf("statuses=%v completed_whole=%d/2 history_listed=%d/2 missing_migrations=%v planted_rows_left=%v",
		statuses, completed, historyRead, missing, left)
	rest := completed == 2 && historyRead == 2 && leftTotal == 0
	pass := rest && len(missing) == 0
	// Until the graph-engine migration lands, it is the one version a full
	// upgrade cannot record; everything else the case owes still holds.
	gap := len(missing) == 1 && missing[0] == graphEngineMigration && completed == 2 && historyRead == 2
	return classifyRestartResult(ctx.Profile, pass, "the database upgraded and its jobs completed: "+detail,
		gap, fmt.Sprintf("migration %d is not in this build: %s", graphEngineMigration, detail), detail), nil
}

// G9a. A 0.16.5 database upgrades.
func runUpgradeFrom0165Database(ctx *restartCaseContext) (restartCaseResult, error) {
	seed, err := ctx.seedUpgradeJobs(false)
	if err != nil {
		return restartCaseResult{}, err
	}
	var plant []string
	for _, id := range seed.active {
		plant = append(plant, plantClassicRows(id)...)
	}
	if err := applySchemaRewind(caseDBPath(ctx), rewindTo0165, plant); err != nil {
		return restartCaseResult{}, fmt.Errorf("rewind to the 0.16.5 schema: %w", err)
	}
	return ctx.judgeUpgrade(seed, rewindTo0165.LedgerFrom)
}

// G9b. A database written before the graph engine became the only engine,
// with both engine stamps on active jobs and on history, upgrades.
func runUpgradeFromEngineStampedDatabase(ctx *restartCaseContext) (restartCaseResult, error) {
	seed, err := ctx.seedUpgradeJobs(true)
	if err != nil {
		return restartCaseResult{}, err
	}
	classic, graph := seed.active[0], seed.active[1]
	plant := []string{
		fmt.Sprintf(`UPDATE active_jobs SET engine = 'classic' WHERE job_id = %d`, classic),
		fmt.Sprintf(`UPDATE active_jobs SET engine = 'graph' WHERE job_id = %d`, graph),
		fmt.Sprintf(`UPDATE job_history SET engine = 'classic' WHERE job_id = %d`, seed.history[0]),
		fmt.Sprintf(`UPDATE job_history SET engine = 'graph' WHERE job_id = %d`, seed.history[1]),
	}
	// A classic job never wrote a graph checkpoint.
	for _, table := range graphTables {
		plant = append(plant, fmt.Sprintf(`DELETE FROM %s WHERE job_id = %d`, table, classic))
	}
	plant = append(plant, plantClassicRows(classic)...)
	if err := applySchemaRewind(caseDBPath(ctx), rewindToPreGraphEngine, plant); err != nil {
		return restartCaseResult{}, fmt.Errorf("rewind to the engine-stamped schema: %w", err)
	}
	return ctx.judgeUpgrade(seed, 66)
}

// G10. One row start-up cannot archive does not stop start-up.
func runUnarchivableRowDoesNotStopStartup(ctx *restartCaseContext) (restartCaseResult, error) {
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	if err := holdPrimaryBodies(); err != nil {
		return restartCaseResult{}, err
	}
	defer liftPrimaryHold()
	ids, err := ctx.submitSlugNTimes(graphCaseSlug, 2)
	if err != nil {
		return restartCaseResult{}, err
	}
	refused, healthy := ids[0], ids[1]
	if err := ctx.killWeaverForRestart(); err != nil {
		return restartCaseResult{}, err
	}
	if err := liftPrimaryHold(); err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	// A row that stopped between its terminal status and its history row is
	// archived at start; the trigger makes the database refuse that one
	// archive, as a write that cannot land would.
	refuse, lift := refuseArchive(refused)
	if err := execAll(dbPath, append([]string{
		fmt.Sprintf(`UPDATE active_jobs SET status = 'complete' WHERE job_id = %d`, refused),
	}, refuse...)); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return classifyRestartResult(ctx.Profile, false, "", false, "", "start-up stopped on the unarchivable row: "+err.Error()), nil
	}
	statuses, err := ctx.waitForAllTerminal([]int{healthy}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	logText, _ := os.ReadFile(ctx.caseLogPath)
	warnings := countLogLines(string(logText), refusedArchiveLog, fmt.Sprintf("job_id=%d", refused))
	stillActive := false
	if rows, err := readAdmissionRows(dbPath, []int{refused}); err == nil {
		_, stillActive = rows[refused]
	}
	if err := ctx.interruptWeaverAndWaitForExit(); err != nil {
		return restartCaseResult{}, err
	}
	if err := execAll(dbPath, lift); err != nil {
		return restartCaseResult{}, err
	}
	if err := ctx.restartWeaver(); err != nil {
		return restartCaseResult{}, err
	}
	if err := waitForJobArchived(dbPath, refused); err != nil {
		return restartCaseResult{}, err
	}
	size, digest, err := singleMKVSource()
	if err != nil {
		return restartCaseResult{}, err
	}
	healthyOK := statuses[healthy] == "COMPLETE" && deliveredCopy(completeDirOf(dbPath, healthy), size, digest)
	detail := fmt.Sprintf("warnings_for_refused_row=%d refused_row_stayed_active=%t healthy_final=%s healthy_output_ok=%t archived_on_next_start=true",
		warnings, stillActive, statuses[healthy], healthyOK)
	pass := warnings == 1 && stillActive && healthyOK
	return classifyRestartResult(ctx.Profile, pass, "start-up warned once, kept the row and went on: "+detail, false, "", detail), nil
}

// G11. A RAR4 trial key that fails the whole-file checksum is condemned and
// the next candidate opens the bytes already held.
func runRAR4TrialKeyCondemnedOnWholeFileChecksum(ctx *restartCaseContext) (restartCaseResult, error) {
	scenario := ctx.Scenarios[rar4TrialKeySlug]
	if scenario == nil || scenario.Password == "" {
		return restartCaseResult{}, fmt.Errorf("%s carries no password", rar4TrialKeySlug)
	}
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	if err := setArchivePasswordsGraphQL(ctx.weaverURL, []string{"e2e-wrong-trial-key", scenario.Password}); err != nil {
		return restartCaseResult{}, err
	}
	if err := resetNntpMetrics(); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitScenarioWithoutPassword(rar4TrialKeySlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	metrics, err := fetchNntpBodyMetrics("")
	if err != nil {
		return restartCaseResult{}, err
	}
	digestErr := assertOutputBLAKE3(dbPath, jobID, scenario.ExpectedOutputBLAKE3)
	detail := fmt.Sprintf("final=%s max_fetches_per_article=%d output=%v", statuses[jobID], maxBodyFetchCount(metrics), digestErr)
	pass := statuses[jobID] == "COMPLETE" && digestErr == nil && maxBodyFetchCount(metrics) == 1
	return classifyRestartResult(ctx.Profile, pass, "the wrong trial key was condemned and the right one opened the held bytes: "+detail, false, "", detail), nil
}

// G5b. A zip's directories and names are delivered as its entries declare.
func runZipDirectoriesAndCP437NamesDelivered(ctx *restartCaseContext) (restartCaseResult, error) {
	scenario := ctx.Scenarios[zipDirectorySlug]
	if err := ctx.startWeaverWithOptions("", 8); err != nil {
		return restartCaseResult{}, err
	}
	jobID, err := ctx.submitSlug(zipDirectorySlug)
	if err != nil {
		return restartCaseResult{}, err
	}
	statuses, err := ctx.waitForAllTerminal([]int{jobID}, ctx.Timeout)
	if err != nil {
		return restartCaseResult{}, err
	}
	dbPath := caseDBPath(ctx)
	if err := waitForJobArchived(dbPath, jobID); err != nil {
		return restartCaseResult{}, err
	}
	outputDir, err := historyOutputDir(dbPath, jobID)
	if err != nil {
		return restartCaseResult{}, err
	}
	var missingDirs []string
	for _, dir := range zipDeclaredDirectories {
		if info, err := os.Stat(filepath.Join(outputDir, filepath.FromSlash(dir))); err != nil || !info.IsDir() {
			missingDirs = append(missingDirs, dir)
		}
	}
	digestErr := assertOutputBLAKE3(dbPath, jobID, scenario.ExpectedOutputBLAKE3)
	delivered, _ := listOutputFiles(outputDir)
	sort.Strings(delivered)
	detail := fmt.Sprintf("final=%s missing_dirs=%v names=%v delivered=%v", statuses[jobID], missingDirs, digestErr, delivered)
	pass := statuses[jobID] == "COMPLETE" && len(missingDirs) == 0 && digestErr == nil
	return classifyRestartResult(ctx.Profile, pass, "the zip's directories and names came out as declared: "+detail, false, "", detail), nil
}

// zipDeclaredDirectories are the directory records zip-cp437-empty-dirs
// carries; Extras/Empty and Empty hold nothing.
var zipDeclaredDirectories = []string{"Extras", "Extras/Empty", "Subs", "Empty"}

// completeDirOf is the job's delivered directory once it is archived, and
// the whole complete root before then.
func completeDirOf(dbPath string, jobID int) string {
	if err := waitForJobArchived(dbPath, jobID); err == nil {
		if dir, err := historyOutputDir(dbPath, jobID); err == nil {
			return dir
		}
	}
	return filepath.Join(localWeaverDir(), "complete")
}
