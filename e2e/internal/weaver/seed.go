package weaver

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// --- seed ---

func cmdSeed(dir string) {
	absDir := resolveRepoPath(dir)
	// Fixtures before infrastructure: a missing payload should fail (or be
	// fetched or generated) before any container is started for it.
	ensureFixtureDir(absDir)
	ensureSeedingInfrastructure()
	scenario, err := loadScenario(absDir)
	if err != nil {
		log.Fatal(err)
	}
	if err := seedFixture(absDir); err != nil {
		log.Fatal(err)
	}
	if scenarioNeedsBackupServerState(scenario) {
		if err := ensureBackupNntpReady(); err != nil {
			log.Fatal(err)
		}
		if scenarioHasBackupFixtureOverride(scenario) {
			if err := seedScenarioBackupOverride(absDir, scenario); err != nil {
				log.Fatal(err)
			}
		}
		if err := applyPrimarySeedMutations(scenario); err != nil {
			log.Fatal(err)
		}
	}
}

func seedFixture(dir string) error {
	return seedFixtureWithRetry(dir, envInt("E2E_SEED_RETRIES", 3))
}

func seedFixtureWithRetry(dir string, attempts int) error {
	absDir := resolveRepoPath(dir)
	ensureFixtureDir(absDir)

	scenario, err := loadScenario(absDir)
	if err != nil {
		return fmt.Errorf("load scenario from %s: %w", absDir, err)
	}

	if attempts < 1 {
		attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		lastErr = seedScenarioRelease(absDir, scenario)
		if lastErr == nil {
			return nil
		}
		if attempt == attempts || !isTransientSeedError(lastErr) {
			return lastErr
		}

		log.Printf("[%s] transient seed failure on attempt %d/%d: %v", scenario.Slug, attempt, attempts, lastErr)
		waitForTCP(nntpHost()+":"+nntpPort(), 30*time.Second)
	}

	return lastErr
}

func isTransientSeedError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	transientMarkers := []string{
		"NNTP connection failed",
		"connect ECONNREFUSED",
		"connect: connection refused",
		"connect: EOF",
		"read greeting",
		"read response",
		"write command",
		"unexpected greeting",
		"empty response",
	}
	for _, marker := range transientMarkers {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func seedScenarioRelease(absDir string, scenario *Scenario) error {
	logSeed := func(format string, args ...interface{}) {
		log.Printf("[%s] %s", scenario.Slug, fmt.Sprintf(format, args...))
	}

	// A uu scenario's articles are already encoded; they are posted from the
	// corpus rather than handed to nyuu, which only speaks yEnc. A scenario
	// may have both kinds, one kind, or — for a pure uu release — nothing at
	// all for nyuu to stage.
	uuPlan, err := loadUUPlan(absDir)
	if err != nil {
		return fmt.Errorf("load uu posting plan for %s: %w", scenario.Slug, err)
	}
	stagesForNyuu, err := scenarioStagesPostableFiles(absDir, scenario)
	if err != nil {
		return fmt.Errorf("inspect staged files in %s: %w", absDir, err)
	}
	if !stagesForNyuu && uuPlan == nil {
		return fmt.Errorf("fixture %s has no staged files and no uu posting plan", absDir)
	}

	var (
		stageDir     string
		files        []string
		totalBytes   int64
		cleanupStage = func() {}
	)
	if stagesForNyuu {
		stageDir, files, totalBytes, cleanupStage, err = prepareFixtureStaging(absDir, scenario)
		if err != nil {
			return fmt.Errorf("prepare staged files in %s: %w", absDir, err)
		}
	}
	defer cleanupStage()

	uuFileCount := 0
	if uuPlan != nil {
		uuFileCount = len(uuPlan.Files)
	}
	logSeed("seeding %d yEnc file(s) and %d uuencoded file(s), title=%s", len(files), uuFileCount, scenario.Title)

	if err := purgeSeededArticles(scenario.Slug); err != nil {
		return fmt.Errorf("purge existing articles for %s: %w", scenario.Slug, err)
	}

	nzbDir := filepath.Join(fixturesDir(), scenario.Slug)
	if err := os.MkdirAll(nzbDir, 0o755); err != nil {
		return fmt.Errorf("create generated NZB dir for %s: %w", scenario.Slug, err)
	}
	nzbPath := filepath.Join(nzbDir, scenario.Slug+".nzb")
	existingDates, err := readNZBDateAttributes(nzbPath)
	if err != nil {
		return fmt.Errorf("read existing NZB dates for %s: %w", scenario.Slug, err)
	}
	if err := os.Remove(nzbPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reset generated NZB for %s: %w", scenario.Slug, err)
	}
	if stagesForNyuu {
		stageDirInNyuu, err := nyuuContainerPathForHost(stageDir)
		if err != nil {
			return fmt.Errorf("resolve nyuu stage path for %s: %w", scenario.Slug, err)
		}
		nzbPathInNyuu, err := nyuuContainerPathForHost(nzbPath)
		if err != nil {
			return fmt.Errorf("resolve nyuu nzb path for %s: %w", scenario.Slug, err)
		}

		if err := runNyuuPost(stageDirInNyuu, files, scenario, "127.0.0.1", "119", nzbPathInNyuu); err != nil {
			return fmt.Errorf("nyuu failed for %s: %w", scenario.Slug, err)
		}
	}

	if uuPlan != nil {
		elements, uuBytes, err := seedUUArticles(nntpHost(), nntpPort(), scenario, absDir, uuPlan)
		if err != nil {
			return fmt.Errorf("post uu articles for %s: %w", scenario.Slug, err)
		}
		totalBytes += uuBytes
		if stagesForNyuu {
			err = spliceUUFilesIntoNZB(nzbPath, elements)
		} else {
			err = writeUUNZB(nzbPath, scenario, elements)
		}
		if err != nil {
			return fmt.Errorf("write uu NZB entries for %s: %w", scenario.Slug, err)
		}
	}

	if err := normalizeNZBDateAttributes(nzbPath, existingDates); err != nil {
		return fmt.Errorf("normalize NZB dates for %s: %w", scenario.Slug, err)
	}

	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		return fmt.Errorf("read NZB for %s: %w", scenario.Slug, err)
	}
	segmentNumbers, err := scenarioNZBSegmentNumbers(nzbData, scenario)
	if err != nil {
		return fmt.Errorf("build NZB segment numbers for %s: %w", scenario.Slug, err)
	}
	changedNZB := false
	if len(segmentNumbers) > 0 {
		nzbData, err = rewriteNZBSegmentNumbers(nzbData, segmentNumbers)
		if err != nil {
			return fmt.Errorf("rewrite NZB segment numbers for %s: %w", scenario.Slug, err)
		}
		changedNZB = true
	}
	if len(scenario.NZBSubjectFilenameOverrides) > 0 {
		nzbData, err = rewriteNZBSubjectFilenames(nzbData, scenario.NZBSubjectFilenameOverrides)
		if err != nil {
			return fmt.Errorf("rewrite NZB subject filenames for %s: %w", scenario.Slug, err)
		}
		changedNZB = true
	}
	if changedNZB {
		if err := os.WriteFile(nzbPath, nzbData, 0o644); err != nil {
			return fmt.Errorf("persist rewritten NZB for %s: %w", scenario.Slug, err)
		}
	}

	if len(scenario.DeleteSubjectContains) > 0 {
		messageIDs, err := extractMessageIDsBySubjectContains(nzbData, scenario.DeleteSubjectContains, scenario.DeleteSubjectTailArticles)
		if err != nil {
			return fmt.Errorf("extract targeted delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no NZB segments matched deleteSubjectContains=%v for %s",
				scenario.DeleteSubjectContains,
				scenario.Slug,
			)
		}
		logSeed(
			"deleting %d article(s) matching subject filters %v...",
			len(messageIDs),
			scenario.DeleteSubjectContains,
		)
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete targeted articles for %s: %w", scenario.Slug, err)
		}
	}

	if len(scenario.DeleteSegmentNumbers) > 0 {
		messageIDs, err := extractMessageIDsBySegmentNumbers(nzbData, segmentDeleteNeedles(scenario), scenario.DeleteSegmentNumbers)
		if err != nil {
			return fmt.Errorf("extract segment-number delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no NZB segments matched deleteSegmentNumbers=%v for %s",
				scenario.DeleteSegmentNumbers,
				scenario.Slug,
			)
		}
		logSeed("deleting %d article(s) at segment number(s) %v...", len(messageIDs), scenario.DeleteSegmentNumbers)
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete segment-number articles for %s: %w", scenario.Slug, err)
		}
	}

	if scenario.DeleteFirstProbeSampleHits > 0 {
		messageIDs, err := extractFirstProbeSampleMessageIDs(nzbData, scenario.DeleteFirstProbeSampleHits)
		if err != nil {
			return fmt.Errorf("extract probe-sample delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no probe-sample message ids available for %s (requested %d)",
				scenario.Slug,
				scenario.DeleteFirstProbeSampleHits,
			)
		}
		logSeed("deleting %d first-round probe sample article(s)...", len(messageIDs))
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete probe-sample articles for %s: %w", scenario.Slug, err)
		}
	}

	if scenario.DeleteFirstMessageIDs > 0 {
		messageIDs, err := extractFirstMessageIDs(nzbData, scenario.DeleteFirstMessageIDs)
		if err != nil {
			return fmt.Errorf("extract first-message delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no leading message ids available for %s (requested %d)",
				scenario.Slug,
				scenario.DeleteFirstMessageIDs,
			)
		}
		logSeed("deleting %d leading article(s)...", len(messageIDs))
		if err := deleteArticlesByMessageID(messageIDs); err != nil {
			return fmt.Errorf("delete leading articles for %s: %w", scenario.Slug, err)
		}
	}

	// For health-failure scenarios, delete a percentage of articles from NNTP
	// to simulate missing segments.
	if scenario.SkipArticlesPct > 0 {
		logSeed("deleting %d%% of articles...", scenario.SkipArticlesPct)
		if err := deleteArticles(scenario.Slug, scenario.SkipArticlesPct); err != nil {
			logSeed("WARNING: delete articles failed: %v", err)
		}
	}

	logSeed("NZB generated: %d bytes", len(nzbData))

	logSeed("done")
	return nil
}

func scenarioHasBackupFixtureOverride(scenario *Scenario) bool {
	return scenario != nil && len(scenario.BackupFixtureAssets) > 0
}

func scenarioNeedsBackupServerState(scenario *Scenario) bool {
	if scenario == nil {
		return false
	}
	return scenarioHasBackupFixtureOverride(scenario) ||
		scenario.PrimaryDeleteFirstMessageIDs > 0 ||
		len(scenario.PrimaryDeleteSubjectContains) > 0 ||
		strings.TrimSpace(scenario.PrimaryChaosConfig) != "" ||
		strings.TrimSpace(scenario.BackupUnavailableUntilFileComplete) != ""
}

// segmentDeleteNeedles narrows which files the segment-number deletion may
// reach. It is its own field because deleteSubjectContains deletes whole files
// wherever it is set: a scenario that wants one interior article of one named
// file — and every other article of that file kept — cannot express that by
// combining the two. With no needles of its own it falls back to the whole-file
// filter, which is the shape the scenarios that predate this field rely on.
func segmentDeleteNeedles(scenario *Scenario) []string {
	if scenario == nil {
		return nil
	}
	if len(scenario.DeleteSegmentSubjectContains) > 0 {
		return scenario.DeleteSegmentSubjectContains
	}
	return scenario.DeleteSubjectContains
}

func scenarioUsesExclusiveNntpState(scenario *Scenario) bool {
	return scenario != nil && (strings.TrimSpace(scenario.PrimaryChaosConfig) != "" ||
		strings.TrimSpace(scenario.BackupUnavailableUntilFileComplete) != "" ||
		scenario.queueLivenessAssertion() != nil)
}

func applyBackupFixtureOverridesForSlugs(slugs []string) error {
	for _, slug := range slugs {
		absDir := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(absDir)
		if err != nil {
			return fmt.Errorf("load scenario from %s: %w", absDir, err)
		}
		if !scenarioHasBackupFixtureOverride(scenario) {
			continue
		}
		if err := seedScenarioBackupOverride(absDir, scenario); err != nil {
			return err
		}
	}
	return nil
}

func seedScenarioBackupOverride(absDir string, scenario *Scenario) error {
	logSeed := func(format string, args ...interface{}) {
		log.Printf("[%s] %s", scenario.Slug, fmt.Sprintf(format, args...))
	}

	stageDir, files, _, cleanupStage, err := prepareBackupFixtureStaging(absDir, scenario)
	if err != nil {
		return fmt.Errorf("prepare backup staged files in %s: %w", absDir, err)
	}
	defer cleanupStage()

	if err := deleteArticlesAt(nntpHost(), backupNntpPort(), scenario.Slug, 100); err != nil {
		return fmt.Errorf("purge backup articles for %s: %w", scenario.Slug, err)
	}

	nzbPath := filepath.Join(stageDir, scenario.Slug+"-backup.nzb")
	nzbPathInNyuu, err := nyuuContainerPathForHost(nzbPath)
	if err != nil {
		return fmt.Errorf("resolve backup nyuu nzb path for %s: %w", scenario.Slug, err)
	}
	stageDirInNyuu, err := nyuuContainerPathForHost(stageDir)
	if err != nil {
		return fmt.Errorf("resolve backup nyuu stage path for %s: %w", scenario.Slug, err)
	}

	logSeed("posting backup override (%d file(s))...", len(files))
	if err := runNyuuPost(stageDirInNyuu, files, scenario, nyuuBackupHost(), nyuuBackupPort(), nzbPathInNyuu); err != nil {
		return fmt.Errorf("post backup override for %s: %w", scenario.Slug, err)
	}

	logSeed("backup override ready")
	return nil
}

func applyPrimarySeedMutationsForSlugs(slugs []string) error {
	for _, slug := range slugs {
		absDir := filepath.Join(testdataDir(), slug)
		scenario, err := loadScenario(absDir)
		if err != nil {
			return fmt.Errorf("load scenario from %s: %w", absDir, err)
		}
		if err := applyPrimarySeedMutations(scenario); err != nil {
			return err
		}
	}
	return nil
}

func applyPrimarySeedMutations(scenario *Scenario) error {
	if scenario == nil {
		return nil
	}
	if scenario.PrimaryDeleteFirstMessageIDs <= 0 && len(scenario.PrimaryDeleteSubjectContains) == 0 {
		return nil
	}

	nzbPath := filepath.Join(fixturesDir(), scenario.Slug, scenario.Slug+".nzb")
	nzbData, err := os.ReadFile(nzbPath)
	if err != nil {
		return fmt.Errorf("read NZB for primary-only seed mutations on %s: %w", scenario.Slug, err)
	}

	logSeed := func(format string, args ...interface{}) {
		log.Printf("[%s] %s", scenario.Slug, fmt.Sprintf(format, args...))
	}

	if scenario.PrimaryDeleteFirstMessageIDs > 0 {
		messageIDs, err := extractFirstMessageIDs(nzbData, scenario.PrimaryDeleteFirstMessageIDs)
		if err != nil {
			return fmt.Errorf("extract primary-only leading delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no primary-only leading message ids available for %s (requested %d)",
				scenario.Slug,
				scenario.PrimaryDeleteFirstMessageIDs,
			)
		}
		logSeed("deleting %d leading article(s) from primary only...", len(messageIDs))
		if err := deleteArticlesByMessageIDOnServers(messageIDs, true, false); err != nil {
			return fmt.Errorf("delete primary-only leading articles for %s: %w", scenario.Slug, err)
		}
	}

	if len(scenario.PrimaryDeleteSubjectContains) > 0 {
		messageIDs, err := extractMessageIDsBySubjectContains(nzbData, scenario.PrimaryDeleteSubjectContains, 0)
		if err != nil {
			return fmt.Errorf("extract primary-only subject delete ids for %s: %w", scenario.Slug, err)
		}
		if len(messageIDs) == 0 {
			return fmt.Errorf(
				"no NZB segments matched primaryDeleteSubjectContains=%v for %s",
				scenario.PrimaryDeleteSubjectContains,
				scenario.Slug,
			)
		}
		logSeed(
			"deleting %d article(s) from primary only matching subject filters %v...",
			len(messageIDs),
			scenario.PrimaryDeleteSubjectContains,
		)
		if err := deleteArticlesByMessageIDOnServers(messageIDs, true, false); err != nil {
			return fmt.Errorf("delete primary-only subject articles for %s: %w", scenario.Slug, err)
		}
	}

	return nil
}

func runNyuuPost(
	stageDirInNyuu string,
	files []string,
	scenario *Scenario,
	host string,
	port string,
	nzbPathInNyuu string,
) error {
	nyuuArgs := append(
		dockerComposeArgs(
			"exec",
			"-T",
			"nyuu",
			"/opt/nyuu/node_modules/.bin/nyuu",
		),
		"-h", host, "-P", port, "--ssl=false",
		"-u", nntpUsername(),
		"-p", nntpPassword(),
		"-n", "1",
		"-g", "alt.binaries.test",
		"-f", "e2e-test@example.invalid",
		"--keep-message-id",
		"--message-id", fmt.Sprintf("e2e-%s-{0filenum}-{0part}@e2e-test", scenario.Slug),
		"-o", nzbPathInNyuu,
		"-O",
		"--check-connections", "0",
		"--skip-errors", "post-reject",
	)

	if scenario.Password != "" {
		nyuuArgs = append(nyuuArgs, "--nzb-password", scenario.Password)
	}

	nyuuArgs = append(nyuuArgs, "-M", "name="+scenario.Title)
	nyuuArgs = append(nyuuArgs, "-M", "category="+scenario.Category)

	segSize := "750K"
	if scenario.SegmentSize > 0 {
		segSize = fmt.Sprintf("%d", scenario.SegmentSize)
	}
	nyuuArgs = append(nyuuArgs, "-a", segSize)

	for _, f := range files {
		nyuuArgs = append(nyuuArgs, strings.TrimRight(stageDirInNyuu, "/")+"/"+f)
	}

	cmd := exec.Command("docker", nyuuArgs...)
	cmd.Dir = e2eDir()
	return runExternalCommand(cmd, "nyuu post")
}

// --- seed-all ---

func seedAllForProfile(profile string) {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		profile = "functional"
	}
	slugs := fixtureSlugsForSeedProfile(profile)
	dirs := fixtureDirsForSlugs(slugs)

	if len(dirs) == 0 {
		log.Fatalf("no fixtures configured for seed profile %q", profile)
	}

	ensureFixtureProfiles(profile)
	var seedImages nntpSeedImageSet
	if nntpSeedImageCacheEnabled() {
		var err error
		seedImages, err = nntpSeedImageSetForProfile(profile, slugs)
		if err != nil {
			log.Fatalf("fingerprint pre-seeded NNTP images for profile %s: %v", profile, err)
		}
		if seedImages.ready() {
			emitProgressEvent(progressEvent{Kind: "seed_total", Total: 1, Detail: "pre-seeded runtime"})
			// The article stores are in the image, but the test commands submit
			// host-side NZB files. Restore the complete profile in one copy rather
			// than replaying one copy per fixture.
			if err := restoreSeededNZBBundle(seedImages.Primary, fixturesDir()); err != nil {
				emitProgressEvent(progressEvent{Kind: "seed_done", Current: 1, Total: 1, Status: "fail"})
				log.Fatalf("restore pre-seeded NZBs for profile %s: %v", profile, err)
			}
			if err := restoreSeedImageCache(seedImages); err != nil {
				emitProgressEvent(progressEvent{Kind: "seed_done", Current: 1, Total: 1, Status: "fail"})
				log.Fatalf("restore pre-seeded NNTP images for profile %s: %v", profile, err)
			}
			emitProgressEvent(progressEvent{Kind: "seed_progress", Current: 1, Total: 1, Status: "pass", Detail: "pre-seeded runtime ready"})
			emitProgressEvent(progressEvent{Kind: "seed_done", Current: 1, Total: 1, Status: "pass"})
			return
		}
	}
	ensureSeedingInfrastructure()
	emitProgressEvent(progressEvent{Kind: "seed_total", Total: len(dirs), Detail: "fixtures"})

	workerCount := envInt("E2E_SEED_JOBS", 4)
	if workerCount < 1 {
		log.Fatalf("invalid E2E_SEED_JOBS=%d (expected >= 1)", workerCount)
	}
	if workerCount > len(dirs) {
		workerCount = len(dirs)
	}

	log.Printf("seeding %d fixtures with %d worker(s) for profile=%s...", len(dirs), workerCount, profile)

	type seedJob struct {
		index int
		dir   string
	}
	type seedResult struct {
		index int
		name  string
		err   error
	}

	jobs := make(chan seedJob)
	results := make(chan seedResult, len(dirs))
	var wg sync.WaitGroup

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				name := filepath.Base(job.dir)
				log.Printf("[%d/%d] %s queued", job.index+1, len(dirs), name)

				var resultErr error
				func() {
					defer func() {
						if r := recover(); r != nil {
							resultErr = fmt.Errorf("panic while seeding %s: %v", name, r)
						}
					}()
					resultErr = seedFixture(job.dir)
				}()

				results <- seedResult{index: job.index, name: name, err: resultErr}
			}
		}()
	}

	go func() {
		for i, dir := range dirs {
			jobs <- seedJob{index: i, dir: dir}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	passed, failed := 0, 0
	var failedNames []string
	for result := range results {
		if result.err != nil {
			failed++
			failedNames = append(failedNames, result.name)
			log.Printf("[%d/%d] %s FAILED: %v", result.index+1, len(dirs), result.name, result.err)
			emitProgressEvent(progressEvent{
				Kind:    "seed_progress",
				Current: passed + failed,
				Total:   len(dirs),
				Status:  "fail",
				Detail:  result.name,
			})
			continue
		}
		passed++
		log.Printf("[%d/%d] %s PASS", result.index+1, len(dirs), result.name)
		emitProgressEvent(progressEvent{
			Kind:    "seed_progress",
			Current: passed + failed,
			Total:   len(dirs),
			Status:  "pass",
			Detail:  result.name,
		})
	}

	log.Printf("seed-all complete: %d passed, %d failed out of %d", passed, failed, len(dirs))
	if len(failedNames) > 0 {
		sort.Strings(failedNames)
		log.Printf("seed failures: %s", strings.Join(failedNames, ", "))
	}
	if failed > 0 {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		os.Exit(1)
	}
	if err := ensureBackupNntpReady(); err != nil {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		log.Fatalf("prepare backup NNTP after seed-all: %v", err)
	}
	if err := applyBackupFixtureOverridesForSlugs(slugs); err != nil {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		log.Fatalf("apply backup fixture overrides after seed-all: %v", err)
	}
	if err := applyPrimarySeedMutationsForSlugs(slugs); err != nil {
		emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "fail"})
		log.Fatalf("apply primary-only fixture mutations after seed-all: %v", err)
	}
	emitProgressEvent(progressEvent{Kind: "seed_done", Current: len(dirs), Total: len(dirs), Status: "pass"})
	if nntpSeedImageCaptureEnabled() {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := captureSeedImageCache(ctx, seedImages, slugs, nntpSeedCacheCaptureConfig{
			Project:   composeProject(),
			StageRoot: os.TempDir(),
			LockRoot:  os.TempDir(),
			OwnerPID:  os.Getpid(),
		}); err != nil {
			// The completed seed remains valid for this phase. The image cache is
			// a local acceleration layer, so a disk or Docker image-build problem
			// must not turn a correctly seeded E2E phase into a false failure.
			log.Printf("warning: pre-seed NNTP image cache unavailable for profile=%s: %v", profile, err)
		}
	}
}

func cmdSeedAll() {
	seedAllForProfile(env("E2E_SEED_PROFILE", "functional"))
}

// syncArticlesToBackup streams all articles from nntp to nntp2 through Docker.
// It is a no-op when nntp2 is not running.
func syncArticlesToBackup() error {
	if !dockerContainerRunning("nntp2") {
		return nil
	}

	sourceID, err := dockerComposeServiceContainerID("nntp")
	if err != nil {
		return fmt.Errorf("resolve primary NNTP container: %w", err)
	}
	backupID, err := dockerComposeServiceContainerID("nntp2")
	if err != nil {
		return fmt.Errorf("resolve backup NNTP container: %w", err)
	}

	log.Printf("syncing articles to backup NNTP server (nntp2)...")
	if err := streamArticlesToBackup(sourceID, backupID); err != nil {
		return err
	}
	// Tell nntp2 to reload its index
	resp := sendNntpCommandTo(nntpHost(), backupNntpPort(), "RELOAD")
	if resp != "" {
		log.Printf("  nntp2 reload: %s", resp)
	}
	log.Printf("  backup NNTP server synced")
	return nil
}

// streamArticlesToBackup avoids materializing the complete fixture corpus on
// the host. The previous docker cp -> temporary directory -> docker cp path
// doubled disk I/O and left both functional datastores contending for it.
func streamArticlesToBackup(sourceID, backupID string) error {
	source, destination := articleSyncCommands(sourceID, backupID)

	var sourceStderr, destinationStderr bytes.Buffer
	source.Stderr = &sourceStderr
	destination.Stderr = &destinationStderr
	archive, err := source.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open primary NNTP article stream: %w", err)
	}
	destination.Stdin = archive
	if err := destination.Start(); err != nil {
		return fmt.Errorf("start backup NNTP article stream: %w", err)
	}
	if err := source.Start(); err != nil {
		_ = destination.Process.Kill()
		_ = destination.Wait()
		return fmt.Errorf("start primary NNTP article stream: %w", err)
	}

	if err := source.Wait(); err != nil {
		_ = destination.Wait()
		return fmt.Errorf("stream primary NNTP articles: %w: %s", err, strings.TrimSpace(sourceStderr.String()))
	}
	if err := destination.Wait(); err != nil {
		return fmt.Errorf("extract backup NNTP articles: %w: %s", err, strings.TrimSpace(destinationStderr.String()))
	}
	return nil
}

// articleSyncCommands streams Docker's archive protocol directly between
// containers. Unlike `docker exec ... tar`, this does not require tar inside
// the intentionally minimal e2e-nntp image.
func articleSyncCommands(sourceID, backupID string) (*exec.Cmd, *exec.Cmd) {
	source := exec.Command("docker", "cp", sourceID+":/data/articles/.", "-")
	destination := exec.Command("docker", "cp", "-", backupID+":/data/articles")
	return source, destination
}
