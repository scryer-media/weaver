// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// The adversarial lane posts hostile and lying archives through the fake NNTP
// server and holds Weaver to one contract for each of them: the job settles
// with the verdict its scenario names, nothing is written outside the job's
// output directory, the process stays up, the job's scratch is released, the
// articles are not fetched again once the verdict stands, and an ordinary job
// submitted next completes with no orphan rows left behind.
//
// A scenario joins the lane by carrying an "adversarial" object in its
// scenario.json; that object is the case's whole contract. The cases run one
// at a time, so every observation taken between a job's submission and its
// settling belongs to that job alone.

// adversarialCleanupSlug is the ordinary job submitted after every hostile
// one. It completing is the proof the hostile job left the stack usable.
const adversarialCleanupSlug = "single-mkv"

// adversarialListPasswords is the archive password list the lane configures.
// None of them opens the encrypted fixtures, so a password case is refused
// with the list tried as well as the job's own password.
var adversarialListPasswords = []string{"adversarial-list-first", "adversarial-list-second"}

// adversarialPollInterval paces the lane's reads of product state. Every wait
// ends on that state; the interval is only how often it is read.
const adversarialPollInterval = 500 * time.Millisecond

// adversarialSlack is what a settled job may still hold in scratch beyond what
// its contract allows: directory entries, markers and the job's own state.
const adversarialSlack = 1 << 20

// adversarialSpec is the "adversarial" object of a scenario.json.
type adversarialSpec struct {
	// Group names the extraction caps the case runs under; see
	// adversarialGroups.
	Group string `json:"group"`
	// Outcome is success, failed, either (success or failed, the contract of
	// a lie the product may see through or refuse), or settles (any terminal
	// status, a failure naming its verdict).
	Outcome string `json:"outcome"`
	// ErrorContains is a substring of the failed job's history error.
	ErrorContains string `json:"errorContains,omitempty"`
	// FailureKind is the failed job's history failure_kind.
	FailureKind string `json:"failureKind,omitempty"`
	// BudgetRefusals is how many times the extraction budget warns that it
	// refused the job: one refusal, said once.
	BudgetRefusals int `json:"budgetRefusals,omitempty"`
	// RejectionMetric is the reason label of
	// weaver_extraction_rejections_total that must move by one.
	RejectionMetric string `json:"rejectionMetric,omitempty"`
	// ExtractsNothing demands an empty output directory and scratch holding
	// no more than what was posted.
	ExtractsNothing bool `json:"extractsNothing,omitempty"`
	// EscapeNames are member names as the archive spells them. Wherever one
	// would land outside a directory Weaver writes into, nothing may exist,
	// and no file of its base name may appear outside the output directory.
	EscapeNames []string `json:"escapeNames,omitempty"`
	// EscapePaths are absolute paths that must not exist after the job.
	EscapePaths []string `json:"escapePaths,omitempty"`
	// MinOutputFiles is the fewest files a completed job delivers.
	MinOutputFiles int `json:"minOutputFiles,omitempty"`
	// RequiredOutputs are output paths a completed job delivers.
	RequiredOutputs []string `json:"requiredOutputs,omitempty"`
	// RequiredOutputSuffixes each name the suffix of at least one output.
	RequiredOutputSuffixes []string `json:"requiredOutputSuffixes,omitempty"`
	// ForbiddenOutputs are base names no output may carry.
	ForbiddenOutputs []string `json:"forbiddenOutputs,omitempty"`
	// Submissions post the NZB once each with the given password; without
	// any it is posted once with the scenario's own.
	Submissions []adversarialSubmission `json:"submissions,omitempty"`
}

type adversarialSubmission struct {
	Label    string `json:"label"`
	Password string `json:"password"`
}

type adversarialCase struct {
	Scenario *Scenario
	Spec     adversarialSpec
}

// adversarialGroup is a set of extraction caps one Weaver process runs under.
// The env is resolved when the group starts, so a cap measured from the host
// (the disk reserve) is measured on the disk the run uses.
type adversarialGroup struct {
	Name string
	Env  func() (map[string]string, error)
}

func adversarialGroups() []adversarialGroup {
	return []adversarialGroup{
		{Name: "default", Env: func() (map[string]string, error) { return nil, nil }},
		{Name: "tight", Env: func() (map[string]string, error) {
			return map[string]string{
				"WEAVER_EXTRACTION_MAX_MEMBER_BYTES": strconv.Itoa(32 << 20),
				"WEAVER_EXTRACTION_MAX_ENTRIES":      "1000",
			}, nil
		}},
		{Name: "reserve", Env: adversarialReserveEnv},
	}
}

// adversarialReserveEnv leaves a gigabyte between the free space Weaver sees
// and the reserve it must keep, so a few gigabytes of expansion cross the
// reserve long before they fill the disk. The ratio floor is raised past the
// bomb's expansion so the reserve, not the ratio, is what refuses it.
func adversarialReserveEnv() (map[string]string, error) {
	intermediate := filepath.Join(localWeaverDir(), "intermediate")
	if err := os.MkdirAll(intermediate, 0o755); err != nil {
		return nil, err
	}
	free, err := adversarialFreeBytes(intermediate)
	if err != nil {
		return nil, fmt.Errorf("read free space under %s: %w", intermediate, err)
	}
	const headroom = 1 << 30
	if free <= 2*headroom {
		return nil, fmt.Errorf("%s has %d bytes free; the reserve case needs more than %d", intermediate, free, 2*headroom)
	}
	return map[string]string{
		"WEAVER_EXTRACTION_RATIO_FLOOR_BYTES": strconv.FormatUint(16<<30, 10),
		"WEAVER_EXTRACTION_MIN_FREE_BYTES":    strconv.FormatUint(free-headroom, 10),
	}, nil
}

func cmdAdversarial(args []string) {
	if currentWeaverDatastore() != weaverDatastoreSQLite {
		log.Fatalf("the adversarial lane runs on SQLite only; unset %s or set it to sqlite", weaverDatastoreEnv)
	}
	if os.Getenv(adversarialBoundedEnv) == "" {
		if err := runAdversarialBounded(args); err != nil {
			log.Fatal(err)
		}
		return
	}
	cases, err := loadAdversarialCases(testdataDir())
	if err != nil {
		log.Fatalf("load adversarial scenarios: %v", err)
	}
	cases, err = selectAdversarialCases(cases, args)
	if err != nil {
		log.Fatal(err)
	}
	cleanup, err := loadScenario(filepath.Join(testdataDir(), adversarialCleanupSlug))
	if err != nil {
		log.Fatalf("load cleanup scenario %s: %v", adversarialCleanupSlug, err)
	}

	ensureFixtureProfiles("adversarial")
	ensureSeedingInfrastructure()
	for _, slug := range append(adversarialCaseSlugs(cases), adversarialCleanupSlug) {
		log.Printf("seeding %s", slug)
		if err := seedFixture(filepath.Join(testdataDir(), slug)); err != nil {
			log.Fatalf("seed %s: %v", slug, err)
		}
	}
	ensureStandardDockerInfrastructure()

	emitProgressEvent(progressEvent{Kind: "phase_total", Total: len(cases), Detail: "adversarial archives"})
	failures := 0
	done := 0
	for _, group := range adversarialGroups() {
		members := adversarialCasesInGroup(cases, group.Name)
		if len(members) == 0 {
			continue
		}
		weaverURL, restore, err := startAdversarialGroup(group)
		if err != nil {
			log.Fatalf("start Weaver for the %s group: %v", group.Name, err)
		}
		for _, tc := range members {
			done++
			emitProgressEvent(progressEvent{Kind: "phase_note", Detail: tc.Scenario.Slug})
			results := runAdversarialCase(weaverURL, tc, cleanup)
			status := "complete"
			for _, result := range results {
				if result.err != nil {
					failures++
					status = "fail"
					fmt.Printf("  FAIL %-40s %-8s job %d: %v\n", tc.Scenario.Slug, result.label, result.jobID, result.err)
				} else {
					fmt.Printf("  PASS %-40s %-8s job %d\n", tc.Scenario.Slug, result.label, result.jobID)
				}
			}
			emitProgressEvent(progressEvent{Kind: "phase_progress", Current: done, Total: len(cases), Status: status, Detail: tc.Scenario.Slug})
			if dead, waitErr := managedWeaverDied(); dead {
				log.Fatalf("Weaver exited during %s (%v):\n%s", tc.Scenario.Slug, waitErr, managedWeaverDeathReport())
			}
		}
		restore()
	}
	fmt.Printf("\nADVERSARIAL SUMMARY: %d case(s), %d failure(s)\n", len(cases), failures)
	if failures > 0 {
		os.Exit(1)
	}
}

// startAdversarialGroup starts a fresh Weaver under the group's caps and
// configures the lane's password list. The returned func puts the
// environment back as it was.
func startAdversarialGroup(group adversarialGroup) (string, func(), error) {
	values, err := group.Env()
	if err != nil {
		return "", func() {}, err
	}
	restore := setAdversarialEnv(values)
	if err := startStandardManagedWeaver(false); err != nil {
		restore()
		return "", func() {}, err
	}
	weaverURL := defaultWeaverURL()
	prepareStandardTestRun(weaverURL, false)
	if err := setArchivePasswordList(weaverURL, adversarialListPasswords); err != nil {
		restore()
		return "", func() {}, fmt.Errorf("configure the archive password list: %w", err)
	}
	log.Printf("adversarial group %s: Weaver at %s with %v", group.Name, weaverURL, values)
	return weaverURL, restore, nil
}

func setAdversarialEnv(values map[string]string) func() {
	type previous struct {
		value string
		set   bool
	}
	saved := map[string]previous{}
	for key, value := range values {
		old, set := os.LookupEnv(key)
		saved[key] = previous{value: old, set: set}
		_ = os.Setenv(key, value)
	}
	return func() {
		for key, old := range saved {
			if old.set {
				_ = os.Setenv(key, old.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}

// loadAdversarialCases is every scenario under root whose scenario.json
// carries an "adversarial" object, sorted by slug.
func loadAdversarialCases(root string) ([]adversarialCase, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var cases []adversarialCase
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		raw, err := os.ReadFile(filepath.Join(dir, "scenario.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		spec, ok, err := parseAdversarialSpec(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", entry.Name(), err)
		}
		if !ok {
			continue
		}
		scenario, err := loadScenario(dir)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", entry.Name(), err)
		}
		cases = append(cases, adversarialCase{Scenario: scenario, Spec: spec})
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i].Scenario.Slug < cases[j].Scenario.Slug })
	return cases, nil
}

// parseAdversarialSpec reads the "adversarial" object of a scenario.json, and
// reports false when there is none.
func parseAdversarialSpec(raw []byte) (adversarialSpec, bool, error) {
	var document struct {
		Adversarial *adversarialSpec `json:"adversarial"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return adversarialSpec{}, false, err
	}
	if document.Adversarial == nil {
		return adversarialSpec{}, false, nil
	}
	spec := *document.Adversarial
	if err := spec.validate(); err != nil {
		return adversarialSpec{}, false, err
	}
	return spec, true, nil
}

func (spec adversarialSpec) validate() error {
	known := false
	for _, group := range adversarialGroups() {
		known = known || group.Name == spec.Group
	}
	if !known {
		return fmt.Errorf("adversarial group %q is not one of default, tight, reserve", spec.Group)
	}
	switch spec.Outcome {
	case "success", "failed", "either", "settles":
	default:
		return fmt.Errorf("adversarial outcome %q is not one of success, failed, either, settles", spec.Outcome)
	}
	if spec.Outcome == "success" && (spec.ErrorContains != "" || spec.FailureKind != "") {
		return errors.New("a success case names no failure")
	}
	for _, path := range spec.EscapePaths {
		if !filepath.IsAbs(path) {
			return fmt.Errorf("escape path %q is not absolute", path)
		}
	}
	return nil
}

func selectAdversarialCases(cases []adversarialCase, slugs []string) ([]adversarialCase, error) {
	if len(slugs) == 0 {
		if len(cases) == 0 {
			return nil, errors.New("no scenario carries an adversarial object")
		}
		return cases, nil
	}
	bySlug := make(map[string]adversarialCase, len(cases))
	for _, tc := range cases {
		bySlug[tc.Scenario.Slug] = tc
	}
	selected := make([]adversarialCase, 0, len(slugs))
	for _, slug := range slugs {
		tc, ok := bySlug[slug]
		if !ok {
			return nil, fmt.Errorf("%q is not an adversarial scenario", slug)
		}
		selected = append(selected, tc)
	}
	return selected, nil
}

func adversarialCasesInGroup(cases []adversarialCase, group string) []adversarialCase {
	var members []adversarialCase
	for _, tc := range cases {
		if tc.Spec.Group == group {
			members = append(members, tc)
		}
	}
	return members
}

func adversarialCaseSlugs(cases []adversarialCase) []string {
	slugs := make([]string, 0, len(cases))
	for _, tc := range cases {
		slugs = append(slugs, tc.Scenario.Slug)
	}
	return slugs
}

// adversarialFixtureSlugs is what the lane seeds: every adversarial scenario
// and the cleanup job.
func adversarialFixtureSlugs() []string {
	cases, err := loadAdversarialCases(testdataDir())
	if err != nil {
		log.Fatalf("load adversarial scenarios: %v", err)
	}
	return append(adversarialCaseSlugs(cases), adversarialCleanupSlug)
}

func (tc adversarialCase) submissions() []adversarialSubmission {
	if len(tc.Spec.Submissions) > 0 {
		return tc.Spec.Submissions
	}
	return []adversarialSubmission{{Label: "posted", Password: tc.Scenario.Password}}
}

type adversarialResult struct {
	label string
	jobID int
	err   error
}

// runAdversarialCase posts the case once per submission, and after each
// settles holds it to the contract, then proves the stack with the cleanup
// job.
func runAdversarialCase(weaverURL string, tc adversarialCase, cleanup *Scenario) []adversarialResult {
	var results []adversarialResult
	for _, submission := range tc.submissions() {
		jobID, err := runAdversarialSubmission(weaverURL, tc, submission, cleanup)
		results = append(results, adversarialResult{label: submission.Label, jobID: jobID, err: err})
	}
	return results
}

func runAdversarialSubmission(weaverURL string, tc adversarialCase, submission adversarialSubmission, cleanup *Scenario) (int, error) {
	before, err := observeAdversarialBefore(weaverURL, tc)
	if err != nil {
		return 0, err
	}
	scenario := *tc.Scenario
	scenario.Password = submission.Password
	jobID, err := submitOneNZBWithOptions(weaverURL, &scenario, submitNZBOptions{force: true})
	if err != nil {
		return 0, fmt.Errorf("submit: %w", err)
	}
	if err := awaitAdversarialJobArchived(jobID); err != nil {
		return jobID, err
	}
	settled, err := observeAdversarialSettled(weaverURL, jobID, before)
	if err != nil {
		return jobID, err
	}
	var problems []error
	problems = append(problems, checkAdversarialVerdict(tc, settled)...)
	problems = append(problems, checkAdversarialContainment(tc, jobID, settled, before)...)
	problems = append(problems, checkAdversarialScratch(tc, jobID, settled, before)...)
	problems = append(problems, checkAdversarialSignals(tc, settled, before)...)
	if err := assertTerminalFixtureStateSweeping(localWeaverDBPath(), jobID, settled.Status, true); err != nil {
		problems = append(problems, err)
	}
	if err := runAdversarialCleanupJob(weaverURL, cleanup); err != nil {
		problems = append(problems, fmt.Errorf("cleanup job: %w", err))
	}
	if err := checkAdversarialFetchesFlat(settled); err != nil {
		problems = append(problems, err)
	}
	return jobID, errors.Join(problems...)
}

// awaitAdversarialJobArchived waits until the job's history row commits, and
// fails at once if Weaver exits while it waits. It has no deadline of its
// own: a job that never settles is the hang the lane exists to catch, and the
// runner's bound ends it.
func awaitAdversarialJobArchived(jobID int) error {
	dbPath := localWeaverDBPath()
	for {
		if dead, waitErr := managedWeaverDied(); dead {
			return fmt.Errorf("Weaver exited while job %d was settling (%v):\n%s", jobID, waitErr, managedWeaverDeathReport())
		}
		archived, err := jobArchived(dbPath, jobID)
		if err != nil {
			return fmt.Errorf("read job %d archive state: %w", jobID, err)
		}
		if archived {
			return nil
		}
		time.Sleep(adversarialPollInterval)
	}
}

// runAdversarialCleanupJob posts the ordinary job and requires it to complete
// with its own bytes and leave no active state behind.
func runAdversarialCleanupJob(weaverURL string, cleanup *Scenario) error {
	jobID, err := submitOneNZBWithOptions(weaverURL, cleanup, submitNZBOptions{force: true})
	if err != nil {
		return fmt.Errorf("submit %s: %w", cleanup.Slug, err)
	}
	if err := awaitAdversarialJobArchived(jobID); err != nil {
		return err
	}
	dbPath := localWeaverDBPath()
	if err := assertTerminalFixtureStateSweeping(dbPath, jobID, "COMPLETE", true); err != nil {
		return fmt.Errorf("%s job %d: %w", cleanup.Slug, jobID, err)
	}
	if len(cleanup.ExpectedOutputBLAKE3) > 0 {
		if err := assertOutputBLAKE3(dbPath, jobID, cleanup.ExpectedOutputBLAKE3); err != nil {
			return fmt.Errorf("%s job %d: %w", cleanup.Slug, jobID, err)
		}
	}
	return nil
}
