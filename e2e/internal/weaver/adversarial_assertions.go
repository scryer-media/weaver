// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// What the adversarial lane reads around one hostile job: the state before
// it is submitted, and the state once its history row has committed.

// adversarialBudgetWarning is the line the graph engine logs, once, when a
// job's extraction budget refuses it.
const adversarialBudgetWarning = "graph job's extraction budget refused it"

const adversarialRejectionMetric = "weaver_extraction_rejections_total"

type adversarialBefore struct {
	LogOffset         int64
	Rejections        map[string]float64
	IntermediateBytes int64
	CompleteBytes     int64
	PostedBytes       int64
	MessageIDs        []string
}

type adversarialSettled struct {
	JobID             int
	Status            string
	Error             string
	FailureKind       string
	OutputDir         string
	Outputs           []string
	LogWindow         string
	Rejections        map[string]float64
	IntermediateBytes int64
	CompleteBytes     int64
	TerminalEvents    int
	MessageIDs        []string
	Bodies            map[string]int
}

func adversarialIntermediateDir() string {
	return filepath.Join(localWeaverDir(), "intermediate")
}

func adversarialCompleteDir() string {
	return filepath.Join(localWeaverDir(), "complete")
}

func adversarialStagingDir(jobID int) string {
	return filepath.Join(adversarialCompleteDir(), ".weaver-staging", strconv.Itoa(jobID))
}

func observeAdversarialBefore(weaverURL string, tc adversarialCase) (adversarialBefore, error) {
	var before adversarialBefore
	for _, escape := range tc.Spec.EscapePaths {
		if _, err := os.Lstat(escape); err == nil {
			return before, fmt.Errorf("%s exists before the job, so the job writing it could not be told apart; remove it", escape)
		}
	}
	nzb, err := os.ReadFile(filepath.Join(fixturesDir(), tc.Scenario.Slug, tc.Scenario.Slug+".nzb"))
	if err != nil {
		return before, fmt.Errorf("read NZB: %w", err)
	}
	if before.MessageIDs, err = extractAllMessageIDsFromNZB(nzb); err != nil {
		return before, fmt.Errorf("parse NZB: %w", err)
	}
	before.PostedBytes = nzbPostedBytes(nzb)
	if before.LogOffset, err = adversarialFileSize(localWeaverLogPath()); err != nil {
		return before, err
	}
	if before.Rejections, err = fetchExtractionRejections(weaverURL); err != nil {
		return before, err
	}
	if before.IntermediateBytes, err = adversarialTreeBytes(adversarialIntermediateDir()); err != nil {
		return before, err
	}
	if before.CompleteBytes, err = adversarialTreeBytes(adversarialCompleteDir()); err != nil {
		return before, err
	}
	return before, nil
}

func observeAdversarialSettled(weaverURL string, jobID int, before adversarialBefore) (adversarialSettled, error) {
	settled := adversarialSettled{JobID: jobID, MessageIDs: before.MessageIDs}
	row, err := loadAdversarialHistory(localWeaverDBPath(), jobID)
	if err != nil {
		return settled, err
	}
	settled.Status = normalizeFacadeState(row.Status)
	settled.Error = row.Error
	settled.FailureKind = row.FailureKind
	settled.OutputDir = row.OutputDir
	if settled.OutputDir != "" {
		if settled.Outputs, err = listOutputFiles(settled.OutputDir); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return settled, fmt.Errorf("list outputs of job %d: %w", jobID, err)
		}
	}
	if settled.LogWindow, err = adversarialReadFrom(localWeaverLogPath(), before.LogOffset); err != nil {
		return settled, err
	}
	if settled.Rejections, err = fetchExtractionRejections(weaverURL); err != nil {
		return settled, fmt.Errorf("metrics after job %d settled: %w", jobID, err)
	}
	if settled.IntermediateBytes, err = adversarialTreeBytes(adversarialIntermediateDir()); err != nil {
		return settled, err
	}
	if settled.CompleteBytes, err = adversarialTreeBytes(adversarialCompleteDir()); err != nil {
		return settled, err
	}
	kinds, err := fetchJobEventKindsGraphQL(weaverURL, jobID)
	if err != nil {
		return settled, err
	}
	for _, kind := range kinds {
		if kind == "JOB_COMPLETED" || kind == "JOB_FAILED" {
			settled.TerminalEvents++
		}
	}
	if settled.Bodies, err = adversarialBodyCounts(before.MessageIDs); err != nil {
		return settled, err
	}
	return settled, nil
}

// checkAdversarialVerdict holds the settled job to the case's outcome and,
// for a failure, to the verdict it names.
func checkAdversarialVerdict(tc adversarialCase, settled adversarialSettled) []error {
	spec := tc.Spec
	var problems []error
	allowed := map[string][]string{
		"success": {"COMPLETE"},
		"failed":  {"FAILED"},
		"either":  {"COMPLETE", "FAILED"},
		"settles": {"COMPLETE", "PARTIAL", "FAILED"},
	}[spec.Outcome]
	if !adversarialHas(allowed, settled.Status) {
		problems = append(problems, fmt.Errorf("settled %s (%q), want one of %v", settled.Status, settled.Error, allowed))
	}
	expected := tc.Scenario.ExpectedOutputBLAKE3
	switch settled.Status {
	case "FAILED":
		if strings.TrimSpace(settled.Error) == "" {
			problems = append(problems, errors.New("failed without naming a verdict"))
		}
		if spec.ErrorContains != "" && !strings.Contains(settled.Error, spec.ErrorContains) {
			problems = append(problems, fmt.Errorf("verdict %q does not contain %q", settled.Error, spec.ErrorContains))
		}
		if spec.FailureKind != "" && settled.FailureKind != spec.FailureKind {
			problems = append(problems, fmt.Errorf("failure kind %q, want %q", settled.FailureKind, spec.FailureKind))
		}
		for name := range expected {
			if adversarialHas(settled.Outputs, name) {
				problems = append(problems, fmt.Errorf("failed but published %s", name))
			}
		}
	case "COMPLETE":
		if len(expected) > 0 {
			if err := assertOutputBLAKE3(localWeaverDBPath(), settled.JobID, expected); err != nil {
				problems = append(problems, err)
			}
		}
		if len(settled.Outputs) < spec.MinOutputFiles {
			problems = append(problems, fmt.Errorf("delivered %d files, want at least %d", len(settled.Outputs), spec.MinOutputFiles))
		}
		for _, name := range spec.RequiredOutputs {
			if !adversarialHas(settled.Outputs, name) {
				problems = append(problems, fmt.Errorf("did not deliver %s", name))
			}
		}
		for _, suffix := range spec.RequiredOutputSuffixes {
			if !adversarialAnyHasSuffix(settled.Outputs, suffix) {
				problems = append(problems, fmt.Errorf("delivered no file ending %s", suffix))
			}
		}
	}
	for _, output := range settled.Outputs {
		if adversarialHas(spec.ForbiddenOutputs, path.Base(output)) {
			problems = append(problems, fmt.Errorf("delivered %s", output))
		}
	}
	if spec.ExtractsNothing && len(settled.Outputs) > 0 {
		posted, err := collectFixtureStagingFiles(filepath.Join(testdataDir(), tc.Scenario.Slug), true,
			filepath.Join(testdataDir(), "shared"), tc.Scenario.SharedAssets, testdataDir(), tc.Scenario.FixtureAssets)
		if err != nil {
			problems = append(problems, fmt.Errorf("identify posted files: %w", err))
		} else {
			problems = append(problems, checkUnextractedFiles(settled, posted)...)
		}
	}
	return problems
}

func checkUnextractedFiles(settled adversarialSettled, posted []stagedSeedFile) []error {
	var problems []error
	sources := map[string]string{}
	for _, file := range posted {
		sources[filepath.ToSlash(file.name)] = file.source
	}
	for _, output := range settled.Outputs {
		if settled.Status != "FAILED" {
			problems = append(problems, fmt.Errorf("published %s despite extractsNothing", output))
			continue
		}
		if output == ".weaver-job-dir" {
			continue
		}
		source, posted := sources[output]
		if !posted {
			problems = append(problems, fmt.Errorf("retained unposted output %s", output))
			continue
		}
		want, sourceErr := fileBLAKE3(source)
		got, outputErr := fileBLAKE3(filepath.Join(settled.OutputDir, filepath.FromSlash(output)))
		if sourceErr != nil || outputErr != nil || got != want {
			problems = append(problems, fmt.Errorf("retained %s differs from posted bytes (source=%v output=%v)", output, sourceErr, outputErr))
		}
	}
	return problems
}

// checkAdversarialContainment proves nothing of the job lies outside its
// output directory: no link in the output tree, no file of a hostile name
// anywhere else under the run, and nothing at any place a hostile name
// resolves to outside a directory Weaver writes into.
func checkAdversarialContainment(tc adversarialCase, jobID int, settled adversarialSettled, before adversarialBefore) []error {
	var problems []error
	if settled.OutputDir != "" {
		problems = append(problems, adversarialOutputTreeProblems(settled.OutputDir)...)
	}
	for _, escape := range tc.Spec.EscapePaths {
		if _, err := os.Lstat(escape); err == nil {
			problems = append(problems, fmt.Errorf("%s exists after the job", escape))
		}
	}
	bases := []string{adversarialIntermediateDir(), adversarialCompleteDir(), adversarialStagingDir(jobID)}
	if settled.OutputDir != "" {
		bases = append(bases, settled.OutputDir)
	}
	for _, name := range tc.Spec.EscapeNames {
		for _, target := range adversarialEscapeTargets(name, bases) {
			if _, err := os.Lstat(target); err == nil {
				problems = append(problems, fmt.Errorf("member %q landed at %s", name, target))
			}
		}
	}
	if len(tc.Spec.EscapeNames) > 0 {
		outputDirs, err := loadAllHistoryOutputDirs(localWeaverDBPath())
		if err != nil {
			return append(problems, err)
		}
		leaks, err := adversarialNameLeaks(localRunDir(), adversarialEscapeBaseNames(tc.Spec.EscapeNames), outputDirs)
		if err != nil {
			return append(problems, err)
		}
		for _, leak := range leaks {
			problems = append(problems, fmt.Errorf("a hostile member's name appears outside every output directory: %s", leak))
		}
	}
	return problems
}

// checkAdversarialScratch proves the job's scratch was released: no staging
// root left, and scratch holding no more than the job is allowed to keep.
// A completed job keeps nothing in scratch; any other keeps at most what
// was posted, never what it expanded to, and a failed job adds nothing to
// the complete directory.
func checkAdversarialScratch(tc adversarialCase, jobID int, settled adversarialSettled, before adversarialBefore) []error {
	var problems []error
	if _, err := os.Lstat(adversarialStagingDir(jobID)); err == nil {
		problems = append(problems, fmt.Errorf("staging root %s is still there", adversarialStagingDir(jobID)))
	}
	grown := settled.IntermediateBytes - before.IntermediateBytes
	allowed := int64(adversarialSlack)
	if settled.Status != "COMPLETE" {
		allowed += before.PostedBytes
	}
	if grown > allowed {
		problems = append(problems, fmt.Errorf("scratch grew by %d bytes across the job, at most %d allowed", grown, allowed))
	}
	if delivered := settled.CompleteBytes - before.CompleteBytes; settled.Status == "FAILED" && delivered > adversarialSlack {
		problems = append(problems, fmt.Errorf("the complete directory grew by %d bytes across a failed job", delivered))
	}
	if reserve, ok := os.LookupEnv("WEAVER_EXTRACTION_MIN_FREE_BYTES"); ok && tc.Spec.Group == "reserve" {
		want, err := strconv.ParseUint(reserve, 10, 64)
		if err != nil {
			return append(problems, fmt.Errorf("reserve %q: %w", reserve, err))
		}
		free, err := adversarialFreeBytes(adversarialIntermediateDir())
		if err != nil {
			return append(problems, err)
		}
		if free < want {
			problems = append(problems, fmt.Errorf("%d bytes free after the job, under the %d byte reserve", free, want))
		}
	}
	return problems
}

// checkAdversarialSignals holds what Weaver said about the job: one terminal
// event, the budget's refusal warned once and never repeated, and the
// rejection counted on the metrics endpoint.
func checkAdversarialSignals(tc adversarialCase, settled adversarialSettled, before adversarialBefore) []error {
	var problems []error
	for _, kind := range tc.Spec.RequiredObjectKinds {
		seen := false
		for _, bound := range graphLogEvents(settled.LogWindow, settled.JobID, graphObjectBoundMessage) {
			seen = seen || bound["kind"] == kind
		}
		if !seen {
			problems = append(problems, fmt.Errorf("job never recognized required object kind %s", kind))
		}
	}
	if settled.TerminalEvents != 1 {
		problems = append(problems, fmt.Errorf("%d terminal job events, want 1", settled.TerminalEvents))
	}
	warnings := strings.Count(settled.LogWindow, adversarialBudgetWarning)
	switch {
	case tc.Spec.BudgetRefusals > 0 && warnings != tc.Spec.BudgetRefusals:
		problems = append(problems, fmt.Errorf("the budget refusal was logged %d times, want %d", warnings, tc.Spec.BudgetRefusals))
	case tc.Spec.BudgetRefusals == 0 && warnings > 1:
		problems = append(problems, fmt.Errorf("the budget refusal was logged %d times; a refusal is said once", warnings))
	}
	if reason := tc.Spec.RejectionMetric; reason != "" {
		moved := settled.Rejections[reason] - before.Rejections[reason]
		if moved != 1 {
			problems = append(problems, fmt.Errorf("%s{reason=%q} moved by %v, want 1", adversarialRejectionMetric, reason, moved))
		}
	}
	return problems
}

// checkAdversarialFetchesFlat proves the job fetched nothing once its verdict
// stood: its articles' BODY counts after the cleanup job are what they were
// when it settled. A password retried against the server, or a refused job
// still draining its queue, would move them.
func checkAdversarialFetchesFlat(settled adversarialSettled) error {
	after, err := adversarialBodyCounts(settled.MessageIDs)
	if err != nil {
		return err
	}
	var moved []string
	for _, id := range settled.MessageIDs {
		if after[id] != settled.Bodies[id] {
			moved = append(moved, fmt.Sprintf("%s %d->%d", id, settled.Bodies[id], after[id]))
		}
	}
	if len(moved) > 0 {
		return fmt.Errorf("%d article(s) fetched after the verdict: %v", len(moved), adversarialFirst(moved, 5))
	}
	return nil
}

// adversarialOutputTreeProblems walks the output directory without following
// links: a symbolic link or a file with a second hard link could point out of
// it, so neither may be delivered.
func adversarialOutputTreeProblems(outputDir string) []error {
	var problems []error
	err := filepath.WalkDir(outputDir, func(current string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && current == outputDir {
				return fs.SkipAll
			}
			return err
		}
		if current == filepath.Join(outputDir, ".weaver-chunks") {
			problems = append(problems, fmt.Errorf("retained graph scratch %s", current))
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			problems = append(problems, fmt.Errorf("delivered a symbolic link %s", current))
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if links := adversarialLinkCount(info); links > 1 {
			problems = append(problems, fmt.Errorf("delivered %s with %d hard links", current, links))
		}
		return nil
	})
	if err != nil {
		problems = append(problems, fmt.Errorf("walk output %s: %w", outputDir, err))
	}
	return problems
}

// adversarialEscapeTargets is where a member name lands when joined onto each
// base, kept only where that is outside the base. A backslash is read as a
// separator, as a writer on Windows would read it.
func adversarialEscapeTargets(name string, bases []string) []string {
	normalized := strings.ReplaceAll(name, `\`, "/")
	var targets []string
	for _, base := range bases {
		target := filepath.FromSlash(normalized)
		if !path.IsAbs(normalized) {
			target = filepath.Join(base, target)
		}
		target = filepath.Clean(target)
		relative, err := filepath.Rel(base, target)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			targets = append(targets, target)
		}
	}
	return targets
}

// adversarialEscapeBaseNames is the last element of every hostile member
// name, the name a leaked file would carry wherever it landed.
func adversarialEscapeBaseNames(names []string) []string {
	var bases []string
	for _, name := range names {
		base := path.Base(strings.ReplaceAll(name, `\`, "/"))
		if base != "." && base != "/" && base != ".." && !adversarialHas(bases, base) {
			bases = append(bases, base)
		}
	}
	return bases
}

// adversarialNameLeaks walks root for files carrying one of names that are
// not under any of the output directories.
func adversarialNameLeaks(root string, names, outputDirs []string) ([]string, error) {
	var leaks []string
	err := filepath.WalkDir(root, func(current string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			for _, dir := range outputDirs {
				if current == filepath.Clean(dir) {
					return fs.SkipDir
				}
			}
			return nil
		}
		if adversarialHas(names, entry.Name()) {
			leaks = append(leaks, current)
		}
		return nil
	})
	return leaks, err
}

type adversarialHistoryRow struct {
	Status      string
	Error       string
	FailureKind string
	OutputDir   string
}

func loadAdversarialHistory(dbPath string, jobID int) (adversarialHistoryRow, error) {
	var row adversarialHistoryRow
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return row, fmt.Errorf("open weaver state db: %w", err)
	}
	defer closeWeaverStateDB(db, datastore)
	var errorMessage, failureKind, outputDir sql.NullString
	query := rebindWeaverSQL(datastore, `SELECT status, error_message, failure_kind, output_dir FROM job_history WHERE job_id = ?`)
	if err := db.QueryRow(query, jobID).Scan(&row.Status, &errorMessage, &failureKind, &outputDir); err != nil {
		return row, fmt.Errorf("load job_history row for job %d: %w", jobID, err)
	}
	row.Error, row.FailureKind, row.OutputDir = errorMessage.String, failureKind.String, outputDir.String
	return row, nil
}

func loadAllHistoryOutputDirs(dbPath string) ([]string, error) {
	db, datastore, err := openWeaverStateDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open weaver state db: %w", err)
	}
	defer closeWeaverStateDB(db, datastore)
	rows, err := db.Query(`SELECT output_dir FROM job_history WHERE output_dir IS NOT NULL AND output_dir <> ''`)
	if err != nil {
		return nil, fmt.Errorf("load history output directories: %w", err)
	}
	defer rows.Close()
	var dirs []string
	for rows.Next() {
		var dir string
		if err := rows.Scan(&dir); err != nil {
			return nil, err
		}
		dirs = append(dirs, dir)
	}
	return dirs, rows.Err()
}

func fetchJobEventKindsGraphQL(weaverURL string, jobID int) ([]string, error) {
	payload, _ := json.Marshal(map[string]any{
		"query":     `query($id: Int!) { jobEvents(jobId: $id) { kind } }`,
		"variables": map[string]any{"id": jobID},
	})
	var result struct {
		Data struct {
			JobEvents []struct {
				Kind string `json:"kind"`
			} `json:"jobEvents"`
		} `json:"data"`
	}
	if err := adversarialGraphQL(weaverURL, payload, &result); err != nil {
		return nil, fmt.Errorf("job events of job %d: %w", jobID, err)
	}
	kinds := make([]string, 0, len(result.Data.JobEvents))
	for _, event := range result.Data.JobEvents {
		kinds = append(kinds, event.Kind)
	}
	return kinds, nil
}

func setArchivePasswordList(weaverURL string, passwords []string) error {
	payload, _ := json.Marshal(map[string]any{
		"query":     `mutation($passwords: [String!]) { updateArchivePasswordSettings(passwords: $passwords) { hasPasswords } }`,
		"variables": map[string]any{"passwords": passwords},
	})
	var result struct {
		Data struct {
			Settings struct {
				HasPasswords bool `json:"hasPasswords"`
			} `json:"updateArchivePasswordSettings"`
		} `json:"data"`
	}
	if err := adversarialGraphQL(weaverURL, payload, &result); err != nil {
		return err
	}
	if !result.Data.Settings.HasPasswords {
		return errors.New("Weaver reports no archive passwords after setting them")
	}
	return nil
}

func adversarialGraphQL(weaverURL string, payload []byte, result any) error {
	response, err := postGraphQL(weaverURL, payload)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("GraphQL returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("gql: %s", envelope.Errors[0].Message)
	}
	return json.Unmarshal(body, result)
}

// adversarialMetricsURL is the metrics endpoint beside the GraphQL one.
func adversarialMetricsURL(weaverURL string) string {
	return weaverBaseURL(weaverURL) + "/metrics"
}

// fetchExtractionRejections reads weaver_extraction_rejections_total by
// reason from the metrics endpoint.
func fetchExtractionRejections(weaverURL string) (map[string]float64, error) {
	response, err := weaverHTTPClient(weaverURL, 10*time.Second).Get(adversarialMetricsURL(weaverURL))
	if err != nil {
		return nil, fmt.Errorf("scrape metrics: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("metrics returned %d", response.StatusCode)
	}
	return prometheusSamplesByLabel(body, adversarialRejectionMetric, "reason"), nil
}

var prometheusSample = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)`)

// prometheusSamplesByLabel is every sample of one metric family in a text
// exposition, keyed by the value of one label.
func prometheusSamplesByLabel(exposition []byte, name, label string) map[string]float64 {
	samples := map[string]float64{}
	labelPattern := regexp.MustCompile(regexp.QuoteMeta(label) + `="((?:[^"\\]|\\.)*)"`)
	scanner := bufio.NewScanner(bytes.NewReader(exposition))
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		match := prometheusSample.FindStringSubmatch(line)
		if match == nil || match[1] != name {
			continue
		}
		value, err := strconv.ParseFloat(match[3], 64)
		if err != nil {
			continue
		}
		key := ""
		if labelMatch := labelPattern.FindStringSubmatch(match[2]); labelMatch != nil {
			key = labelMatch[1]
		}
		samples[key] += value
	}
	return samples
}

// adversarialBodyCounts is the primary fake NNTP server's BODY count for each
// of the ids.
func adversarialBodyCounts(ids []string) (map[string]int, error) {
	metrics, err := fetchNntpBodyMetrics("")
	if err != nil {
		return nil, fmt.Errorf("read NNTP BODY counters: %w", err)
	}
	byID := make(map[string]int, len(metrics.BodyCounts))
	for id, count := range metrics.BodyCounts {
		byID[strings.TrimSuffix(strings.TrimPrefix(id, "<"), ">")] += count
	}
	counts := make(map[string]int, len(ids))
	for _, id := range ids {
		counts[id] = byID[id]
	}
	return counts, nil
}

var nzbSegmentBytes = regexp.MustCompile(`<segment[^>]*\sbytes="(\d+)"`)

// nzbPostedBytes is the sum of the NZB's segment sizes: the most a job can
// hold of what was posted.
func nzbPostedBytes(nzb []byte) int64 {
	var total int64
	for _, match := range nzbSegmentBytes.FindAllSubmatch(nzb, -1) {
		size, err := strconv.ParseInt(string(match[1]), 10, 64)
		if err == nil {
			total += size
		}
	}
	return total
}

// treeBytes is the size of every regular file under root; a missing root
// holds nothing.
func adversarialTreeBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func adversarialFileSize(name string) (int64, error) {
	info, err := os.Stat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func adversarialReadFrom(name string, offset int64) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	window, err := io.ReadAll(file)
	return string(window), err
}

func adversarialHas(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func adversarialAnyHasSuffix(values []string, suffix string) bool {
	for _, value := range values {
		if strings.HasSuffix(value, suffix) {
			return true
		}
	}
	return false
}

func adversarialFirst(values []string, n int) []string {
	if len(values) > n {
		return values[:n]
	}
	return values
}
