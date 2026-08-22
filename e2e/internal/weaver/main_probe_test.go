package weaver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDetectE2EDirRecognizesCanonicalCLIs(t *testing.T) {
	for _, cli := range []string{"weaver-e2e"} {
		t.Run(cli, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "docker-compose.yml"), []byte("services: {}\n"), 0o644); err != nil {
				t.Fatalf("write compose file: %v", err)
			}
			cliDir := filepath.Join(root, "cmd", cli)
			if err := os.MkdirAll(cliDir, 0o755); err != nil {
				t.Fatalf("create cli dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(cliDir, "main.go"), []byte("package main\n"), 0o644); err != nil {
				t.Fatalf("write cli main: %v", err)
			}
			start := filepath.Join(root, ".bin")
			if err := os.MkdirAll(start, 0o755); err != nil {
				t.Fatalf("create start dir: %v", err)
			}

			if got := detectE2EDir(start); got != root {
				t.Fatalf("detectE2EDir() = %q, want %q", got, root)
			}
		})
	}
}

func TestDockerHostPortBindCollisionRecognition(t *testing.T) {
	if !isDockerHostPortBindCollision(fmt.Errorf(
		"docker compose up: exit status 1: failed to bind host port 0.0.0.0:55482/tcp: address already in use",
	)) {
		t.Fatal("expected Docker host-port bind collision to be retryable")
	}
	if isDockerHostPortBindCollision(fmt.Errorf("docker compose up: invalid compose file")) {
		t.Fatal("non-bind Docker error must not be retried")
	}
}

func TestReallocateRuntimePortsForDockerRetryPreservesExplicitNNTPAliases(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "runtime-ports.json")
	previous, err := allocateRuntimePortState()
	if err != nil {
		t.Fatalf("allocate previous runtime ports: %v", err)
	}
	if err := saveRuntimePortState(statePath, previous); err != nil {
		t.Fatalf("save previous runtime ports: %v", err)
	}
	t.Setenv("E2E_RUNTIME_PORTS_FILE", statePath)
	for key, value := range runtimePortEnvValues(previous) {
		t.Setenv(key, value)
	}
	t.Setenv("NNTP_PORT", "39991")
	t.Setenv("NNTP_BACKUP_PORT", "39992")

	if err := reallocateRuntimePortsForDockerRetry(); err != nil {
		t.Fatalf("reallocate runtime ports: %v", err)
	}
	next, err := loadRuntimePortState(statePath)
	if err != nil {
		t.Fatalf("load reallocated runtime ports: %v", err)
	}
	for key, want := range runtimePortEnvValues(next) {
		if strings.HasPrefix(key, "E2E_") && os.Getenv(key) != want {
			t.Fatalf("env[%s] = %q, want %q", key, os.Getenv(key), want)
		}
	}
	if got := os.Getenv("NNTP_PORT"); got != "39991" {
		t.Fatalf("NNTP_PORT = %q, want preserved explicit port", got)
	}
	if got := os.Getenv("NNTP_BACKUP_PORT"); got != "39992" {
		t.Fatalf("NNTP_BACKUP_PORT = %q, want preserved explicit port", got)
	}
}

func TestStatOnlyChaosConfigDropsBodyChaos(t *testing.T) {
	got := statOnlyChaosConfig("stat_bad_code=100,drop_mid_body=5, stat_short=25")
	want := "stat_bad_code=100,stat_short=25"
	if got != want {
		t.Fatalf("statOnlyChaosConfig() = %q, want %q", got, want)
	}
}

func TestExtractFirstProbeSampleMessageIDsUsesDeterministicFirstRoundSamples(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="sample.bin"><segments>`)
	for i := 0; i < 30; i++ {
		fmt.Fprintf(&b, `<segment bytes="1024" number="%d">&lt;id-%02d@example.test&gt;</segment>`, i+1, i)
	}
	b.WriteString(`</segments></file></nzb>`)

	got, err := extractFirstProbeSampleMessageIDs([]byte(b.String()), 4)
	if err != nil {
		t.Fatalf("extract first probe sample ids: %v", err)
	}

	want := []string{
		"id-00@example.test",
		"id-03@example.test",
		"id-06@example.test",
		"id-09@example.test",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d ids, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected id %d = %q, got %q", i, want[i], got[i])
		}
	}
}

func TestExtractFirstProbeSampleMessageIDsClampsToAvailableSamples(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><nzb><file subject="sample.bin"><segments>`)
	for i := 0; i < 12; i++ {
		fmt.Fprintf(&b, `<segment bytes="1024" number="%d">&lt;id-%02d@example.test&gt;</segment>`, i+1, i)
	}
	b.WriteString(`</segments></file></nzb>`)

	got, err := extractFirstProbeSampleMessageIDs([]byte(b.String()), 50)
	if err != nil {
		t.Fatalf("extract clamped probe sample ids: %v", err)
	}

	want := []string{
		"id-00@example.test",
		"id-01@example.test",
		"id-02@example.test",
		"id-03@example.test",
		"id-04@example.test",
		"id-05@example.test",
		"id-06@example.test",
		"id-07@example.test",
		"id-08@example.test",
		"id-09@example.test",
		"id-10@example.test",
		"id-11@example.test",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d ids, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected clamped id %d = %q, got %q", i, want[i], got[i])
		}
	}
}

func TestFunctionalPollIntervalOnlyAcceleratesPendingRewriteAssertions(t *testing.T) {
	jobs := []testJob{
		{slug: "rewrite", scenario: rewriteAssertionScenarioForTest()},
		{slug: "plain", scenario: &Scenario{}},
	}

	if got := functionalPollInterval(jobs); got != functionalRewritePollInterval {
		t.Fatalf("expected rewrite polling interval %s, got %s", functionalRewritePollInterval, got)
	}

	jobs[0].fileIdentityRewriteObserved = true
	if got := functionalPollInterval(jobs); got != functionalNormalPollInterval {
		t.Fatalf("expected normal polling interval %s after rewrite observation, got %s", functionalNormalPollInterval, got)
	}

	jobs[0].fileIdentityRewriteObserved = false
	jobs[0].status = "COMPLETE"
	if got := functionalPollInterval(jobs); got != functionalNormalPollInterval {
		t.Fatalf("expected normal polling interval after terminal rewrite job, got %s", got)
	}
}

func TestFunctionalFastStatusPollIndexesBatchesPendingJobs(t *testing.T) {
	jobs := make([]testJob, functionalFastStatusPollBatchSize+5)
	for i := range jobs {
		jobs[i] = testJob{slug: fmt.Sprintf("job-%02d", i)}
	}
	jobs[2].status = "COMPLETE"

	cursor := 0
	first := functionalStatusPollIndexes(jobs, true, &cursor)
	if len(first) != functionalFastStatusPollBatchSize {
		t.Fatalf("expected first fast batch size %d, got %d", functionalFastStatusPollBatchSize, len(first))
	}
	for _, idx := range first {
		if idx == 2 {
			t.Fatalf("fast status batch included resolved job index %d", idx)
		}
	}

	second := functionalStatusPollIndexes(jobs, true, &cursor)
	if len(second) != functionalFastStatusPollBatchSize {
		t.Fatalf("expected second fast batch size %d, got %d", functionalFastStatusPollBatchSize, len(second))
	}
	if sameIntSlices(first, second) {
		t.Fatalf("expected rotating fast batches, got identical indexes %v", second)
	}
}

func TestFunctionalSlowStatusPollIndexesIncludesAllPendingJobs(t *testing.T) {
	jobs := []testJob{
		{slug: "done", status: "COMPLETE"},
		{slug: "pending-a"},
		{slug: "pending-b"},
	}
	cursor := 9

	got := functionalStatusPollIndexes(jobs, false, &cursor)
	want := []int{1, 2}
	if !sameIntSlices(got, want) {
		t.Fatalf("expected all pending indexes %v, got %v", want, got)
	}
	if cursor != 0 {
		t.Fatalf("expected slow polling to reset cursor, got %d", cursor)
	}
}

func TestFunctionalRegularBatchesExcludeExclusiveAndCapAtEight(t *testing.T) {
	jobs := make([]testJob, 19)
	for i := range jobs {
		jobs[i] = testJob{slug: fmt.Sprintf("job-%02d", i), status: "queued_regular"}
	}
	jobs[3].status = "queued_exclusive"
	jobs[12].status = "queued_exclusive"

	batches := functionalRegularBatches(jobs, functionalRegularBatchSize)
	if len(batches) != 3 {
		t.Fatalf("expected three batches, got %d: %v", len(batches), batches)
	}
	if got, want := len(batches[0]), 8; got != want {
		t.Fatalf("first batch size = %d, want %d", got, want)
	}
	if got, want := len(batches[1]), 8; got != want {
		t.Fatalf("second batch size = %d, want %d", got, want)
	}
	if got, want := len(batches[2]), 1; got != want {
		t.Fatalf("final batch size = %d, want %d", got, want)
	}
	for _, batch := range batches {
		for _, index := range batch {
			if jobs[index].status != "queued_regular" {
				t.Fatalf("exclusive job index %d leaked into regular batch", index)
			}
		}
	}
}

func TestFunctionalBatchActivationIsolatesPollingAndProgress(t *testing.T) {
	jobs := make([]testJob, 10)
	for i := range jobs {
		jobs[i] = testJob{slug: fmt.Sprintf("job-%02d", i), status: "queued_regular"}
	}
	batches := functionalRegularBatches(jobs, functionalRegularBatchSize)

	for _, index := range batches[0] {
		jobs[index].status = ""
	}
	if got, want := functionalStatusPollIndexes(jobs, false, nil), batches[0]; !sameIntSlices(got, want) {
		t.Fatalf("first batch poll indexes = %v, want %v", got, want)
	}
	if got := countResolvedTestJobs(jobs); got != 0 {
		t.Fatalf("queued future batch counted as resolved: %d", got)
	}

	for _, index := range batches[0] {
		jobs[index].status = "timeout"
	}
	for _, index := range batches[1] {
		jobs[index].status = ""
	}
	if got, want := functionalStatusPollIndexes(jobs, false, nil), batches[1]; !sameIntSlices(got, want) {
		t.Fatalf("second batch poll indexes = %v, want %v", got, want)
	}
}

func TestFunctionalCompletionTimeoutIsDatastoreSpecific(t *testing.T) {
	t.Setenv(weaverDatastoreEnv, "sqlite")
	if got, want := functionalCompletionTimeout(), 3*time.Minute; got != want {
		t.Fatalf("sqlite timeout = %s, want %s", got, want)
	}

	t.Setenv(weaverDatastoreEnv, "postgres")
	if got, want := functionalCompletionTimeout(), 8*time.Minute; got != want {
		t.Fatalf("postgres timeout = %s, want %s", got, want)
	}
}

func rewriteAssertionScenarioForTest() *Scenario {
	return &Scenario{
		RuntimeAssertions: &ScenarioRuntimeAssertions{
			FileIdentityRewrite: &ScenarioFileIdentityRewriteAssertion{
				RequiredCurrentFilenames: []string{"sample.mkv"},
			},
		},
	}
}

func sameIntSlices(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
