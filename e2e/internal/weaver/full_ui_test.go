package weaver

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFullSeedJobsOverrideDefaultsWhenUnset(t *testing.T) {
	t.Setenv("E2E_SEED_JOBS", "")

	got, ok := fullSeedJobsOverride()
	if !ok {
		t.Fatalf("expected full suite to provide a default seed worker override")
	}
	if got != defaultFullSeedJobs {
		t.Fatalf("expected default full seed jobs %q, got %q", defaultFullSeedJobs, got)
	}
}

func TestFullSeedJobsOverrideRespectsExplicitParentSetting(t *testing.T) {
	t.Setenv("E2E_SEED_JOBS", "5")

	if got, ok := fullSeedJobsOverride(); ok {
		t.Fatalf("expected no override when parent sets E2E_SEED_JOBS, got %q", got)
	}
}

func TestCountPreseedableFixturesDeduplicatesProfiles(t *testing.T) {
	phases := []*fullPhaseContext{
		{SeedProfile: "functional"},
		{SeedProfile: "functional"},
		{SeedProfile: "restart"},
		{SeedProfile: "chaos", SkipSeed: true},
	}

	got, err := countPreseedableFixtures(phases)
	if err != nil {
		t.Fatalf("count preseedable fixtures: %v", err)
	}
	want := len(fixtureSlugsForSeedProfile("functional")) + len(fixtureSlugsForSeedProfile("restart"))
	if got != want {
		t.Fatalf("preseedable fixture count = %d, want %d", got, want)
	}
}

func TestNewFullPreseedBootstrapUsesDedicatedStackAndSharedFixtures(t *testing.T) {
	tempRoot := t.TempDir()
	fixturesRoot := filepath.Join(tempRoot, "preseeded", "functional", "fixtures")
	source := &fullPhaseContext{
		Datastore: "sqlite",
		Project:   "e2e-functional-sqlite",
		RuntimePorts: runtimePortState{
			NNTPPort:           1119,
			NNTPTLSPort:        1563,
			NNTP2Port:          2119,
			ToxiproxyAPIPort:   8474,
			ToxiproxyNNTP1Port: 3119,
			ToxiproxyNNTP2Port: 4119,
			WeaverPort:         9090,
			PostgresPort:       5432,
			NzbgetPort:         6789,
			SabnzbdPort:        8085,
			LocalWeaverPort:    19090,
		},
	}

	bootstrap, err := newFullPreseedBootstrap(tempRoot, "functional", source, fixturesRoot)
	if err != nil {
		t.Fatalf("create pre-seed bootstrap: %v", err)
	}
	if bootstrap.Project == source.Project {
		t.Fatalf("bootstrap reused source project %q", source.Project)
	}
	if bootstrap.FixturesDir != fixturesRoot {
		t.Fatalf("bootstrap fixtures = %q, want %q", bootstrap.FixturesDir, fixturesRoot)
	}
	if bootstrap.RuntimePorts != source.RuntimePorts {
		t.Fatalf("bootstrap runtime ports = %#v, want %#v", bootstrap.RuntimePorts, source.RuntimePorts)
	}
	if _, err := os.Stat(bootstrap.RunDir); err != nil {
		t.Fatalf("bootstrap run directory: %v", err)
	}
	if _, err := os.Stat(bootstrap.FixturesDir); err != nil {
		t.Fatalf("bootstrap fixture directory: %v", err)
	}
	stored, err := loadRuntimePortState(bootstrap.RuntimePortsFile)
	if err != nil {
		t.Fatalf("read bootstrap runtime ports: %v", err)
	}
	if stored != source.RuntimePorts {
		t.Fatalf("stored bootstrap runtime ports = %#v, want %#v", stored, source.RuntimePorts)
	}
}

func TestFullPhaseEnvKeepsRestartSeedRetries(t *testing.T) {
	t.Setenv("E2E_SEED_JOBS", "")
	t.Setenv("E2E_RESTART_PROFILE", "")

	phase := &fullPhaseContext{
		Command:          "restart-all",
		Project:          "test-project",
		SeedProfile:      "restart",
		FixturesDir:      "/tmp/fixtures",
		RunDir:           "/tmp/run",
		RuntimePortsFile: "/tmp/runtime-ports.json",
	}

	env := phase.env()
	if got := env["E2E_SEED_JOBS"]; got != defaultFullSeedJobs {
		t.Fatalf("expected restart phase seed jobs %q, got %q", defaultFullSeedJobs, got)
	}
	if got := env["E2E_SEED_RETRIES"]; got != "5" {
		t.Fatalf("expected restart phase seed retries 5, got %q", got)
	}
	if got := env["E2E_RESTART_PROFILE"]; got != "hardened" {
		t.Fatalf("expected default restart profile hardened, got %q", got)
	}
	if got := env[nntpSeedImageCaptureEnv]; got != "0" {
		t.Fatalf("expected full runner to own NNTP cache warming, got child capture setting %q", got)
	}
}

func TestContainerRestartPhaseClearsFixedEncryptionKey(t *testing.T) {
	phase := &fullPhaseContext{Command: "container-restart"}

	env := phase.env()
	if value, ok := env["E2E_WEAVER_ENCRYPTION_KEY"]; !ok || value != "" {
		t.Fatalf("container restart phase encryption key override = %q, present=%v; want explicit empty value", value, ok)
	}
}

func TestFullPhaseLocalWeaverImageRequirements(t *testing.T) {
	for _, command := range []string{"container-restart", "release-gate"} {
		if !fullPhaseNeedsLocalWeaverImage(&fullPhaseContext{Command: command}) {
			t.Fatalf("phase %q must wait for the shared local Weaver image", command)
		}
	}
	for _, command := range []string{"test-all", "chaos-test", "tcp-chaos", "restart-all"} {
		if fullPhaseNeedsLocalWeaverImage(&fullPhaseContext{Command: command}) {
			t.Fatalf("native phase %q must not trigger a Docker Weaver build", command)
		}
	}
}

func TestFullSuitePreparesOneSharedWeaverImage(t *testing.T) {
	phases := []*fullPhaseContext{
		{Command: "container-restart"},
		{Command: "release-gate"},
		{Command: "test-all"},
	}
	calls := 0
	await := prepareFullSuiteWeaverImageWith(phases, func() error {
		calls++
		return nil
	})
	if await == nil {
		t.Fatal("container-backed phases did not start shared image preparation")
	}
	if err := await(); err != nil {
		t.Fatalf("first image waiter: %v", err)
	}
	if err := await(); err != nil {
		t.Fatalf("second image waiter: %v", err)
	}
	if calls != 1 {
		t.Fatalf("prepared local Weaver image %d times, want 1", calls)
	}

	calls = 0
	if await := prepareFullSuiteWeaverImageWith(
		[]*fullPhaseContext{{Command: "test-all"}},
		func() error { calls++; return nil },
	); await != nil {
		t.Fatal("native-only phase selection unexpectedly prepared a Docker image")
	}
	if calls != 0 {
		t.Fatalf("native-only phase selection prepared local Weaver image %d times", calls)
	}
}

func TestRecordPhaseSetupFailurePersistsDiagnosticArtifacts(t *testing.T) {
	root := t.TempDir()
	phase := &fullPhaseContext{
		Name:             "Container Restart",
		Command:          "container-restart",
		Datastore:        "sqlite",
		Project:          "test-project",
		RootDir:          root,
		RunDir:           filepath.Join(root, "run"),
		RuntimePortsFile: filepath.Join(root, "runtime-ports.env"),
		LogTail:          &lineTail{},
	}
	startedAt := time.Now().Add(-2 * time.Second)
	setupErr := errors.New("prepare local Weaver image: fixture build failed")

	result := recordPhaseSetupFailure(phase, phase.Command, startedAt, setupErr)
	if !errors.Is(result.Err, setupErr) {
		t.Fatalf("result error = %v", result.Err)
	}
	if result.Duration < time.Second {
		t.Fatalf("result duration = %s, want setup wait", result.Duration)
	}

	logPath := filepath.Join(root, phase.Command+".log")
	logBody, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read setup failure log: %v", err)
	}
	if string(logBody) != setupErr.Error()+"\n" {
		t.Fatalf("setup failure log = %q", logBody)
	}

	statusPath := filepath.Join(root, phase.Command+".status.json")
	statusBody, err := os.ReadFile(statusPath)
	if err != nil {
		t.Fatalf("read setup failure status: %v", err)
	}
	var status phaseRunStatus
	if err := json.Unmarshal(statusBody, &status); err != nil {
		t.Fatalf("decode setup failure status: %v", err)
	}
	if status.Status != "fail" || status.Error != setupErr.Error() {
		t.Fatalf("setup failure status = %q error = %q", status.Status, status.Error)
	}
	if status.LastLogLine != setupErr.Error() || status.LogPath != logPath {
		t.Fatalf("setup failure diagnostic pointers = %#v", status)
	}
	if status.Duration != result.Duration || !status.StartedAt.Equal(startedAt) {
		t.Fatalf("setup failure timing = %s from %s", status.Duration, status.StartedAt)
	}
}

func TestFullPhaseContextsIncludeContainerAndManagedRestarts(t *testing.T) {
	tempRoot := t.TempDir()
	phases, err := newFullPhaseContexts(tempRoot)
	if err != nil {
		t.Fatalf("newFullPhaseContexts: %v", err)
	}

	type phaseKey struct {
		name      string
		command   string
		datastore string
		skipSeed  bool
	}
	got := make([]phaseKey, 0, len(phases))
	for _, phase := range phases {
		got = append(got, phaseKey{
			name:      phase.Name,
			command:   phase.Command,
			datastore: phase.Datastore,
			skipSeed:  phase.SkipSeed,
		})
		if !hasPathPrefix(phase.RootDir, tempRoot) {
			t.Fatalf("phase %s root %q is outside temp root %q", phase.Name, phase.RootDir, tempRoot)
		}
	}

	want := []phaseKey{
		{name: "Functional SQLite", command: "test-all", datastore: "sqlite"},
		{name: "Functional Postgres", command: "test-all", datastore: "postgres"},
		{name: "NNTP Chaos", command: "chaos-test", datastore: "sqlite"},
		{name: "TCP Chaos", command: "tcp-chaos", datastore: "sqlite"},
		{name: "Container Restart", command: "container-restart", datastore: "sqlite", skipSeed: true},
		{name: "Restart SQLite", command: "restart-all", datastore: "sqlite"},
		{name: "Restart Postgres", command: "restart-all", datastore: "postgres"},
		{name: "Product Behavior Gate", command: "release-gate", datastore: "sqlite", skipSeed: true},
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d phases, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("phase %d mismatch: got %#v want %#v", i, got[i], want[i])
		}
	}
}

func TestFunctionalFullPhaseContextsOnlyRunFunctionalDatastores(t *testing.T) {
	tempRoot := t.TempDir()
	phases, err := newFullPhaseContextsFor(tempRoot, functionalFullPhase)
	if err != nil {
		t.Fatalf("newFullPhaseContextsFor: %v", err)
	}

	type phaseKey struct {
		name        string
		command     string
		seedProfile string
		datastore   string
	}
	got := make([]phaseKey, 0, len(phases))
	for _, phase := range phases {
		got = append(got, phaseKey{
			name:        phase.Name,
			command:     phase.Command,
			seedProfile: phase.SeedProfile,
			datastore:   phase.Datastore,
		})
		if !hasPathPrefix(phase.RootDir, tempRoot) {
			t.Fatalf("phase %s root %q is outside temp root %q", phase.Name, phase.RootDir, tempRoot)
		}
	}

	want := []phaseKey{
		{name: "Functional SQLite", command: "test-all", seedProfile: "functional", datastore: "sqlite"},
		{name: "Functional Postgres", command: "test-all", seedProfile: "functional", datastore: "postgres"},
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d functional phases, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("functional phase %d mismatch: got %#v want %#v", i, got[i], want[i])
		}
	}
}

func TestFullPhaseContextsPreassignUniqueRuntimePorts(t *testing.T) {
	tempRoot := t.TempDir()
	phases, err := newFullPhaseContextsFor(tempRoot, functionalFullPhase)
	if err != nil {
		t.Fatalf("newFullPhaseContextsFor: %v", err)
	}

	seen := make(map[int]string)
	for _, phase := range phases {
		if err := validateRuntimePortState(phase.RuntimePorts); err != nil {
			t.Fatalf("phase %s has invalid runtime ports: %v", phase.Name, err)
		}
		for _, assignment := range runtimePortAssignments(&phase.RuntimePorts) {
			if prior := seen[*assignment.value]; prior != "" {
				t.Fatalf(
					"runtime port %d reused by %s/%s and %s/%s",
					*assignment.value,
					prior,
					assignment.name,
					phase.Name,
					assignment.name,
				)
			}
			seen[*assignment.value] = phase.Name
		}

		persisted, err := loadRuntimePortState(phase.RuntimePortsFile)
		if err != nil {
			t.Fatalf("load %s runtime ports: %v", phase.Name, err)
		}
		if persisted != phase.RuntimePorts {
			t.Fatalf("phase %s runtime ports file mismatch: got %#v want %#v", phase.Name, persisted, phase.RuntimePorts)
		}

		env := phase.env()
		if got := env["E2E_NNTP_PORT"]; got != runtimePortEnvValues(phase.RuntimePorts)["E2E_NNTP_PORT"] {
			t.Fatalf("phase %s child env missing preassigned NNTP port: got %q", phase.Name, got)
		}
	}
}

func TestPostgresFullPhaseEnvConfiguresWeaverDatastoreAndComposeDB(t *testing.T) {
	t.Setenv("E2E_SEED_JOBS", "")
	t.Setenv("E2E_WEAVER_POSTGRES_DB", "")
	t.Setenv("E2E_WEAVER_POSTGRES_USER", "")
	t.Setenv("E2E_WEAVER_POSTGRES_PASSWORD", "")

	phase := &fullPhaseContext{
		Command:          "test-all",
		Datastore:        "postgres",
		Project:          "test-project",
		SeedProfile:      "functional",
		FixturesDir:      "/tmp/fixtures",
		RunDir:           "/tmp/run",
		RuntimePortsFile: "/tmp/runtime-ports.json",
	}

	env := phase.env()
	if got := env["E2E_WEAVER_DATASTORE"]; got != "postgres" {
		t.Fatalf("expected postgres datastore env, got %q", got)
	}
	if got := env["E2E_WEAVER_POSTGRES_DB"]; got != "weaver" {
		t.Fatalf("expected Weaver Postgres DB env, got %q", got)
	}
	if got := env["E2E_WEAVER_POSTGRES_USER"]; got != "weaver" {
		t.Fatalf("expected Weaver Postgres user env, got %q", got)
	}
	if got := env["E2E_WEAVER_POSTGRES_PASSWORD"]; got != "weaver-pass" {
		t.Fatalf("expected Weaver Postgres password env, got %q", got)
	}
}

func TestDockerComposePublishesPostgresRuntimePort(t *testing.T) {
	versionCmd := exec.Command("docker", "compose", "version")
	if output, err := versionCmd.CombinedOutput(); err != nil {
		t.Skipf("docker compose unavailable: %v: %s", err, strings.TrimSpace(string(output)))
	}

	const sentinelPort = "65432"
	ports := dockerComposePostgresPorts(t, map[string]string{"E2E_WEAVER_POSTGRES_PORT": sentinelPort})
	if !hasDockerComposePort(ports, sentinelPort, 5432) {
		t.Fatalf("expected weaver-postgres to publish host port %s to target 5432, got %#v", sentinelPort, ports)
	}

	ports = dockerComposePostgresPorts(t, map[string]string{"E2E_WEAVER_POSTGRES_PORT": ""})
	if !hasDockerComposePort(ports, "0", 5432) {
		t.Fatalf("expected weaver-postgres to default to a dynamic host port for target 5432, got %#v", ports)
	}
}

type dockerComposePort struct {
	Published string `json:"published"`
	Target    int    `json:"target"`
}

func dockerComposePostgresPorts(t *testing.T, overrides map[string]string) []dockerComposePort {
	t.Helper()

	cmd := exec.Command("docker", "compose", "config", "--format", "json")
	cmd.Dir = e2eDir()
	cmd.Env = composeTestEnv(overrides)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose config: %v\n%s", err, strings.TrimSpace(string(output)))
	}

	var config struct {
		Services map[string]struct {
			Ports []dockerComposePort `json:"ports"`
		} `json:"services"`
	}
	if err := json.Unmarshal(output, &config); err != nil {
		t.Fatalf("parse docker compose config: %v\n%s", err, strings.TrimSpace(string(output)))
	}

	postgres, ok := config.Services["weaver-postgres"]
	if !ok {
		t.Fatalf("weaver-postgres service missing from docker compose config")
	}
	return postgres.Ports
}

func composeTestEnv(overrides map[string]string) []string {
	filtered := make(map[string]struct{}, len(overrides))
	for key := range overrides {
		filtered[key] = struct{}{}
	}

	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			if _, overridden := filtered[key]; overridden {
				continue
			}
		}
		env = append(env, entry)
	}
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}

func hasDockerComposePort(ports []dockerComposePort, published string, target int) bool {
	for _, port := range ports {
		if port.Published == published && port.Target == target {
			return true
		}
	}
	return false
}

func TestNNTPChaosUsesCompactRepresentativeFixtureProfile(t *testing.T) {
	seedSlugs := fixtureSlugsForSeedProfile("chaos")
	if got, want := len(chaosFixtureSlugs), 8; got != want {
		t.Fatalf("expected two four-job NNTP chaos batches, got %d fixtures: %v", got, chaosFixtureSlugs)
	}
	for _, required := range []string{
		"single-mkv",
		"rar5-multivolume",
		"rar4-encrypted",
		"7z-encrypted",
		"par2-repair",
		"split-7z",
		"obfuscated-rar",
		"large-segments",
	} {
		if !containsString(chaosFixtureSlugs, required) {
			t.Fatalf("NNTP chaos profile is missing representative fixture %q: %v", required, chaosFixtureSlugs)
		}
	}
	if !containsString(seedSlugs, chaosStatProbeSlug) {
		t.Fatalf("expected chaos seed profile to include STAT probe fixture %q", chaosStatProbeSlug)
	}
	if got, want := len(seedSlugs), 9; got != want {
		t.Fatalf("expected NNTP chaos seed profile to include eight workload fixtures plus STAT probe, got %d: %v", got, seedSlugs)
	}
	if containsString(chaosFixtureSlugs, chaosStatProbeSlug) {
		t.Fatalf("expected STAT probe fixture %q to stay out of the success chaos workload", chaosStatProbeSlug)
	}
}

func TestTCPChaosUsesHalfSizedRepresentativeFixtureProfile(t *testing.T) {
	seedSlugs := fixtureSlugsForSeedProfile("tcp-chaos")
	if len(seedSlugs) != 8 {
		t.Fatalf("expected two four-job TCP chaos batches per round, got %d fixtures: %v", len(seedSlugs), seedSlugs)
	}
	for _, required := range []string{
		"single-mkv",
		"rar5-multivolume",
		"rar4-encrypted",
		"7z-encrypted",
		"par2-repair",
		"split-7z",
		"obfuscated-rar",
		"large-segments",
	} {
		if !containsString(seedSlugs, required) {
			t.Fatalf("TCP chaos profile is missing representative fixture %q: %v", required, seedSlugs)
		}
	}
	if containsString(seedSlugs, chaosStatProbeSlug) {
		t.Fatalf("TCP chaos profile must not seed NNTP STAT-only probe %q", chaosStatProbeSlug)
	}

	for _, phase := range fullPhaseDefinitions {
		if phase.command == "tcp-chaos" && phase.seedProfile != "tcp-chaos" {
			t.Fatalf("TCP chaos full phase uses seed profile %q", phase.seedProfile)
		}
	}
}

func TestFullDashboardAlignsProgressBarsForDatastorePhaseNames(t *testing.T) {
	phaseNames := []string{
		"Functional SQLite",
		"Functional Postgres",
		"NNTP Chaos",
		"TCP Chaos",
		"Restart SQLite",
		"Restart Postgres",
	}
	dashboard := &fullDashboard{
		start: time.Unix(0, 0),
		seed: dashboardBar{
			Label:   "Seeding",
			Current: 218,
			Total:   218,
			Status:  "pass",
			Detail:  "Functional Postgres: zstd-single",
		},
		phases: make(map[string]*dashboardBar, len(phaseNames)),
		order:  append([]string(nil), phaseNames...),
	}
	for _, name := range phaseNames {
		dashboard.phases[name] = &dashboardBar{
			Label:  name,
			Status: "seeding",
			Detail: "zstd-single",
		}
	}

	frame := dashboard.buildFrameLocked()
	barColumn := -1
	checked := 0
	for _, line := range strings.Split(frame, "\n") {
		if !strings.Contains(line, "]") {
			continue
		}
		column := strings.Index(line, "[")
		if column < 0 {
			continue
		}
		if barColumn < 0 {
			barColumn = column
		} else if column != barColumn {
			t.Fatalf("progress bar column mismatch: got %d want %d in line %q", column, barColumn, line)
		}
		checked++
	}
	if checked != len(phaseNames)+1 {
		t.Fatalf("expected %d dashboard bars, checked %d in frame:\n%s", len(phaseNames)+1, checked, frame)
	}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func hasPathPrefix(path, prefix string) bool {
	rel, err := filepath.Rel(prefix, path)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func TestFullDashboardRendersOneBarPerReleaseGateFlow(t *testing.T) {
	const phase = "Product Behavior Gate"
	dashboard := newFullDashboard("weaver e2e full", []string{phase}, 0)

	dashboard.updatePhase(phase, progressEvent{Kind: "phase_total", Total: 3, Detail: "release flows"})
	for _, flow := range []string{"ui-post-processing/sqlite", "ui-post-processing/postgres", "ui-security/sqlite"} {
		dashboard.updatePhase(phase, progressEvent{Kind: "flow_pending", Name: flow, Detail: "queued"})
	}

	// Every flow gets its own bar as soon as the gate announces the set, before
	// any of them have started.
	frame := dashboard.buildFrameLocked()
	for _, flow := range []string{"ui-post-processing/sqlite", "ui-post-processing/postgres", "ui-security/sqlite"} {
		if !strings.Contains(frame, flow) {
			t.Fatalf("expected a bar for flow %q in frame:\n%s", flow, frame)
		}
	}

	dashboard.updatePhase(phase, progressEvent{Kind: "flow_start", Name: "ui-post-processing/sqlite", Detail: "running"})
	dashboard.updatePhase(phase, progressEvent{Kind: "flow_done", Status: "pass", Name: "ui-post-processing/sqlite", Detail: "1m47s"})
	dashboard.updatePhase(phase, progressEvent{Kind: "flow_done", Status: "fail", Name: "ui-security/sqlite", Detail: "42s"})

	passed := dashboard.flows[phase]["ui-post-processing/sqlite"]
	if passed.Status != "pass" || passed.Current != passed.Total {
		t.Fatalf("finished flow bar not filled: %+v", passed)
	}
	if failed := dashboard.flows[phase]["ui-security/sqlite"]; failed.Status != "fail" {
		t.Fatalf("failed flow bar status = %q, want fail", failed.Status)
	}
	if pending := dashboard.flows[phase]["ui-post-processing/postgres"]; pending.Status != "waiting" {
		t.Fatalf("untouched flow bar status = %q, want waiting", pending.Status)
	}

	// The indented flow labels must not break bar alignment.
	frame = dashboard.buildFrameLocked()
	barColumn := -1
	bars := 0
	for _, line := range strings.Split(frame, "\n") {
		// Skip the cursor-home/clear escape, which also contains a bare "[".
		if !strings.Contains(line, "]") {
			continue
		}
		column := strings.Index(line, "[")
		if column < 0 {
			continue
		}
		if barColumn < 0 {
			barColumn = column
		} else if column != barColumn {
			t.Fatalf("progress bar column mismatch: got %d want %d in line %q", column, barColumn, line)
		}
		bars++
	}
	// seed + phase + three flows
	if bars != 5 {
		t.Fatalf("expected 5 bars, got %d in frame:\n%s", bars, frame)
	}
}

// The pre-seed bootstrap reuses each phase's reserved ports before the real
// phases start, so phase port isolation must still be explicit in the plan.
func TestFullPhasesHaveIsolatedPortsAndProfiles(t *testing.T) {
	tempRoot := t.TempDir()
	phases, err := newFullPhaseContexts(tempRoot)
	if err != nil {
		t.Fatalf("build phases: %v", err)
	}

	bySlug := map[string]*fullPhaseContext{}
	for _, phase := range phases {
		bySlug[phase.Slug] = phase
	}

	for slug, profile := range map[string]string{
		"functional-sqlite":   "functional",
		"functional-postgres": "functional",
		"nntp-chaos":          "chaos",
		"tcp-chaos":           "tcp-chaos",
		"restart-sqlite":      "restart",
		"restart-postgres":    "restart",
	} {
		phase, ok := bySlug[slug]
		if !ok {
			t.Fatalf("missing %s phase", slug)
		}
		if phase.SkipSeed {
			t.Fatalf("%s unexpectedly skips pre-seeding", phase.Name)
		}
		if phase.SeedProfile != profile {
			t.Fatalf("%s seed profile = %q, want %q", phase.Name, phase.SeedProfile, profile)
		}
		if got, want := phase.env()["NNTP_PORT"], strconv.Itoa(phase.RuntimePorts.NNTPPort); got != want {
			t.Fatalf("%s NNTP_PORT = %q, want its own port %q", phase.Name, got, want)
		}
	}
}
