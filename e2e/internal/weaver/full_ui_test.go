package weaver

import (
	"encoding/json"
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
		// Seeds nothing: it reads the SQLite lane's NNTP.
		{name: "Functional Postgres", command: "test-all", datastore: "postgres", skipSeed: true},
		{name: "NNTP Chaos", command: "chaos-test", datastore: "sqlite"},
		// Seeds nothing: it proxies to the SQLite lane's NNTP.
		{name: "TCP Chaos", command: "tcp-chaos", datastore: "sqlite", skipSeed: true},
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

func TestChaosSeedProfileIncludesStatProbeOutsideSuccessWorkload(t *testing.T) {
	seedSlugs := fixtureSlugsForSeedProfile("chaos")
	if !containsString(seedSlugs, chaosStatProbeSlug) {
		t.Fatalf("expected chaos seed profile to include STAT probe fixture %q", chaosStatProbeSlug)
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

// The NNTP pairing is only safe under conditions that are easy to erode by
// editing the phase table, so they are pinned here rather than left to review:
// a borrower must not seed, must point both its consumers at the donor's
// published ports, and the donor must seed a superset profile of its own.
func TestBorrowedNNTPPhasesReadTheDonorInsteadOfSeeding(t *testing.T) {
	tempRoot := t.TempDir()
	phases, err := newFullPhaseContexts(tempRoot)
	if err != nil {
		t.Fatalf("build phases: %v", err)
	}

	bySlug := map[string]*fullPhaseContext{}
	for _, phase := range phases {
		bySlug[phase.Slug] = phase
	}

	borrowers := 0
	for _, phase := range phases {
		if phase.NNTPDonor == "" {
			continue
		}
		borrowers++
		donor, ok := bySlug[phase.NNTPDonor]
		if !ok {
			t.Fatalf("%s borrows from %q, which is not a selected phase", phase.Name, phase.NNTPDonor)
		}
		if donor.NNTPDonor != "" {
			t.Fatalf("%s borrows from %s, which is itself a borrower", phase.Name, donor.Name)
		}
		if !phase.SkipSeed {
			t.Fatalf("%s borrows an NNTP server but still seeds one", phase.Name)
		}
		if donor.SkipSeed {
			t.Fatalf("%s borrows from %s, which seeds nothing", phase.Name, donor.Name)
		}
		if phase.DonorFixturesDir != donor.FixturesDir {
			t.Fatalf(
				"%s donor fixtures = %q, want %q",
				phase.Name,
				phase.DonorFixturesDir,
				donor.FixturesDir,
			)
		}
		// The donor need not seed the same profile, but it must seed a
		// superset: every fixture the borrower's suite expects has to already
		// be on the server it reads. Checked against the real slug lists
		// rather than by comparing profile names, so narrowing a profile
		// breaks this instead of silently starving a lane.
		donorSlugs := map[string]bool{}
		for _, slug := range fixtureSlugsForSeedProfile(donor.SeedProfile) {
			donorSlugs[slug] = true
		}
		for _, slug := range fixtureSlugsForSeedProfile(phase.SeedProfile) {
			if !donorSlugs[slug] {
				t.Fatalf(
					"%s needs fixture %q, which %s (profile %q) does not seed",
					phase.Name, slug, donor.Name, donor.SeedProfile,
				)
			}
		}

		// Both consumers must be redirected: the host-side harness over
		// loopback, and the containerised Weaver through the host gateway.
		env := phase.env()
		if env["FIXTURES_DIR"] != donor.FixturesDir {
			t.Fatalf(
				"%s env[FIXTURES_DIR] = %q, want donor fixtures %q",
				phase.Name,
				env["FIXTURES_DIR"],
				donor.FixturesDir,
			)
		}
		donorPort := strconv.Itoa(donor.RuntimePorts.NNTPPort)
		for key, want := range map[string]string{
			"NNTP_HOST":            "127.0.0.1",
			"NNTP_PORT":            donorPort,
			"E2E_NNTP_CLIENT_HOST": "host.docker.internal",
			"E2E_NNTP_CLIENT_PORT": donorPort,
		} {
			if env[key] != want {
				t.Fatalf("%s env[%s] = %q, want %q", phase.Name, key, env[key], want)
			}
		}
		if env["E2E_NNTP_CLIENT_PORT"] == strconv.Itoa(phase.RuntimePorts.NNTPPort) {
			t.Fatalf("%s points Weaver at its own NNTP port instead of the donor's", phase.Name)
		}
	}

	if borrowers == 0 {
		t.Fatal("no phase borrows an NNTP server; the pairing was removed without updating this test")
	}
}
