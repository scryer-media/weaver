package weaver

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestWeaverPlaywrightAuditUsesOwnedProject(t *testing.T) {
	var gotCommand *exec.Cmd
	var gotSummary string
	err := runWeaverPlaywrightAuditWith(func(cmd *exec.Cmd, summary string) error {
		gotCommand = cmd
		gotSummary = summary
		return nil
	})
	if err != nil {
		t.Fatalf("run Weaver Playwright audit: %v", err)
	}
	if gotCommand == nil {
		t.Fatal("Weaver Playwright audit did not execute a command")
	}
	if want := []string{"npm", "run", "audit"}; !slices.Equal(gotCommand.Args, want) {
		t.Fatalf("Weaver Playwright audit args = %q, want %q", gotCommand.Args, want)
	}
	if want := filepath.Join(e2eDir(), "playwright-weaver"); gotCommand.Dir != want {
		t.Fatalf("Weaver Playwright audit dir = %q, want %q", gotCommand.Dir, want)
	}
	if gotSummary != "npm run audit (Weaver Playwright)" {
		t.Fatalf("Weaver Playwright audit summary = %q", gotSummary)
	}
}

func TestWeaverReleaseFlowRegistryIsUniqueAndSelectable(t *testing.T) {
	if len(weaverReleaseFlowSpecs) == 0 {
		t.Fatal("Weaver release flow registry is empty")
	}

	seen := make(map[string]struct{}, len(weaverReleaseFlowSpecs))
	for _, want := range weaverReleaseFlowSpecs {
		if want.Name == "" {
			t.Fatal("registered Weaver release flow has an empty name")
		}
		if _, duplicate := seen[want.Name]; duplicate {
			t.Fatalf("duplicate Weaver release flow %q", want.Name)
		}
		seen[want.Name] = struct{}{}

		got, ok := weaverReleaseFlowSpecFor(want.Name)
		if !ok {
			t.Fatalf("registered Weaver release flow %q is not individually selectable", want.Name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("selected Weaver release flow %q = %#v, want %#v", want.Name, got, want)
		}
		if want.Timeout <= 0 {
			t.Fatalf("registered Weaver release flow %q has no timeout", want.Name)
		}
		if len(want.Artifacts) == 0 {
			t.Fatalf("registered Weaver release flow %q has no artifact contract", want.Name)
		}
		if want.Kind != weaverReleaseFlowCommand {
			if !slices.Contains(want.Services, "weaver") {
				t.Fatalf("registered Weaver release flow %q does not start Weaver", want.Name)
			}
			specPath := filepath.Join(
				weaverE2ETestRoot(t),
				"playwright-weaver",
				"tests",
				want.PlaywrightScript+".spec.ts",
			)
			if _, err := os.Stat(specPath); err != nil {
				t.Fatalf("registered Weaver release flow %q has no Playwright spec %s: %v", want.Name, specPath, err)
			}
		}

		resolved, err := resolveWeaverReleaseGateSpecs(want.Name)
		if err != nil {
			t.Fatalf("resolve Weaver release flow %q: %v", want.Name, err)
		}
		if len(resolved) != 1 || resolved[0].Name != want.Name {
			t.Fatalf("resolved Weaver release flow %q = %#v", want.Name, resolved)
		}
	}

	all, err := resolveWeaverReleaseGateSpecs("all")
	if err != nil {
		t.Fatalf("resolve all Weaver release flows: %v", err)
	}
	if len(all) != len(weaverReleaseFlowSpecs) {
		t.Fatalf("all resolved %d Weaver flows, want %d", len(all), len(weaverReleaseFlowSpecs))
	}
	for _, spec := range all {
		if _, ok := seen[spec.Name]; !ok {
			t.Fatalf("all contains unregistered Weaver flow %q", spec.Name)
		}
	}

	matrix, err := resolveWeaverReleaseGateSpecs("datastore-matrix")
	if err != nil {
		t.Fatalf("resolve Weaver datastore matrix: %v", err)
	}
	if !reflect.DeepEqual(matrix, all) {
		t.Fatalf("datastore-matrix and all resolved different flow registries")
	}
	if _, err := resolveWeaverReleaseGateSpecs("not-a-weaver-flow"); err == nil {
		t.Fatal("unknown Weaver release flow unexpectedly resolved")
	}
}

func TestWeaverIngressPrivateNetworkOverrideIsFlowScoped(t *testing.T) {
	const key = "WEAVER_RSS_ALLOW_PRIVATE_NETWORK"
	for _, spec := range weaverReleaseFlowSpecs {
		value, configured := spec.Env[key]
		if spec.Name == "ui-ingress-automation" {
			if !configured || value != "true" {
				t.Fatalf("%s %s = %q, want true", spec.Name, key, value)
			}
			continue
		}
		if configured {
			t.Fatalf("%s unexpectedly configures %s", spec.Name, key)
		}
	}
}

func TestWeaverReleaseConsoleReopensLatestRun(t *testing.T) {
	root := t.TempDir()
	t.Setenv("E2E_WEAVER_RELEASE_GATE_ROOT", root)
	runDir := filepath.Join(root, "run-1")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := weaverReleaseManifest{
		Version: 1,
		Mode:    "all",
		Status:  "passed",
		RunDir:  runDir,
		Phases: []*weaverReleasePhase{
			{
				Flow:      "ui-settings-crud",
				Datastore: "sqlite",
				RootDir:   filepath.Join(runDir, "ui-settings-crud-sqlite"),
			},
		},
	}
	if err := writeWeaverReleaseJSON(filepath.Join(runDir, "release-gate.json"), manifest); err != nil {
		t.Fatal(err)
	}
	if err := writeWeaverReleaseLatestPointer(runDir); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveWeaverReleaseConsoleRunDir("latest")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != runDir {
		t.Fatalf("latest console run = %q, want %q", resolved, runDir)
	}

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	weaverReleaseConsoleHandler(runDir).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console status = %d, body: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "ui-settings-crud / sqlite") {
		t.Fatalf("console body does not index flow: %s", response.Body.String())
	}
}

func TestWeaverReleaseArtifactIndex(t *testing.T) {
	root := t.TempDir()
	phase := &weaverReleasePhase{
		Flow:         "ui-security",
		RootDir:      root,
		ArtifactsDir: filepath.Join(root, "artifacts"),
	}
	if err := os.MkdirAll(filepath.Join(phase.ArtifactsDir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(phase.ArtifactsDir, "nested", "trace.zip"), []byte("trace"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeWeaverReleaseArtifactIndex(phase); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(phase.ArtifactsDir, "artifact-index.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "artifacts/nested/trace.zip") {
		t.Fatalf("artifact index does not include nested trace: %s", body)
	}
}

func TestWeaverReleaseTimingThresholdStartsAfterThreeBaselines(t *testing.T) {
	t.Setenv("E2E_WEAVER_RELEASE_GATE_ROOT", t.TempDir())
	t.Setenv("E2E_WEAVER_RELEASE_GATE_MAX_MINUTES", "45")
	for run := 1; run <= 3; run++ {
		if err := recordWeaverReleaseTimingBaseline(46 * time.Minute); err != nil {
			t.Fatalf("baseline run %d enforced too early: %v", run, err)
		}
	}
	if err := recordWeaverReleaseTimingBaseline(46 * time.Minute); err == nil {
		t.Fatal("fourth over-threshold release run was not rejected")
	}
}

func TestWeaverReleaseFlowDatastoreMatrix(t *testing.T) {
	backupDirections := map[string][2]string{
		"ui-backup-restore-sqlite-to-sqlite":     {"sqlite", "sqlite"},
		"ui-backup-restore-sqlite-to-postgres":   {"sqlite", "postgres"},
		"ui-backup-restore-postgres-to-sqlite":   {"postgres", "sqlite"},
		"ui-backup-restore-postgres-to-postgres": {"postgres", "postgres"},
	}

	for _, spec := range weaverReleaseFlowSpecs {
		if direction, backup := backupDirections[spec.Name]; backup {
			if len(spec.Datastores) != 1 || string(spec.Datastores[0]) != direction[1] {
				t.Fatalf("%s target datastores = %v, want [%s]", spec.Name, spec.Datastores, direction[1])
			}
			if got := spec.Env["E2E_WEAVER_BACKUP_SOURCE_DATASTORE"]; got != direction[0] {
				t.Fatalf("%s source datastore = %q, want %q", spec.Name, got, direction[0])
			}
			if got := spec.Env["E2E_WEAVER_BACKUP_TARGET_DATASTORE"]; got != direction[1] {
				t.Fatalf("%s target datastore env = %q, want %q", spec.Name, got, direction[1])
			}
			continue
		}

		switch spec.Name {
		case "adaptive-dispatch":
			if !reflect.DeepEqual(spec.Datastores, []weaverDatastore{weaverDatastoreSQLite}) {
				t.Fatalf("adaptive-dispatch datastores = %v, want SQLite only", spec.Datastores)
			}
		default:
			if !reflect.DeepEqual(spec.Datastores, releaseDatastoreMatrix()) {
				t.Fatalf("%s datastores = %v, want SQLite and PostgreSQL", spec.Name, spec.Datastores)
			}
		}
	}

	for name := range backupDirections {
		if _, ok := weaverReleaseFlowSpecFor(name); !ok {
			t.Fatalf("backup direction %q is not registered", name)
		}
	}
}

func TestWeaverReleasePhasesHaveIndependentOwnership(t *testing.T) {
	runDir := t.TempDir()
	specs, err := resolveWeaverReleaseGateSpecs("all")
	if err != nil {
		t.Fatal(err)
	}
	phases, err := newWeaverReleasePhases(runDir, specs)
	if errors.Is(err, syscall.EPERM) {
		t.Skipf("runtime-port reservation is not permitted in this sandbox: %v", err)
	}
	if err != nil {
		t.Fatalf("create Weaver release phases: %v", err)
	}

	const wantPhaseCount = 25
	if len(phases) != wantPhaseCount {
		t.Fatalf("release phase count = %d, want %d", len(phases), wantPhaseCount)
	}

	projects := make(map[string]struct{}, len(phases))
	ownedPaths := make(map[string]struct{}, len(phases)*5)
	ports := make(map[int]string, len(phases)*14)
	for _, phase := range phases {
		assertUniqueReleaseValue(t, projects, phase.Project, "Compose project")
		for label, path := range map[string]string{
			"root":         phase.RootDir,
			"run":          phase.RunDir,
			"fixtures":     phase.FixturesDir,
			"artifacts":    phase.ArtifactsDir,
			"runtime-port": phase.RuntimePortsFile,
		} {
			assertUniqueReleaseValue(t, ownedPaths, path, label+" path")
			relative, err := filepath.Rel(runDir, path)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				t.Fatalf("%s path %q escapes release run %q", label, path, runDir)
			}
		}
		for _, assignment := range runtimePortAssignments(&phase.RuntimePorts) {
			if previous, duplicate := ports[*assignment.value]; duplicate {
				t.Fatalf("runtime port %d for %s/%s is already owned by %s", *assignment.value, phase.Flow, assignment.name, previous)
			}
			ports[*assignment.value] = phase.Flow + "/" + phase.Datastore + "/" + assignment.name
		}
	}
}

func TestWeaverReleaseGateJobsClamp(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		flowCount int
		want      int
	}{
		{name: "default", value: "", flowCount: 20, want: 8},
		{name: "lower-bound", value: "0", flowCount: 20, want: 1},
		{name: "upper-bound", value: "99", flowCount: 20, want: 16},
		{name: "flow-count", value: "12", flowCount: 3, want: 3},
		{name: "invalid-uses-default", value: "invalid", flowCount: 20, want: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("E2E_WEAVER_RELEASE_GATE_JOBS", tt.value)
			if got := weaverReleaseGateJobs(tt.flowCount); got != tt.want {
				t.Fatalf("weaverReleaseGateJobs(%d) = %d, want %d", tt.flowCount, got, tt.want)
			}
		})
	}
}

func TestWeaverCleanupUsesExactComposeProject(t *testing.T) {
	t.Setenv("E2E_PROJECT", "weaver-release-flow-a")
	gotA := dockerComposeArgs("down", "-v", "--remove-orphans")
	wantA := []string{"compose", "-p", "weaver-release-flow-a", "down", "-v", "--remove-orphans"}
	if !slices.Equal(gotA, wantA) {
		t.Fatalf("flow A cleanup args = %q, want %q", gotA, wantA)
	}

	t.Setenv("E2E_PROJECT", "weaver-release-flow-b")
	gotB := dockerComposeArgs("down", "-v", "--remove-orphans")
	wantB := []string{"compose", "-p", "weaver-release-flow-b", "down", "-v", "--remove-orphans"}
	if !slices.Equal(gotB, wantB) {
		t.Fatalf("flow B cleanup args = %q, want %q", gotB, wantB)
	}
	if slices.Equal(gotA, gotB) {
		t.Fatal("independent Weaver flows resolved identical cleanup targets")
	}
}

func TestWeaverReleaseCleanupRejectsMissingExactProject(t *testing.T) {
	t.Setenv("E2E_PROJECT", "")
	err := cleanupExactWeaverReleaseProject()
	if err == nil || !strings.Contains(err.Error(), "E2E_PROJECT is required") {
		t.Fatalf("cleanup without an exact project returned %v", err)
	}
}

func TestWeaverEncryptionKeyFaultSpecsAreExactAndComplete(t *testing.T) {
	specs := weaverEncryptionKeyFaultSpecs()
	if got, want := len(specs), 3; got != want {
		t.Fatalf("encryption key fault count = %d, want %d", got, want)
	}
	wantStages := []string{"missing", "corrupt", "unreadable"}
	for index, spec := range specs {
		if spec.Stage != wantStages[index] {
			t.Fatalf("encryption key fault %d stage = %q, want %q", index, spec.Stage, wantStages[index])
		}
		if !strings.Contains(spec.MutationScript, "/data/encryption.key") {
			t.Fatalf("encryption key fault %s does not target the exact key path", spec.Stage)
		}
		if len(spec.ExpectedLogMarkers) == 0 {
			t.Fatalf("encryption key fault %s has no fail-closed log oracle", spec.Stage)
		}
	}
}

func TestParseWeaverContainerRuntimeState(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   weaverContainerRuntimeState
	}{
		{
			name:   "failed startup",
			output: "exited\tunhealthy\n",
			want:   weaverContainerRuntimeState{Status: "exited", Health: "unhealthy"},
		},
		{
			name:   "exited without health",
			output: "exited\t\n",
			want:   weaverContainerRuntimeState{Status: "exited"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWeaverContainerRuntimeState(tt.output)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("runtime state = %+v, want %+v", got, tt.want)
			}
		})
	}
	if _, err := parseWeaverContainerRuntimeState("\t\n"); err == nil {
		t.Fatal("empty runtime state unexpectedly parsed")
	}
}

func TestParseAndValidateWeaverEncryptionKeyFaultStates(t *testing.T) {
	const original = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	const corrupt = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	missing, err := parseWeaverEncryptionKeyVolumeState("missing\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWeaverEncryptionKeyFaultState("missing", original, missing); err != nil {
		t.Fatal(err)
	}

	corruptState, err := parseWeaverEncryptionKeyVolumeState(
		"present\n" + corrupt + "  /data/encryption.key\n600\n",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWeaverEncryptionKeyFaultState("corrupt", original, corruptState); err != nil {
		t.Fatal(err)
	}

	unreadable, err := parseWeaverEncryptionKeyVolumeState(
		"present\n" + original + "  /data/encryption.key\n0\n",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWeaverEncryptionKeyFaultState("unreadable", original, unreadable); err != nil {
		t.Fatal(err)
	}
	if err := validateWeaverEncryptionKeyFaultState("unreadable", original, corruptState); err == nil {
		t.Fatal("wrong unreadable-key fingerprint and mode unexpectedly validated")
	}
}

func TestWeaverCoverageLedgerOwnersAreRegistered(t *testing.T) {
	type coverageEntry struct {
		Name      string `json:"name"`
		Path      string `json:"path"`
		Owner     string `json:"owner"`
		Oracle    string `json:"oracle"`
		Rationale string `json:"rationale"`
	}
	var ledger struct {
		Version   int             `json:"version"`
		Routes    []coverageEntry `json:"routes"`
		Mutations []coverageEntry `json:"mutations"`
		Behaviors []coverageEntry `json:"behaviors"`
	}
	body, err := os.ReadFile(filepath.Join(weaverE2ETestRoot(t), "playwright-weaver", "coverage-ledger.v1.json"))
	if err != nil {
		t.Fatalf("read Weaver coverage ledger: %v", err)
	}
	if err := json.Unmarshal(body, &ledger); err != nil {
		t.Fatalf("decode Weaver coverage ledger: %v", err)
	}
	if ledger.Version != 1 {
		t.Fatalf("coverage ledger version = %d, want 1", ledger.Version)
	}

	owners := map[string]struct{}{"existing-download-pipeline": {}}
	for _, spec := range weaverReleaseFlowSpecs {
		owners[spec.Name] = struct{}{}
	}
	validOracles := map[string]struct{}{
		"browser":           {},
		"api-metrics":       {},
		"existing-pipeline": {},
		"unit-only":         {},
	}
	seen := make(map[string]string)
	for section, entries := range map[string][]coverageEntry{
		"route":    ledger.Routes,
		"mutation": ledger.Mutations,
		"behavior": ledger.Behaviors,
	} {
		if len(entries) == 0 {
			t.Fatalf("coverage ledger has no %s entries", section)
		}
		for _, entry := range entries {
			key := entry.Name
			if section == "route" {
				key = entry.Path
			}
			if key == "" {
				t.Fatalf("coverage ledger %s entry has no key: %#v", section, entry)
			}
			qualified := section + ":" + key
			if previous, duplicate := seen[qualified]; duplicate {
				t.Fatalf("duplicate coverage ledger entry %s (previous owner %s)", qualified, previous)
			}
			seen[qualified] = entry.Owner
			if _, ok := owners[entry.Owner]; !ok {
				t.Fatalf("coverage ledger %s owner %q is neither a registered Weaver flow nor an existing pipeline owner", qualified, entry.Owner)
			}
			if _, ok := validOracles[entry.Oracle]; !ok {
				t.Fatalf("coverage ledger %s has invalid oracle %q", qualified, entry.Oracle)
			}
			if entry.Oracle == "unit-only" && strings.TrimSpace(entry.Rationale) == "" {
				t.Fatalf("coverage ledger %s is unit-only without a rationale", qualified)
			}
		}
	}
}

func TestWeaverComposeKeepsPrivateRSSDisabledByDefault(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(weaverE2ETestRoot(t), "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker compose file: %v", err)
	}
	const mapping = `WEAVER_RSS_ALLOW_PRIVATE_NETWORK: "${WEAVER_RSS_ALLOW_PRIVATE_NETWORK:-false}"`
	if !strings.Contains(string(body), mapping) {
		t.Fatalf("Weaver Compose RSS private-network mapping is missing secure default %q", mapping)
	}
}

func TestWeaverPlaywrightComposeServiceIsSelfContained(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(weaverE2ETestRoot(t), "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker compose file: %v", err)
	}
	var serviceLines []string
	inService := false
	for _, line := range strings.Split(string(body), "\n") {
		if line == "  weaver-playwright:" {
			inService = true
			serviceLines = append(serviceLines, line)
			continue
		}
		if inService && strings.HasPrefix(line, "  ") && len(line) > 2 && line[2] != ' ' {
			break
		}
		if inService {
			serviceLines = append(serviceLines, line)
		}
	}
	if len(serviceLines) == 0 {
		t.Fatal("weaver-playwright service is missing")
	}
	service := strings.Join(serviceLines, "\n")
	if !strings.Contains(service, "context: ./playwright-weaver") {
		t.Fatalf("weaver-playwright does not use its Weaver-owned build context:\n%s", service)
	}
	if !strings.Contains(service, "weaver-e2e-playwright:local") {
		t.Fatalf("weaver-playwright does not default to its Weaver-owned image tag:\n%s", service)
	}
	// Every path this service names is resolved against the harness directory.
	// None of them may climb above it, or a run stops being reproducible from a
	// clean checkout of this repository alone.
	if strings.Contains(service, "../") {
		t.Fatalf("weaver-playwright service reaches outside the harness directory:\n%s", service)
	}
}

func assertUniqueReleaseValue(t *testing.T, seen map[string]struct{}, value, label string) {
	t.Helper()
	if value == "" {
		t.Fatalf("%s is empty", label)
	}
	if _, duplicate := seen[value]; duplicate {
		t.Fatalf("%s %q is shared by multiple Weaver release phases", label, value)
	}
	seen[value] = struct{}{}
}

func weaverE2ETestRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve Weaver release test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
