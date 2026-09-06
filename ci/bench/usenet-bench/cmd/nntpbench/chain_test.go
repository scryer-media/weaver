package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// writeChainConfigFile puts a session description on disk and returns its path,
// so each test exercises the real loader rather than a hand-built struct.
func writeChainConfigFile(t *testing.T, config map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "chain.json")
	contents, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func minimalChainConfig() map[string]any {
	return map[string]any{
		"schema_version": ChainSchemaVersion,
		"name":           "series",
		"compose_file":   "compose.yml",
		"adapters":       "adapters.json",
		"phases": []map[string]any{{
			"name":          "B3",
			"mode":          "sequential",
			"plan":          "plan-B3.json",
			"fixtures_root": "fixtures",
			"artifacts":     "artifacts-B3",
			"server_link":   "1gbit",
			"server_rtt":    "10ms",
		}},
	}
}

func TestLoadChainConfigResolvesPathsAgainstTheConfigDirectory(t *testing.T) {
	path := writeChainConfigFile(t, minimalChainConfig())
	base := filepath.Dir(path)
	config, err := loadChainConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if config.ComposeFile != filepath.Join(base, "compose.yml") {
		t.Fatalf("compose file %q was not anchored to the config directory", config.ComposeFile)
	}
	if config.Phases[0].Plan != filepath.Join(base, "plan-B3.json") {
		t.Fatalf("plan %q was not anchored to the config directory", config.Phases[0].Plan)
	}
	if config.Phases[0].Artifacts != filepath.Join(base, "artifacts-B3") {
		t.Fatalf("artifacts %q was not anchored to the artifacts directory", config.Phases[0].Artifacts)
	}
	if config.ComposeProject != "nntp-bench" || config.ShaperService != "nntp-shaper" || config.ServerService != "nntp" {
		t.Fatalf("compose defaults were not applied: %+v", config)
	}
}

func TestLoadChainConfigKeepsAbsolutePaths(t *testing.T) {
	absolute := filepath.Join(t.TempDir(), "corpus")
	raw := minimalChainConfig()
	raw["phases"].([]map[string]any)[0]["fixtures_root"] = absolute
	config, err := loadChainConfig(writeChainConfigFile(t, raw))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if config.Phases[0].FixturesRoot != absolute {
		t.Fatalf("absolute fixtures root was rewritten to %q", config.Phases[0].FixturesRoot)
	}
}

func TestLoadChainConfigRejectsAnUnknownSchemaVersion(t *testing.T) {
	raw := minimalChainConfig()
	raw["schema_version"] = ChainSchemaVersion + 1
	if _, err := loadChainConfig(writeChainConfigFile(t, raw)); err == nil {
		t.Fatal("a future schema version was accepted")
	}
}

// A misspelled key would otherwise be dropped in silence, and the session would
// measure something other than what the operator described.
func TestLoadChainConfigRejectsAnUnknownField(t *testing.T) {
	raw := minimalChainConfig()
	raw["server_rt"] = "10ms"
	_, err := loadChainConfig(writeChainConfigFile(t, raw))
	if err == nil || !strings.Contains(err.Error(), "server_rt") {
		t.Fatalf("expected the unknown field to be named, got %v", err)
	}
}

func TestValidateChainConfigRejectsMalformedPhases(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]any)
		wantSub string
	}{
		{"unknown mode", func(raw map[string]any) {
			raw["phases"].([]map[string]any)[0]["mode"] = "drain"
		}, "is not one of"},
		{"unknown link", func(raw map[string]any) {
			raw["phases"].([]map[string]any)[0]["server_link"] = "40gbit"
		}, "unsupported server link profile"},
		{"fractional millisecond rtt", func(raw map[string]any) {
			raw["phases"].([]map[string]any)[0]["server_rtt"] = "1500us"
		}, "whole number of milliseconds"},
		{"rtt under the floor", func(raw map[string]any) {
			raw["phases"].([]map[string]any)[0]["server_rtt"] = "500us"
		}, "must be zero or between"},
		{"rtt beyond the ceiling", func(raw map[string]any) {
			raw["phases"].([]map[string]any)[0]["server_rtt"] = "30s"
		}, "exceeds"},
		{"rate on an unlimited link", func(raw map[string]any) {
			phase := raw["phases"].([]map[string]any)[0]
			phase["server_link"] = "unlimited"
			phase["server_egress_bps"] = 1000
		}, "cannot set a rate"},
		{"duplicate phase name", func(raw map[string]any) {
			phases := raw["phases"].([]map[string]any)
			second := map[string]any{}
			for key, value := range phases[0] {
				second[key] = value
			}
			second["artifacts"] = "artifacts-other"
			raw["phases"] = append(phases, second)
		}, "declared twice"},
		{"shared artifact root", func(raw map[string]any) {
			phases := raw["phases"].([]map[string]any)
			second := map[string]any{}
			for key, value := range phases[0] {
				second[key] = value
			}
			second["name"] = "P3"
			raw["phases"] = append(phases, second)
		}, "every phase needs its own artifact root"},
		{"no phases", func(raw map[string]any) {
			raw["phases"] = []map[string]any{}
		}, "declares no phases"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			raw := minimalChainConfig()
			testCase.mutate(raw)
			_, err := loadChainConfig(writeChainConfigFile(t, raw))
			if err == nil {
				t.Fatal("the malformed configuration was accepted")
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error %q does not mention %q", err, testCase.wantSub)
			}
		})
	}
}

// Zero is the one round trip below the shaper's floor that is legal: it means
// the shaper adds no delay at all.
func TestChainPhaseAcceptsAZeroRoundTrip(t *testing.T) {
	for _, rtt := range []string{"", "0s", "0ms"} {
		phase := ChainPhase{ServerLink: "10gbit", ServerRTT: rtt}
		profile, err := phase.linkProfile()
		if err != nil {
			t.Fatalf("rtt %q: %v", rtt, err)
		}
		if profile.RTTMicros != 0 {
			t.Fatalf("rtt %q resolved to %d micros", rtt, profile.RTTMicros)
		}
		if profile.EgressBitsPerSecond != 10_000_000_000 {
			t.Fatalf("rtt %q resolved to %d bits per second", rtt, profile.EgressBitsPerSecond)
		}
	}
}

func TestSelectChainPhasesKeepsTheDeclaredOrder(t *testing.T) {
	phases := []ChainPhase{{Name: "B3"}, {Name: "P3"}, {Name: "Q10"}}
	selected, err := selectChainPhases(phases, "Q10, B3")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(selected) != 2 || selected[0].Name != "B3" || selected[1].Name != "Q10" {
		t.Fatalf("selection did not preserve the declared order: %+v", selected)
	}
}

func TestSelectChainPhasesRejectsAnUndeclaredName(t *testing.T) {
	_, err := selectChainPhases([]ChainPhase{{Name: "B3"}}, "B4")
	if err == nil || !strings.Contains(err.Error(), "B4") {
		t.Fatalf("expected the undeclared phase to be named, got %v", err)
	}
}

func TestSelectChainPhasesWithoutAFilterRunsEverything(t *testing.T) {
	phases := []ChainPhase{{Name: "B3"}, {Name: "P3"}}
	selected, err := selectChainPhases(phases, "  ")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(selected) != len(phases) {
		t.Fatalf("an empty filter selected %d of %d phases", len(selected), len(phases))
	}
}

func TestChainPhaseArgsCarryEveryConfiguredFlagInAStableOrder(t *testing.T) {
	config := ChainConfig{
		Adapters: "/bench/adapters.json", Target: "docker-linux", NNTPHost: "nntp",
		ShaperControlURL: "http://127.0.0.1:8080", CAFile: "/bench/ca.pem",
		Username: "fixture-user", PasswordFile: "/bench/password", Connections: 8,
	}
	phase := ChainPhase{
		Name: "C3", Mode: "sequential", Plan: "/bench/plan.json",
		FixturesRoot: "/bench/fixtures", Artifacts: "/bench/artifacts-C3",
		NFS: &ChainNFS{Container: "nfs-1", Network: "storage", HelperImage: "helper:dev", VerifyBinary: "/bench/nntpbench"},
	}
	args := chainPhaseArgs(config, phase)
	if args[0] != "sequential" {
		t.Fatalf("the mode must lead the command line, got %q", args[0])
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"--plan /bench/plan.json", "--artifacts /bench/artifacts-C3",
		"--adapters /bench/adapters.json", "--fixtures-root /bench/fixtures",
		"--target docker-linux", "--nntp-host nntp", "--tls-ca-file /bench/ca.pem",
		"--username fixture-user", "--password-file /bench/password",
		"--shaper-control-url http://127.0.0.1:8080", "--connections 8",
		"--nfs-container nfs-1", "--nfs-network storage",
		"--nfs-helper-image helper:dev", "--nfs-verify-binary /bench/nntpbench",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("command line is missing %q: %s", want, joined)
		}
	}
	// Two builds of one phase must produce identical command lines, or the same
	// phase looks different in the log and in its recorded manifest.
	for attempt := 0; attempt < 8; attempt++ {
		if again := strings.Join(chainPhaseArgs(config, phase), " "); again != joined {
			t.Fatalf("the flag order is not stable:\n%s\n%s", joined, again)
		}
	}
}

func TestChainPhaseArgsOmitUnsetOptionalFlags(t *testing.T) {
	config := ChainConfig{Adapters: "/bench/adapters.json"}
	phase := ChainPhase{Mode: "queue", Plan: "/p.json", FixturesRoot: "/f", Artifacts: "/a"}
	joined := strings.Join(chainPhaseArgs(config, phase), " ")
	for _, absent := range []string{"--target", "--nfs-container", "--connections", "--timeout", "--username"} {
		if strings.Contains(joined, absent) {
			t.Fatalf("unset flag %s reached the command line: %s", absent, joined)
		}
	}
}

func TestChainServerEnvNameSeparatesConditions(t *testing.T) {
	cases := map[string]ChainPhase{
		"server-1gbit-rtt10ms.env":   {ServerLink: "1gbit", ServerRTT: "10ms"},
		"server-1gbit-rtt100ms.env":  {ServerLink: "1gbit", ServerRTT: "100ms"},
		"server-10gbit-rtt0s.env":    {ServerLink: "10gbit"},
		"server-unlimited-rtt0s.env": {},
	}
	for want, phase := range cases {
		if got := chainServerEnvName(phase); got != want {
			t.Fatalf("phase %+v produced %q, want %q", phase, got, want)
		}
	}
}

// The environment file records the conditions a run was measured under, so a
// second session may reuse a matching file but must never silently take over
// one describing something else.
func TestWriteChainServerEnvReusesAMatchingFileAndRefusesAConflict(t *testing.T) {
	tenMillis, err := benchmark.ResolveServerLinkProfile("1gbit", 0, 0, 10_000)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	path := filepath.Join(t.TempDir(), "link.env")
	if err := writeChainServerEnv(path, tenMillis); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := writeChainServerEnv(path, tenMillis); err != nil {
		t.Fatalf("rewriting the same conditions must be accepted, got %v", err)
	}
	hundredMillis, err := benchmark.ResolveServerLinkProfile("1gbit", 0, 0, 100_000)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	err = writeChainServerEnv(path, hundredMillis)
	if err == nil || !strings.Contains(err.Error(), "does not describe") {
		t.Fatalf("expected a conflict to be refused, got %v", err)
	}
	contents, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read back: %v", readErr)
	}
	if !strings.Contains(string(contents), "NNTP_RTT_MICROS=10000") {
		t.Fatalf("the refused write changed the file: %s", contents)
	}
}

func TestCountChainSuitesCountsOnlySuiteDirectories(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"sequential-0001", "sequential-0002", "queue-transition-0001"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "scratch"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest-0001"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := countChainSuites(root); got != 3 {
		t.Fatalf("counted %d suites, want 3", got)
	}
	if got := countChainSuites(filepath.Join(root, "missing")); got != 0 {
		t.Fatalf("a missing artifact root counted %d suites", got)
	}
}

func TestSanitizeChainNameProducesPortableFileNames(t *testing.T) {
	cases := map[string]string{
		"B3-rtt10":      "B3-rtt10",
		"B3 @ 100ms":    "B3---100ms",
		"weaver/0.11.0": "weaver-0.11.0",
		"a:b*c?d":       "a-b-c-d",
		"":              "chain",
	}
	for input, want := range cases {
		if got := sanitizeChainName(input); got != want {
			t.Fatalf("sanitize(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestShaperKeyDistinguishesConditions(t *testing.T) {
	tenMillis := ChainPhase{ServerLink: "1gbit", ServerRTT: "10ms"}
	same := ChainPhase{Name: "other", ServerLink: "1gbit", ServerRTT: "10ms"}
	if tenMillis.shaperKey() != same.shaperKey() {
		t.Fatal("two phases with identical conditions must share a shaper key")
	}
	for _, different := range []ChainPhase{
		{ServerLink: "1gbit", ServerRTT: "100ms"},
		{ServerLink: "10gbit", ServerRTT: "10ms"},
		{ServerLink: "1gbit"},
	} {
		if tenMillis.shaperKey() == different.shaperKey() {
			t.Fatalf("phase %+v must not share a shaper key with the 1gbit/10ms phase", different)
		}
	}
}

func TestAcquireChainLockRefusesASecondSession(t *testing.T) {
	dir := t.TempDir()
	release, err := acquireChainLock(dir)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	if _, err := acquireChainLock(dir); err == nil {
		t.Fatal("a second chain acquired the lock")
	}
	release()
	release2, err := acquireChainLock(dir)
	if err != nil {
		t.Fatalf("the lock was not released: %v", err)
	}
	release2()
}

func TestAdapterClientImageReadsThePinnedImage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapters.json")
	contents := `{"adapters":[
		{"client":"sabnzbd","environment":{"CLIENT_IMAGE":"sab@sha256:aaa"}},
		{"client":"weaver","environment":{"CLIENT_IMAGE":"weaver@sha256:bbb"}}]}`
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	image, err := adapterClientImage(path, "weaver")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if image != "weaver@sha256:bbb" {
		t.Fatalf("read the wrong pin: %q", image)
	}
	if _, err := adapterClientImage(path, "nzbget"); err == nil {
		t.Fatal("an undeclared client was accepted")
	}
}

func TestChainDurationDefaultsAndRejections(t *testing.T) {
	if got, err := chainDuration("", defaultPhaseSettle); err != nil || got != defaultPhaseSettle {
		t.Fatalf("empty value gave (%v, %v)", got, err)
	}
	if got, err := chainDuration("45m", 0); err != nil || got.Minutes() != 45 {
		t.Fatalf("45m gave (%v, %v)", got, err)
	}
	if _, err := chainDuration("-1s", 0); err == nil {
		t.Fatal("a negative duration was accepted")
	}
	if _, err := chainDuration("soon", 0); err == nil {
		t.Fatal("a non-duration was accepted")
	}
}

func TestExcludeFixturesDropsOnlyTheNamedFixtures(t *testing.T) {
	corpus := []string{"rar5-data", "rar4-headers", "sevenzip-lzma2", "direct-mkv"}
	kept, err := excludeFixtures(corpus, []string{"rar4-headers", "sevenzip-lzma2"})
	if err != nil {
		t.Fatalf("exclude: %v", err)
	}
	if len(kept) != 2 || kept[0] != "rar5-data" || kept[1] != "direct-mkv" {
		t.Fatalf("exclusion changed the surviving fixtures or their order: %v", kept)
	}
	// The corpus order is the plan's, and reordering it would reshuffle every
	// randomized block the seed produces.
	unchanged, err := excludeFixtures(corpus, nil)
	if err != nil || len(unchanged) != len(corpus) {
		t.Fatalf("an empty exclusion changed the corpus: %v, %v", unchanged, err)
	}
}

// A misspelled exclusion would silently keep the fixture the operator meant to
// drop, and the run would fail on it hours later.
func TestExcludeFixturesRefusesAnAbsentFixture(t *testing.T) {
	_, err := excludeFixtures([]string{"rar5-data"}, []string{"rar5-dat"})
	if err == nil || !strings.Contains(err.Error(), "rar5-dat") {
		t.Fatalf("expected the absent id to be named, got %v", err)
	}
}

func TestExcludeFixturesRefusesAnEmptyResult(t *testing.T) {
	if _, err := excludeFixtures([]string{"only"}, []string{"only"}); err == nil {
		t.Fatal("excluding every fixture produced a plan anyway")
	}
}

func planSpecPhase() ChainPhase {
	return ChainPhase{
		Name: "B3", Mode: "sequential", Plan: "plan-B3.json",
		FixturesRoot: "fixtures", Artifacts: "artifacts-B3",
		ServerLink: "1gbit", ServerRTT: "10ms",
		PlanSpec: &ChainPlanSpec{
			Fixtures:    []string{"alpha", "bravo", "charlie", "delta"},
			Transports:  []string{"tls"},
			Profile:     "equivalent-throughput",
			Repetitions: 3,
			Seed:        20260905,
		},
	}
}

// A plan built on one machine must be the plan built on the next, or two
// sessions of the same series cannot be compared with each other.
func TestBuildChainPlanIsDeterministic(t *testing.T) {
	first, err := buildChainPlan(planSpecPhase())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	second, err := buildChainPlan(planSpecPhase())
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if len(first.Runs) == 0 {
		t.Fatal("the built plan has no runs")
	}
	if len(first.Runs) != len(second.Runs) {
		t.Fatalf("two builds produced %d and %d runs", len(first.Runs), len(second.Runs))
	}
	for index := range first.Runs {
		if first.Runs[index] != second.Runs[index] {
			t.Fatalf("run %d differs between builds:\n%+v\n%+v", index, first.Runs[index], second.Runs[index])
		}
	}
}

// The plan must record the phase's own link conditions, so a plan and the
// shaper it was measured under can never disagree.
func TestBuildChainPlanTakesItsLinkFromThePhase(t *testing.T) {
	phase := planSpecPhase()
	phase.ServerLink = "10gbit"
	phase.ServerRTT = ""
	plan, err := buildChainPlan(phase)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if plan.ServerLink.ID != "10gbit" || plan.ServerLink.RTTMicros != 0 {
		t.Fatalf("the plan recorded link %+v", plan.ServerLink)
	}
}

func TestBuildChainPlanAppliesFixtureExclusions(t *testing.T) {
	phase := planSpecPhase()
	phase.PlanSpec.ExcludeFixtures = []string{"bravo"}
	plan, err := buildChainPlan(phase)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(plan.FixtureIDs) != 3 {
		t.Fatalf("the plan kept %d fixtures, want 3: %v", len(plan.FixtureIDs), plan.FixtureIDs)
	}
	for _, id := range plan.FixtureIDs {
		if id == "bravo" {
			t.Fatal("an excluded fixture reached the plan")
		}
	}
	for _, run := range plan.Runs {
		if run.FixtureID == "bravo" {
			t.Fatal("an excluded fixture reached a run")
		}
	}
}

func TestBuildChainPlanCarriesClientExclusionsWithTheirReasons(t *testing.T) {
	phase := planSpecPhase()
	phase.PlanSpec.ExcludeClients = []ChainClientExclusion{
		{Client: "sabnzbd", FixtureID: "alpha", Reason: "does not read recovery volumes"},
	}
	plan, err := buildChainPlan(phase)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(plan.ClientExclusions) != 1 {
		t.Fatalf("the plan recorded %d exclusions", len(plan.ClientExclusions))
	}
	if plan.ClientExclusions[0].Reason == "" {
		t.Fatal("the exclusion lost its reason, so an absent result would be unexplained")
	}
	for _, run := range plan.Runs {
		if run.FixtureID == "alpha" && string(run.Client) == "sabnzbd" {
			t.Fatal("an excluded client still has a run on that fixture")
		}
	}
}

func TestBuildChainPlanRejectsAnIncompleteSpec(t *testing.T) {
	noProfile := planSpecPhase()
	noProfile.PlanSpec.Profile = ""
	if _, err := buildChainPlan(noProfile); err == nil {
		t.Fatal("a spec without a profile was accepted")
	}
	noFixtures := planSpecPhase()
	noFixtures.PlanSpec.Fixtures = nil
	if _, err := buildChainPlan(noFixtures); err == nil {
		t.Fatal("a spec naming neither fixtures nor a corpus was accepted")
	}
	badReason := planSpecPhase()
	badReason.PlanSpec.ExcludeClients = []ChainClientExclusion{{Client: "sabnzbd", FixtureID: "alpha"}}
	if _, err := buildChainPlan(badReason); err == nil {
		t.Fatal("a client exclusion without a reason was accepted")
	}
}

// An existing plan is the record of what a past session measured, so a rerun
// must reuse it rather than rebuild over it.
func TestBuildChainPlansLeavesAnExistingPlanAlone(t *testing.T) {
	dir := t.TempDir()
	phase := planSpecPhase()
	phase.Plan = filepath.Join(dir, "plan-B3.json")
	original := []byte(`{"schema_version":6}`)
	if err := os.WriteFile(phase.Plan, original, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	quiet := func(string, ...any) {}
	if _, err := buildChainPlans([]ChainPhase{phase}, false, quiet); err != nil {
		t.Fatalf("build: %v", err)
	}
	after, err := os.ReadFile(phase.Plan)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(after) != string(original) {
		t.Fatal("an existing plan was rewritten")
	}
}

func TestBuildChainPlansWritesNothingOnADryRun(t *testing.T) {
	dir := t.TempDir()
	phase := planSpecPhase()
	phase.Plan = filepath.Join(dir, "plan-B3.json")
	quiet := func(string, ...any) {}
	built, err := buildChainPlans([]ChainPhase{phase}, true, quiet)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if _, ok := built["B3"]; !ok {
		t.Fatal("a dry run did not build the plan in memory")
	}
	if _, err := os.Stat(phase.Plan); err == nil {
		t.Fatal("a dry run wrote a plan to disk")
	}
	if _, err := buildChainPlans([]ChainPhase{phase}, false, quiet); err != nil {
		t.Fatalf("real run: %v", err)
	}
	if _, err := os.Stat(phase.Plan); err != nil {
		t.Fatalf("the real run did not write the plan: %v", err)
	}
}

// A summary reads every suite under its root, so reusing a root would report
// this session pooled with the last one as a single result.
func TestCheckChainArtifactRootsRefusesAPopulatedRoot(t *testing.T) {
	dir := t.TempDir()
	fresh := ChainPhase{Name: "B3", Artifacts: filepath.Join(dir, "artifacts-B3")}
	if err := checkChainArtifactRoots([]ChainPhase{fresh}); err != nil {
		t.Fatalf("an absent artifact root must be accepted, got %v", err)
	}
	if err := os.MkdirAll(filepath.Join(fresh.Artifacts, "sequential-0001"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err := checkChainArtifactRoots([]ChainPhase{fresh})
	if err == nil || !strings.Contains(err.Error(), "already holds") {
		t.Fatalf("expected a populated root to be refused, got %v", err)
	}
}

func TestFixtureSetsAreSharedByThePhasesThatNameThem(t *testing.T) {
	raw := minimalChainConfig()
	raw["fixture_sets"] = map[string][]string{"full": {"alpha", "bravo", "charlie"}}
	phase := raw["phases"].([]map[string]any)[0]
	phase["plan_spec"] = map[string]any{
		"fixture_set": "full", "exclude_fixtures": []string{"bravo"},
		"profile": "equivalent-throughput", "repetitions": 3, "seed": 20260905,
		"transports": []string{"tls"},
	}
	config, err := loadChainConfig(writeChainConfigFile(t, raw))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	resolved := config.Phases[0].PlanSpec.Fixtures
	if len(resolved) != 3 || resolved[0] != "alpha" {
		t.Fatalf("the named set did not reach the spec: %v", resolved)
	}
	plan, err := buildChainPlan(config.Phases[0])
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(plan.FixtureIDs) != 2 {
		t.Fatalf("the plan kept %d fixtures after the exclusion: %v", len(plan.FixtureIDs), plan.FixtureIDs)
	}
}

func TestFixtureSetsRejectAnUndeclaredName(t *testing.T) {
	raw := minimalChainConfig()
	phase := raw["phases"].([]map[string]any)[0]
	phase["plan_spec"] = map[string]any{"fixture_set": "absent", "profile": "stock", "repetitions": 1, "seed": 1}
	_, err := loadChainConfig(writeChainConfigFile(t, raw))
	if err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("expected the undeclared set to be named, got %v", err)
	}
}
