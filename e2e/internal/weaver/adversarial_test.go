// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
)

func TestEveryAdversarialScenarioCarriesAValidContract(t *testing.T) {
	cases, err := loadAdversarialCases(testdataDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("no scenario carries an adversarial object")
	}
	groups := map[string]int{}
	for _, tc := range cases {
		groups[tc.Spec.Group]++
		if !strings.HasPrefix(tc.Scenario.Slug, "adv-") {
			t.Errorf("%s carries an adversarial object outside the adv- namespace", tc.Scenario.Slug)
		}
		if tc.Spec.Outcome == "failed" && tc.Spec.ErrorContains == "" && tc.Spec.FailureKind == "" {
			t.Errorf("%s fails without naming the verdict it expects", tc.Scenario.Slug)
		}
		if tc.Spec.RejectionMetric != "" && tc.Spec.BudgetRefusals == 0 {
			t.Errorf("%s counts a rejection the budget never warns about", tc.Scenario.Slug)
		}
	}
	for _, group := range adversarialGroups() {
		if groups[group.Name] == 0 {
			t.Errorf("no case runs in the %s group", group.Name)
		}
	}
}

func TestAdversarialProfileHoldsEveryFileTheLaneSeeds(t *testing.T) {
	root := e2eDir()
	ledger, _, err := corpus.LoadLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := corpus.LoadProfiles(root)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := profiles.Resolve("adversarial", ledger.Paths())
	if err != nil {
		t.Fatal(err)
	}
	inProfile := map[string]bool{}
	for _, path := range resolved {
		inProfile[path] = true
	}
	var wanted []string
	for _, slug := range adversarialFixtureSlugs() {
		paths := fixturegen.ScenarioPaths(ledger, slug)
		scenario, err := loadScenario(filepath.Join(testdataDir(), slug))
		if err != nil {
			t.Fatal(err)
		}
		for _, asset := range scenario.FixtureAssets {
			source, _, _ := strings.Cut(asset, "::")
			paths = append(paths, "testdata/"+strings.TrimSpace(source))
		}
		if len(paths) == 0 {
			t.Errorf("%s seeds nothing the ledger holds", slug)
		}
		wanted = append(wanted, paths...)
	}
	for _, path := range wanted {
		if !inProfile[path] {
			t.Errorf("the adversarial profile misses %s", path)
		}
	}
	wantedSet := map[string]bool{}
	for _, path := range wanted {
		wantedSet[path] = true
	}
	for _, path := range resolved {
		if !wantedSet[path] {
			t.Errorf("the adversarial profile holds %s, which the lane never seeds", path)
		}
	}
}

func TestAdversarialSeedProfileIsTheLane(t *testing.T) {
	got := fixtureSlugsForSeedProfile("adversarial")
	if got[len(got)-1] != adversarialCleanupSlug {
		t.Fatalf("the cleanup job is not seeded: %v", got)
	}
	cases, err := loadAdversarialCases(testdataDir())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[:len(got)-1], adversarialCaseSlugs(cases)) {
		t.Fatalf("seed profile %v is not the lane's cases", got)
	}
}

func TestParseAdversarialSpec(t *testing.T) {
	spec, ok, err := parseAdversarialSpec([]byte(`{"slug":"x","adversarial":{"group":"tight","outcome":"failed","errorContains":"WEAVER_EXTRACTION_REJECTED[entries]","submissions":[{"label":"wrong","password":"p"}]}}`))
	if err != nil || !ok {
		t.Fatalf("parse: ok=%v err=%v", ok, err)
	}
	if spec.Group != "tight" || spec.Outcome != "failed" || len(spec.Submissions) != 1 || spec.Submissions[0].Password != "p" {
		t.Fatalf("parsed %+v", spec)
	}
	if _, ok, err := parseAdversarialSpec([]byte(`{"slug":"plain"}`)); ok || err != nil {
		t.Fatalf("a scenario without the object: ok=%v err=%v", ok, err)
	}
	for _, raw := range []string{
		`{"adversarial":{"group":"loose","outcome":"failed"}}`,
		`{"adversarial":{"group":"default","outcome":"hangs"}}`,
		`{"adversarial":{"group":"default","outcome":"success","failureKind":"unpack"}}`,
		`{"adversarial":{"group":"default","outcome":"success","escapePaths":["tmp/x"]}}`,
	} {
		if _, _, err := parseAdversarialSpec([]byte(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestSelectAdversarialCasesKeepsTheAskedOrderAndRefusesStrangers(t *testing.T) {
	cases := []adversarialCase{
		{Scenario: &Scenario{Slug: "adv-a"}},
		{Scenario: &Scenario{Slug: "adv-b"}},
	}
	selected, err := selectAdversarialCases(cases, []string{"adv-b", "adv-a"})
	if err != nil || adversarialCaseSlugs(selected)[0] != "adv-b" {
		t.Fatalf("selected %v, err %v", adversarialCaseSlugs(selected), err)
	}
	if _, err := selectAdversarialCases(cases, []string{"single-mkv"}); err == nil {
		t.Fatal("selected a scenario that is not adversarial")
	}
}

func TestAdversarialEscapeTargetsKeepOnlyWhatLeavesTheBase(t *testing.T) {
	bases := []string{"/run/complete/job"}
	cases := map[string][]string{
		"../../adv-escape-parent.txt":        {"/run/adv-escape-parent.txt"},
		"/tmp/adv-escape-absolute.txt":       {"/tmp/adv-escape-absolute.txt"},
		`..\..\adv-escape-backslash.txt`:     {"/run/adv-escape-backslash.txt"},
		"inside/../../adv-escape-middle.txt": {"/run/complete/adv-escape-middle.txt"},
		`C:\adv-escape-drive.txt`:            nil,
		"plain.txt":                          nil,
		"..":                                 {"/run/complete"},
	}
	for name, want := range cases {
		got := adversarialEscapeTargets(name, bases)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%q escapes to %v, want %v", name, got, want)
		}
	}
	if got := adversarialEscapeBaseNames([]string{"../../a.txt", `..\b.txt`, "/tmp/a.txt", ".."}); !reflect.DeepEqual(got, []string{"a.txt", "b.txt"}) {
		t.Errorf("base names %v", got)
	}
}

func TestAdversarialNameLeaksSkipEveryOutputDirectory(t *testing.T) {
	root := t.TempDir()
	write := func(parts ...string) string {
		path := filepath.Join(append([]string{root}, parts...)...)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	write("complete", "job-1", "adv-escape-parent.txt")
	write("complete", "job-2", "nested", "adv-escape-parent.txt")
	leaked := write("adv-escape-parent.txt")
	write("intermediate", "other.txt")
	leaks, err := adversarialNameLeaks(root, []string{"adv-escape-parent.txt"},
		[]string{filepath.Join(root, "complete", "job-1"), filepath.Join(root, "complete", "job-2") + "/"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(leaks, []string{leaked}) {
		t.Fatalf("leaks %v, want only %s", leaks, leaked)
	}
}

func TestAdversarialOutputTreeRefusesLinks(t *testing.T) {
	output := t.TempDir()
	if problems := adversarialOutputTreeProblems(filepath.Join(output, "never-made")); len(problems) != 0 {
		t.Fatalf("a missing output directory is not a problem: %v", problems)
	}
	plain := filepath.Join(output, "plain.txt")
	if err := os.WriteFile(plain, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if problems := adversarialOutputTreeProblems(output); len(problems) != 0 {
		t.Fatalf("a plain tree has problems: %v", problems)
	}
	if err := os.Symlink("/tmp", filepath.Join(output, "dir-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(plain, filepath.Join(output, "hard-link")); err != nil {
		t.Fatal(err)
	}
	problems := adversarialOutputTreeProblems(output)
	var text []string
	for _, problem := range problems {
		text = append(text, problem.Error())
	}
	sort.Strings(text)
	joined := strings.Join(text, "\n")
	if !strings.Contains(joined, "symbolic link") || !strings.Contains(joined, "hard links") {
		t.Fatalf("links not flagged: %v", text)
	}
}

func TestPrometheusSamplesByLabel(t *testing.T) {
	exposition := []byte(`# HELP weaver_extraction_rejections_total Rejections.
# TYPE weaver_extraction_rejections_total counter
weaver_extraction_rejections_total{reason="member_bytes"} 2
weaver_extraction_rejections_total{reason="disk_reserve"} 0
weaver_extraction_rejections_total_other{reason="member_bytes"} 9
weaver_jobs_total 4
`)
	got := prometheusSamplesByLabel(exposition, adversarialRejectionMetric, "reason")
	want := map[string]float64{"member_bytes": 2, "disk_reserve": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("samples %v, want %v", got, want)
	}
}

func TestNzbPostedBytesSumsEverySegment(t *testing.T) {
	nzb := []byte(`<nzb><file><segments>
<segment bytes="100" number="1">a@b</segment>
<segment number="2" bytes="250">c@d</segment>
</segments></file></nzb>`)
	if got := nzbPostedBytes(nzb); got != 350 {
		t.Fatalf("posted %d bytes, want 350", got)
	}
}

func TestSetAdversarialEnvPutsTheEnvironmentBack(t *testing.T) {
	t.Setenv("WEAVER_EXTRACTION_MAX_ENTRIES", "7")
	t.Setenv("WEAVER_EXTRACTION_MAX_MEMBER_BYTES", "")
	os.Unsetenv("WEAVER_EXTRACTION_MAX_MEMBER_BYTES")
	restore := setAdversarialEnv(map[string]string{
		"WEAVER_EXTRACTION_MAX_ENTRIES":      "1000",
		"WEAVER_EXTRACTION_MAX_MEMBER_BYTES": "1",
	})
	if os.Getenv("WEAVER_EXTRACTION_MAX_ENTRIES") != "1000" || os.Getenv("WEAVER_EXTRACTION_MAX_MEMBER_BYTES") != "1" {
		t.Fatal("the group's caps were not set")
	}
	restore()
	if os.Getenv("WEAVER_EXTRACTION_MAX_ENTRIES") != "7" {
		t.Fatal("a cap set before the group was not put back")
	}
	if _, set := os.LookupEnv("WEAVER_EXTRACTION_MAX_MEMBER_BYTES"); set {
		t.Fatal("a cap unset before the group is still set")
	}
}

func TestAdversarialTimeoutReadsTheLaneBound(t *testing.T) {
	env := map[string]string{}
	getenv := func(key string) string { return env[key] }
	if got, err := adversarialTimeout(getenv); err != nil || got != adversarialDefaultTimeout {
		t.Fatalf("unset bound = %v, %v; want %v", got, err, adversarialDefaultTimeout)
	}
	env[adversarialTimeoutEnv] = "90"
	if got, err := adversarialTimeout(getenv); err != nil || got != 90*time.Second {
		t.Fatalf("bound 90 = %v, %v; want 90s", got, err)
	}
	for _, bad := range []string{"0", "-5", "1.5", "ten", "90s"} {
		env[adversarialTimeoutEnv] = bad
		if _, err := adversarialTimeout(getenv); err == nil {
			t.Fatalf("bound %q accepted", bad)
		}
	}
}
