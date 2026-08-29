package weaver

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
)

// Fixture bytes are not in git; before anything is seeded the harness makes
// sure they are on disk. The order is deliberate and the same everywhere:
// reuse what is present and matches the ledger, fetch what is missing from
// the published corpus, and generate locally only what is still missing after
// that. E2E_FIXTURES picks how far it goes:
//
//	auto   (default) fetch, then generate
//	fetch  fetch only; anything still missing fails the run
//	off    do nothing; a missing fixture fails wherever it is first read
//
// Two checks exist. A pre-flight over a whole profile re-hashes every file
// (the size alone cannot tell a stale fixture from a current one) and runs
// once at the top of full, functional, release-gate and seed-all. A quick
// size-only check runs for every fixture as it is seeded, so a scenario seeded
// on its own — adaptive-dispatch, a restart case, one `seed <dir>` — is
// covered too. The parent marks E2E_FIXTURES_CHECKED so its child phases skip
// the pre-flight they would otherwise repeat.

const (
	fixtureModeAuto  = "auto"
	fixtureModeFetch = "fetch"
	fixtureModeOff   = "off"

	fixturesCheckedEnv = "E2E_FIXTURES_CHECKED"
)

var ensureFixturesMu sync.Mutex

func fixtureMode() string {
	mode := strings.ToLower(strings.TrimSpace(env("E2E_FIXTURES", fixtureModeAuto)))
	switch mode {
	case "", fixtureModeAuto:
		return fixtureModeAuto
	case fixtureModeFetch, fixtureModeOff:
		return mode
	default:
		log.Fatalf("invalid E2E_FIXTURES=%q (expected auto|fetch|off)", mode)
		return ""
	}
}

// ensureFixtureProfiles is the pre-flight: every path in the named corpus
// profiles present and matching the ledger, digest and all.
func ensureFixtureProfiles(profiles ...string) {
	if fixtureMode() == fixtureModeOff || envBool(fixturesCheckedEnv, false) || len(profiles) == 0 {
		return
	}
	profiles = uniqueSorted(profiles)
	log.Printf("checking fixture corpus for profile(s) %s...", strings.Join(profiles, ", "))
	report := runEnsure(fixturegen.EnsureConfig{Profiles: profiles, Digest: true})
	log.Printf("fixture corpus ready: %d file(s) — %d present, %d fetched, %d generated",
		len(report.Wanted), len(report.Present), len(report.Fetched), len(report.Generated))
	// Child phases inherit the environment; they seed from the same tree and
	// need only the quick per-fixture check from here on.
	os.Setenv(fixturesCheckedEnv, "1")
}

// ensureFixtureDir is the quick check for one scenario directory about to be
// seeded: its own ledger paths and the paths of every fixture asset it stages
// from another scenario, trusted by size.
func ensureFixtureDir(absDir string) {
	if fixtureMode() == fixtureModeOff {
		return
	}
	slug := filepath.Base(absDir)
	ledger, _, err := corpus.LoadLedger(e2eDir())
	if err != nil {
		log.Fatalf("load fixture ledger: %v", err)
	}
	paths := fixturegen.ScenarioPaths(ledger, slug)
	if scenario, err := loadScenario(absDir); err == nil {
		for _, asset := range append(append([]string(nil), scenario.FixtureAssets...), scenario.BackupFixtureAssets...) {
			source, _, _ := strings.Cut(asset, "::")
			source = strings.TrimSpace(source)
			if source == "" {
				continue
			}
			if p := "testdata/" + filepath.ToSlash(source); ledgerHas(ledger, p) {
				paths = append(paths, p)
			}
		}
	}
	if len(paths) == 0 {
		return
	}
	runEnsure(fixturegen.EnsureConfig{Paths: uniqueSorted(paths)})
}

func runEnsure(config fixturegen.EnsureConfig) fixturegen.EnsureReport {
	ensureFixturesMu.Lock()
	defer ensureFixturesMu.Unlock()
	config.Root = e2eDir()
	config.NoGenerate = fixtureMode() == fixtureModeFetch
	config.Verbose = envBool("E2E_VERBOSE", false)
	config.Log = log.Writer()
	report, err := fixturegen.Ensure(context.Background(), config)
	if err != nil {
		hint := "run `go run ./cmd/corpus ensure` in e2e/ to see why"
		if config.NoGenerate {
			hint = "E2E_FIXTURES=fetch forbids local generation; unset it or run `go run ./cmd/corpus ensure` in e2e/"
		}
		log.Fatalf("fixture corpus: %v\n  %s", err, hint)
	}
	if report.LedgerChanged {
		log.Printf("note: fixtures were generated locally; %s now describes a local corpus revision (do not commit it unless publishing)", corpus.LedgerFile)
	}
	return report
}

func ledgerHas(ledger *corpus.Ledger, path string) bool {
	_, ok := ledger.Entry(path)
	return ok
}

func uniqueSorted(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
