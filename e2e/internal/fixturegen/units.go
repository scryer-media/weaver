package fixturegen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// The publish workflow generates the corpus one runner per unit, the way the
// rarpar corpus workflow does. A unit is a recipe family; the one family whose
// bytes every other family derives from is stage 0 and runs first.
//
// Stage 0 is the shared clips plus the artifact cache. The cache is the real
// hub: the encoder is not byte-reproducible — two encodes of the same clip
// spec differ — and 139 ledger entries record an encoded clip (or an archive
// of one) as their input, with scenario.json pinning the extracted bytes.
// Every archive of a clip therefore has to be an archive of the clip that gets
// published, so the cache is built once and handed to every stage-1 runner.

// SharedFamily is the stage-0 unit: the clips under testdata/shared/.
const SharedFamily = "shared clips"

// Unit is one generation job: a recipe family, the slugs it owns, and the
// pinned toolchain ids that write its bytes (from the ledger, which records
// per fixture what actually wrote it).
type Unit struct {
	// Name is the recipe family, verbatim.
	Name string `json:"name"`
	// Key is the family as a job-safe token (lowercase, hyphenated), for
	// matrix names and artifact names. FamilySelector accepts either form.
	Key string `json:"key"`
	// Stage 0 runs first and alone; stage 1 fans out.
	Stage int `json:"stage"`
	// Slugs are the scenario directories the unit generates, sorted.
	Slugs []string `json:"slugs"`
	// Toolchains are the pinned ids the ledger credits to this unit's bytes,
	// sorted. A runner builds these images and no others.
	Toolchains []string `json:"toolchains"`
}

// Listing is what `fixturegen --list-json` prints: the publish workflow's
// generation matrix, derived from the same recipe table the generators run
// from, so a recipe added there gets a runner without anyone editing YAML.
type Listing struct {
	// Units, stage 0 first, then stage 1 by family name.
	Units []Unit `json:"units"`
	// ArtifactToolchains are the pinned ids the artifact-cache builders
	// invoke, sorted. Stage 0 builds these images too: it produces the whole
	// cache, not only its own fixtures.
	ArtifactToolchains []string `json:"artifact_toolchains"`
}

// FamilyKey is the family as a job-safe token: lowercase, every run of
// non-alphanumerics a single hyphen ("RAR recovery volumes" →
// "rar-recovery-volumes").
func FamilyKey(family string) string {
	var out strings.Builder
	hyphen := false
	for _, r := range strings.ToLower(family) {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			out.WriteRune(r)
			hyphen = false
		default:
			if !hyphen && out.Len() > 0 {
				out.WriteByte('-')
				hyphen = true
			}
		}
	}
	return strings.TrimSuffix(out.String(), "-")
}

// Units derives the generation matrix from the recipe table and the ledger.
func Units(root string) (Listing, error) {
	ledger, _, err := corpus.LoadLedger(root)
	if err != nil {
		return Listing{}, err
	}
	slugsByFamily := map[string][]string{}
	familyBySlug := map[string]string{}
	for _, recipe := range Recipes() {
		slugsByFamily[recipe.Family] = append(slugsByFamily[recipe.Family], recipe.Slug)
		familyBySlug[recipe.Slug] = recipe.Family
	}
	toolchains := map[string]map[string]struct{}{}
	for _, path := range ledger.Paths() {
		entry, _ := ledger.Entry(path)
		slug := pathSlug(path)
		family, ok := familyBySlug[slug]
		if !ok {
			return Listing{}, fmt.Errorf("%s: ledger path %s belongs to no recipe", corpus.LedgerFile, path)
		}
		set := toolchains[family]
		if set == nil {
			set = map[string]struct{}{}
			toolchains[family] = set
		}
		for _, id := range entry.Source.Toolchains {
			set[id] = struct{}{}
		}
	}
	listing := Listing{ArtifactToolchains: artifactToolchains()}
	for family, slugs := range slugsByFamily {
		sort.Strings(slugs)
		stage := 1
		if family == SharedFamily {
			stage = 0
		}
		listing.Units = append(listing.Units, Unit{
			Name:       family,
			Key:        FamilyKey(family),
			Stage:      stage,
			Slugs:      slugs,
			Toolchains: sortedKeys(toolchains[family]),
		})
	}
	sort.Slice(listing.Units, func(left, right int) bool {
		if listing.Units[left].Stage != listing.Units[right].Stage {
			return listing.Units[left].Stage < listing.Units[right].Stage
		}
		return listing.Units[left].Name < listing.Units[right].Name
	})
	return listing, nil
}

// FamilySelector resolves family names or keys to their slugs.
func FamilySelector(families []string) ([]string, error) {
	byName := map[string][]string{}
	for _, recipe := range Recipes() {
		byName[recipe.Family] = append(byName[recipe.Family], recipe.Slug)
		byName[FamilyKey(recipe.Family)] = append(byName[FamilyKey(recipe.Family)], recipe.Slug)
	}
	var slugs []string
	for _, family := range families {
		matched, ok := byName[family]
		if !ok {
			known := map[string]struct{}{}
			for _, recipe := range Recipes() {
				known[FamilyKey(recipe.Family)] = struct{}{}
			}
			return nil, fmt.Errorf("no recipe family %q (known: %s)", family, strings.Join(sortedKeys(known), ", "))
		}
		slugs = append(slugs, matched...)
	}
	sort.Strings(slugs)
	return unique(slugs), nil
}

// LedgerPathsForSlugs partitions the ledger the way rarpar's `test-corpus
// paths --generator` does: every fixture path a slug owns, from the ledger and
// nothing else, so what a generation job packs is exactly what the ledger
// describes. WithScenarios appends each slug's scenario.json when the checkout
// carries one — generation rewrites the pinned output digests in those files,
// and the rewritten copies have to travel with the bytes they describe.
func LedgerPathsForSlugs(root string, slugs []string, withScenarios bool) ([]string, error) {
	ledger, _, err := corpus.LoadLedger(root)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, slug := range slugs {
		owned := ScenarioPaths(ledger, slug)
		if len(owned) == 0 {
			if _, only := ScenarioOnly[slug]; !only {
				return nil, fmt.Errorf("no ledger path is under testdata/%s/", slug)
			}
		}
		paths = append(paths, owned...)
		if withScenarios {
			scenario := "testdata/" + slug + "/scenario.json"
			if _, err := os.Stat(corpus.HostPath(root, scenario)); err == nil {
				paths = append(paths, scenario)
			}
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// VerifyProduced is what a generation job proves before it hands its share of
// the corpus on: every path it owns is present as non-empty bytes, and it
// produced nothing the ledger does not list. The two failures this catches are
// a recipe that quietly stopped writing a fixture, and one that writes a
// fixture nobody ledgered — which would travel no further than the runner it
// was written on.
func VerifyProduced(root string, paths []string) error {
	ledger, _, err := corpus.LoadLedger(root)
	if err != nil {
		return err
	}
	var problems []string
	for _, path := range paths {
		if strings.HasSuffix(path, "/scenario.json") {
			continue
		}
		info, err := os.Stat(corpus.HostPath(root, path))
		switch {
		case err != nil:
			problems = append(problems, path+": no recipe produced it")
		case !info.Mode().IsRegular():
			problems = append(problems, path+": is not a regular file")
		case info.Size() == 0:
			problems = append(problems, path+": was produced empty")
		}
	}
	listed := map[string]struct{}{}
	for _, path := range ledger.Paths() {
		listed[path] = struct{}{}
	}
	testdata := filepath.Join(root, "testdata")
	err = filepath.WalkDir(testdata, func(host string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		name := entry.Name()
		if name == "scenario.json" || strings.HasPrefix(name, ".") {
			return nil
		}
		relative, err := filepath.Rel(root, host)
		if err != nil {
			return err
		}
		path := filepath.ToSlash(relative)
		if _, ok := listed[path]; !ok {
			problems = append(problems, path+": produced but not listed in "+corpus.LedgerFile)
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("%d problem(s) with this group's output:\n  %s", len(problems), strings.Join(problems, "\n  "))
}

// BuildArtifacts materialises the whole artifact cache — every clip, payload
// and intermediate archive in the table — without generating any fixture.
// Stage 0 runs this once; the cache travels to every stage-1 runner, which
// reuses complete entries and rebuilds nothing.
func BuildArtifacts(ctx context.Context, config Config) error {
	if config.Workers < 1 {
		config.Workers = 4
	}
	if config.Log == nil {
		config.Log = io.Discard
	}
	lock, err := LoadLock(config.Root)
	if err != nil {
		return err
	}
	table := Artifacts()
	cache := NewArtifactCache(filepath.Join(config.Root, "target", "fixturegen", "artifacts"), table).
		WithBuildIdentity(lock, config.Root)
	docker := &Docker{Root: config.Root, Verbose: config.Verbose}
	workRoot := filepath.Join(config.Root, "target", "fixturegen", "work")
	if err := os.MkdirAll(workRoot, 0o755); err != nil {
		return err
	}
	names := make([]string, 0, len(table))
	for name := range table {
		names = append(names, name)
	}
	sort.Strings(names)

	queue := make(chan string)
	failures := make(chan error, len(names))
	done := make(chan struct{})
	for range min(config.Workers, len(names)) {
		go func() {
			for name := range queue {
				env := &Env{
					Root: config.Root, Work: workRoot, Slug: "artifact:" + name,
					Lock: lock, Docker: docker, Artifacts: cache,
				}
				if _, err := cache.Files(ctx, env, name); err != nil {
					failures <- err
				} else {
					fmt.Fprintf(config.Log, "  artifact %-28s ready\n", name)
					failures <- nil
				}
			}
		}()
	}
	go func() {
		for _, name := range names {
			queue <- name
		}
		close(queue)
		close(done)
	}()
	var failed []error
	for range names {
		if err := <-failures; err != nil {
			failed = append(failed, err)
		}
	}
	<-done
	return errors.Join(failed...)
}

// artifactToolchains is the union of every artifact builder's pinned ids.
func artifactToolchains() []string {
	set := map[string]struct{}{}
	for _, artifact := range Artifacts() {
		for _, id := range artifact.Toolchains {
			set[id] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func pathSlug(path string) string {
	rest := strings.TrimPrefix(path, "testdata/")
	slug, _, _ := strings.Cut(rest, "/")
	return slug
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
