package fixturegen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// Config drives one generator run.
type Config struct {
	// Root is the harness root: the directory holding go.mod.
	Root string
	// Slugs selects scenario directories; empty means every recipe.
	Slugs []string
	// Out overrides the destination, so a run can be inspected before it
	// replaces the working tree. Empty writes into testdata/.
	Out string
	// OnlyMissing skips a scenario whose ledger files are all present.
	OnlyMissing bool
	// Workers bounds how many scenarios run at once.
	Workers int
	// UpdateLedger refreshes sizes, digests and provenance in the ledger.
	UpdateLedger bool
	// Verbose echoes oracle output.
	Verbose bool
	// Log receives progress lines.
	Log io.Writer
}

// Result is what one scenario produced.
type Result struct {
	Slug       string
	Family     string
	Files      []string
	Bytes      int64
	Toolchains []string
	Elapsed    time.Duration
}

// Run generates the selected scenarios and, when asked, rewrites the ledger.
func Run(ctx context.Context, config Config) ([]Result, error) {
	if config.Workers < 1 {
		config.Workers = 4
	}
	if config.Log == nil {
		config.Log = io.Discard
	}
	lock, err := LoadLock(config.Root)
	if err != nil {
		return nil, err
	}
	ledger, _, err := corpus.LoadLedger(config.Root)
	if err != nil {
		return nil, err
	}
	selected, err := selectRecipes(config.Slugs)
	if err != nil {
		return nil, err
	}
	destination := config.Out
	if destination == "" {
		destination = filepath.Join(config.Root, "testdata")
	}
	if config.OnlyMissing {
		selected = filterPresent(selected, ledger, destination)
	}
	if len(selected) == 0 {
		return nil, nil
	}

	cache := NewArtifactCache(filepath.Join(config.Root, "target", "fixturegen", "artifacts"), Artifacts())
	docker := &Docker{Root: config.Root, Verbose: config.Verbose}
	workRoot := filepath.Join(config.Root, "target", "fixturegen", "work")
	if err := os.MkdirAll(workRoot, 0o755); err != nil {
		return nil, err
	}

	results := make([]Result, len(selected))
	failures := make([]error, len(selected))
	queue := make(chan int)
	var workers sync.WaitGroup
	var logMutex sync.Mutex
	worker := func() {
		defer workers.Done()
		for index := range queue {
			recipe := selected[index]
			started := time.Now()
			result, err := generate(ctx, config, lock, ledger, cache, docker, workRoot, destination, recipe)
			if err != nil {
				failures[index] = fmt.Errorf("%s: %w", recipe.Slug, err)
				continue
			}
			result.Elapsed = time.Since(started)
			results[index] = result
			logMutex.Lock()
			fmt.Fprintf(config.Log, "  %-46s %2d files %9.1f MiB  %s\n",
				recipe.Slug, len(result.Files), float64(result.Bytes)/(1<<20), result.Elapsed.Round(time.Second))
			logMutex.Unlock()
		}
	}
	count := min(config.Workers, len(selected))
	workers.Add(count)
	for range count {
		go worker()
	}
	for index := range selected {
		queue <- index
	}
	close(queue)
	workers.Wait()

	// The ledger is refreshed for whatever succeeded even when something else
	// failed. A half-finished run must still leave an honest tree: the
	// scenarios that were rebuilt say so, and the ones that were not keep the
	// provenance and the digests they already had.
	failure := errors.Join(failures...)
	if config.UpdateLedger && config.Out == "" {
		if err := updateLedger(config.Root, ledger, lock, results, selected); err != nil {
			return results, errors.Join(failure, err)
		}
	}
	return results, failure
}

func generate(
	ctx context.Context,
	config Config,
	lock Lock,
	ledger *corpus.Ledger,
	cache *ArtifactCache,
	docker *Docker,
	workRoot, destination string,
	recipe Recipe,
) (Result, error) {
	work := filepath.Join(workRoot, recipe.Slug)
	if err := os.RemoveAll(work); err != nil {
		return Result{}, err
	}
	for _, sub := range []string{stageDir, outputDir} {
		if err := os.MkdirAll(filepath.Join(work, sub), 0o755); err != nil {
			return Result{}, err
		}
	}
	env := &Env{Root: config.Root, Work: work, Slug: recipe.Slug, Lock: lock, Docker: docker, Artifacts: cache}
	if err := recipe.Build(ctx, env); err != nil {
		return Result{}, err
	}
	produced, err := env.Outputs()
	if err != nil {
		return Result{}, err
	}
	if err := matchesLedger(ledger, recipe.Slug, produced); err != nil {
		return Result{}, err
	}

	target := filepath.Join(destination, recipe.Slug)
	if err := clearFixtures(target); err != nil {
		return Result{}, err
	}
	var total int64
	for _, name := range produced {
		source := filepath.Join(work, outputDir, filepath.FromSlash(name))
		if err := CopyFile(source, filepath.Join(target, filepath.FromSlash(name))); err != nil {
			return Result{}, err
		}
		size, err := FileSize(source)
		if err != nil {
			return Result{}, err
		}
		total += size
	}

	// A preview run into --out has no scenario.json to keep in step; the
	// digests are only rewritten where the scenarios actually live.
	if recipe.ExpectedOutputs != nil && config.Out == "" {
		digests, err := recipe.ExpectedOutputs(ctx, env)
		if err != nil {
			return Result{}, err
		}
		rendered := make(map[string]string, len(digests))
		for member, hostFile := range digests {
			digest, err := corpus.DigestFile(hostFile)
			if err != nil {
				return Result{}, err
			}
			rendered[member] = digest.BLAKE3
		}
		scenario := filepath.Join(destination, recipe.Slug, corpus.ScenarioFile)
		if err := RewriteScenarioDigests(scenario, rendered); err != nil {
			return Result{}, err
		}
	}
	if err := os.RemoveAll(work); err != nil {
		return Result{}, err
	}
	return Result{Slug: recipe.Slug, Family: recipe.Family, Files: produced, Bytes: total, Toolchains: env.UsedToolchains()}, nil
}

// matchesLedger is the contract check: a recipe must produce exactly the file
// names its scenario's ledger entries list, no more and no fewer.
func matchesLedger(ledger *corpus.Ledger, slug string, produced []string) error {
	want := ledgerNames(ledger, slug)
	if len(want) == 0 {
		return fmt.Errorf("no ledger entries describe testdata/%s", slug)
	}
	missing := difference(want, produced)
	extra := difference(produced, want)
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	var problems []string
	if len(missing) > 0 {
		problems = append(problems, "did not produce "+strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		problems = append(problems, "produced unledgered "+strings.Join(extra, ", "))
	}
	return errors.New(strings.Join(problems, "; "))
}

func ledgerNames(ledger *corpus.Ledger, slug string) []string {
	prefix := "testdata/" + slug + "/"
	var names []string
	for _, file := range ledger.Files {
		if strings.HasPrefix(file.Path, prefix) {
			names = append(names, strings.TrimPrefix(file.Path, prefix))
		}
	}
	sort.Strings(names)
	return names
}

func difference(left, right []string) []string {
	index := make(map[string]struct{}, len(right))
	for _, value := range right {
		index[value] = struct{}{}
	}
	var only []string
	for _, value := range left {
		if _, ok := index[value]; !ok {
			only = append(only, value)
		}
	}
	return only
}

// clearFixtures empties a scenario directory of everything but scenario.json,
// which is tracked in git and never a corpus object.
func clearFixtures(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dir, 0o755)
		}
		return err
	}
	for _, entry := range entries {
		if entry.Name() == corpus.ScenarioFile {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func selectRecipes(slugs []string) ([]Recipe, error) {
	all := Recipes()
	if len(slugs) == 0 {
		return all, nil
	}
	byslug := make(map[string]Recipe, len(all))
	for _, recipe := range all {
		byslug[recipe.Slug] = recipe
	}
	selected := make([]Recipe, 0, len(slugs))
	for _, slug := range slugs {
		recipe, ok := byslug[slug]
		if !ok {
			if reason, only := ScenarioOnly[slug]; only {
				return nil, fmt.Errorf("%s owns no fixture bytes: it %s", slug, reason)
			}
			return nil, fmt.Errorf("no recipe for %q; run --list to see the corpus", slug)
		}
		selected = append(selected, recipe)
	}
	return selected, nil
}

func filterPresent(recipes []Recipe, ledger *corpus.Ledger, destination string) []Recipe {
	var pending []Recipe
	for _, recipe := range recipes {
		complete := true
		for _, name := range ledgerNames(ledger, recipe.Slug) {
			if _, err := os.Stat(filepath.Join(destination, recipe.Slug, filepath.FromSlash(name))); err != nil {
				complete = false
				break
			}
		}
		if !complete {
			pending = append(pending, recipe)
		}
	}
	return pending
}

// updateLedger refreshes size, digest and provenance for every path a
// successful recipe produced, and leaves every other entry exactly as it was.
func updateLedger(root string, ledger *corpus.Ledger, lock Lock, results []Result, recipes []Recipe) error {
	provenance := make(map[string]corpus.Source, 256)
	used := map[string]struct{}{}
	for index, result := range results {
		if result.Slug == "" {
			continue
		}
		recipe := recipes[index]
		toolchains := append([]string(nil), result.Toolchains...)
		toolchains = append(toolchains, goWriterIDs(lock, result.Files)...)
		sort.Strings(toolchains)
		toolchains = unique(toolchains)
		for _, id := range toolchains {
			used[id] = struct{}{}
		}
		for _, name := range result.Files {
			provenance["testdata/"+result.Slug+"/"+name] = corpus.Source{
				Kind:       corpus.SourceGenerated,
				Generator:  GeneratorID,
				Toolchains: toolchains,
				Inputs:     recipe.Inputs,
			}
		}
	}

	for index := range ledger.Files {
		entry := &ledger.Files[index]
		source, ok := provenance[entry.Path]
		if !ok {
			continue
		}
		digest, err := corpus.DigestFile(corpus.HostPath(root, entry.Path))
		if err != nil {
			return err
		}
		entry.Size = digest.Size
		entry.BLAKE3 = digest.BLAKE3
		entry.Source = source
	}

	declared := make([]string, 0, len(used))
	for id := range used {
		declared = append(declared, id)
	}
	sort.Strings(declared)
	if ledger.Generators == nil {
		ledger.Generators = map[string]corpus.Generator{}
	}
	existing := ledger.Generators[GeneratorID]
	merged := unique(sortedUnion(existing.Toolchains, declared))
	ledger.Generators[GeneratorID] = corpus.Generator{
		Path:             "cmd/fixturegen",
		Toolchains:       merged,
		ByteReproducible: false,
		Notes: "The Go fixture generator. Payload synthesis, every byte edit, and the zip, tar, gzip, DEFLATE, zstd, bzip2 and brotli containers are Go; " +
			"RAR comes only from RARLAB's own writer, PAR2 from par2cmdline-turbo, 7z from the official 7-Zip console binary and video from the pinned FFmpeg image. " +
			"Not byte-reproducible as a whole: RAR and 7z stamp creation times and draw encryption salts, and the video encoders are not bit-exact across builds. " +
			"The zip, tar and stream-codec families are byte-reproducible from their recipe alone.",
	}
	demoteUnrunnableGenerators(root, ledger)
	pruneGenerators(ledger)
	return ledger.Save(root)
}

// demoteUnrunnableGenerators marks a fixture blocked when the generator it
// credits is no longer in the tree. That is exactly what blocked means — the
// bytes are still described and still verify, but nobody can reproduce them —
// and it is what keeps a half-finished regeneration honest instead of leaving
// entries pointing at a program that has been deleted.
func demoteUnrunnableGenerators(root string, ledger *corpus.Ledger) {
	runnable := make(map[string]bool, len(ledger.Generators))
	for name, generator := range ledger.Generators {
		_, err := os.Stat(corpus.HostPath(root, generator.Path))
		runnable[name] = err == nil
	}
	for index := range ledger.Files {
		entry := &ledger.Files[index]
		if entry.Source.Kind != corpus.SourceGenerated || runnable[entry.Source.Generator] {
			continue
		}
		entry.Source = corpus.Source{
			Kind: corpus.SourceBlocked,
			Reason: fmt.Sprintf("generator %s is no longer in the tree; these bytes predate cmd/fixturegen and cannot be reproduced until %s is regenerated",
				entry.Source.Generator, path.Dir(entry.Path)),
		}
	}
}

// goWriterIDs credits the Go writers a scenario's file extensions imply.
func goWriterIDs(lock Lock, files []string) []string {
	ids := []string{"go-fixture-bytes"}
	for _, name := range files {
		switch {
		case strings.HasSuffix(name, ".zip"):
			ids = append(ids, "go-archive-zip")
		case strings.HasSuffix(name, ".tar"):
			ids = append(ids, "go-archive-tar")
		case strings.HasSuffix(name, ".tar.gz"), strings.HasSuffix(name, ".tgz"), strings.HasSuffix(name, ".tar.gzip"):
			ids = append(ids, "go-archive-tar", "go-compress-gzip")
		case strings.HasSuffix(name, ".tar.bz2"), strings.HasSuffix(name, ".tbz2"), strings.HasSuffix(name, ".tar.bzip2"):
			ids = append(ids, "go-archive-tar", "go-dsnet-bzip2@v0.0.1")
		case strings.HasSuffix(name, ".gz"):
			ids = append(ids, "go-compress-gzip")
		case strings.HasSuffix(name, ".deflate"):
			ids = append(ids, "go-compress-flate")
		case strings.HasSuffix(name, ".bz2"):
			ids = append(ids, "go-dsnet-bzip2@v0.0.1")
		case strings.HasSuffix(name, ".zst"):
			ids = append(ids, "go-klauspost-zstd@v1.19.2")
		case strings.HasSuffix(name, ".br"):
			ids = append(ids, "go-andybalholm-brotli@v1.2.2")
		}
	}
	known := map[string]struct{}{}
	for _, writer := range lock.GoWriters {
		known[writer.ID] = struct{}{}
	}
	filtered := ids[:0]
	for _, id := range ids {
		if _, ok := known[id]; ok {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// pruneGenerators drops generator declarations no file credits any more, which
// is how the retired shell scripts leave the ledger.
func pruneGenerators(ledger *corpus.Ledger) {
	referenced := map[string]struct{}{GeneratorID: {}}
	for _, file := range ledger.Files {
		if file.Source.Kind == corpus.SourceGenerated {
			referenced[file.Source.Generator] = struct{}{}
		}
	}
	for name := range ledger.Generators {
		if _, ok := referenced[name]; !ok {
			delete(ledger.Generators, name)
		}
	}
}

func sortedUnion(left, right []string) []string {
	merged := append(append([]string(nil), left...), right...)
	sort.Strings(merged)
	return merged
}

func unique(values []string) []string {
	out := values[:0]
	var previous string
	for index, value := range values {
		if index > 0 && value == previous {
			continue
		}
		out = append(out, value)
		previous = value
	}
	return out
}
