// Command fixturegen rebuilds the Weaver e2e fixture corpus from the
// declarative recipes in internal/fixturegen.
//
//	go run ./cmd/fixturegen --list
//	go run ./cmd/fixturegen --all
//	go run ./cmd/fixturegen --scenario rar5-single --scenario par2-repair
//	go run ./cmd/fixturegen --family rar-recovery-volumes
//	go run ./cmd/fixturegen --all --only-missing
//	go run ./cmd/fixturegen --all --out target/fixturegen/preview
//
// The publish workflow's fan-out surface (see .github/workflows/e2e-corpus-publish.yml):
//
//	go run ./cmd/fixturegen --list-json
//	go run ./cmd/fixturegen --build-artifacts
//	go run ./cmd/fixturegen --paths --family zip --with-scenarios --out group.txt
//	go run ./cmd/fixturegen --paths --family zip --verify
//
// Exit codes: 0 success, 1 failure, 2 usage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/fixturegen"
)

type slugList []string

func (list *slugList) String() string { return strings.Join(*list, ",") }

func (list *slugList) Set(value string) error {
	for _, slug := range strings.Split(value, ",") {
		if slug = strings.TrimSpace(slug); slug != "" {
			*list = append(*list, slug)
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		var usage usageError
		if errors.As(err, &usage) {
			fmt.Fprintf(os.Stderr, "fixturegen: %v\n", err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "fixturegen: %v\n", err)
		os.Exit(1)
	}
}

type usageError struct{ error }

func run() error {
	var scenarios, families slugList
	list := flag.Bool("list", false, "print every recipe, its family and its declared oracles, then exit")
	listJSON := flag.Bool("list-json", false, "print the generation matrix (stage-0 and stage-1 units) as JSON, then exit")
	buildArtifacts := flag.Bool("build-artifacts", false, "build the whole artifact cache under target/fixturegen/artifacts, then exit")
	paths := flag.Bool("paths", false, "print the ledger paths the selection owns instead of generating")
	verify := flag.Bool("verify", false, "with --paths: require every path present and non-empty, and nothing unledgered on disk")
	withScenarios := flag.Bool("with-scenarios", false, "with --paths: include each slug's scenario.json")
	all := flag.Bool("all", false, "generate every recipe")
	out := flag.String("out", "", "write fixtures here instead of testdata/ (the ledger is then left alone); with --paths, write the list here")
	onlyMissing := flag.Bool("only-missing", false, "skip a scenario whose ledger files are all present")
	workers := flag.Int("workers", 4, "how many scenarios to build at once")
	root := flag.String("root", "", "harness root (defaults to the directory holding go.mod)")
	verbose := flag.Bool("verbose", false, "echo oracle output")
	skipLedger := flag.Bool("skip-ledger", false, "do not refresh sizes, digests and provenance in test-corpus/sources.json")
	flag.Var(&scenarios, "scenario", "scenario slug to generate; repeatable, or comma-separated")
	flag.Var(&families, "family", "recipe family (name or key from --list-json) to select; repeatable, or comma-separated")
	flag.Parse()

	harnessRoot, err := resolveRoot(*root)
	if err != nil {
		return err
	}
	if *list {
		return printRecipes(harnessRoot)
	}
	if *listJSON {
		return printListing(harnessRoot)
	}
	if *buildArtifacts {
		return buildArtifactCache(harnessRoot, *workers, *verbose)
	}
	if len(families) > 0 {
		familySlugs, err := fixturegen.FamilySelector(families)
		if err != nil {
			return usageError{err}
		}
		scenarios = append(scenarios, familySlugs...)
	}
	if *paths {
		return printPaths(harnessRoot, *all, scenarios, *withScenarios, *verify, *out)
	}
	if *verify || *withScenarios {
		return usageError{errors.New("--verify and --with-scenarios only make sense with --paths")}
	}
	if !*all && len(scenarios) == 0 {
		flag.Usage()
		return usageError{errors.New("choose --list, --all, or one or more --scenario/--family values")}
	}
	if *all && len(scenarios) > 0 {
		return usageError{errors.New("--all and --scenario/--family are mutually exclusive")}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	started := time.Now()
	fmt.Printf("fixturegen: building %s\n", describeSelection(*all, scenarios))
	results, err := fixturegen.Run(ctx, fixturegen.Config{
		Root:         harnessRoot,
		Slugs:        scenarios,
		Out:          *out,
		OnlyMissing:  *onlyMissing,
		Workers:      *workers,
		UpdateLedger: !*skipLedger,
		Verbose:      *verbose,
		Log:          os.Stdout,
	})
	summarise(results, time.Since(started))
	return err
}

func describeSelection(all bool, scenarios slugList) string {
	if all {
		return "every recipe"
	}
	return strings.Join(scenarios, ", ")
}

func summarise(results []fixturegen.Result, elapsed time.Duration) {
	var files int
	var bytes int64
	families := map[string]int{}
	for _, result := range results {
		if result.Slug == "" {
			continue
		}
		files += len(result.Files)
		bytes += result.Bytes
		families[result.Family]++
	}
	if files == 0 {
		fmt.Printf("fixturegen: nothing to do (%s)\n", elapsed.Round(time.Second))
		return
	}
	names := make([]string, 0, len(families))
	for family := range families {
		names = append(names, family)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, family := range names {
		parts = append(parts, fmt.Sprintf("%s %d", family, families[family]))
	}
	fmt.Printf("fixturegen: %d files, %.1f MiB, in %s (%s)\n",
		files, float64(bytes)/(1<<20), elapsed.Round(time.Second), strings.Join(parts, ", "))
}

func printRecipes(root string) error {
	lock, err := fixturegen.LoadLock(root)
	if err != nil {
		return err
	}
	recipes := fixturegen.Recipes()
	sort.Slice(recipes, func(left, right int) bool {
		if recipes[left].Family != recipes[right].Family {
			return recipes[left].Family < recipes[right].Family
		}
		return recipes[left].Slug < recipes[right].Slug
	})
	family := ""
	for _, recipe := range recipes {
		if recipe.Family != family {
			family = recipe.Family
			fmt.Printf("\n%s\n", strings.ToUpper(family))
		}
		reproducible := "shape"
		if recipe.ByteReproducible {
			reproducible = "bytes"
		}
		fmt.Printf("  %-46s %-6s %s\n", recipe.Slug, reproducible, recipe.Notes)
	}
	fmt.Printf("\n%d recipes\n", len(recipes))

	slugs := make([]string, 0, len(fixturegen.ScenarioOnly))
	for slug := range fixturegen.ScenarioOnly {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)
	fmt.Printf("\nSCENARIO-ONLY (no fixture bytes of their own)\n")
	for _, slug := range slugs {
		fmt.Printf("  %-46s %s\n", slug, fixturegen.ScenarioOnly[slug])
	}

	fmt.Printf("\nPINNED ORACLES\n")
	ids := lock.IDs()
	sort.Strings(ids)
	for _, id := range ids {
		fmt.Printf("  %s\n", id)
	}
	return nil
}

func printListing(root string) error {
	listing, err := fixturegen.Units(root)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(listing)
}

func buildArtifactCache(root string, workers int, verbose bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := time.Now()
	fmt.Println("fixturegen: building the artifact cache")
	err := fixturegen.BuildArtifacts(ctx, fixturegen.Config{
		Root: root, Workers: workers, Verbose: verbose, Log: os.Stdout,
	})
	fmt.Printf("fixturegen: artifact cache ready in %s\n", time.Since(started).Round(time.Second))
	return err
}

func printPaths(root string, all bool, scenarios slugList, withScenarios, verify bool, out string) error {
	slugs := []string(scenarios)
	if all {
		if len(slugs) > 0 {
			return usageError{errors.New("--all and --scenario/--family are mutually exclusive")}
		}
		for _, recipe := range fixturegen.Recipes() {
			slugs = append(slugs, recipe.Slug)
		}
	}
	if len(slugs) == 0 {
		return usageError{errors.New("--paths needs --all or one or more --scenario/--family values")}
	}
	paths, err := fixturegen.LedgerPathsForSlugs(root, slugs, withScenarios)
	if err != nil {
		return err
	}
	if verify {
		if err := fixturegen.VerifyProduced(root, paths); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "fixturegen: all %d selected path(s) were produced, and nothing outside the ledger was\n", len(paths))
	}
	rendered := strings.Join(paths, "\n") + "\n"
	if out == "" {
		fmt.Print(rendered)
		return nil
	}
	return os.WriteFile(out, []byte(rendered), 0o644)
}

func resolveRoot(override string) (string, error) {
	if override != "" {
		return filepath.Abs(override)
	}
	directory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", errors.New("run this from inside the e2e harness, or pass --root")
		}
		directory = parent
	}
}
