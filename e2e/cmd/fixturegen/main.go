// Command fixturegen rebuilds the Weaver e2e fixture corpus from the
// declarative recipes in internal/fixturegen.
//
//	go run ./cmd/fixturegen --list
//	go run ./cmd/fixturegen --all
//	go run ./cmd/fixturegen --scenario rar5-single --scenario par2-repair
//	go run ./cmd/fixturegen --all --only-missing
//	go run ./cmd/fixturegen --all --out target/fixturegen/preview
//
// Exit codes: 0 success, 1 failure, 2 usage.
package main

import (
	"context"
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
	var scenarios slugList
	list := flag.Bool("list", false, "print every recipe, its family and its declared oracles, then exit")
	all := flag.Bool("all", false, "generate every recipe")
	out := flag.String("out", "", "write fixtures here instead of testdata/ (the ledger is then left alone)")
	onlyMissing := flag.Bool("only-missing", false, "skip a scenario whose ledger files are all present")
	workers := flag.Int("workers", 4, "how many scenarios to build at once")
	root := flag.String("root", "", "harness root (defaults to the directory holding go.mod)")
	verbose := flag.Bool("verbose", false, "echo oracle output")
	skipLedger := flag.Bool("skip-ledger", false, "do not refresh sizes, digests and provenance in test-corpus/sources.json")
	flag.Var(&scenarios, "scenario", "scenario slug to generate; repeatable, or comma-separated")
	flag.Parse()

	harnessRoot, err := resolveRoot(*root)
	if err != nil {
		return err
	}
	if *list {
		return printRecipes(harnessRoot)
	}
	if !*all && len(scenarios) == 0 {
		flag.Usage()
		return usageError{errors.New("choose --list, --all, or one or more --scenario values")}
	}
	if *all && len(scenarios) > 0 {
		return usageError{errors.New("--all and --scenario are mutually exclusive")}
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
