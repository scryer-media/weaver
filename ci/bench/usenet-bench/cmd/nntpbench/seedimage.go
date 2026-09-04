package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntp"
	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/seedimage"
)

// seedImageFixturePlaceholder stands in for the fixture id inside the
// recorded message-id scheme. It contains only characters the poster's id
// sanitizer preserves, so it survives the round trip unchanged.
const seedImageFixturePlaceholder = "fixtureidplaceholder"

// seedImage is a self-contained command group: it caches an already-seeded
// NNTP article store as a local Docker image so repeat runs stop reposting an
// unchanged corpus. It measures nothing.
func seedImage(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: nntpbench seed-image <status|capture|restore> [options]")
	}
	switch args[0] {
	case "status":
		return seedImageStatus(args[1:])
	case "capture":
		return seedImageCapture(args[1:])
	case "restore":
		return seedImageRestore(args[1:])
	default:
		return fmt.Errorf("unknown seed-image action %q (want status, capture, or restore)", args[0])
	}
}

type seedImageFlags struct {
	options      seedimage.Options
	corpusPath   string
	fixturesCSV  string
	provenance   string
	dockerBinary string
}

func registerSeedImageFlags(flags *flag.FlagSet, shared *seedImageFlags) {
	flags.StringVar(&shared.options.Corpus.FixturesRoot, "fixtures-root", "generated", "directory holding one subdirectory per generated fixture")
	flags.StringVar(&shared.corpusPath, "corpus", "fixtures/corpus.json", "declared corpus JSON used when --fixtures is omitted")
	flags.StringVar(&shared.fixturesCSV, "fixtures", "", "comma-separated fixture ids to cache")
	flags.StringVar(&shared.options.Corpus.RunID, "run-id", "", "seed run identifier used when the corpus was posted")
	flags.IntVar(&shared.options.Corpus.SegmentBytes, "segment-bytes", 750<<10, "raw bytes per yEnc article used when the corpus was posted")
	flags.StringVar(&shared.options.Corpus.Group, "group", "alt.binaries.test", "newsgroup the corpus was posted to")
	flags.StringVar(&shared.options.Corpus.BaseImage, "nntp-image", "e2e-nntp:local", "NNTP server image the articles are baked into")
	flags.StringVar(&shared.options.Corpus.BaseImageID, "nntp-image-id", "", "override the resolved NNTP server image id")
	flags.StringVar(&shared.options.Container, "container", "", "running seeded NNTP container (capture only)")
	flags.StringVar(&shared.options.ComposeProject, "compose-project", "", "Compose project holding the seeded NNTP service")
	flags.StringVar(&shared.options.ComposeService, "compose-service", "nntp", "Compose service name of the NNTP server")
	flags.StringVar(&shared.options.StageRoot, "stage-root", "", "directory for the temporary Docker build context")
	flags.StringVar(&shared.provenance, "provenance", "", "new artifact path for seed provenance JSON")
	flags.StringVar(&shared.dockerBinary, "docker", "docker", "Docker executable")
}

func (shared *seedImageFlags) resolve() (seedimage.Options, error) {
	options := shared.options
	options.Docker = seedimage.CLI{Binary: shared.dockerBinary}
	// The message-id scheme is part of every article's identity, so it is a
	// fingerprint input rather than a flag: a harness change that alters it
	// must invalidate every cached image. The per-fixture component stays a
	// placeholder because the scheme, not one fixture's expansion of it, is
	// what a cached image has to agree about. The placeholder is spelled
	// without punctuation so the poster's own id sanitizer leaves it intact,
	// then swapped for a readable brace form.
	options.Corpus.MessageIDTemplate = strings.Replace(
		nntp.MessageIDTemplate(options.Corpus.RunID, seedImageFixturePlaceholder),
		seedImageFixturePlaceholder,
		"{fixture}",
		1,
	)
	ids, err := resolveSeedImageFixtures(shared.fixturesCSV, shared.corpusPath)
	if err != nil {
		return seedimage.Options{}, err
	}
	options.Corpus.FixtureIDs = ids
	return options, nil
}

func resolveSeedImageFixtures(fixturesCSV, corpusPath string) ([]string, error) {
	if strings.TrimSpace(fixturesCSV) != "" {
		ids := make([]string, 0)
		for _, id := range strings.Split(fixturesCSV, ",") {
			if trimmed := strings.TrimSpace(id); trimmed != "" {
				ids = append(ids, trimmed)
			}
		}
		if len(ids) == 0 {
			return nil, fmt.Errorf("--fixtures listed no fixture ids")
		}
		return ids, nil
	}
	corpus, err := fixture.LoadCorpus(corpusPath)
	if err != nil {
		return nil, err
	}
	return corpus.FixtureIDs, nil
}

func seedImageStatus(args []string) error {
	flags := flag.NewFlagSet("seed-image status", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var shared seedImageFlags
	registerSeedImageFlags(flags, &shared)
	if err := flags.Parse(args); err != nil {
		return err
	}
	options, err := shared.resolve()
	if err != nil {
		return err
	}
	status, err := seedimage.Inspect(context.Background(), options)
	if err != nil {
		return err
	}
	return printJSON(status)
}

func seedImageCapture(args []string) error {
	flags := flag.NewFlagSet("seed-image capture", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var shared seedImageFlags
	registerSeedImageFlags(flags, &shared)
	if err := flags.Parse(args); err != nil {
		return err
	}
	options, err := shared.resolve()
	if err != nil {
		return err
	}
	record, err := seedimage.Capture(context.Background(), options)
	if err != nil {
		return err
	}
	if err := writeSeedImageProvenance(shared.provenance, record); err != nil {
		return err
	}
	return printJSON(record)
}

func seedImageRestore(args []string) error {
	flags := flag.NewFlagSet("seed-image restore", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var shared seedImageFlags
	registerSeedImageFlags(flags, &shared)
	if err := flags.Parse(args); err != nil {
		return err
	}
	options, err := shared.resolve()
	if err != nil {
		return err
	}
	record, err := seedimage.Restore(context.Background(), options)
	if err != nil {
		return err
	}
	if err := writeSeedImageProvenance(shared.provenance, record); err != nil {
		return err
	}
	return printJSON(record)
}

// writeSeedImageProvenance keeps seed provenance immutable for the same reason
// every other artifact here is: a rerun must not quietly rewrite the record of
// how a previous run's server was populated.
func writeSeedImageProvenance(path string, record seedimage.Provenance) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	contents, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	return writeBytesExclusive(path, append(contents, '\n'))
}
