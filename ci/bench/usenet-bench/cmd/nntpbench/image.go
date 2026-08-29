package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func imageBuild(args []string) error {
	flags := flag.NewFlagSet("image build", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var options benchmark.E2EImageBuildOptions
	flags.StringVar(&options.Version, "version", "", "exact e2e-nntp Go module version")
	flags.StringVar(&options.SourceDir, "source-dir", "", "local e2e-nntp module root for private development")
	flags.StringVar(&options.Tag, "tag", "e2e-nntp:local", "local Docker image tag")
	flags.StringVar(&options.ProvenancePath, "provenance", "", "new benchmark artifact path for image provenance JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	provenance, err := benchmark.BuildE2EImage(context.Background(), options)
	if err != nil {
		return err
	}
	if provenance.Source != "module-version" && provenance.Source != "source-directory" {
		return fmt.Errorf("unrecognized e2e-nntp provenance source %q", provenance.Source)
	}
	return printJSON(provenance)
}
