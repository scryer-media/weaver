package main

import (
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/nntp"
)

// The recorded scheme has to name the run and leave the fixture component as a
// readable placeholder. Feeding a braced placeholder straight to the poster's
// template builder would let its id sanitizer chew the braces into dashes and
// silently record a scheme no poster ever used.
func TestSeedImageRecordsAReadableMessageIDScheme(t *testing.T) {
	shared := seedImageFlags{dockerBinary: "docker", fixturesCSV: "fixture-one,fixture-two"}
	shared.options.Corpus.RunID = "corpus-1"
	options, err := shared.resolve()
	if err != nil {
		t.Fatal(err)
	}
	scheme := options.Corpus.MessageIDTemplate
	if !strings.Contains(scheme, "{fixture}") {
		t.Fatalf("scheme does not carry a fixture placeholder: %q", scheme)
	}
	if strings.Contains(scheme, seedImageFixturePlaceholder) {
		t.Fatalf("scheme leaked its internal placeholder: %q", scheme)
	}
	if !strings.Contains(scheme, "corpus-1") {
		t.Fatalf("scheme does not name the seed run: %q", scheme)
	}
	// The scheme must stay the poster's own shape, with only the fixture
	// component swapped, so a cached image is fingerprinted against the naming
	// the poster actually used.
	want := strings.Replace(nntp.MessageIDTemplate("corpus-1", seedImageFixturePlaceholder), seedImageFixturePlaceholder, "{fixture}", 1)
	if scheme != want {
		t.Fatalf("scheme = %q, want %q", scheme, want)
	}
	if got := options.Corpus.FixtureIDs; len(got) != 2 || got[0] != "fixture-one" || got[1] != "fixture-two" {
		t.Fatalf("fixture ids = %v", got)
	}
}
