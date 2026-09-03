package fixturegen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/scryer-media/weaver/e2e/internal/corpus"
)

// ValidateSaltedEntries refuses a salted ledger entry whose recipe claims to be
// byte-reproducible.
//
// A salted entry buys out of content verification entirely: present is the only
// check it ever gets. That is the right answer for a writer that draws a random
// salt — 7-Zip's AES chains have no switch to fix theirs — and the wrong answer
// for everything else, because it is also the easiest way to silence a genuine
// reproducibility failure. Tying the escape hatch to `ByteReproducible: false`
// means using it requires admitting, in the recipe, that the bytes cannot be
// pinned.
//
// The reverse is deliberately not an error: a non-reproducible recipe may still
// carry hashed entries, which is what every pre-existing 7z fixture does. Those
// are pinned to whatever machine last generated them, which is a weaker
// position than salting but not one this guard should force a change to.
func ValidateSaltedEntries(ledger *corpus.Ledger, recipes []Recipe) error {
	reproducible := make(map[string]bool, len(recipes))
	for _, recipe := range recipes {
		reproducible[recipe.Slug] = recipe.ByteReproducible
	}

	var problems []string
	for _, file := range ledger.Files {
		if !file.Salted {
			continue
		}
		slug, ok := slugOfPath(file.Path)
		if !ok {
			continue
		}
		if reproducible[slug] {
			problems = append(problems, fmt.Sprintf(
				"%s is salted but recipe %s declares ByteReproducible: a reproducible recipe must pin its bytes",
				file.Path, slug))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("salted ledger entries are only legal for non-reproducible recipes:\n  %s",
		strings.Join(problems, "\n  "))
}

// slugOfPath pulls the scenario slug out of a `testdata/<slug>/<file>` path.
func slugOfPath(path string) (string, bool) {
	rest, ok := strings.CutPrefix(path, "testdata/")
	if !ok {
		return "", false
	}
	slug, _, ok := strings.Cut(rest, "/")
	if !ok || slug == "" {
		return "", false
	}
	return slug, true
}
