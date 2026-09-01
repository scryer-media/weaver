package fixturegen

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Artifacts are cached under `<name>@<identity>` rather than under `<name>`.
//
// The old layout was keyed by name alone and checked only that the declared
// files existed, so an artifact built by one revision of a recipe was reused
// verbatim by the next — silently, for as long as the directory survived. That
// is not a staleness bug that shows up where it happens: the artifact is fine,
// it is the *scenario* built from it that fails a ledger digest, one family per
// run, on whichever machine happens to hold the old cache.
//
// Keying by identity makes the failure impossible rather than detectable. A
// changed build yields a different directory, so the stale one is never
// consulted again — including the ones already sitting on a machine today,
// which is the property that matters for a fix nobody has to remediate by hand.
const artifactIdentityKeyLength = 12

// artifactIdentity is what an artifact's bytes depend on, as far as this
// process can know it statically.
//
// Two halves. The **declaration** — name, files, toolchains and their pinned
// image digests — covers a toolchain bump or a change in what the artifact
// claims to produce. The **source digest** covers the rest: the build function
// itself, the helpers it calls, everything that is code rather than data.
//
// The source term is deliberately omitted for resumable artifacts. Those are
// the video clips, which take the better part of an hour to encode and are the
// one thing in the corpus that must not rebuild because an unrelated recipe
// moved. They are protected instead by their declaration, by their own size
// floors, and — where a scenario consumes them — by the ledger digest of that
// scenario, which is precisely the hard error that caught this. The residual is
// stated rather than hidden: a `ClipSpec` edited without any declaration change
// will not invalidate its clip on its own, and wants a `--force` or a manual
// clear. Everything cheap enough to rebuild in seconds carries the source term
// and cannot rot at all.
func artifactIdentity(artifact Artifact, lock Lock, root string) string {
	hash := sha256.New()
	fmt.Fprintf(hash, "artifact\x00%s\x00", artifact.Name)
	for _, file := range artifact.Files {
		fmt.Fprintf(hash, "file\x00%s\x00", file)
	}
	fmt.Fprintf(hash, "resumable\x00%t\x00", artifact.Resumable)

	toolchains := append([]string(nil), artifact.Toolchains...)
	sort.Strings(toolchains)
	for _, id := range toolchains {
		// The pin, not just the name: a toolchain that moves to a new image
		// produces different bytes from the same recipe.
		fmt.Fprintf(hash, "toolchain\x00%s\x00%s\x00", id, lock.Pin(id))
	}

	if !artifact.Resumable {
		fmt.Fprintf(hash, "source\x00%s\x00", generatorSourceDigest(root))
	}
	return hex.EncodeToString(hash.Sum(nil))[:artifactIdentityKeyLength]
}

var (
	sourceDigestOnce sync.Once
	sourceDigestVal  string
)

// generatorSourceDigest hashes the generator's own Go source.
//
// Any edit to a recipe, an artifact builder, an oracle invocation or a helper
// changes it, which is the point: the author does not have to remember to bump
// anything, and there is no version string to forget. Read from the harness
// root rather than from the binary so it works under `go run`, which is how the
// generator is always invoked.
//
// A tree without readable sources — a prebuilt binary run elsewhere — falls
// back to a constant, which degrades to the old name-keyed behaviour rather
// than failing. That case does not generate fixtures.
func generatorSourceDigest(root string) string {
	sourceDigestOnce.Do(func() {
		dir := filepath.Join(root, "internal", "fixturegen")
		entries, err := os.ReadDir(dir)
		if err != nil {
			sourceDigestVal = "unreadable"
			return
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			names = append(names, name)
		}
		sort.Strings(names)
		hash := sha256.New()
		for _, name := range names {
			contents, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				sourceDigestVal = "unreadable"
				return
			}
			fmt.Fprintf(hash, "%s\x00%d\x00", name, len(contents))
			hash.Write(contents)
		}
		sourceDigestVal = hex.EncodeToString(hash.Sum(nil))
	})
	return sourceDigestVal
}

// pruneLegacyArtifactDirs removes cache directories from the name-keyed layout.
//
// They can never be consulted again, and leaving them behind would strand
// several gigabytes on every machine that ever ran the old generator.
func pruneLegacyArtifactDirs(dir string, table map[string]Artifact) {
	for name := range table {
		legacy := filepath.Join(dir, name)
		info, err := os.Stat(legacy)
		if err != nil || !info.IsDir() {
			continue
		}
		_ = os.RemoveAll(legacy)
	}
}

// onceReset hands tests a fresh source-digest memo. The digest is process-wide
// by design — it is read once and reused for every artifact — which a test that
// rewrites the source has to be able to defeat.
func onceReset() sync.Once {
	sourceDigestVal = ""
	return sync.Once{}
}
