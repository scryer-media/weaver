package fixturegen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testCache(t *testing.T, root string) *ArtifactCache {
	t.Helper()
	table := map[string]Artifact{
		"cheap": {Name: "cheap", Files: []string{"archive.7z"}, Toolchains: []string{"sevenzip-26.02"}},
		"clip":  {Name: "clip", Files: []string{"small.mkv"}, Resumable: true},
	}
	return NewArtifactCache(filepath.Join(root, "artifacts"), table).
		WithBuildIdentity(Lock{}, root)
}

func writeGeneratorSource(t *testing.T, root, contents string) {
	t.Helper()
	dir := filepath.Join(root, "internal", "fixturegen")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "recipes.go"), []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

// (a) The round-5 defect: a recipe changed and the artifact built by the old
// one was reused verbatim. The identity must move with the source, so the stale
// directory can never be read again.
func TestArtifactIdentityMovesWhenTheGeneratorSourceChanges(t *testing.T) {
	root := t.TempDir()
	writeGeneratorSource(t, root, "package fixturegen // revision one")
	sourceDigestOnce = onceReset()
	before := artifactIdentity(Artifact{Name: "cheap", Files: []string{"a"}}, Lock{}, root)

	writeGeneratorSource(t, root, "package fixturegen // revision two, a recipe changed")
	sourceDigestOnce = onceReset()
	after := artifactIdentity(Artifact{Name: "cheap", Files: []string{"a"}}, Lock{}, root)

	if before == after {
		t.Fatal("a changed generator source must change the artifact identity")
	}
}

// The expensive half of the rule: a video clip must NOT rebuild because some
// unrelated recipe moved. Its identity is declaration-only.
func TestResumableArtifactIdentityIgnoresGeneratorSource(t *testing.T) {
	root := t.TempDir()
	writeGeneratorSource(t, root, "package fixturegen // revision one")
	sourceDigestOnce = onceReset()
	clip := Artifact{Name: "clip", Files: []string{"small.mkv"}, Resumable: true}
	before := artifactIdentity(clip, Lock{}, root)

	writeGeneratorSource(t, root, "package fixturegen // revision two")
	sourceDigestOnce = onceReset()
	after := artifactIdentity(clip, Lock{}, root)

	if before != after {
		t.Fatal("a resumable artifact must not rebuild for an unrelated source change")
	}
}

// A declaration change moves the identity for any artifact, resumable or not.
func TestArtifactIdentityMovesWithItsDeclaration(t *testing.T) {
	root := t.TempDir()
	base := Artifact{Name: "clip", Files: []string{"small.mkv"}, Resumable: true}
	renamed := Artifact{Name: "clip", Files: []string{"other.mkv"}, Resumable: true}
	if artifactIdentity(base, Lock{}, root) == artifactIdentity(renamed, Lock{}, root) {
		t.Fatal("a changed file list must change the identity")
	}
}

// (c) A fresh artifact is reused: identity is stable across calls, so a warm
// cache stays warm.
func TestArtifactIdentityIsStableForAnUnchangedBuild(t *testing.T) {
	root := t.TempDir()
	writeGeneratorSource(t, root, "package fixturegen // steady")
	sourceDigestOnce = onceReset()
	cache := testCache(t, root)
	first := cache.Identity("cheap")
	second := cache.Identity("cheap")
	if first == "" || first != second {
		t.Fatalf("identity must be stable, got %q then %q", first, second)
	}
	if !strings.Contains(cache.dirFor("cheap"), "cheap@"+first) {
		t.Fatalf("cache dir must carry the identity, got %s", cache.dirFor("cheap"))
	}
}

// (2) The stale cache on a machine today lives at the old name-keyed path.
// Nothing consults it, and nothing should leave gigabytes of it behind.
func TestLegacyNameKeyedCacheDirsArePruned(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "artifacts", "cheap")
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "archive.7z"), []byte("sunday's bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	testCache(t, root)

	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("the name-keyed cache dir must be pruned, stat gave %v", err)
	}
}

// (b) A salted scenario is accepted on presence, so only its stamp can say the
// artifact underneath it changed. No stamp — the state on a machine that
// generated salted fixtures before stamps existed — must count as stale.
func TestSaltedScenarioWithoutAStampIsStale(t *testing.T) {
	root := t.TempDir()
	cache := testCache(t, root)
	if !saltedScenarioIsStale(root, "direct-unpack-aes256", cache) {
		t.Fatal("a salted scenario with no stamp must be treated as stale")
	}
}

func TestSaltedScenarioIsStaleWhenItsArtifactIdentityMoved(t *testing.T) {
	root := t.TempDir()
	writeGeneratorSource(t, root, "package fixturegen // revision one")
	sourceDigestOnce = onceReset()
	cache := testCache(t, root)

	if err := writeScenarioStamp(root, "salty", map[string]string{"cheap": cache.Identity("cheap")}); err != nil {
		t.Fatal(err)
	}
	if saltedScenarioIsStale(root, "salty", cache) {
		t.Fatal("a stamp matching the current identity is fresh")
	}

	// The recipe changes underneath it.
	writeGeneratorSource(t, root, "package fixturegen // revision two")
	sourceDigestOnce = onceReset()
	moved := testCache(t, root)
	if !saltedScenarioIsStale(root, "salty", moved) {
		t.Fatal("a stamp naming an identity that has moved must be stale")
	}
}

func TestSaltedSlugsNeedingRebuildDeduplicatesByScenario(t *testing.T) {
	root := t.TempDir()
	cache := testCache(t, root)
	stale := saltedSlugsNeedingRebuild(root, []string{
		"testdata/direct-unpack-aes256-split/archive.7z.001",
		"testdata/direct-unpack-aes256-split/archive.7z.002",
		"testdata/direct-unpack-aes256/archive.7z",
	}, cache)
	if len(stale) != 2 {
		t.Fatalf("expected one entry per scenario, got %v", stale)
	}
}
