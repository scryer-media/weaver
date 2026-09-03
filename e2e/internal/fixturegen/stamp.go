package fixturegen

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// A salted scenario's bytes cannot be pinned, so the ledger accepts it on
// presence alone. That is correct for the salt and wrong for everything else:
// a salted fixture built from a stale artifact is just as poisoned as a hashed
// one, and presence-only acceptance means nothing would ever rebuild it.
//
// The stamp closes that. It records which artifacts a scenario was built from
// and what their identities were at the time, so "present" can be qualified
// with "and built from the artifacts we would build it from now". It lives
// under `target/` and is never committed: it describes this machine's cache,
// not the corpus.
type scenarioStamp struct {
	Artifacts map[string]string `json:"artifacts"`
}

func stampPath(root, slug string) string {
	return filepath.Join(root, "target", "fixturegen", "stamps", slug+".json")
}

// writeScenarioStamp records the artifact identities a scenario was built from.
func writeScenarioStamp(root, slug string, artifacts map[string]string) error {
	if len(artifacts) == 0 {
		// A scenario that consumed no artifact has nothing that can go stale
		// underneath it; an empty stamp would only be noise.
		return nil
	}
	path := stampPath(root, slug)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(scenarioStamp{Artifacts: artifacts}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(contents, '\n'), 0o644)
}

// saltedScenarioIsStale reports whether a salted scenario's outputs were built
// from artifacts that no longer exist in that form.
//
// A missing stamp counts as stale. That is what repairs a machine holding
// salted outputs from before stamps existed — the case that would otherwise sit
// there forever, accepted on presence, quietly wrong.
func saltedScenarioIsStale(root, slug string, cache *ArtifactCache) bool {
	contents, err := os.ReadFile(stampPath(root, slug))
	if err != nil {
		return true
	}
	var stamp scenarioStamp
	if err := json.Unmarshal(contents, &stamp); err != nil {
		return true
	}
	if len(stamp.Artifacts) == 0 {
		return true
	}
	for name, recorded := range stamp.Artifacts {
		if cache.Identity(name) != recorded {
			return true
		}
	}
	return false
}

// saltedSlugsNeedingRebuild lists the salted scenarios among `wanted` whose
// stamps no longer match the artifacts that would build them now.
func saltedSlugsNeedingRebuild(root string, salted []string, cache *ArtifactCache) []string {
	seen := map[string]struct{}{}
	var stale []string
	for _, path := range salted {
		slug, ok := slugOfPath(path)
		if !ok {
			continue
		}
		if _, done := seen[slug]; done {
			continue
		}
		seen[slug] = struct{}{}
		if saltedScenarioIsStale(root, slug, cache) {
			stale = append(stale, slug)
		}
	}
	sort.Strings(stale)
	return stale
}

// identityCache builds a cache purely to answer identity questions. It also
// prunes the name-keyed directories the old layout left behind, which is the
// only place that cleanup can happen without a dedicated command.
func identityCache(root string) *ArtifactCache {
	lock, err := LoadLock(root)
	if err != nil {
		lock = Lock{}
	}
	return NewArtifactCache(filepath.Join(root, "target", "fixturegen", "artifacts"), Artifacts()).
		WithBuildIdentity(lock, root)
}

func containsPath(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}
