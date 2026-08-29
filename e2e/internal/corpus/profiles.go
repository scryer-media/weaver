package corpus

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Profiles is test-corpus/profiles.json: named hydration subsets as root-relative
// path globs. `*` matches within one path segment, `**` matches any number of
// segments (including none).
type Profiles struct {
	SchemaVersion int                `json:"schema_version"`
	Profiles      map[string]Profile `json:"profiles"`
}

// Profile is one named subset.
type Profile struct {
	Description string   `json:"description,omitempty"`
	Include     []string `json:"include"`
	Exclude     []string `json:"exclude,omitempty"`
}

// LoadProfiles reads and validates the profile table.
func LoadProfiles(root string) (*Profiles, error) {
	contents, err := os.ReadFile(HostPath(root, ProfilesFile))
	if err != nil {
		return nil, fmt.Errorf("read profiles: %w", err)
	}
	var profiles Profiles
	if err := json.Unmarshal(contents, &profiles); err != nil {
		return nil, fmt.Errorf("decode %s: %w", ProfilesFile, err)
	}
	if profiles.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%s: schema_version %d is not %d", ProfilesFile, profiles.SchemaVersion, SchemaVersion)
	}
	if len(profiles.Profiles) == 0 {
		return nil, fmt.Errorf("%s declares no profiles", ProfilesFile)
	}
	for name, profile := range profiles.Profiles {
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, " /\\") {
			return nil, fmt.Errorf("%s: profile name %q is invalid", ProfilesFile, name)
		}
		if len(profile.Include) == 0 {
			return nil, fmt.Errorf("%s: profile %s has no include globs", ProfilesFile, name)
		}
		for _, glob := range append(append([]string(nil), profile.Include...), profile.Exclude...) {
			if !ValidRelativePath(glob) {
				return nil, fmt.Errorf("%s: profile %s glob %q is not root-relative", ProfilesFile, name, glob)
			}
		}
	}
	return &profiles, nil
}

// Names returns the profile names, sorted.
func (profiles *Profiles) Names() []string {
	names := make([]string, 0, len(profiles.Profiles))
	for name := range profiles.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Resolve returns the sorted ledger paths matching any include glob and no
// exclude glob. A profile that resolves to nothing is an error: the manifest
// must never freeze an empty bundle.
func (profiles *Profiles) Resolve(name string, paths []string) ([]string, error) {
	profile, ok := profiles.Profiles[name]
	if !ok {
		return nil, fmt.Errorf("profile %q is not declared in %s (known: %s)", name, ProfilesFile, strings.Join(profiles.Names(), ", "))
	}
	var resolved []string
	for _, path := range paths {
		if !matchesAny(profile.Include, path) || matchesAny(profile.Exclude, path) {
			continue
		}
		resolved = append(resolved, path)
	}
	if len(resolved) == 0 {
		return nil, fmt.Errorf("profile %s resolves to no ledger paths", name)
	}
	sort.Strings(resolved)
	return resolved, nil
}

// ResolveAll resolves every profile; used when freezing the manifest.
func (profiles *Profiles) ResolveAll(paths []string) (map[string][]string, error) {
	resolved := make(map[string][]string, len(profiles.Profiles))
	for _, name := range profiles.Names() {
		members, err := profiles.Resolve(name, paths)
		if err != nil {
			return nil, err
		}
		resolved[name] = members
	}
	return resolved, nil
}

func matchesAny(globs []string, path string) bool {
	for _, glob := range globs {
		if MatchGlob(glob, path) {
			return true
		}
	}
	return false
}

// MatchGlob matches a `/`-separated path against a glob whose segments may
// contain `*` (within a segment) or be exactly `**` (zero or more segments).
func MatchGlob(glob, path string) bool {
	return matchSegments(strings.Split(glob, "/"), strings.Split(path, "/"))
}

func matchSegments(glob, path []string) bool {
	if len(glob) == 0 {
		return len(path) == 0
	}
	if glob[0] == "**" {
		for skip := 0; skip <= len(path); skip++ {
			if matchSegments(glob[1:], path[skip:]) {
				return true
			}
		}
		return false
	}
	if len(path) == 0 || !matchSegment(glob[0], path[0]) {
		return false
	}
	return matchSegments(glob[1:], path[1:])
}

func matchSegment(pattern, segment string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == segment
	}
	parts := strings.Split(pattern, "*")
	if !strings.HasPrefix(segment, parts[0]) {
		return false
	}
	segment = segment[len(parts[0]):]
	for index := 1; index < len(parts)-1; index++ {
		position := strings.Index(segment, parts[index])
		if position < 0 {
			return false
		}
		segment = segment[position+len(parts[index]):]
	}
	return strings.HasSuffix(segment, parts[len(parts)-1])
}
