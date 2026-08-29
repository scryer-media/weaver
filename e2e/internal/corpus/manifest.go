package corpus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

// Manifest is the published description of one corpus revision. It is a pure
// function of the ledger, the profile table and the toolchain lock — no build
// metadata — so `verify` can recompute it from a checkout and compare its
// digest with the lock. Field order is fixed and maps are emitted with sorted
// keys, which makes the encoding canonical.
type Manifest struct {
	SchemaVersion   int                  `json:"schema_version"`
	DigestAlgorithm string               `json:"digest_algorithm"`
	Files           []ManifestFile       `json:"files"`
	Generators      map[string]Generator `json:"generators"`
	Profiles        map[string][]string  `json:"profiles"`
	Toolchains      ManifestToolchains   `json:"toolchains"`
}

// ManifestFile is one fixture as published.
type ManifestFile struct {
	Path       string   `json:"path"`
	Size       int64    `json:"size"`
	BLAKE3     string   `json:"blake3"`
	Format     string   `json:"format,omitempty"`
	SourceKind string   `json:"source_kind"`
	Generator  string   `json:"generator,omitempty"`
	Toolchains []string `json:"toolchains,omitempty"`
}

// ManifestToolchains records which lock the generators were pinned to.
type ManifestToolchains struct {
	Path   string   `json:"path"`
	BLAKE3 string   `json:"blake3"`
	IDs    []string `json:"ids"`
}

// BuildManifest freezes the ledger, resolved profiles and toolchain lock.
func BuildManifest(ledger *Ledger, profiles *Profiles, lock ToolchainLock) (*Manifest, error) {
	paths := ledger.Paths()
	resolved, err := profiles.ResolveAll(paths)
	if err != nil {
		return nil, err
	}
	files := make([]ManifestFile, 0, len(ledger.Files))
	for _, entry := range ledger.Files {
		file := ManifestFile{
			Path:       entry.Path,
			Size:       entry.Size,
			BLAKE3:     entry.BLAKE3,
			Format:     entry.Format,
			SourceKind: entry.Source.Kind,
			Generator:  entry.Source.Generator,
		}
		if len(entry.Source.Toolchains) > 0 {
			file.Toolchains = append([]string(nil), entry.Source.Toolchains...)
			sort.Strings(file.Toolchains)
		}
		files = append(files, file)
	}
	sort.Slice(files, func(left, right int) bool { return files[left].Path < files[right].Path })
	generators := make(map[string]Generator, len(ledger.Generators))
	for name, generator := range ledger.Generators {
		copied := generator
		copied.Toolchains = append([]string(nil), generator.Toolchains...)
		sort.Strings(copied.Toolchains)
		generators[name] = copied
	}
	return &Manifest{
		SchemaVersion:   SchemaVersion,
		DigestAlgorithm: DigestAlgorithm,
		Files:           files,
		Generators:      generators,
		Profiles:        resolved,
		Toolchains:      ManifestToolchains{Path: ledger.Toolchains, BLAKE3: lock.BLAKE3, IDs: append([]string(nil), lock.IDs...)},
	}, nil
}

// Encode returns the canonical bytes (compact JSON, sorted map keys, trailing
// newline). Its BLAKE3 is the manifest's address.
func (manifest *Manifest) Encode() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(manifest); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// DecodeManifest parses published manifest bytes and re-validates the shape.
func DecodeManifest(contents []byte) (*Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(contents, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.SchemaVersion != SchemaVersion || manifest.DigestAlgorithm != DigestAlgorithm {
		return nil, fmt.Errorf("manifest schema %d/%s is not %d/%s", manifest.SchemaVersion, manifest.DigestAlgorithm, SchemaVersion, DigestAlgorithm)
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		if !ValidRelativePath(file.Path) || !IsDigest(file.BLAKE3) || file.Size < 0 {
			return nil, fmt.Errorf("manifest entry %q is malformed", file.Path)
		}
		if _, duplicate := seen[file.Path]; duplicate {
			return nil, fmt.Errorf("manifest lists %s twice", file.Path)
		}
		seen[file.Path] = struct{}{}
	}
	for name, members := range manifest.Profiles {
		if len(members) == 0 {
			return nil, fmt.Errorf("manifest profile %s is empty", name)
		}
		for _, member := range members {
			if _, ok := seen[member]; !ok {
				return nil, fmt.Errorf("manifest profile %s names %s, which is not a manifest file", name, member)
			}
		}
	}
	return &manifest, nil
}

// File finds one manifest entry by path.
func (manifest *Manifest) File(path string) (ManifestFile, bool) {
	for _, file := range manifest.Files {
		if file.Path == path {
			return file, true
		}
	}
	return ManifestFile{}, false
}

// Provenance is the build metadata published beside a manifest. It is signed
// the same way but deliberately kept out of the manifest so the manifest stays
// recomputable.
type Provenance struct {
	SchemaVersion    int    `json:"schema_version"`
	ManifestBLAKE3   string `json:"manifest_blake3"`
	ToolchainsBLAKE3 string `json:"toolchains_blake3"`
	SourceCommit     string `json:"source_commit"`
	WorkflowRun      string `json:"workflow_run"`
	Actor            string `json:"actor"`
	PublishedAt      string `json:"published_at"`
}

// NewProvenance fills the record from the GitHub Actions environment when
// present; local builds record what they can.
func NewProvenance(manifestDigest, toolchainsDigest string, now time.Time) Provenance {
	run := ""
	if server, repository, id := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID"); server != "" && repository != "" && id != "" {
		run = server + "/" + repository + "/actions/runs/" + id
	}
	return Provenance{
		SchemaVersion:    SchemaVersion,
		ManifestBLAKE3:   manifestDigest,
		ToolchainsBLAKE3: toolchainsDigest,
		SourceCommit:     os.Getenv("GITHUB_SHA"),
		WorkflowRun:      run,
		Actor:            os.Getenv("GITHUB_ACTOR"),
		PublishedAt:      now.UTC().Format(time.RFC3339),
	}
}
