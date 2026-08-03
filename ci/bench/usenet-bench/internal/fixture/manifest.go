package fixture

import (
	"encoding/json"
	"fmt"
	"os"
)

// FileDigest describes a fixture input or generated RAR volume. Paths are
// always relative to the fixture directory.
type FileDigest struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// GeneratedManifest is written beside every generated fixture. It is the
// oracle for a client run: clients must produce all ExpectedFiles exactly,
// independent of how they download or unpack the archive.
type GeneratedManifest struct {
	SchemaVersion int           `json:"schema_version"`
	Case          ArchiveCase   `json:"case"`
	Toolchain     ToolchainID   `json:"toolchain"`
	PayloadRecipe PayloadRecipe `json:"payload_recipe"`
	ExpectedFiles []FileDigest  `json:"expected_files"`
	ArchiveFiles  []FileDigest  `json:"archive_files"`
}

// PayloadRecipe records every size and count used by the generator. The
// matrix describes the kind of workload; this recipe records the exact scale
// used for this specific generated fixture.
type PayloadRecipe struct {
	Layout              PayloadLayout `json:"layout"`
	UniformBytesPerFile int64         `json:"uniform_bytes_per_file,omitempty"`
	LargeFileBytes      int64         `json:"large_file_bytes,omitempty"`
	SmallFileCount      int           `json:"small_file_count,omitempty"`
	SmallFileBytes      int64         `json:"small_file_bytes,omitempty"`
}

// ToolchainID is stored separately so reports identify both the requested
// archive compatibility family and the exact RARLAB generator release.
type ToolchainID struct {
	ID       string `json:"id"`
	Image    string `json:"image"`
	URL      string `json:"url"`
	SHA256   string `json:"sha256"`
	Platform string `json:"platform"`
	Binary   string `json:"binary,omitempty"`
}

func LoadGeneratedManifest(path string) (GeneratedManifest, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return GeneratedManifest{}, fmt.Errorf("read fixture manifest %s: %w", path, err)
	}
	var manifest GeneratedManifest
	if err := json.Unmarshal(contents, &manifest); err != nil {
		return GeneratedManifest{}, fmt.Errorf("decode fixture manifest %s: %w", path, err)
	}
	if manifest.SchemaVersion != 1 && manifest.SchemaVersion != 2 && manifest.SchemaVersion != 3 {
		return GeneratedManifest{}, fmt.Errorf("unsupported generated fixture schema version %d", manifest.SchemaVersion)
	}
	if manifest.Case.ID == "" || len(manifest.ExpectedFiles) == 0 || len(manifest.ArchiveFiles) == 0 {
		return GeneratedManifest{}, fmt.Errorf("fixture manifest %s is incomplete", path)
	}
	return manifest, nil
}
