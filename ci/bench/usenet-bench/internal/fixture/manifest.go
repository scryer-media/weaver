package fixture

import (
	"encoding/json"
	"fmt"
	"os"
)

// GeneratedManifestSchemaVersion is the schema every newly generated fixture
// manifest is written at. Older manifests stay readable; the loader fills in
// the fields their schema predates.
const GeneratedManifestSchemaVersion = 6

// FileDigest describes a fixture input, archive volume, or repair artifact.
// Paths are always relative to the fixture directory.
type FileDigest struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	BLAKE3 string `json:"blake3"`
}

// GeneratedManifest is written beside every generated fixture. It is the
// oracle for a client run: clients must produce all ExpectedFiles exactly,
// independent of how they download or unpack the archive.
type GeneratedManifest struct {
	SchemaVersion int         `json:"schema_version"`
	Case          ArchiveCase `json:"case"`
	Toolchain     ToolchainID `json:"toolchain"`
	// ArchiveWriterToolchain identifies the pinned writer that produced the
	// container. For the RAR lanes it repeats Toolchain; for the 7z lane it is
	// the official 7-Zip build, while Toolchain remains the image that
	// rendered the payload.
	ArchiveWriterToolchain ToolchainID   `json:"archive_writer_toolchain"`
	PayloadRecipe          PayloadRecipe `json:"payload_recipe"`
	ExpectedFiles          []FileDigest  `json:"expected_files"`
	// SourceArchiveFiles records the intact archive volumes before deliberate
	// corruption. It makes the repair target independently auditable.
	SourceArchiveFiles []FileDigest `json:"source_archive_files,omitempty"`
	// ArchiveFiles is the exact set of files posted through NNTP: damaged
	// archive volumes plus all PAR2 or RAR recovery material.
	ArchiveFiles []FileDigest `json:"archive_files"`
	// WithheldFiles are listed in the NZB but never posted. Their bytes stay
	// on disk so the repair target remains auditable; the seeder emits NZB
	// entries whose article identifiers were never sent to the server, which
	// is what a real short post looks like to a client.
	WithheldFiles []FileDigest `json:"withheld_files,omitempty"`
	// NZBFileOrder is the exact posting order for this fixture, and
	// NZBOrderSeed the value the permutation was drawn from. Both are
	// functions of the fixture id, so a reseed reproduces them.
	NZBFileOrder []string      `json:"nzb_file_order"`
	NZBOrderSeed uint64        `json:"nzb_order_seed"`
	Repair       RepairDetails `json:"repair"`
}

// RepairDetails describes the bounded fault injected into a fixture. The
// expected extracted files remain the output oracle; this metadata makes the
// input fault and repair strength reproducible without retaining duplicate
// intact archive bytes.
type RepairDetails struct {
	Profile               RepairProfile      `json:"profile"`
	PAR2RedundancyPercent int                `json:"par2_redundancy_percent,omitempty"`
	RARRecoveryVolumes    int                `json:"rar_recovery_volumes,omitempty"`
	Corruptions           []CorruptionDetail `json:"corruptions,omitempty"`
}

// CorruptionDetail records a deterministic mutation or intentional omission.
// Offset and Length are populated for byte-flip faults only. Kind is one of
// byte-flip, missing-volume (absent from the NZB entirely) or withheld-volume
// (listed in the NZB, never posted).
type CorruptionDetail struct {
	Kind   string `json:"kind"`
	Path   string `json:"path"`
	Offset int64  `json:"offset,omitempty"`
	Length int    `json:"length,omitempty"`
}

// PayloadRecipe records every size and count used by the generator. The
// matrix describes the kind of workload; this recipe records the exact scale
// used for this specific generated fixture.
type PayloadRecipe struct {
	Layout              PayloadLayout `json:"layout"`
	UniformBytesPerFile int64         `json:"uniform_bytes_per_file,omitempty"`
	// SampleNoiseBits is the width of the deterministic per-sample noise added
	// to a compressible video payload after rendering; zero for every other
	// payload. It is what sets a compressible archive's size, so it is part
	// of the recipe.
	SampleNoiseBits uint  `json:"sample_noise_bits,omitempty"`
	LargeFileBytes  int64 `json:"large_file_bytes,omitempty"`
	MediumFileCount int   `json:"medium_file_count,omitempty"`
	MediumFileBytes int64 `json:"medium_file_bytes,omitempty"`
	SmallFileCount  int   `json:"small_file_count,omitempty"`
	SmallFileBytes  int64 `json:"small_file_bytes,omitempty"`
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
	// Version is the upstream release the toolchain installs, where the
	// toolchain declares one separately from its id.
	Version string `json:"version,omitempty"`
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
	if manifest.SchemaVersion < 1 || manifest.SchemaVersion > GeneratedManifestSchemaVersion {
		return GeneratedManifest{}, fmt.Errorf("unsupported generated fixture schema version %d", manifest.SchemaVersion)
	}
	if manifest.Case.ID == "" || len(manifest.ExpectedFiles) == 0 || len(manifest.ArchiveFiles) == 0 {
		return GeneratedManifest{}, fmt.Errorf("fixture manifest %s is incomplete", path)
	}
	if manifest.SchemaVersion < 4 {
		manifest.Case.RepairProfile = CleanRepairProfile
		manifest.Repair = RepairDetails{Profile: CleanRepairProfile}
		manifest.SourceArchiveFiles = append([]FileDigest(nil), manifest.ArchiveFiles...)
	} else if !manifest.Case.RepairProfile.Valid() || manifest.Case.RepairProfile != manifest.Repair.Profile || len(manifest.SourceArchiveFiles) == 0 {
		return GeneratedManifest{}, fmt.Errorf("fixture manifest %s has invalid repair metadata", path)
	}
	if manifest.SchemaVersion < 6 {
		// Schema 5 and earlier predate the posting-order axis. Every one of
		// those fixtures was posted in sorted archive order.
		manifest.Case.NZBOrder = SequentialNZBOrder
		manifest.NZBFileOrder = postedPaths(manifest.ArchiveFiles)
		manifest.NZBOrderSeed = 0
		return manifest, nil
	}
	if !manifest.Case.NZBOrder.Valid() {
		return GeneratedManifest{}, fmt.Errorf("fixture manifest %s has unsupported nzb_order %q", path, manifest.Case.NZBOrder)
	}
	if err := validateNZBFileOrder(manifest); err != nil {
		return GeneratedManifest{}, fmt.Errorf("fixture manifest %s: %w", path, err)
	}
	return manifest, nil
}

// MinimumPostedBytes is the smallest archive a benchmark fixture may post.
// Below it a download finishes inside the clients' start-up and settle time
// and the comparison measures process launch, not the pipeline. The generator
// refuses to write a manifest under the floor and the controller refuses to
// run one, so a corpus that predates the floor cannot produce a result. The
// floor sits under the 150 MiB movie the defaults produce with room for the
// withheld-volume sets, which post one 32 MiB volume less than they list.
const MinimumPostedBytes int64 = 100 << 20

// PostedBytes is the number of archive bytes the seeder actually posts:
// ArchiveFiles only, since withheld files are listed in the NZB but never
// reach the server.
func (m GeneratedManifest) PostedBytes() int64 {
	var total int64
	for _, file := range m.ArchiveFiles {
		total += file.Size
	}
	return total
}

// ValidatePostedSize fails a fixture that posts fewer than MinimumPostedBytes.
func (m GeneratedManifest) ValidatePostedSize() error {
	if posted := m.PostedBytes(); posted < MinimumPostedBytes {
		return fmt.Errorf("fixture posts %d bytes (%.1f MiB); every benchmark fixture must post at least %d MiB — regenerate the corpus with the current fixturegen defaults", posted, float64(posted)/(1<<20), MinimumPostedBytes>>20)
	}
	return nil
}

// PostedFiles is every file the seeder gives the poster: the posted archive
// files plus the withheld ones, which are listed in the NZB without ever
// reaching the server.
func (m GeneratedManifest) PostedFiles() []FileDigest {
	files := append([]FileDigest(nil), m.ArchiveFiles...)
	return append(files, m.WithheldFiles...)
}

func (m GeneratedManifest) IsWithheld(path string) bool {
	for _, file := range m.WithheldFiles {
		if file.Path == path {
			return true
		}
	}
	return false
}

func postedPaths(files []FileDigest) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return paths
}

func validateNZBFileOrder(manifest GeneratedManifest) error {
	posted := manifest.PostedFiles()
	if len(manifest.NZBFileOrder) != len(posted) {
		return fmt.Errorf("nzb_file_order lists %d files, expected %d", len(manifest.NZBFileOrder), len(posted))
	}
	remaining := make(map[string]int, len(posted))
	for _, file := range posted {
		remaining[file.Path]++
	}
	for _, name := range manifest.NZBFileOrder {
		if remaining[name] == 0 {
			return fmt.Errorf("nzb_file_order names unknown posted file %q", name)
		}
		remaining[name]--
	}
	return nil
}
